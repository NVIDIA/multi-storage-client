package driver

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

const (
	unmountTimeout = 10 * time.Second
	killGrace      = 5 * time.Second
)

type mountEntry struct {
	cmd       *exec.Cmd
	configDir string
}

type nodeServer struct {
	csi.UnimplementedNodeServer
	nodeID     string
	msfsBinary string

	mu     sync.Mutex
	mounts map[string]*mountEntry
}

func newNodeServer(nodeID, msfsBinary string) *nodeServer {
	return &nodeServer{
		nodeID:     nodeID,
		msfsBinary: msfsBinary,
		mounts:     make(map[string]*mountEntry),
	}
}

func (ns *nodeServer) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	volCtx := req.GetVolumeContext()
	secrets := req.GetSecrets()

	bucketName := volCtx["bucketName"]
	if bucketName == "" {
		return nil, status.Error(codes.InvalidArgument, "volumeAttributes.bucketName is required")
	}

	ns.mu.Lock()
	if _, ok := ns.mounts[targetPath]; ok {
		ns.mu.Unlock()
		klog.Infof("volume already mounted at %s", targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}
	ns.mu.Unlock()

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create target path %s: %v", targetPath, err)
	}

	configDir, configPath, err := ns.writeConfig(targetPath, volCtx, secrets)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write msfs config: %v", err)
	}

	cmd := exec.Command(ns.msfsBinary, configPath)
	cmd.Env = ns.buildEnv(secrets)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	klog.Infof("starting msfs: %s %s (mountpoint=%s, bucket=%s)", ns.msfsBinary, configPath, targetPath, bucketName)
	if err := cmd.Start(); err != nil {
		os.RemoveAll(configDir)
		return nil, status.Errorf(codes.Internal, "failed to start msfs: %v", err)
	}

	if err := ns.waitForMount(targetPath, 30*time.Second); err != nil {
		_ = cmd.Process.Kill()
		os.RemoveAll(configDir)
		return nil, status.Errorf(codes.Internal, "msfs did not mount within timeout: %v", err)
	}

	ns.mu.Lock()
	ns.mounts[targetPath] = &mountEntry{cmd: cmd, configDir: configDir}
	ns.mu.Unlock()

	klog.Infof("msfs mounted at %s (pid=%d)", targetPath, cmd.Process.Pid)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	ns.mu.Lock()
	entry, ok := ns.mounts[targetPath]
	if ok {
		delete(ns.mounts, targetPath)
	}
	ns.mu.Unlock()

	if ok && entry.cmd != nil && entry.cmd.Process != nil {
		klog.Infof("stopping msfs (pid=%d) for %s", entry.cmd.Process.Pid, targetPath)
		_ = entry.cmd.Process.Signal(syscall.SIGTERM)

		done := make(chan error, 1)
		go func() { done <- entry.cmd.Wait() }()

		select {
		case <-done:
		case <-time.After(killGrace):
			klog.Warningf("msfs did not exit after SIGTERM, sending SIGKILL (pid=%d)", entry.cmd.Process.Pid)
			_ = entry.cmd.Process.Kill()
			<-done
		}
	}

	if err := fuseUnmount(targetPath); err != nil {
		klog.Warningf("fusermount failed for %s: %v (trying umount)", targetPath, err)
		if err2 := syscall.Unmount(targetPath, 0); err2 != nil {
			klog.Warningf("umount also failed for %s: %v", targetPath, err2)
		}
	}

	if err := os.RemoveAll(targetPath); err != nil {
		klog.Warningf("failed to remove target path %s: %v", targetPath, err)
	}

	if ok && entry.configDir != "" {
		os.RemoveAll(entry.configDir)
	}

	klog.Infof("volume unpublished from %s", targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: ns.nodeID}, nil
}

func (ns *nodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{Capabilities: []*csi.NodeServiceCapability{}}, nil
}

func valOrDefault(m map[string]string, key, dflt string) string {
	if v, ok := m[key]; ok && v != "" {
		return v
	}
	return dflt
}

func (ns *nodeServer) writeConfig(targetPath string, volCtx, secrets map[string]string) (string, string, error) {
	configDir, err := os.MkdirTemp("", "msfs-csi-*")
	if err != nil {
		return "", "", fmt.Errorf("failed to create temp config dir: %w", err)
	}

	region := valOrDefault(volCtx, "region", "us-east-1")
	endpoint := valOrDefault(volCtx, "endpoint", fmt.Sprintf("https://s3.%s.amazonaws.com", region))

	readonlyStr := "true"
	if volCtx["readonly"] == "false" {
		readonlyStr = "false"
	}

	var backendExtra strings.Builder
	optionalBackendStr := func(key, yamlField string) {
		if v := volCtx[key]; v != "" {
			fmt.Fprintf(&backendExtra, "    %s: %s\n", yamlField, v)
		}
	}
	optionalBackendQuoted := func(key, yamlField string) {
		if v := volCtx[key]; v != "" {
			fmt.Fprintf(&backendExtra, "    %s: %q\n", yamlField, v)
		}
	}

	optionalBackendQuoted("manifestPath", "manifest_path")
	optionalBackendStr("manifestGenWorkers", "manifest_gen_workers")
	optionalBackendStr("flatDirConfirmationPages", "flat_dir_confirmation_pages")
	optionalBackendStr("traceLevel", "trace_level")
	optionalBackendStr("directoryPageSize", "directory_page_size")
	optionalBackendStr("uid", "uid")
	optionalBackendStr("gid", "gid")
	optionalBackendQuoted("dirPerm", "dir_perm")
	optionalBackendQuoted("filePerm", "file_perm")
	optionalBackendStr("flushOnClose", "flush_on_close")
	optionalBackendStr("multipartCacheLineThreshold", "multipart_cache_line_threshold")
	optionalBackendStr("uploadPartCacheLines", "upload_part_cache_lines")
	optionalBackendStr("uploadPartConcurrency", "upload_part_concurrency")

	var globalExtra strings.Builder
	optionalGlobalStr := func(key, yamlField string) {
		if v := volCtx[key]; v != "" {
			fmt.Fprintf(&globalExtra, "%s: %s\n", yamlField, v)
		}
	}

	optionalGlobalStr("cacheLineSize", "cache_line_size")
	optionalGlobalStr("cacheLines", "cache_lines")
	optionalGlobalStr("cacheLinesToPrefetch", "cache_lines_to_prefetch")
	optionalGlobalStr("dirtyCacheLinesFlushTrigger", "dirty_cache_lines_flush_trigger")
	optionalGlobalStr("dirtyCacheLinesMax", "dirty_cache_lines_max")
	if v := volCtx["allowOther"]; v != "" {
		fmt.Fprintf(&globalExtra, "allow_other: %s\n", v)
	}

	config := fmt.Sprintf(`msfs_version: 1
endpoint: "http://0.0.0.0:0"
mountpoint: %s
%sbackends:
  - dir_name: s3
    bucket_container_name: %s
    prefix: %q
    readonly: %s
%s    backend_type: S3
    S3:
      region: %q
      endpoint: %q
      access_key_id: "${AWS_ACCESS_KEY_ID}"
      secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
      virtual_hosted_style_request: false
`, targetPath, globalExtra.String(), volCtx["bucketName"], volCtx["prefix"], readonlyStr, backendExtra.String(), region, endpoint)

	configPath := filepath.Join(configDir, "msfs.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		os.RemoveAll(configDir)
		return "", "", fmt.Errorf("failed to write config: %w", err)
	}
	return configDir, configPath, nil
}

func (ns *nodeServer) buildEnv(secrets map[string]string) []string {
	env := os.Environ()
	if v, ok := secrets["access_key_id"]; ok {
		env = append(env, "AWS_ACCESS_KEY_ID="+v)
	}
	if v, ok := secrets["secret_access_key"]; ok {
		env = append(env, "AWS_SECRET_ACCESS_KEY="+v)
	}
	if v, ok := secrets["session_token"]; ok {
		env = append(env, "AWS_SESSION_TOKEN="+v)
	}
	return env
}

func (ns *nodeServer) waitForMount(targetPath string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isMountPoint(targetPath) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for mount at %s", targetPath)
}

func isMountPoint(path string) bool {
	var stat, parentStat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return false
	}
	if err := syscall.Stat(filepath.Dir(path), &parentStat); err != nil {
		return false
	}
	return stat.Dev != parentStat.Dev
}

func fuseUnmount(path string) error {
	cmd := exec.Command("fusermount", "-u", path)
	return cmd.Run()
}
