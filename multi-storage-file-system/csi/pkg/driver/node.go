package driver

import (
	"context"
	"encoding/json"
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

// credentialMode controls how the CSI node plugin supplies AWS credentials to
// the msfs child process for a given volume mount.
//
//   - credentialModeStatic: read access_key_id / secret_access_key (and
//     optional session_token) from the K8s Secret referenced by
//     nodePublishSecretRef and inject them as AWS_* env vars.
//   - credentialModeIRSA: do not inject any AWS_* env vars and do not emit
//     credential placeholders in msfs.yaml. The AWS SDK picks up the
//     projected ServiceAccount token via AWS_ROLE_ARN /
//     AWS_WEB_IDENTITY_TOKEN_FILE that EKS sets on the pod.
//   - credentialModeAuto: pick static when a secret with at least the two
//     required keys is present, otherwise IRSA. This is the default so
//     existing manifests keep working unchanged.
type credentialMode string

const (
	credentialModeAuto   credentialMode = "auto"
	credentialModeStatic credentialMode = "static"
	credentialModeIRSA   credentialMode = "irsa"
)

// Canonical backend types accepted in volumeAttributes.backendType.
const (
	backendTypeS3      = "S3"
	backendTypeAIStore = "AIStore"
)

const (
	// serviceAccountTokensVolCtxKey is the volume_context key the kubelet
	// populates with the workload pod's projected ServiceAccount token(s) when
	// the CSIDriver object declares tokenRequests (per-workload IRSA). The
	// value is a JSON object keyed by token audience.
	serviceAccountTokensVolCtxKey = "csi.storage.k8s.io/serviceAccount.tokens"

	// stsAudience is the token audience used for AWS STS
	// AssumeRoleWithWebIdentity. It must match the audience the chart requests
	// in CSIDriver.tokenRequests (auth.perWorkloadIrsa.audience, default
	// sts.amazonaws.com).
	stsAudience = "sts.amazonaws.com"

	// webIdentityTokenFileName is the per-mount file the workload token is
	// written to inside the mount's temp config dir. It lives alongside
	// msfs.yaml so the existing NodeUnpublishVolume cleanup removes it.
	webIdentityTokenFileName = "aws-web-identity-token"
)

// serviceAccountToken mirrors one entry of the kubelet-provided
// serviceAccount.tokens JSON map:
// {"<audience>": {"token": "...", "expirationTimestamp": "..."}}.
type serviceAccountToken struct {
	Token               string `json:"token"`
	ExpirationTimestamp string `json:"expirationTimestamp"`
}

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

	// Reservation pattern: check-and-insert under one lock so two concurrent
	// calls for the same targetPath cannot both start an msfs process. The
	// reserved entry has a nil cmd; we replace it with the real mount info on
	// success, or delete it on any failure path.
	ns.mu.Lock()
	if existing, ok := ns.mounts[targetPath]; ok {
		ns.mu.Unlock()
		// Already mounted (or a publish is in flight). When per-workload IRSA
		// is enabled the CSIDriver sets requiresRepublish, so the kubelet
		// re-invokes NodePublishVolume periodically with a freshly minted
		// workload token in volume_context. In that case rewrite the per-mount
		// token file in place; never re-spawn msfs (the AWS SDK re-reads the
		// file on credential refresh). Static and driver-SA IRSA mounts carry
		// no per-workload token and fall through to the original idempotent
		// no-op below, unchanged.
		if existing.cmd != nil && existing.configDir != "" && volCtx[serviceAccountTokensVolCtxKey] != "" {
			mode, err := resolveCredentialMode(volCtx, secrets)
			if err != nil {
				return nil, err
			}
			tokenFile, _, err := ns.resolveWorkloadIdentity(existing.configDir, volCtx, mode)
			if err != nil {
				return nil, err
			}
			if tokenFile != "" {
				klog.Infof("refreshed workload identity token for %s (republish)", targetPath)
				return &csi.NodePublishVolumeResponse{}, nil
			}
		}
		klog.Infof("volume already mounted at %s", targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}
	ns.mounts[targetPath] = &mountEntry{}
	ns.mu.Unlock()

	releaseReservation := func() {
		ns.mu.Lock()
		delete(ns.mounts, targetPath)
		ns.mu.Unlock()
	}

	mode, err := resolveCredentialMode(volCtx, secrets)
	if err != nil {
		releaseReservation()
		return nil, err
	}

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		releaseReservation()
		return nil, status.Errorf(codes.Internal, "failed to create target path %s: %v", targetPath, err)
	}

	configDir, configPath, err := ns.writeConfig(targetPath, volCtx, secrets, req.GetReadonly(), mode)
	if err != nil {
		releaseReservation()
		return nil, status.Errorf(codes.Internal, "failed to write msfs config: %v", err)
	}

	// Per-workload IRSA: if the kubelet delivered the workload pod's projected
	// ServiceAccount token, persist it next to msfs.yaml and assume the PV's
	// roleArn. No-op for static mode or when no token was supplied (driver-SA
	// IRSA / older kubelet), preserving existing behavior.
	tokenFile, roleArn, err := ns.resolveWorkloadIdentity(configDir, volCtx, mode)
	if err != nil {
		os.RemoveAll(configDir)
		releaseReservation()
		return nil, err
	}

	cmd := exec.Command(ns.msfsBinary, configPath)
	cmd.Env = ns.buildEnv(secrets, mode, tokenFile, roleArn)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	klog.Infof("starting msfs: %s %s (mountpoint=%s, bucket=%s, authMode=%s)", ns.msfsBinary, configPath, targetPath, bucketName, mode)
	if err := cmd.Start(); err != nil {
		os.RemoveAll(configDir)
		releaseReservation()
		return nil, status.Errorf(codes.Internal, "failed to start msfs: %v", err)
	}

	if err := ns.waitForMount(targetPath, 30*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		os.RemoveAll(configDir)
		releaseReservation()
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
			// Stay idempotent: if the path is no longer a mount point, both
			// failures most likely mean it was already unmounted. Only
			// surface a hard error when the mount is still visible.
			if isMountPoint(targetPath) {
				return nil, status.Errorf(codes.Internal, "unmount %s failed: fusermount: %v, umount: %v", targetPath, err, err2)
			}
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

// hasStaticSecretKeys reports whether the secret map carries both required
// static credential fields. Treats empty strings as missing so that a Secret
// keyed with empty values is rejected the same as a missing one.
func hasStaticSecretKeys(secrets map[string]string) bool {
	return secrets["access_key_id"] != "" && secrets["secret_access_key"] != ""
}

// resolveCredentialMode determines how to supply credentials for this mount.
//
// Resolution rules:
//   - If volumeAttributes.authType is set, it is honored verbatim. Unsupported
//     values surface as gRPC InvalidArgument so misconfigured PVs fail fast.
//   - "static" requires both access_key_id and secret_access_key in the
//     referenced Secret; partial credentials are rejected.
//   - "irsa" / "wif" never require a Secret. Either alias resolves to
//     credentialModeIRSA.
//   - "auto" (or unset) picks static if a complete Secret was provided,
//     otherwise IRSA. This preserves backward compatibility with existing
//     static-secret manifests while letting new IRSA-based PVs omit
//     nodePublishSecretRef entirely.
func resolveCredentialMode(volCtx, secrets map[string]string) (credentialMode, error) {
	requested := strings.ToLower(strings.TrimSpace(volCtx["authType"]))
	switch requested {
	case "", string(credentialModeAuto):
		if hasStaticSecretKeys(secrets) {
			return credentialModeStatic, nil
		}
		// No (complete) secret provided: fall through to workload identity.
		// If the cluster isn't actually IRSA-configured the AWS SDK will
		// surface a clear auth error at first request time.
		return credentialModeIRSA, nil
	case string(credentialModeStatic):
		if !hasStaticSecretKeys(secrets) {
			return "", status.Error(codes.InvalidArgument,
				"authType=static requires both access_key_id and secret_access_key in nodePublishSecretRef")
		}
		return credentialModeStatic, nil
	case string(credentialModeIRSA), "wif":
		return credentialModeIRSA, nil
	default:
		return "", status.Errorf(codes.InvalidArgument,
			"unsupported authType %q (expected one of: auto, static, irsa, wif)", requested)
	}
}

// resolveBackendType normalizes volumeAttributes.backendType to its canonical
// form (backendTypeS3 or backendTypeAIStore), defaulting to S3. An unsupported
// value is rejected so a misconfigured PV fails fast.
func resolveBackendType(volCtx map[string]string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(valOrDefault(volCtx, "backendType", backendTypeS3))) {
	case "S3":
		return backendTypeS3, nil
	case "AISTORE":
		return backendTypeAIStore, nil
	default:
		return "", fmt.Errorf("unsupported backendType %q (expected S3 or AIStore)", volCtx["backendType"])
	}
}

// parseWorkloadToken extracts the projected ServiceAccount token for the given
// audience from the kubelet-provided serviceAccount.tokens JSON in
// volume_context (populated only when the CSIDriver declares tokenRequests).
//
// It returns ok=false with no error when the tokens key is absent — the signal
// to fall back to today's driver-SA behavior (older kubelet, static mode, or
// per-workload IRSA disabled). It returns an error when the key is present but
// malformed or lacks the requested audience, so a misconfigured mount fails
// fast rather than silently using the wrong identity.
func parseWorkloadToken(volCtx map[string]string, audience string) (string, bool, error) {
	raw := volCtx[serviceAccountTokensVolCtxKey]
	if strings.TrimSpace(raw) == "" {
		return "", false, nil
	}
	var byAudience map[string]serviceAccountToken
	if err := json.Unmarshal([]byte(raw), &byAudience); err != nil {
		return "", false, status.Errorf(codes.InvalidArgument,
			"failed to parse %s: %v", serviceAccountTokensVolCtxKey, err)
	}
	tok, ok := byAudience[audience]
	if !ok || tok.Token == "" {
		return "", false, status.Errorf(codes.InvalidArgument,
			"%s present but has no token for audience %q", serviceAccountTokensVolCtxKey, audience)
	}
	return tok.Token, true, nil
}

func (ns *nodeServer) writeConfig(targetPath string, volCtx, secrets map[string]string, requestReadonly bool, mode credentialMode) (string, string, error) {
	configDir, err := os.MkdirTemp("", "msfs-csi-*")
	if err != nil {
		return "", "", fmt.Errorf("failed to create temp config dir: %w", err)
	}

	backendType, err := resolveBackendType(volCtx)
	if err != nil {
		os.RemoveAll(configDir)
		return "", "", err
	}
	dirName := valOrDefault(volCtx, "dirName", strings.ToLower(backendType))
	if backendType == backendTypeAIStore && dirName == "aistore" {
		dirName = "ais"
	}

	region := valOrDefault(volCtx, "region", "us-east-1")
	endpoint := valOrDefault(volCtx, "endpoint", fmt.Sprintf("https://s3.%s.amazonaws.com", region))

	// Honor the CSI request's readonly flag (set by kubelet from the PV's
	// access mode). Fall back to volumeAttributes["readonly"] only when the
	// request flag is false. Default is read-only.
	readonlyStr := "true"
	if !requestReadonly && volCtx["readonly"] == "false" {
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

	// In IRSA mode we deliberately omit the access_key_id / secret_access_key
	// placeholders so the AWS SDK falls through to its credential chain and
	// picks up the projected ServiceAccount token (AWS_ROLE_ARN +
	// AWS_WEB_IDENTITY_TOKEN_FILE) that EKS sets on the pod. Including the
	// placeholders with empty env vars would make the SDK think static creds
	// were intended and fail with "missing credentials".
	credentialBlock := ""
	if backendType == backendTypeS3 && mode == credentialModeStatic {
		credentialBlock = `      access_key_id: "${AWS_ACCESS_KEY_ID}"
      secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
`
	}

	var backendSpecific strings.Builder
	switch backendType {
	case backendTypeS3:
		fmt.Fprintf(&backendSpecific, `    S3:
      region: %q
      endpoint: %q
%s      virtual_hosted_style_request: false
`, region, endpoint, credentialBlock)
	case backendTypeAIStore:
		aisOptionalQuoted := func(key, yamlField string) {
			if v := volCtx[key]; v != "" {
				fmt.Fprintf(&backendSpecific, "      %s: %q\n", yamlField, v)
			}
		}
		aisOptionalStr := func(key, yamlField string) {
			if v := volCtx[key]; v != "" {
				fmt.Fprintf(&backendSpecific, "      %s: %s\n", yamlField, v)
			}
		}
		backendSpecific.WriteString("    AIStore:\n")
		aisOptionalQuoted("aisEndpoint", "endpoint")
		aisOptionalStr("aisSkipTLSCertificateVerify", "skip_tls_certificate_verify")
		aisOptionalQuoted("aisAuthnToken", "authn_token")
		aisOptionalQuoted("aisAuthnTokenFile", "authn_token_file")
		aisOptionalQuoted("aisProvider", "provider")
		aisOptionalStr("aisTimeout", "timeout")
		aisOptionalQuoted("aisManifestGenBackend", "manifest_gen_backend")
	}

	config := fmt.Sprintf(`msfs_version: 1
endpoint: "http://0.0.0.0:0"
mountpoint: %s
%sbackends:
  - dir_name: %s
    bucket_container_name: %s
    prefix: %q
    readonly: %s
%s    backend_type: %s
%s`, targetPath, globalExtra.String(), dirName, volCtx["bucketName"], volCtx["prefix"], readonlyStr, backendExtra.String(), backendType, backendSpecific.String())

	configPath := filepath.Join(configDir, "msfs.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		os.RemoveAll(configDir)
		return "", "", fmt.Errorf("failed to write config: %w", err)
	}
	return configDir, configPath, nil
}

// resolveWorkloadIdentity implements per-workload IRSA. For workload-identity
// mounts where the kubelet supplied the workload pod's projected SA token, it
// writes the token to a 0600 file in configDir and returns that path together
// with the role ARN to assume (from volumeAttributes.roleArn). Rewriting an
// existing file in place (republish) is intentional: the AWS SDK re-reads the
// token file when it refreshes credentials.
//
// It returns ("", "", nil) when per-workload IRSA does not apply — static mode,
// a non-S3 backend, or no workload token in volume_context — so the caller keeps
// today's driver-SA behavior.
func (ns *nodeServer) resolveWorkloadIdentity(configDir string, volCtx map[string]string, mode credentialMode) (string, string, error) {
	if mode == credentialModeStatic {
		return "", "", nil
	}
	// Per-workload IRSA assumes an AWS IAM role, so it only applies to S3
	// mounts. AIStore (and any future non-S3 backend) needs no AWS identity;
	// without this guard a tokenRequests-enabled CSIDriver would hand every
	// mount a workload token and force AIStore PVs to set a meaningless
	// volumeAttributes.roleArn.
	backendType, err := resolveBackendType(volCtx)
	if err != nil {
		return "", "", status.Error(codes.InvalidArgument, err.Error())
	}
	if backendType != backendTypeS3 {
		return "", "", nil
	}
	token, ok, err := parseWorkloadToken(volCtx, stsAudience)
	if err != nil {
		return "", "", err
	}
	if !ok {
		return "", "", nil
	}
	roleArn := strings.TrimSpace(volCtx["roleArn"])
	if roleArn == "" {
		return "", "", status.Error(codes.InvalidArgument,
			"volumeAttributes.roleArn is required for per-workload IRSA "+
				"(CSIDriver tokenRequests is enabled and the kubelet supplied a workload ServiceAccount token)")
	}
	tokenFile := filepath.Join(configDir, webIdentityTokenFileName)
	// Write to a temp file in the same dir and rename into place so a republish
	// rewrite is atomic: a concurrent reader (the AWS SDK refreshing creds)
	// observes either the complete old token or the complete new one, never a
	// truncated/empty file.
	tmpFile := tokenFile + ".tmp"
	if err := os.WriteFile(tmpFile, []byte(token), 0600); err != nil {
		return "", "", status.Errorf(codes.Internal, "failed to write workload token file: %v", err)
	}
	if err := os.Rename(tmpFile, tokenFile); err != nil {
		_ = os.Remove(tmpFile)
		return "", "", status.Errorf(codes.Internal, "failed to persist workload token file: %v", err)
	}
	return tokenFile, roleArn, nil
}

// setEnv returns env with exactly one KEY=value entry, dropping any inherited
// occurrences first so the override wins regardless of how getenv() resolves
// duplicates in the child process.
func setEnv(env []string, key, value string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env)+1)
	for _, e := range env {
		if !strings.HasPrefix(e, prefix) {
			out = append(out, e)
		}
	}
	return append(out, prefix+value)
}

func (ns *nodeServer) buildEnv(secrets map[string]string, mode credentialMode, webIdentityTokenFile, roleArn string) []string {
	env := os.Environ()

	// Static mode: inject the access keys from the Secret. (No web identity.)
	if mode == credentialModeStatic {
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

	// IRSA mode must not inject AWS_ACCESS_KEY_ID etc. — doing so short-
	// circuits the SDK credential chain and prevents it from using the
	// projected web identity token.
	//
	// Per-workload IRSA: when the kubelet supplied the workload pod's token,
	// point the msfs child at it (and the PV's role) instead of the driver
	// pod's. Replace rather than append so an inherited
	// AWS_WEB_IDENTITY_TOKEN_FILE / AWS_ROLE_ARN from the driver's own IRSA
	// env cannot win. When no per-workload token was supplied, pass the host
	// environment through unchanged so EKS-set vars (AWS_ROLE_ARN,
	// AWS_WEB_IDENTITY_TOKEN_FILE, AWS_REGION) reach msfs — today's driver-SA
	// behavior.
	if webIdentityTokenFile != "" {
		env = setEnv(env, "AWS_WEB_IDENTITY_TOKEN_FILE", webIdentityTokenFile)
		if roleArn != "" {
			env = setEnv(env, "AWS_ROLE_ARN", roleArn)
		}
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
