package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/NVIDIA/multi-storage-client/multi-storage-file-system/csi/pkg/driver"
	"k8s.io/klog/v2"
)

var (
	version = "dev"
)

func main() {
	var (
		endpoint   string
		nodeID     string
		driverName string
		msfsBinary string
	)

	flag.StringVar(&endpoint, "endpoint", "unix:///csi/csi.sock", "CSI gRPC endpoint")
	flag.StringVar(&nodeID, "nodeid", "", "Node ID (defaults to hostname)")
	flag.StringVar(&driverName, "driver-name", driver.DefaultDriverName, "CSI driver name")
	flag.StringVar(&msfsBinary, "msfs-binary", "/usr/local/bin/msfs", "Path to msfs binary")

	klog.InitFlags(nil)
	flag.Parse()

	if nodeID == "" {
		var err error
		nodeID, err = os.Hostname()
		if err != nil {
			klog.Fatalf("failed to get hostname: %v", err)
		}
	}

	d := driver.NewDriver(driverName, version, nodeID, endpoint, msfsBinary)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		fmt.Fprintf(os.Stderr, "received signal %v, shutting down\n", sig)
		d.Stop()
	}()

	if err := d.Run(); err != nil {
		klog.Fatalf("driver failed: %v", err)
	}
}
