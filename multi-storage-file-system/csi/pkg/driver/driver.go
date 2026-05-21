package driver

import (
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

const (
	DefaultDriverName = "msfs.csi.nvidia.com"
)

type Driver struct {
	name     string
	version  string
	nodeID   string
	endpoint string

	srv *grpc.Server
	ns  *nodeServer
	ids *identityServer
	cs  *controllerServer
}

func NewDriver(name, version, nodeID, endpoint, msfsBinary string) *Driver {
	if name == "" {
		name = DefaultDriverName
	}
	return &Driver{
		name:     name,
		version:  version,
		nodeID:   nodeID,
		endpoint: endpoint,
		ns:       newNodeServer(nodeID, msfsBinary),
		ids:      newIdentityServer(name, version),
		cs:       newControllerServer(),
	}
}

func (d *Driver) Run() error {
	scheme, addr, err := parseEndpoint(d.endpoint)
	if err != nil {
		return fmt.Errorf("failed to parse endpoint %q: %w", d.endpoint, err)
	}

	listener, err := listen(scheme, addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s://%s: %w", scheme, addr, err)
	}

	d.srv = grpc.NewServer(grpc.UnaryInterceptor(logInterceptor))
	csi.RegisterIdentityServer(d.srv, d.ids)
	csi.RegisterNodeServer(d.srv, d.ns)
	csi.RegisterControllerServer(d.srv, d.cs)

	klog.Infof("MSFS CSI driver %s version %s listening on %s://%s", d.name, d.version, scheme, addr)
	return d.srv.Serve(listener)
}

func (d *Driver) Stop() {
	if d.srv != nil {
		d.srv.GracefulStop()
	}
}
