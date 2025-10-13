all: fmt build lint

GIT_TAG := $(shell git describe --tags --always --dirty)
LDFLAGS := -ldflags="-X 'main.GitTag=$(GIT_TAG)'"

.PHONY: all artifacts build clean fmt lint lint-update publish replace install uninstall

# Installation directories (can be overridden)
PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin
SBINDIR ?= /usr/sbin
LOGDIR ?= /var/log/msc

artifacts:
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o msc-posix-linux-amd64 .
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o msc-posix-linux-arm64 .

build:
	go build $(LDFLAGS) .

clean:
	go clean -i .
	rm -f msc-posix-linux-amd64
	rm -f msc-posix-linux-arm64

fmt:
	go fmt .

lint:
	golangci-lint run --config .golangci.yml .

lint-update:
	rm -f $(GOPATH)/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin latest

publish:
	 (cd ../../.. && docker build --file posix/fuse/mscp/Dockerfile --target built --tag mscp_built:$(GIT_TAG) .)
	 docker create --name mscp_built mscp_built:$(GIT_TAG) --entrypoint /mscp && \
	 docker cp mscp_built:/msc-posix-linux-amd64 ./msc-posix-linux-amd64 && \
	 docker cp mscp_built:/msc-posix-linux-arm64 ./msc-posix-linux-arm64 && \
	 docker rm mscp_built

install: build
	@echo "Installing MSC to $(PREFIX)"
	@if [ "$$(id -u)" -ne 0 ]; then \
		echo "ERROR: Installation requires root privileges. Use: sudo make install"; \
		exit 1; \
	fi
	install -d $(BINDIR)
	install -d $(SBINDIR)
	install -d $(LOGDIR)
	install -m 755 mscp $(BINDIR)/mscp
	install -m 755 mount.msc $(SBINDIR)/mount.msc
	@echo ""
	@echo "Installation complete!"
	@echo "  Binary:      $(BINDIR)/mscp"
	@echo "  Mount:       $(SBINDIR)/mount.msc"
	@echo "  Log dir:     $(LOGDIR)"
	@echo ""
	@echo "Usage:"
	@echo "  mount -t msc <config_file> <mountpoint>"
	@echo "  umount <mountpoint>"
	@echo ""
	@echo "For automatic mounting at boot, add to /etc/fstab:"
	@echo "  /path/to/config.yaml  /mnt/point  msc  defaults,_netdev  0  0"
	@echo ""
	@echo "See MOUNT_HELPERS.md for documentation"

uninstall:
	@echo "Uninstalling MSC from $(PREFIX)"
	@if [ "$$(id -u)" -ne 0 ]; then \
		echo "ERROR: Uninstallation requires root privileges. Use: sudo make uninstall"; \
		exit 1; \
	fi
	rm -f $(BINDIR)/mscp
	rm -f $(SBINDIR)/mount.msc
	@echo "MSC uninstalled. Log directory $(LOGDIR) was preserved."
	@echo "To remove logs: sudo rm -rf $(LOGDIR)"

