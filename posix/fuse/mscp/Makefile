all: fmt build lint test

GIT_TAG := $(shell git describe --tags --always --dirty)
BUILD_LDFLAGS := -ldflags="-X 'main.GitTag=$(GIT_TAG)'"
TEST_LDFLAGS := -ldflags="-X 'github.com/NVIDIA/multi-storage-client/posix/fuse/mscp.GitTag=$(GIT_TAG)'"

.PHONY: all build clean cover fmt lint lint-update msc-posix-linux-amd64 msc-posix-linux-arm64 publish test install uninstall deb-packages deb-amd64 deb-arm64 _build-deb clean-deb

# Installation directories (can be overridden)
PREFIX ?= /usr
BINDIR ?= $(PREFIX)/bin
SBINDIR ?= $(PREFIX)/sbin
LOGDIR ?= /var/log/msc

build:
	go build $(BUILD_LDFLAGS) .

clean: clean-deb
	go clean -i .
	rm -f mscp mscp.test
	rm -f msc-posix-linux-amd64
	rm -f msc-posix-linux-arm64

cover:
	go test $(TEST_LDFLAGS) -cover

fmt:
	go fmt .

lint:
	golangci-lint run --config .golangci.yml .

lint-update:
	rm -f $(GOPATH)/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin latest

msc-posix-linux-amd64:
	GOOS=linux GOARCH=amd64 go build $(BUILD_LDFLAGS) -o msc-posix-linux-amd64 .

msc-posix-linux-arm64:
	GOOS=linux GOARCH=arm64 go build $(BUILD_LDFLAGS) -o msc-posix-linux-arm64 .

publish:
	 (cd ../../.. && docker build --file posix/fuse/mscp/Dockerfile --target built --tag mscp_built:$(GIT_TAG) .)
	 docker create --name mscp_built mscp_built:$(GIT_TAG) --entrypoint /mscp && \
	 docker cp mscp_built:/msc-posix-linux-amd64 ./msc-posix-linux-amd64 && \
	 docker cp mscp_built:/msc-posix-linux-arm64 ./msc-posix-linux-arm64 && \
	 docker rm mscp_built

test:
	go test $(TEST_LDFLAGS)

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

# Debian package variables
DEB_PACKAGE_NAME := msc-posix
DEB_VERSION := $(shell echo $(GIT_TAG) | sed 's/^v//')
DEB_MAINTAINER := NVIDIA <multi-storage-client@nvidia.com>
DEB_DESCRIPTION := Multi-Storage Client POSIX FUSE filesystem
DEB_BUILD_DIR := build/deb

# Build both ARM64 and AMD64 Debian packages
deb-packages: deb-amd64 deb-arm64

# Build AMD64 Debian package
deb-amd64: msc-posix-linux-amd64
	@echo "Building AMD64 Debian package..."
	$(MAKE) _build-deb ARCH=amd64 BINARY=msc-posix-linux-amd64

# Build ARM64 Debian package  
deb-arm64: msc-posix-linux-arm64
	@echo "Building ARM64 Debian package..."
	$(MAKE) _build-deb ARCH=arm64 BINARY=msc-posix-linux-arm64

# Internal target to build a Debian package for specified architecture
_build-deb:
	@echo "Creating Debian package structure for $(ARCH)..."
	rm -rf $(DEB_BUILD_DIR)/$(ARCH)
	mkdir -p $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN
	mkdir -p $(DEB_BUILD_DIR)/$(ARCH)/usr/bin
	mkdir -p $(DEB_BUILD_DIR)/$(ARCH)/usr/sbin
	
	# Copy binaries
	cp $(BINARY) $(DEB_BUILD_DIR)/$(ARCH)/usr/bin/mscp
	cp mount.msc $(DEB_BUILD_DIR)/$(ARCH)/usr/sbin/mount.msc
	chmod 755 $(DEB_BUILD_DIR)/$(ARCH)/usr/bin/mscp
	chmod 755 $(DEB_BUILD_DIR)/$(ARCH)/usr/sbin/mount.msc
	
	# Create control file
	echo "Package: $(DEB_PACKAGE_NAME)" > $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	echo "Version: $(DEB_VERSION)" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	echo "Section: utils" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	echo "Priority: optional" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	echo "Architecture: $(ARCH)" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	echo "Depends: fuse3" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	echo "Maintainer: $(DEB_MAINTAINER)" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	echo "Description: $(DEB_DESCRIPTION)" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	echo " MSCP provides a FUSE-based filesystem that enables POSIX file operations" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	echo " across multiple cloud storage providers including AWS S3, Google Cloud Storage," >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	echo " Azure Blob Storage, and OCI Object Storage." >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	
	# Create postinst script
	echo "#!/bin/sh" > $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	echo "set -e" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	echo "# Create log directory" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	echo "mkdir -p /var/log/msc" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	echo "chmod 755 /var/log/msc" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	echo "exit 0" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	chmod 755 $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	
	# Create prerm script
	echo "#!/bin/sh" > $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	echo "set -e" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	echo "# Stop any running mscp processes before removal" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	echo "pkill -f '/usr/bin/mscp' || true" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	echo "exit 0" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	chmod 755 $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	
	# Build the package
	dpkg-deb --build $(DEB_BUILD_DIR)/$(ARCH) $(DEB_PACKAGE_NAME)_$(DEB_VERSION)_$(ARCH).deb
	@echo "Debian package created: $(DEB_PACKAGE_NAME)_$(DEB_VERSION)_$(ARCH).deb"

# Clean up Debian package build artifacts
clean-deb:
	rm -rf $(DEB_BUILD_DIR)
	rm -f $(DEB_PACKAGE_NAME)_*_*.deb

