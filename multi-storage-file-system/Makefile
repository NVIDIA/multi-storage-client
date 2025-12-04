quick: build

all: fmt build lint test cover assets

GIT_TAG := $(shell git describe --tags --always --dirty)
BUILD_LDFLAGS := -ldflags="-X 'main.GitTag=$(GIT_TAG)'"
TEST_LDFLAGS := -ldflags="-X 'github.com/NVIDIA/multi-storage-client/multi-storage-file-system/msfs.GitTag=$(GIT_TAG)'"
NATIVE_GOARCH := $(shell go env GOARCH)
NATIVE_UNAMEM := $(shell uname -m)

NUM_DEB_PKGS := $(shell (dpkg -l  2>/dev/null) | wc -l)
NUM_RPM_PKGS := $(shell (rpm -qa  2>/dev/null) | wc -l)

PKG_TYPE := $(shell \
		if [ "$(NUM_DEB_PKGS)" -ne 0 ]; then \
			echo "deb"; \
		elif [ "$(NUM_RPM_PKGS)" -ne 0 ]; then \
			echo "rpm"; \
		else \
			echo "<unknown>"; \
		fi)


.PHONY: quick all build clean cover fmt lint lint-update msfs-linux-amd64 msfs-linux-arm64 test install uninstall assets deb-packages deb-amd64 deb-arm64 _build-deb clean-deb rpm-packages rpm-amd64 rpm-arm64 _build-rpm clean-rpm

build:
	go build $(BUILD_LDFLAGS) -o msfs . 

clean: clean-deb clean-rpm
	go clean -cache
	go clean -i .
	rm -f msfs
	rm -f msfs-linux-amd64
	rm -f msfs-linux-arm64
	rm -f msfs.tar.gz
	rm -f msfs.zip
	rm -rf $(ASSETS_BUILD_DIR)

cover:
	go test $(TEST_LDFLAGS) -cover

fmt:
	go fmt .

lint:
	golangci-lint run --config .golangci.yml .

lint-update:
	rm -f $(GOPATH)/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin latest

msfs-linux-amd64:
	GOOS=linux GOARCH=amd64 go build $(BUILD_LDFLAGS) -o msfs-linux-amd64 .

msfs-linux-arm64:
	GOOS=linux GOARCH=arm64 go build $(BUILD_LDFLAGS) -o msfs-linux-arm64 .

test:
	go test $(TEST_LDFLAGS)

install:
	@echo "Installing MSFS"
	@if [ "$$(id -u)" -ne 0 ]; then \
		echo "ERROR: Installation requires root privileges. Use: sudo make install"; \
		exit 1; \
	fi
	@if [ "$(PKG_TYPE)" = "deb" ]; then \
		dpkg -i msfs_$(DEB_VERSION)_$(NATIVE_GOARCH).deb; \
	elif [ "$(PKG_TYPE)" = "rpm" ]; then \
		rpm -ivh msfs-$(RPM_VERSION)-$(RPM_RELEASE).$(NATIVE_UNAMEM).rpm; \
	else \
		echo "ERROR: Installation for PKG_TYPE $(PKG_TYPE) not supported."; \
		exit 1; \
	fi
	@echo "Usage:"
	@echo "  mount -t msfs <config_file> <mountpoint>"
	@echo "  umount <mountpoint>"

uninstall:
	@echo "Uninstalling MSFS"
	@if [ "$$(id -u)" -ne 0 ]; then \
		echo "ERROR: Uninstallation requires root privileges. Use: sudo make uninstall"; \
		exit 1; \
	fi
	@if [ "$(PKG_TYPE)" = "deb" ]; then \
		dpkg -r msfs; \
	elif [ "$(PKG_TYPE)" = "rpm" ]; then \
		rpm -e msfs; \
	else \
		echo "ERROR: Installation for PKG_TYPE $(PKG_TYPE) not supported."; \
		exit 1; \
	fi

ASSETS_BUILD_DIR := build/assets

assets: deb-packages rpm-packages
	@echo "Creating release .tar.gz/.zip assets"
	rm -rf $(ASSETS_BUILD_DIR)
	mkdir -p $(ASSETS_BUILD_DIR)
	cp msfs_$(DEB_VERSION)_amd64.deb $(ASSETS_BUILD_DIR)/.
	cp msfs_$(DEB_VERSION)_arm64.deb $(ASSETS_BUILD_DIR)/.
	cp msfs-$(RPM_VERSION)-$(RPM_RELEASE).aarch64.rpm $(ASSETS_BUILD_DIR)/.
	cp msfs-$(RPM_VERSION)-$(RPM_RELEASE).x86_64.rpm $(ASSETS_BUILD_DIR)/.
	@cp msfs_install_uninstall_template.sh $(ASSETS_BUILD_DIR)/msfs_install.sh
	@sed -i '/DEB_AMD64_PACKAGE_BASENAME=/s/=/=msfs_$(DEB_VERSION)_amd64.deb/' $(ASSETS_BUILD_DIR)/msfs_install.sh
	@sed -i '/DEB_ARM64_PACKAGE_BASENAME=/s/=/=msfs_$(DEB_VERSION)_arm64.deb/' $(ASSETS_BUILD_DIR)/msfs_install.sh
	@sed -i '/RPM_AMD64_PACKAGE_BASENAME=/s/=/=msfs-$(RPM_VERSION)-$(RPM_RELEASE).x86_64.rpm/' $(ASSETS_BUILD_DIR)/msfs_install.sh
	@sed -i '/RPM_ARM64_PACKAGE_BASENAME=/s/=/=msfs-$(RPM_VERSION)-$(RPM_RELEASE).aarch64.rpm/' $(ASSETS_BUILD_DIR)/msfs_install.sh
	@chmod +x $(ASSETS_BUILD_DIR)/msfs_install.sh
	@cp $(ASSETS_BUILD_DIR)/msfs_install.sh $(ASSETS_BUILD_DIR)/msfs_uninstall.sh
	(rm -f msfs.tar.gz && cd $(ASSETS_BUILD_DIR) && tar -czvf ../../msfs.tar.gz .)
	(rm -f msfs.zip    && cd $(ASSETS_BUILD_DIR) && zip       ../../msfs.zip *)

# Debian package variables
DEB_PACKAGE_NAME := msfs
DEB_VERSION := $(GIT_TAG)
DEB_MAINTAINER := NVIDIA <multi-storage-client@nvidia.com>
DEB_DESCRIPTION := Multi-Storage-File-System
DEB_BUILD_DIR := build/deb

# Build both ARM64 and AMD64 Debian packages
deb-packages: deb-amd64 deb-arm64

# Build AMD64 Debian package
deb-amd64: msfs-linux-amd64
	@echo "Building AMD64 Debian package..."
	$(MAKE) _build-deb ARCH=amd64 BINARY=msfs-linux-amd64

# Build ARM64 Debian package  
deb-arm64: msfs-linux-arm64
	@echo "Building ARM64 Debian package..."
	$(MAKE) _build-deb ARCH=arm64 BINARY=msfs-linux-arm64

# Internal target to build a Debian package for specified architecture
_build-deb:
	@echo "Creating Debian package structure for $(ARCH)..."
	@rm -rf $(DEB_BUILD_DIR)/$(ARCH)
	@mkdir -p $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN
	@mkdir -p $(DEB_BUILD_DIR)/$(ARCH)/usr/bin
	@mkdir -p $(DEB_BUILD_DIR)/$(ARCH)/usr/sbin
	
	# Copy binaries
	@cp $(BINARY) $(DEB_BUILD_DIR)/$(ARCH)/usr/bin/msfs
	@cp mount.msfs $(DEB_BUILD_DIR)/$(ARCH)/usr/sbin/mount.msfs
	@chmod 755 $(DEB_BUILD_DIR)/$(ARCH)/usr/bin/msfs
	@chmod 755 $(DEB_BUILD_DIR)/$(ARCH)/usr/sbin/mount.msfs
	
	# Create control file
	@echo "Package: $(DEB_PACKAGE_NAME)" > $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	@echo "Version: $(DEB_VERSION)" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	@echo "Section: utils" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	@echo "Priority: optional" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	@echo "Architecture: $(ARCH)" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	@echo "Depends: fuse3" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	@echo "Maintainer: $(DEB_MAINTAINER)" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	@echo "Description: $(DEB_DESCRIPTION)" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	@echo " MSFS provides a FUSE-based filesystem that enables POSIX file operations" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	@echo " across multiple cloud storage providers including AWS S3 and others" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	@echo " supporting the S3 API." >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/control
	
	# Create postinst script
	@echo "#!/bin/sh" > $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	@echo "set -e" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	@echo "# Create log directory" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	@echo "mkdir -p /var/log/msfs" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	@echo "chmod 755 /var/log/msfs" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	@echo "exit 0" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	@chmod 755 $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/postinst
	
	# Create prerm script
	@echo "#!/bin/sh" > $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	@echo "set -e" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	@echo "# Stop any running msfs processes before removal" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	@echo "pkill -f '/usr/bin/msfs' || true" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	@echo "exit 0" >> $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	@chmod 755 $(DEB_BUILD_DIR)/$(ARCH)/DEBIAN/prerm
	
	# Build the package
	dpkg-deb --build $(DEB_BUILD_DIR)/$(ARCH) $(DEB_PACKAGE_NAME)_$(DEB_VERSION)_$(ARCH).deb
	@echo "Debian package created: $(DEB_PACKAGE_NAME)_$(DEB_VERSION)_$(ARCH).deb"

# Clean up Debian package build artifacts
clean-deb:
	rm -rf $(DEB_BUILD_DIR)
	rm -f $(DEB_PACKAGE_NAME)_*_*.deb

# RPM package variables
RPM_PACKAGE_NAME := msfs
# RPM_VERSION := $(GIT_TAG)
RPM_VERSION := $(shell echo $(GIT_TAG) | sed 's/-/_/g')
RPM_RELEASE := 1
RPM_MAINTAINER := NVIDIA <multi-storage-client@nvidia.com>
RPM_SUMMARY := Multi-Storage-File-System
RPM_LICENSE := Apache-2.0
RPM_BUILD_DIR := build/rpm

# Build both AMD64 and ARM64 RPM packages
rpm-packages: rpm-amd64 rpm-arm64

# Build AMD64 RPM package
rpm-amd64: msfs-linux-amd64
	@echo "Building AMD64 RPM package..."
	$(MAKE) _build-rpm ARCH=x86_64 BINARY=msfs-linux-amd64

# Build ARM64 RPM package
rpm-arm64: msfs-linux-arm64
	@echo "Building ARM64 RPM package..."
	$(MAKE) _build-rpm ARCH=aarch64 BINARY=msfs-linux-arm64

# Internal target to build an RPM package for specified architecture
_build-rpm:
	@echo "Creating RPM package structure for $(ARCH)..."
	@rm -rf $(RPM_BUILD_DIR)/$(ARCH)
	@mkdir -p $(RPM_BUILD_DIR)/$(ARCH)/BUILD
	@mkdir -p $(RPM_BUILD_DIR)/$(ARCH)/RPMS
	@mkdir -p $(RPM_BUILD_DIR)/$(ARCH)/SOURCES
	@mkdir -p $(RPM_BUILD_DIR)/$(ARCH)/SPECS
	@mkdir -p $(RPM_BUILD_DIR)/$(ARCH)/SRPMS
	@mkdir -p $(RPM_BUILD_DIR)/$(ARCH)/BUILDROOT/$(RPM_PACKAGE_NAME)-$(RPM_VERSION)-$(RPM_RELEASE).$(ARCH)/usr/bin
	@mkdir -p $(RPM_BUILD_DIR)/$(ARCH)/BUILDROOT/$(RPM_PACKAGE_NAME)-$(RPM_VERSION)-$(RPM_RELEASE).$(ARCH)/usr/sbin
	
	# Copy binaries
	@cp $(BINARY) $(RPM_BUILD_DIR)/$(ARCH)/BUILDROOT/$(RPM_PACKAGE_NAME)-$(RPM_VERSION)-$(RPM_RELEASE).$(ARCH)/usr/bin/msfs
	@cp mount.msfs $(RPM_BUILD_DIR)/$(ARCH)/BUILDROOT/$(RPM_PACKAGE_NAME)-$(RPM_VERSION)-$(RPM_RELEASE).$(ARCH)/usr/sbin/mount.msfs
	@chmod 755 $(RPM_BUILD_DIR)/$(ARCH)/BUILDROOT/$(RPM_PACKAGE_NAME)-$(RPM_VERSION)-$(RPM_RELEASE).$(ARCH)/usr/bin/msfs
	@chmod 755 $(RPM_BUILD_DIR)/$(ARCH)/BUILDROOT/$(RPM_PACKAGE_NAME)-$(RPM_VERSION)-$(RPM_RELEASE).$(ARCH)/usr/sbin/mount.msfs
	
	# Create spec file
	@echo "Name: $(RPM_PACKAGE_NAME)" > $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "Version: $(RPM_VERSION)" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "Release: $(RPM_RELEASE)" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "Summary: $(RPM_SUMMARY)" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "License: $(RPM_LICENSE)" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "ExclusiveArch: x86_64 aarch64" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "Requires: fuse" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "Requires: procps-ng" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "%description" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "MSFS provides a FUSE-based filesystem that enables POSIX file operations" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "across multiple cloud storage providers including AWS S3 and others" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "supporting the S3 API." >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "%files" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "/usr/bin/msfs" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "/usr/sbin/mount.msfs" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "%post" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "# Create log directory" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "mkdir -p /var/log/msfs" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "chmod 755 /var/log/msfs" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "%preun" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "# Stop any running msfs processes before removal" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	@echo "pkill -f '/usr/bin/msfs' || true" >> $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec
	
	# Build the RPM package
	rpmbuild --define "_topdir $(shell pwd)/$(RPM_BUILD_DIR)/$(ARCH)" \
		--define "_rpmdir $(shell pwd)/$(RPM_BUILD_DIR)/$(ARCH)/RPMS" \
		--buildroot $(shell pwd)/$(RPM_BUILD_DIR)/$(ARCH)/BUILDROOT/$(RPM_PACKAGE_NAME)-$(RPM_VERSION)-$(RPM_RELEASE).$(ARCH) \
		-bb $(RPM_BUILD_DIR)/$(ARCH)/SPECS/msfs.spec --target $(ARCH)
	@mv $(RPM_BUILD_DIR)/$(ARCH)/RPMS/$(ARCH)/msfs-$(RPM_VERSION)-$(RPM_RELEASE).$(ARCH).rpm .
	@echo "RPM package created: $(RPM_PACKAGE_NAME)-$(RPM_VERSION)-$(RPM_RELEASE).$(ARCH).rpm"

# Clean up RPM package build artifacts
clean-rpm:
	rm -rf $(RPM_BUILD_DIR)
	rm -f $(RPM_PACKAGE_NAME)-*-*.*.rpm

