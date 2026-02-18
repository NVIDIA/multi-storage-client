#
# RPM spec.
#
# https://rpm-software-management.github.io/rpm/manual/spec.html
# https://rpm-packaging-guide.github.io#what-is-a-spec-file
#

Name: msfs
Version: %{getenv:VERSION}
Release: 1
License: Apache-2.0
Summary: Multi-Storage File System
URL: https://github.com/NVIDIA/multi-storage-client
Requires: fuse3

%description
MSFS provides a FUSE-based filesystem that enables POSIX file operations
across multiple cloud storage providers including AWS S3 and others
supporting the S3 API.

%install
mkdir --parents %{buildroot}
cp --recursive --target-directory %{buildroot} %{_sourcedir}/%{getenv:ARCH}/*

%files
/usr/bin/mount.msfs
/usr/bin/msfs

%post
# Create log directory.
mkdir --parents /var/log/msfs
chmod 755 /var/log/msfs

%preun
# Stop any running msfs processes before removal.
pkill -f "/usr/bin/msfs" || true
