#!/bin/bash
#
# test_ais.sh - Test AIStore native backend for Multi-Storage File System
#
# This script tests the native AIStore backend implementation (using AIStore's
# native protocol, not S3-compatible API). It verifies POSIX filesystem operations
# against a native ais:// bucket.
#
# IMPORTANT: This script MUST run inside the msfs_dev Docker container.
#
# Prerequisites:
# - Docker environment running: cd multi-storage-file-system && docker-compose up -d  
# - Run dev_setup.sh first: docker exec msfs_dev bash -c "cd /multi-storage-client/multi-storage-file-system && ./dev_setup.sh aisMinio"
#
# Usage (run from host):
#   docker exec msfs_dev bash -c "/multi-storage-client/multi-storage-client/tests/test_mscp/test_ais.sh"
#

set -e

MSFS_DIR="/multi-storage-client/multi-storage-file-system"
CONFIG_FILE="${MSFS_DIR}/test_aistore_local.yaml"
MOUNT_POINT="/tmp/ais-test"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    umount "${MOUNT_POINT}" 2>/dev/null || true
}

trap cleanup EXIT

echo ""
echo "================================================================"
echo "  AIStore Native Backend Test"
echo "================================================================"
echo ""

# Mount filesystem
log_info "Mounting AIStore native backend"
umount "${MOUNT_POINT}" 2>/dev/null || true
mkdir -p "${MOUNT_POINT}"
mount -t msfs "${CONFIG_FILE}" "${MOUNT_POINT}"
sleep 1

if ! mount | grep -q "${MOUNT_POINT}"; then
    log_error "Mount failed"
    exit 1
fi
log_info "✓ Filesystem mounted"

# Test list
echo ""
log_info "Testing listDirectory()..."
ls -la "${MOUNT_POINT}/ais-test/" || exit 1
log_info "✓ List succeeded"

# Test read
echo ""
log_info "Testing readFile()..."
cat "${MOUNT_POINT}/ais-test/file1.txt"
log_info "✓ Read succeeded"

# Test stat
echo ""
log_info "Testing statFile()..."
stat "${MOUNT_POINT}/ais-test/large.txt" | head -3
log_info "✓ Stat succeeded"

# Test delete (expected to fail)
echo ""
log_info "Testing deleteFile() - expecting 'Function not implemented'"
rm -v "${MOUNT_POINT}/ais-test/large.txt" 2>&1 || echo "Expected: FUSE layer TODO"

# Verify still works
echo ""
log_info "Verifying filesystem still functional..."
ls -la "${MOUNT_POINT}/ais-test/"
log_info "✓ Still accessible"

echo ""
echo "================================================================"
echo -e "${GREEN}  ✓ AIStore Native Backend Tests Complete!${NC}"
echo "================================================================"
echo ""
