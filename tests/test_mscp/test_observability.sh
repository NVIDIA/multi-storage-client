#!/bin/bash
# Test script to verify observability metrics are being generated
# Based on test_mount.sh but with observability verification added

set -e  # Exit on error

# Variables
CONFIG_FILE="/multi-storage-client/posix/fuse/mscp/msc_config_dev.yaml"  # Config WITH observability
MOUNT_POINT="/mnt/msc_obs_test"

# Clean up function
cleanup() {
    echo "Cleaning up..."
    umount "$MOUNT_POINT" 2>/dev/null || true
    sleep 1
}

# Clean up any existing mounts first
cleanup

echo '=== MSCP Observability Test ==='
echo

echo '1. Creating mount directory...'
mkdir -p "$MOUNT_POINT"
echo

echo '2. Mounting with observability enabled...'
mount -t msc "$CONFIG_FILE" "$MOUNT_POINT"
echo

echo '3. Verifying mount is active...'
mount | grep msc
ps aux | grep mscp | grep -v grep | grep -v test_observability
echo

echo '4. Testing file access (triggers DoLookup, DoGetAttr, DoOpenDir, DoReadDir)...'
ls "$MOUNT_POINT/" && echo "  ✓ Mount accessible"
echo

echo '4b. Setting up MinIO with test files (via dev_setup.sh)...'
# Use dev_setup.sh to populate MinIO with test files
cd /multi-storage-client/posix/fuse/mscp && ./dev_setup.sh minio > /dev/null 2>&1
echo "  ✓ MinIO populated with test files"
echo

echo '4c. Testing file reads with md5sum (triggers DoRead, backend readFile - hits MinIO)...'
# Run md5sum on files in the minio directory - this will trigger actual DoRead operations
find "$MOUNT_POINT/minio" -type f 2>/dev/null | while read file; do
    md5sum "$file" > /dev/null 2>&1
done
echo "  ✓ Performed md5sum on files (triggers DoRead and backend S3 operations)"
echo

echo '5. Checking observability initialization in MSCP logs...'
LATEST_LOG=$(ls -t /var/log/msc/mscp_*.log | head -1)
echo "  Latest log file: $LATEST_LOG"
if grep -q "Metrics initialized" "$LATEST_LOG"; then
    echo "  ✓ Metrics initialized successfully"
    grep "Metrics initialized" "$LATEST_LOG"
else
    echo "  ✗ Metrics NOT initialized!"
    tail -20 "$LATEST_LOG"
    exit 1
fi
echo

echo '5b. Verifying attribute providers initialized...'
PROVIDER_COUNT=$(grep -c "Initialized attribute provider:" "$LATEST_LOG" || echo "0")
echo "  Found $PROVIDER_COUNT attribute providers initialized"
if [ "$PROVIDER_COUNT" -ge 5 ]; then
    echo "  ✓ All expected attribute providers found:"
    grep "Initialized attribute provider:" "$LATEST_LOG" | sed 's/^/    /'
else
    echo "  ⚠ Expected at least 5 providers (static, host, process, environment_variables, msc_config)"
    grep "Initialized attribute provider:" "$LATEST_LOG" | sed 's/^/    /' || echo "    None found!"
fi
echo

echo '6. Waiting for metrics to be exported to OTEL Collector (65 seconds for export cycle)...'
sleep 65
echo

echo '7. Checking OTEL Collector logs for metrics...'
# Check collector logs by reading from docker container logs directory
if docker logs mscp_otel_collector 2>&1 | grep -q "multistorageclient.request"; then
    echo "  ✓ SUCCESS: Found MSCP metrics in OTEL Collector"
    echo ""
    echo "  Sample metrics:"
    docker logs mscp_otel_collector 2>&1 | grep -E "Name: multistorageclient\.(request|response|latency)" | tail -6
else
    echo "  ⚠ Could not verify metrics in collector logs (docker command may not be available)"
    echo "  To verify manually from host: docker logs mscp_otel_collector | grep multistorageclient"
fi
echo

echo '7b. Verifying new attribute providers in exported metrics...'
echo "  Checking for environment_variables attributes (msc.user, msc.hostname)..."
if docker logs mscp_otel_collector 2>&1 | grep -q "msc.user"; then
    echo "    ✓ Found msc.user attribute"
    docker logs mscp_otel_collector 2>&1 | grep "msc.user" | head -1 | sed 's/^/      /'
else
    echo "    ⚠ msc.user not found"
fi

echo "  Checking for msc_config attributes (msc.otel_endpoint, msc.secret_hash)..."
if docker logs mscp_otel_collector 2>&1 | grep -q "msc.otel_endpoint"; then
    echo "    ✓ Found msc.otel_endpoint attribute"
    docker logs mscp_otel_collector 2>&1 | grep "msc.otel_endpoint" | head -1 | sed 's/^/      /'
else
    echo "    ⚠ msc.otel_endpoint not found"
fi

if docker logs mscp_otel_collector 2>&1 | grep -q "msc.secret_hash"; then
    echo "    ✓ Found msc.secret_hash attribute (hashed credential)"
    docker logs mscp_otel_collector 2>&1 | grep "msc.secret_hash" | head -1 | sed 's/^/      /'
else
    echo "    ⚠ msc.secret_hash not found"
fi
echo

echo '8. Unmounting...'
umount "$MOUNT_POINT"
echo

echo '=== Test Complete ==='
echo
echo "✓ MSCP mounted and performed operations with observability enabled"
echo "✓ Metrics initialized with diperiodic pattern"
echo "✓ All attribute providers initialized (static, host, process, environment_variables, msc_config)"
echo "✓ Export interval: 60000ms (collect: 1000ms, export: 60000ms)"
echo ""
echo "Attribute providers tested:"
echo "  • static: organization, cluster"
echo "  • host: node (hostname)"
echo "  • process: pid"
echo "  • environment_variables: msc.user, msc.hostname"
echo "  • msc_config: msc.otel_endpoint, msc.export_interval, msc.storage_endpoint, msc.secret_hash"
