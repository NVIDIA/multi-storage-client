#!/bin/bash
set -e

echo "=== Testing PID/Log File Cleanup ==="
echo

# Setup
CONFIG_FILE="/multi-storage-client/posix/fuse/mscp/mscp_config_dev.yaml"
MOUNT1="/mnt/msc_cleanup_test"
LOG_DIR="/var/log/msc"

# Create mount directory
mkdir -p "$MOUNT1"

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    umount "$MOUNT1" 2>/dev/null || true
    rm -rf "$MOUNT1"
}
trap cleanup EXIT

echo "1. Creating stale PID and log files (simulating crashed processes)..."

# Create stale PID file with non-existent PID
STALE_PID="999999"
STALE_PID_FILE="$LOG_DIR/mscp_stale_999999.pid"
STALE_LOG_FILE="$LOG_DIR/mscp_stale_999999.log"

echo "$STALE_PID" > "$STALE_PID_FILE"
echo "/fake/mount" >> "$STALE_PID_FILE"
echo "This is a stale log file from a crashed process" > "$STALE_LOG_FILE"

# Create another stale file
STALE_PID2="999998"
STALE_PID_FILE2="$LOG_DIR/mscp_stale_999998.pid"
STALE_LOG_FILE2="$LOG_DIR/mscp_stale_999998.log"

echo "$STALE_PID2" > "$STALE_PID_FILE2"
echo "/fake/mount2" >> "$STALE_PID_FILE2"
echo "Another stale log file" > "$STALE_LOG_FILE2"

echo "  Created stale PID file: $STALE_PID_FILE"
echo "  Created stale log file: $STALE_LOG_FILE"
echo "  Created stale PID file: $STALE_PID_FILE2"
echo "  Created stale log file: $STALE_LOG_FILE2"
echo

echo "2. Verifying stale files exist before mount..."
if [[ -f "$STALE_PID_FILE" ]] && [[ -f "$STALE_LOG_FILE" ]] && \
   [[ -f "$STALE_PID_FILE2" ]] && [[ -f "$STALE_LOG_FILE2" ]]; then
    echo "  ✓ All stale files present"
else
    echo "  ✗ ERROR: Stale files not created properly"
    exit 1
fi
echo

echo "3. Mounting new instance (this should trigger cleanup)..."
mount -t msc "$CONFIG_FILE" "$MOUNT1"
sleep 1
echo

echo "4. Verifying stale files were removed..."
ERRORS=0

if [[ -f "$STALE_PID_FILE" ]]; then
    echo "  ✗ ERROR: Stale PID file still exists: $STALE_PID_FILE"
    ERRORS=$((ERRORS + 1))
else
    echo "  ✓ Stale PID file removed: $STALE_PID_FILE"
fi

if [[ -f "$STALE_LOG_FILE" ]]; then
    echo "  ✗ ERROR: Stale log file still exists: $STALE_LOG_FILE"
    ERRORS=$((ERRORS + 1))
else
    echo "  ✓ Stale log file removed: $STALE_LOG_FILE"
fi

if [[ -f "$STALE_PID_FILE2" ]]; then
    echo "  ✗ ERROR: Stale PID file still exists: $STALE_PID_FILE2"
    ERRORS=$((ERRORS + 1))
else
    echo "  ✓ Stale PID file removed: $STALE_PID_FILE2"
fi

if [[ -f "$STALE_LOG_FILE2" ]]; then
    echo "  ✗ ERROR: Stale log file still exists: $STALE_LOG_FILE2"
    ERRORS=$((ERRORS + 1))
else
    echo "  ✓ Stale log file removed: $STALE_LOG_FILE2"
fi
echo

echo "5. Verifying current mount's files still exist..."
CURRENT_PID_FILE=$(ls -t $LOG_DIR/mscp_*.pid 2>/dev/null | head -1)
CURRENT_LOG_FILE="${CURRENT_PID_FILE%.pid}.log"

if [[ -f "$CURRENT_PID_FILE" ]]; then
    echo "  ✓ Active PID file exists: $(basename $CURRENT_PID_FILE)"
else
    echo "  ✗ ERROR: Active PID file not found"
    ERRORS=$((ERRORS + 1))
fi

if [[ -f "$CURRENT_LOG_FILE" ]]; then
    echo "  ✓ Active log file exists: $(basename $CURRENT_LOG_FILE)"
else
    echo "  ✗ ERROR: Active log file not found"
    ERRORS=$((ERRORS + 1))
fi
echo

echo "6. Unmounting..."
umount "$MOUNT1"
echo

if [[ $ERRORS -eq 0 ]]; then
    echo "=== Cleanup test completed successfully! ==="
    exit 0
else
    echo "=== Cleanup test FAILED with $ERRORS errors ==="
    exit 1
fi

