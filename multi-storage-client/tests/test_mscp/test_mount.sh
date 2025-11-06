#!/bin/bash
# Test script for mount -t msc functionality with multiple instances

set -e  # Exit on error

# Variables
CONFIG_FILE="/multi-storage-client/posix/fuse/mscp/mscp_config_dev.yaml"
MOUNT1="/mnt/msc1"
MOUNT2="/mnt/msc2"

# Function to verify environment variables in a process
verify_env_vars() {
    local expected_config="$1"
    local expected_mountpath="$2"
    local description="$3"
    
    echo "  Verifying environment variables for $description..."
    sleep 1  # Give process time to start
    
    local pid=$(pgrep -n mscp)
    if [[ -z "$pid" ]]; then
        echo "  ✗ ERROR: mscp process not found"
        return 1
    fi
    
    echo "  Found mscp process: $pid"
    
    # Check environment variables in the process
    if [[ -f "/proc/$pid/environ" ]]; then
        local msc_config_var=$(tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | grep "^MSC_CONFIG=" || echo "")
        local msc_mountpoint_var=$(tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | grep "^MSC_MOUNTPOINT=" || echo "")
        
        echo "  Environment variables in process:"
        echo "    $msc_config_var"
        echo "    $msc_mountpoint_var"
        
        if [[ "$msc_config_var" == "MSC_CONFIG=$expected_config" ]]; then
            echo "  ✓ MSC_CONFIG correctly set"
        else
            echo "  ✗ MSC_CONFIG not set correctly (expected: MSC_CONFIG=$expected_config)"
        fi
        
        if [[ "$msc_mountpoint_var" == "MSC_MOUNTPOINT=$expected_mountpath" ]]; then
            echo "  ✓ MSC_MOUNTPOINT correctly set"
        else
            echo "  ✗ MSC_MOUNTPOINT not set correctly (expected: MSC_MOUNTPOINT=$expected_mountpath)"
        fi
    else
        echo "  Note: /proc not available, skipping env var check (may be macOS)"
        echo "  Checking command line arguments instead..."
        local cmdline=$(ps -p "$pid" -o args= 2>/dev/null || echo "")
        if [[ "$cmdline" == *"$expected_config"* ]] && [[ "$cmdline" == *"$expected_mountpath"* ]]; then
            echo "  ✓ Arguments passed correctly in command line"
        else
            echo "  Command line: $cmdline"
        fi
    fi
}

# Clean up function
cleanup() {
    echo "Cleaning up..."
    umount "$MOUNT1" 2>/dev/null || true
    umount "$MOUNT2" 2>/dev/null || true
    sleep 1
}

# Clean up any existing mounts or processes first
cleanup

echo '=== Complete mount -t msc Test Workflow ==='
echo

echo '1. Creating mount directories...'
mkdir -p "$MOUNT1" "$MOUNT2"
echo

echo '2. Mounting first instance with mount -t msc...'
mount -t msc "$CONFIG_FILE" "$MOUNT1"
echo

echo '3. Verifying environment variables for first instance...'
verify_env_vars "$CONFIG_FILE" "$MOUNT1" "first instance"
echo

echo '4. Mounting second instance at different location...'
mount -t msc "$CONFIG_FILE" "$MOUNT2"
echo

echo '5. Verifying environment variables for second instance...'
verify_env_vars "$CONFIG_FILE" "$MOUNT2" "second instance"
echo

echo '6. Verifying both mounts are active...'
mount | grep msc
ps aux | grep mscp | grep -v grep
echo

echo '7. Testing file access on both mounts...'
ls "$MOUNT1/" && echo "  ✓ msc1 accessible"
ls "$MOUNT2/" && echo "  ✓ msc2 accessible"
echo

echo '8. Unmounting first instance only...'
umount "$MOUNT1"
echo

echo '9. Verifying second mount still works...'
mount | grep msc
ls "$MOUNT2/" && echo "  ✓ msc2 still accessible"
echo

echo '10. Unmounting second instance...'
umount "$MOUNT2"
echo

echo '11. Verifying all unmounted...'
mount | grep msc || echo '  ✓ No msc mounts'
ps aux | grep mscp | grep -v grep || echo '  ✓ No mscp processes'
echo

echo '=== Test completed successfully! ==='
