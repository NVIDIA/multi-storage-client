# MSC POSIX FUSE Tests

Test scripts for the MSC POSIX FUSE mount functionality with multi-instance support.

## Test Scripts

### test_mount.sh
Tests the standard `mount -t msc` command with multiple instances:
- Mount two instances at different mountpoints
- Verify environment variables are set correctly
- Test selective unmounting (unmount one, other stays active)
- Verify no zombie processes (with tini/systemd)

### test_cleanup.sh
Tests automatic cleanup of stale PID and log files:
- Creates fake stale PID/log files (simulating crashed processes)
- Mounts a new instance (triggers cleanup)
- Verifies stale files are removed
- Verifies active files are preserved

## Running the Tests

### Docker 

```bash
# Start Docker environment
cd posix/fuse/mscp
docker-compose up -d

# Build and install
docker-compose exec dev bash -c "cd /multi-storage-client/posix/fuse/mscp && make build && make install"

# Run mount test
docker-compose exec dev bash -c "/multi-storage-client/tests/test_mscp/test_mount.sh"

# Run cleanup test
docker-compose exec dev bash -c "/multi-storage-client/tests/test_mscp/test_cleanup.sh"

# Clean up
docker-compose down
```

### Local Linux System

```bash
# Build and install
cd posix/fuse/mscp
make build
sudo make install

# Create mount directories
sudo mkdir -p /mnt/msc1 /mnt/msc2

# Run test
sudo bash /path/to/tests/test_mscp/test_mount.sh
```

## What It Tests

### test_mount.sh
1. **Multi-instance support** - Multiple independent mounts
2. **Environment variables** - MSC_CONFIG and MSC_MOUNTPOINT propagation  
3. **Selective unmounting** - Unmount specific instance without affecting others
4. **Process management** - No zombie processes with proper init (tini/systemd)
5. **Filesystem access** - Basic read operations on mounted filesystems

### test_cleanup.sh
1. **Stale file detection** - Identifies PID files for dead processes
2. **Automatic cleanup** - Removes stale PID and log files
3. **Active file preservation** - Keeps files for running processes
4. **Pre-mount cleanup** - Runs cleanup before each new mount

## Expected Output

```
=== Complete mount -t msc Test Workflow ===

1. Creating mount directories...
2. Mounting first instance with mount -t msc...
3. Verifying environment variables for first instance...
  ✓ MSC_CONFIG correctly set
  ✓ MSC_MOUNTPOINT correctly set
4. Mounting second instance at different location...
5. Verifying environment variables for second instance...
  ✓ MSC_CONFIG correctly set
  ✓ MSC_MOUNTPOINT correctly set
6. Verifying both mounts are active...
7. Testing file access on both mounts...
  ✓ msc1 accessible
  ✓ msc2 accessible
8. Unmounting first instance only...
9. Verifying second mount still works...
  ✓ msc2 still accessible
10. Unmounting second instance...
11. Verifying all unmounted...
  ✓ No msc mounts
  ✓ No mscp processes

=== Test completed successfully! ===
```
