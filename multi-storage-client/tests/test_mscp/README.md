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

### test_observability.sh

Tests OpenTelemetry metrics instrumentation:

- Assumes OTEL Collector (mscp_otel_collector) is already running
- Mounts MSCP with observability enabled (msc_config_dev.yaml)
- Sets up MinIO with test files (via dev_setup.sh)
- Performs file operations:
  - `ls` (triggers DoLookup, DoGetAttr, DoOpenDir, DoReadDir)
  - `md5sum` on files (triggers DoRead and backend S3 operations)
- Verifies metrics initialization in MSCP logs
- Verifies 5+ attribute providers initialized (static, host, process, environment_variables, msc_config)
- Waits for metrics export (5s)
- Validates metrics in OTEL Collector logs (checks for msc.request)
- Verifies specific attributes exported: msc.user, msc.hostname, msc.otel_endpoint, msc.secret_hash
- Note: Metrics only (tracing not yet implemented)

### test_ais.sh

Tests the native AIStore backend implementation:

- Uses AIStore's native protocol (not S3-compatible API)
- Requires Docker environment with AIStore cluster running
- Automatically creates native `ais://` bucket and uploads test files
- Tests POSIX operations: list, read, stat, delete
- Verifies all backend operations work correctly
- Validates backend logs for operation traces

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

# Run observability test (waits 65 seconds for metrics export)
docker-compose exec dev bash -c "/multi-storage-client/tests/test_mscp/test_observability.sh"

# Verify metrics were exported to OTEL collector
docker logs mscp_otel_collector 2>&1 | grep -E "multistorageclient\.(latency|request|response|data)" | head -30

# Check specific metric attributes and values
docker logs mscp_otel_collector 2>&1 | grep -A 15 "multistorageclient.request.sum" | grep -E "(operation:|Value:)" | head -10

# Clean up
docker-compose down
```

### AIStore Native Backend Test (multi-storage-file-system)

```bash
# Start Docker environment with AIStore
cd multi-storage-file-system
docker-compose up -d

# Wait for AIStore to initialize (~45 seconds)
sleep 45

# Run dev_setup.sh to create native AIStore bucket and upload test files
# Note: This runs from HOST (not inside container) as it uses docker exec commands
./dev_setup.sh ais

# Run automated AIStore native backend test (runs inside msfs_dev container)
docker exec msfs_dev /multi-storage-client/multi-storage-client/tests/test_mscp/test_ais.sh

# Clean up
docker-compose down
```

The test will:
- Mount filesystem using native AIStore backend
- Test listDirectory() - list all files
- Test readFile() - read file contents  
- Test statFile() - get file metadata
- Test deleteFile() - verify expected behavior (FUSE layer TODO)
- Verify filesystem remains functional

**Note:** The `dev_setup.sh ais` mode creates a native `ais://testbucket` bucket (not S3-backed).
This is different from `dev_setup.sh aisMinio` which creates an S3-backed bucket.

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

### test_observability.sh

```
=== MSCP Observability Test ===

1. Creating mount directory...

2. Mounting with observability enabled...
[mount.msc] mscp daemon started successfully (PID: 5056)
[mount.msc] MSC filesystem mounted

3. Verifying mount is active...
msc-posix on /mnt/msc_obs_test type fuse.msc-posix

4. Testing file access (triggers DoLookup, DoGetAttr, DoOpenDir, DoReadDir)...
  ✓ Mount accessible

4b. Setting up MinIO with test files (via dev_setup.sh)...
  ✓ MinIO populated with test files

4c. Testing file reads with md5sum (triggers DoRead, backend readFile)...
  ✓ Performed md5sum on files

5. Checking observability initialization in MSCP logs...
  ✓ Metrics initialized successfully
2025/10/22 15:55:55 Metrics initialized with diperiodic pattern (collect=1000ms, export=60000ms)

5b. Verifying attribute providers initialized...
  ✓ All expected attribute providers found:
    - static
    - host
    - process
    - environment_variables
    - msc_config

6. Waiting for metrics to be exported to OTEL Collector (65 seconds)...

7. Checking OTEL Collector logs for metrics...
  ✓ SUCCESS: Found MSCP metrics in OTEL Collector

8. Unmounting...

=== Test Complete ===

# Expected metrics in OTEL collector logs:
$ docker logs mscp_otel_collector 2>&1 | grep -E "multistorageclient\.(latency|request|response|data)" | head -30

All 6 metrics exported:
- multistorageclient.latency
- multistorageclient.data_size
- multistorageclient.data_rate
- multistorageclient.request.sum
- multistorageclient.response.sum
- multistorageclient.data_size.sum

Example metric with low-cardinality attributes:
  Name: multistorageclient.request.sum
  Data point attributes:
    -> multistorageclient.operation: Str(list)
    -> multistorageclient.provider: Str(minio)
    -> multistorageclient.version: Str(0.32.0-86-gda3ea3e-dirty)
  Value: 7

✓ All metrics exported with low-cardinality attributes only (operation, provider, version)
✓ No high-cardinality attributes (paths, offsets, item counts) present
```
