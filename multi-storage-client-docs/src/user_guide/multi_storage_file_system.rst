################################
Multi-Storage File System (MSFS)
################################

The Multi-Storage File System (MSFS) provides POSIX filesystem access to object storage backends through FUSE (Filesystem in Userspace). This enables applications that require traditional filesystem operations to work seamlessly with cloud object storage without code modifications.

Overview
========

While the Python Multi-Storage Client is designed for easy adoption of object storage by Python applications, some applications prefer or require POSIX filesystem access. MSFS bridges this gap by:

- Providing a POSIX-compliant filesystem interface to object storage
- Supporting S3-compatible object storage (AWS S3, AIS, etc.)
- Enabling applications written in any language to access object storage
- Sharing the same configuration format as the Python MSC for consistency

.. note::

   **Current Release Focus: Read Operations**
   
   This release of MSFS focuses on read-only access to object storage. Write support (create, modify, delete operations) is currently under development and planned for a future release. The configuration schema includes write-related settings for forward compatibility, but write operations are not yet functional.

Key Features
============

- **FUSE-based:** Mounts object storage as a standard filesystem
- **S3 backend support:** AWS S3 and S3-compatible object stores
- **High-performance caching:** Configurable cache for improved read performance
- **Dynamic configuration:** Add or remove backends without unmounting via SIGHUP
- **Standard Unix tools:** Use with ``mount``, ``umount``, and ``/etc/fstab``
- **Observability:** Integrated telemetry with OpenTelemetry metrics

Installation
============

The MSFS binary can be installed using the provided installation script:

.. code-block:: bash

   cd multi-storage-file-system
   sudo make install

This installs:

- ``/usr/local/bin/msfs`` - The FUSE daemon binary
- ``/usr/sbin/mount.msfs`` - Mount helper for standard ``mount`` command

.. note::

   Installation paths and procedures are for Debian-based systems. RPM-based systems may have different installation paths and requirements.

Alternatively, you can build from source:

.. code-block:: bash

   cd multi-storage-file-system
   make
   sudo make install

Configuration
=============

MSFS uses the standard MSC configuration format, providing seamless integration with existing MSC configurations.

MSFS searches for configuration files in the same locations as the Python MSC:

1. Path specified by ``MSC_CONFIG`` environment variable
2. ``${XDG_CONFIG_HOME}/msc/config.yaml`` or ``${XDG_CONFIG_HOME}/msc/config.json``
3. ``${HOME}/.msc_config.yaml`` or ``${HOME}/.msc_config.json``
4. ``${HOME}/.config/msc/config.yaml`` or ``${HOME}/.config/msc/config.json``
5. ``${XDG_CONFIG_DIRS:-/etc/xdg}/msc/config.yaml`` or ``${XDG_CONFIG_DIRS:-/etc/xdg}/msc/config.json``
6. ``/etc/msc_config.yaml`` or ``/etc/msc_config.json``

See :doc:`/references/configuration` for the complete MSC configuration schema.

.. note::

   **Advanced Configuration Mode**
   
   For advanced users requiring fine-grained control over FUSE behavior, caching parameters, and other low-level settings, MSFS provides an extended configuration mode (``msfs_version: 1``). This advanced mode is intended for specialized use cases and performance tuning. For details, see the `MSFS README <https://github.com/NVIDIA/multi-storage-client/blob/main/multi-storage-file-system/README.md>`_.

Environment Variables
=====================

Configuration files support environment variable expansion using ``$VAR`` or ``${VAR}`` syntax:

.. code-block:: yaml

   profiles:
     my-profile:
       storage_provider:
         type: s3
         options:
           base_path: ${BUCKET_NAME}
           access_key_id: ${AWS_ACCESS_KEY_ID}
           secret_access_key: ${AWS_SECRET_ACCESS_KEY}

**MSFS-Specific Environment Variables:**

- ``MSC_CONFIG`` - Path to configuration file
- ``MSFS_MOUNTPOINT`` - Mount point (overrides config file setting)
- ``MSFS_BINARY`` - Path to msfs binary (default: ``/usr/local/bin/msfs``)
- ``MSFS_LOG_DIR`` - Log directory (default: ``/var/log/msfs``)

Usage
=====

Basic Usage
===========

Manual mount/unmount using the MSFS binary directly:

.. code-block:: bash

   # Start MSFS daemon with config file
   export MSC_CONFIG=/path/to/config.yaml
   /usr/local/bin/msfs

   # In another terminal, verify mount
   mount | grep msfs
   df -h /mnt

   # Access files
   ls -l /mnt/backend-name/
   cat /mnt/backend-name/path/to/file.txt

   # Stop daemon (unmount)
   umount /mnt

Mount Helpers
=============

After installation, MSFS can be mounted using standard Unix ``mount`` and ``umount`` commands:

Mounting
--------

.. code-block:: bash

   # Mount with config file and mountpoint
   sudo mount -t msfs /path/to/config.yaml /mnt/storage

   # Mount multiple instances with different configs
   sudo mount -t msfs /path/to/config1.yaml /mnt/storage1
   sudo mount -t msfs /path/to/config2.json /mnt/storage2

**How It Works:**

When you run ``mount -t msfs <config> <mountpoint>``, the ``mount`` command automatically calls ``/usr/sbin/mount.msfs``, which:

1. Exports ``MSC_CONFIG`` environment variable from the config file argument
2. Exports ``MSFS_MOUNTPOINT`` environment variable from the mountpoint argument
3. Creates log directory if needed (``/var/log/msfs/``)
4. Launches the ``msfs`` daemon in the background using ``setsid``
5. Stores the process ID in ``/var/log/msfs/msfs_*.pid``

.. note::

   The ``mount`` command behaves differently based on arguments:
   
   - ``mount`` (no args) → Lists all mounted filesystems
   - ``mount -t msfs`` (type only) → Lists all MSFS filesystems (does NOT call mount.msfs)
   - ``mount -t msfs <config> <mountpoint>`` → Calls mount.msfs to perform the mount

Unmounting
----------

To unmount the filesystem, use the standard ``umount`` command:

.. code-block:: bash

   # Unmount MSFS filesystem
   umount <mount_point>

   # Example
   umount /mnt/storage1

Automatic Mounting with /etc/fstab
===================================

MSFS filesystems can be automatically mounted at boot time using ``/etc/fstab``:

.. code-block:: text
   :caption: /etc/fstab entries for MSFS

   # MSFS filesystem with S3 backend
   /etc/msfs/s3-config.yaml  /mnt/s3-data  msfs  defaults,_netdev  0  0

   # MSFS filesystem with local config
   /home/user/msfs.json      /mnt/storage  msfs  defaults,noauto   0  0

**Field Explanation:**

1. **Device** - Path to MSFS configuration file (YAML or JSON)
2. **Mount Point** - Directory where the filesystem will be mounted
3. **Type** - Filesystem type (``msfs``)
4. **Options** - Mount options (comma-separated):
   
   - ``defaults`` - Standard mount options
   - ``_netdev`` - Wait for network before mounting (recommended for remote storage)
   - ``noauto`` - Don't mount automatically at boot (mount manually)
   - ``user`` - Allow non-root users to mount (requires ``allow_other`` in config)

5. **Dump** - Backup frequency (usually ``0``)
6. **Pass** - fsck pass number (usually ``0``)

After editing ``/etc/fstab``, test the configuration:

.. code-block:: bash

   # Mount all filesystems in fstab
   sudo mount -a
   
   # Verify mount
   df -h /mnt/s3-data

Dynamic Configuration Reload
=============================

MSFS supports dynamic configuration changes without unmounting:

.. code-block:: bash

   # Edit configuration file
   vim /path/to/config.yaml

   # Send SIGHUP to reload configuration
   sudo kill -SIGHUP $(pidof msfs)

Configuration changes are processed as follows:

- **Existing backends** - Cannot be modified (unmount and remount required)
- **New backends** - Automatically mounted and appear as new subdirectories
- **Removed backends** - Automatically unmounted and subdirectories disappear

Alternatively, enable automatic periodic configuration reloading:

.. code-block:: yaml

   msfs_version: 1
   auto_sighup_interval: 300  # Check config every 5 minutes
   backends:
     # ...

Performance
===========

MSFS includes a sophisticated caching layer to optimize read performance.

.. note::

   Cache settings related to write operations (``dirty_cache_lines_flush_trigger``, ``dirty_cache_lines_max``) are reserved for future write support and are not used in the current read-only implementation.

Cache Configuration
===================

The cache uses a line-based architecture where each cache line represents a fixed-size chunk of data:

.. code-block:: yaml

   cache_line_size: 1048576       # 1 MiB per cache line
   cache_lines: 4096              # 4096 cache lines = 4 GiB total cache

**Cache Tuning Guidelines:**

- **Larger cache line size** - Better for sequential access patterns, fewer cache lines needed
- **Smaller cache line size** - Better for random access patterns, more granular caching
- **More cache lines** - Allows caching more files or larger portions of files
- **Less cache lines** - Reduces memory usage

Read Performance
================

Read performance is optimized through:

- **Read-ahead caching** - Cache lines are prefetched for sequential reads
- **Cache hit reuse** - Frequently accessed data remains cached
- **Parallel prefetching** - Multiple cache lines loaded concurrently

Best practices:

- Size ``cache_lines`` to accommodate your working set
- Use larger ``cache_line_size`` for large files
- Use smaller ``cache_line_size`` for many small files

Observability
=============

MSFS supports OpenTelemetry metrics for monitoring performance and operations. Metrics configuration uses the same schema as the Python MSC for consistency.

Configuration
=============

Enable metrics collection by adding observability configuration:

.. code-block:: yaml
   :caption: Metrics with OTLP exporter
   :linenos:

   opentelemetry:
     metrics:
       attributes:
         - type: static
           options:
             attributes:
               service.name: msc-posix
               deployment.environment: production
         - type: host
         - type: process
       
       reader:
         type: periodic
         options:
           collect_interval_millis: 1000
           export_interval_millis: 60000
       
       exporter:
         type: otlp
         options:
           endpoint: "http://otel-collector:4318"
           insecure: true

   backends:
     # ...

See :doc:`/user_guide/telemetry` for complete observability configuration options.

Metrics Exported
================

MSFS exports the following metrics:

**Cache Metrics:**

- ``msfs.cache.hits`` - Number of cache hits
- ``msfs.cache.misses`` - Number of cache misses
- ``msfs.cache.evictions`` - Number of cache evictions

**I/O Metrics:**

- ``msfs.io.bytes_read`` - Total bytes read
- ``msfs.io.read_operations`` - Number of read operations

**Backend Metrics:**

- ``msfs.backend.operations`` - Operations per backend (with labels)
- ``msfs.backend.errors`` - Errors per backend (with labels)

Logs
====

MSFS logs are written to stdout by default. When using mount helpers, logs are redirected to ``/var/log/msfs/msfs_<pid>.log``.

Configure log verbosity per backend:

.. code-block:: yaml

   backends:
     - dir_name: debug-backend
       trace_level: 3  # 0=none, 1=errors, 2=successes, 3+=details
       # ...

Development
===========

Docker Development Environment
==============================

A Docker-based development environment is provided for testing:

.. code-block:: bash

   # Pull MinIO image
   docker pull minio/minio:latest

   # Build development container
   docker-compose build

   # Start containers (MinIO + dev)
   docker-compose up -d dev

   # Enter development container
   docker-compose exec dev bash

Inside the container:

.. code-block:: bash

   # Setup development environment with MinIO backend
   ./dev_setup.sh minio

   # Build MSFS
   make

   # Run MSFS in background
   ./msfs &

   # Test filesystem
   mount | grep fuse
   df -h /mnt
   ls -lR /mnt

   # Reload configuration
   kill -SIGHUP $(pidof ./msfs)

   # Stop daemon
   kill -SIGTERM $(pidof ./msfs)

   # Exit container
   exit

   # Stop containers
   docker-compose down

Testing
=======

Test scripts are provided in the ``multi-storage-client/tests/test_mscp/`` directory:

.. code-block:: bash

   cd multi-storage-client/tests/test_mscp

   # Test mount/unmount
   ./test_mount.sh

   # Test cleanup
   ./test_cleanup.sh

   # Test observability
   ./test_observability.sh

Deployment
==========

Building for Production
=======================

Build optimized binaries for production deployment:

.. code-block:: bash

   cd multi-storage-file-system
   
   # Build for current platform
   make

   # Build and extract binaries for multiple platforms
   make publish

This creates platform-specific binaries:

- ``msfs-linux-amd64`` - Linux x86_64
- ``msfs-linux-arm64`` - Linux ARM64

Docker Deployment
=================

Deploy MSFS using Docker containers:

.. code-block:: dockerfile
   :caption: Dockerfile for MSFS deployment

   FROM ubuntu:22.04
   
   RUN apt-get update && apt-get install -y fuse
   
   COPY msfs-linux-amd64 /usr/local/bin/msfs
   COPY mount.msfs /usr/sbin/mount.msfs
   
   RUN chmod +x /usr/local/bin/msfs /usr/sbin/mount.msfs
   
   CMD ["/usr/local/bin/msfs"]

.. code-block:: bash

   # Build container
   docker build -t msfs:latest .

   # Run with config from environment
   docker run -d \
     --device /dev/fuse \
     --cap-add SYS_ADMIN \
     --security-opt apparmor:unconfined \
     -e MSC_CONFIG=/config/msfs.yaml \
     -v /path/to/config:/config \
     -v /mnt/storage:/mnt/storage:shared \
     msfs:latest

Troubleshooting
===============

Common Issues
=============

**FUSE device not found**

.. code-block:: text

   Error: /dev/fuse: open: no such file or directory

**Solution:** Load the FUSE kernel module:

.. code-block:: bash

   sudo modprobe fuse

**Permission denied when mounting**

.. code-block:: text

   Error: fusermount: mount failed: Operation not permitted

**Solution:** Ensure your user is in the ``fuse`` group or run with ``sudo``:

.. code-block:: bash

   sudo usermod -aG fuse $USER
   # Log out and back in for group changes to take effect

**Backend not appearing after SIGHUP**

**Solution:** Check logs in ``/var/log/msfs/`` for configuration errors. Ensure new backend configurations are valid.

**Cache thrashing with many small files**

**Solution:** Decrease ``cache_line_size`` for better cache utilization:

.. code-block:: yaml

   cache_line_size: 262144  # 256 KiB instead of 1 MiB
   cache_lines: 16384       # Increase count to maintain total cache size

Debug Mode
==========

Enable verbose logging to diagnose issues:

.. code-block:: yaml

   backends:
     - dir_name: debug-backend
       trace_level: 3  # Maximum verbosity
       # ...

Check daemon logs:

.. code-block:: bash

   # If using mount helper
   tail -f /var/log/msfs/msfs_*.log

   # If running manually
   ./msfs  # Logs go to stdout

Limitations
===========

Current limitations of MSFS:

- **Read-only:** Currently only read operations are supported. Write support is planned for a future release
- **Backend modifications:** Existing backends cannot be modified via SIGHUP; only additions and removals are supported

See Also
========

- :doc:`/user_guide/quickstart` - Getting started with MSC configuration
- :doc:`/references/configuration` - Complete configuration schema
- :doc:`/user_guide/telemetry` - Observability and metrics configuration
- :doc:`/user_guide/concepts` - Core MSC concepts

