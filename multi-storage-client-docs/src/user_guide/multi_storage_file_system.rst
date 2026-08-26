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

   **S3 Read and Write Support**

   MSFS supports file creation, modification, truncation, deletion, and explicit
   ``fsync``/``fdatasync`` durability for writable S3 backends. The default
   ``flush_on_close: false`` mode favors close latency and completes the remote
   commit asynchronously; applications requiring an acknowledgement must call
   ``fsync``/``fdatasync`` or configure ``flush_on_close: true``.

   If an asynchronous commit fails after ``close`` has already been
   acknowledged, the failure is recorded against the file and the next
   ``fsync``/``fdatasync`` on it returns ``EIO`` once. The data is not lost —
   it stays dirty and a later flush retries it — so a successful retry clears
   the record and reports no error.

Key Features
============

- **FUSE-based:** Mounts object storage as a standard filesystem
- **S3 backend support:** AWS S3 and S3-compatible object stores
- **High-performance caching:** Configurable cache for improved read performance
- **Write-to-read cache promotion:** Optional reuse of committed write bytes for immediate readback without another object-store download
- **Dynamic configuration:** Add or remove backends without unmounting via SIGHUP
- **Standard Unix tools:** Use with ``mount``, ``umount``, and ``/etc/fstab``
- **Observability:** Integrated telemetry with OpenTelemetry metrics

Deployment Model
================

MSFS is not a centralized storage or caching service. It is a FUSE process that runs on each compute node, started either directly through ``mount -t msfs`` or by the Kubernetes CSI node plugin on behalf of a pod. Nothing is provisioned outside the compute node, so MSFS can be deployed without dedicated storage appliances. The consequence is that **every MSFS process owns an independent cache**: two mounts on one node, or the same mount on two nodes, share nothing.

Access is granted per mount rather than per user. Anyone who can read the mount point can read everything the backend credentials can reach, so deployments that need tenant isolation must separate tenants by mount.

.. list-table:: Capability Summary
   :widths: 70 30
   :header-rows: 1

   * - Capability
     - Status
   * - POSIX read access to S3, AIStore, and GCS backends
     - Supported
   * - POSIX write access (create, modify, truncate, delete)
     - S3 backends only
   * - Write-to-read cache promotion after a successful commit
     - Supported (opt-in)
   * - Multiple buckets or bucket+prefixes as sibling subdirectories of one mount
     - Supported
   * - Adding and removing backends without unmounting (SIGHUP)
     - Supported
   * - Manifest-based bootstrap for large namespaces
     - Supported
   * - Node-local read cache with read-ahead, capacity bound, and LRU eviction
     - Supported
   * - Cache backed by RAM, a shared mapped file, or per-inode files
     - Supported
   * - OpenTelemetry metrics and a Prometheus endpoint
     - Supported
   * - Kubernetes deployment via the CSI node plugin
     - Supported
   * - Cache shared or coordinated across mounts or nodes
     - Not implemented
   * - Cache surviving unmount
     - Not implemented
   * - Explicit data pre-warm API
     - Not implemented
   * - Per-user (UID/GID) authorization within a mount
     - Not implemented
   * - Modifying an existing backend in place via SIGHUP
     - Not implemented

Installation
============

Download from GitHub Actions Artifacts
======================================

The easiest way to install MSFS is to download pre-built packages from our GitHub Actions artifacts.

Download the artifact archive:

#. Navigate to the `GitHub Actions Default Branch workflow <https://github.com/NVIDIA/multi-storage-client/actions/workflows/default_branch.yml>`_
#. Select a workflow run for the desired commit
#. Download the ``multi-storage-file-system`` artifact

Extract the archive:

.. code-block:: bash
   :caption: Extract the artifact archive.

   unzip multi-storage-file-system.zip

The archive contains:

- **RPM packages**: ``msfs-<version>-1.x86_64.rpm`` and ``msfs-<version>-1.aarch64.rpm``
- **DEB packages**: ``msfs_<version>_amd64.deb`` and ``msfs_<version>_arm64.deb``

After installation, MSFS provides:

- ``/usr/bin/msfs`` - The FUSE daemon binary
- ``/usr/bin/mount.msfs`` - Mount helper for standard ``mount`` command

Build from Source
=================

Alternatively, you can build MSFS from source:

.. code-block:: bash
   :caption: Build MSFS from source.

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
- ``MSFS_BINARY`` - Path to msfs binary (default: ``/usr/bin/msfs``)
- ``MSFS_LOG_DIR`` - Log directory (default: ``/var/log/msfs``)

Usage
=====

Basic Usage
===========

Manual mount/unmount using the MSFS binary directly:

.. code-block:: bash

   # Start MSFS daemon with config file
   export MSC_CONFIG=/path/to/config.yaml
   /usr/bin/msfs

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

When you run ``mount -t msfs <config> <mountpoint>``, the ``mount`` command automatically calls ``/usr/bin/mount.msfs``, which:

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

Manifest-Based Bootstrap
========================

For large-scale datasets (millions of objects), MSFS can pre-generate a manifest of directory listings at mount time. This enables immediate POSIX access without per-file S3 calls.

Enable manifest generation by adding ``manifest_path`` to a backend:

.. code-block:: yaml

   backends:
     - dir_name: s3
       readonly: true
       manifest_path: "/home/user/.msfs_manifest"
       backend_type: S3
       S3:
         # ...

On first mount, MSFS generates per-directory TSV manifests via parallel BFS listing of S3, then ingests entries into sharded B+Trees backed by PebbleDB for persistent, memory-efficient lookups.

Writable Manifest-Backed Mounts
-------------------------------

A manifest-backed mount can also be writable. Manifest **ingest** runs at mount whenever ``manifest_path`` is set, regardless of ``readonly``, so files created, overwritten, or deleted through a previous session are reconstructed.

Manifest **generation** at mount is restricted to ``readonly: true`` backends, because generation lists the entire backend namespace and that listing is only consistent while nothing can mutate it. Generate the manifest out of band before mounting a backend writable:

.. code-block:: bash

   msfs generate-manifest -backend s3 -output /home/user/.msfs_manifest

A writable backend whose ``manifest_path`` contains no manifest logs ``skipping generation`` and mounts without manifest metadata.

The generated manifest is an immutable snapshot and is never rewritten on the write path. Instead, committed mutations are recorded as upsert and tombstone records in an append-only delta log under ``<manifest_path>/_msfs_delta/``, which lookup, readdir, and ingest overlay on top of the base manifest. Records are appended only after the object-store commit succeeds, so the delta log never advertises an object the backend does not hold.

Manifest Configuration Options
------------------------------

.. code-block:: yaml

   backends:
     - dir_name: s3
       manifest_path: "/home/user/.msfs_manifest"
       manifest_gen_workers: 200             # Number of parallel BFS listing workers (default: 200)
       flat_dir_confirmation_pages: 5        # Pages to confirm a flat dir before parallel listing (default: 5)

**Flat Directory Acceleration**

For buckets with flat layouts (millions of files in a single prefix with no subdirectories),
MSFS automatically detects large flat directories and parallelizes listing using prefix-based
or range-based splitting. User-provided hints can further optimize this:

.. code-block:: yaml

   backends:
     - dir_name: s3
       manifest_path: "/home/user/.msfs_manifest"
       flat_dir_hints:
         - path: "training-data/"
           key_prefix_chars: "0123456789"
           split_depth: 2

.. list-table:: Flat Directory Hint Fields
   :widths: 20 15 65
   :header-rows: 1

   * - Field
     - Type
     - Description
   * - ``path``
     - string
     - Directory path relative to the backend prefix (must end with ``/``)
   * - ``key_prefix_chars``
     - string
     - Characters that appear at the start of basenames (default: ``0123456789abcdefghijklmnopqrstuvwxyz``)
   * - ``split_depth``
     - int
     - Number of leading characters for sub-prefix generation (default: 1; depth 2 with 10 chars = 100 sub-workers)

Without hints, MSFS detects flat directories automatically and selects the best parallelization strategy (prefix discovery or lexicographic range splitting).

Performance
===========

MSFS includes a sophisticated caching layer to optimize read performance and
optionally reuse exact bytes retained from successful writes.

Cache Configuration
===================

The cache uses a line-based architecture where each cache line represents a fixed-size chunk of data:

.. code-block:: yaml

   cache_line_size: 1048576       # 1 MiB per cache line
   cache_lines: 4096              # 4096 cache lines = 4 GiB total cache
   write_cache_promotion: false   # Opt in to write-to-read cache reuse

**Cache Tuning Guidelines:**

- **Larger cache line size** - Better for sequential access patterns, fewer cache lines needed
- **Smaller cache line size** - Better for random access patterns, more granular caching
- **More cache lines** - Allows caching more files or larger portions of files
- **Less cache lines** - Reduces memory usage

Write-to-Read Cache Promotion
=============================

When ``write_cache_promotion: true``, MSFS admits locally retained bytes into
the read cache only after the object-store commit succeeds. Admission starts at
offset zero and stops when cache capacity is exhausted; MSFS never downloads
data merely to populate the cache. Deferred single-PUT writes and retained
multipart buffers can populate complete objects. Existing-object overlays
populate only cache lines fully covered by local write ranges.

Promotion is disabled by default. Cache population runs asynchronously through
a bounded worker pool, while an immediate read waits for any matching Inbound
cache line to become Clean. Local cache-population failure does not change the
success of the already-completed object-store write.

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

Cache Capacity and Lifetime
===========================

``cache_line_size`` (default 10 MiB) is the fetch and residency granularity and ``cache_lines`` (default 128) is how many lines are provisioned, so **the default capacity is about 1.25 GiB**. The benchmarks in :ref:`msfs-measured-scale` used ``cache_lines: 10000``, or roughly 100 GiB. Capacity is a hard bound and eviction is LRU, so a dataset larger than the cache holds only its active working set. For very large datasets, size the cache to the working set rather than to the dataset.

``cache_storage`` selects where lines live: ``ram`` (anonymous mmap), ``mapped-file`` (one shared memory-mapped file, the default), or ``per-inode-file`` (per-inode contiguous files served with ``pread``). All three are node-local.

Each mount creates its own private cache directory under ``cache_dir_path`` and removes that directory on unmount. The cache does not survive the mount, and a remount starts cold. Files left behind by a crash are not discovered or reused.

.. note::

   Pointing ``cache_dir_path`` at a shared filesystem such as Lustre places each mount's private directory on shared storage, but does not produce a shared cache. The catalog that makes cached bytes findable — the object-to-line index, cache-line state and ETags, in-flight fetch tracking, LRU order, and capacity accounting — lives in the memory of a single MSFS process. Two MSFS processes over the same shared path still use different directories, fetch the same object twice, cannot see each other's entries, and cannot coordinate fills, invalidation, or eviction. A shared filesystem guarantees consistency for shared files; it does not supply object-cache semantics such as key/version lookup, single-flight fetches, or ETag invalidation.

Pre-warming
===========

There is no pre-warm API. Reading files warms the cache of the MSFS process that served those reads, so one job can warm a mount that a later job reuses — but only if that MSFS process is still running and the working set still fits in cache. A pre-warm job that unmounts when it finishes, or a later job that creates its own mount, starts cold.

Manifest generation is a separate mechanism and warms only namespace metadata. It makes directory traversal and attribute lookups fast without per-object backend calls; it does not fetch file contents.

.. _msfs-measured-scale:

Measured Scale and Performance
==============================

These are single-node measurements of the **read path and namespace bootstrap** against same-region storage. They record what has been measured, not a supported configuration limit; write-path throughput is not characterized here. See :ref:`msfs-scale-boundary`.

Namespace Scale: 100M Objects
-----------------------------

Measured on an EC2 ``c5a.12xlarge`` (48 vCPU, 96 GiB) in ``us-west-2`` against an S3 bucket in the same region, over a dataset of 100,237,498 objects across 101,339 directories.

.. list-table:: 100M-object bootstrap
   :widths: 46 18 22 14
   :header-rows: 1

   * - Phase
     - Elapsed
     - Throughput
     - Peak RSS
   * - Manifest generation (parallel BFS listing, 200 workers)
     - 1m 45s
     - 954,680 obj/s
     -
   * - Manifest ingest (per-directory TSV into sharded B+Tree/PebbleDB)
     - 16m 41s
     - 100,129 obj/s
     - ~7.4 GiB
   * - Total bootstrap
     - ~18m 26s
     -
     -

The mount is **browsable when generation finishes**, at about 105 seconds, not when ingest finishes. During ingest, metadata is served from the manifest while the optimized index is built in the background, so traversal and enumeration — enough to compute dataset splits and begin streaming — work well before the 18m 26s mark. Generation held at about 1m 43s and ingest at about 16m 50s across repeated runs.

.. note::

   Set ``process_memory_limit`` generously for an ingest of this size. The 4 GiB default sits below the working set of a 100M-object ingest, which drives continuous garbage collection and collapses throughput.

These figures assume a hierarchical layout. Both phases degrade sharply when one directory holds the entire namespace, because generation finds a single key prefix to split across roughly 20 range workers instead of 200 directory workers, and every directory entry lands in one B+Tree shard.

.. list-table:: Directory-width sensitivity (100M objects)
   :widths: 34 24 24 18
   :header-rows: 1

   * - Layout
     - Generation
     - Ingest
     - Peak RSS
   * - 101,339 directories
     - 1m 43s (977K obj/s)
     - 16m 29s (101K obj/s)
     - ~6.5 GiB
   * - 1 directory
     - 30m 35s (~55K obj/s)
     - 44m 19s (37.8K obj/s)
     - ~21 GiB

The penalty is super-linear in directory width: 10M objects in a single directory generate in 1m 41s and ingest in 2m 5s, so the same flat shape is far cheaper an order of magnitude smaller.

Read Throughput: 24-Cell Matrix
-------------------------------

Measured on an EC2 ``c5n.18xlarge`` (72 vCPU, 184 GiB) in ``us-west-2`` against same-region S3 with a ~100 GiB cache, over a ~88 GiB dataset of 8,192 x 1 MiB plus 80 x 1 GiB files. Six workload families — 4 KiB and 64 KiB request sizes, small-file and large-file, sequential and random — at 1, 2, 4, and 8 application threads, each with a cold and a cache-resident pass, driven by ``elbencho -r --direct`` and compared against s3fs-fuse 1.93 with a local disk cache.

The result was **18 wins, 3 ties, and 3 losses** across the 24 cells.

.. list-table:: Cache-resident throughput relative to s3fs
   :widths: 60 40
   :header-rows: 1

   * - Workload family
     - MSFS vs s3fs
   * - Small files, 4 KiB sequential
     - 20x - 52x
   * - Small files, 64 KiB sequential
     - 6.5x - 27x
   * - Large files, 4 KiB sequential
     - 0.69x - 1.04x
   * - Large files, 64 KiB sequential
     - 0.45x - 1.05x
   * - Large files, 4 KiB random
     - 3.9x - 6.9x
   * - Large files, 64 KiB random
     - 5.4x - 14x

Cold reads scale with thread count because each reader issues concurrent ranged GETs: large-file 64 KiB sequential moves 92 MiB/s at one thread and 192 MiB/s at eight, while s3fs cold stays flat near 125 MiB/s on the same families. Cache-resident reads reach 4,536 MiB/s on that family at eight threads. At eight threads MSFS wins or ties every family.

Two caveats keep the losses honest. The large-file 64 KiB losses at one and two threads are largely a page-cache artifact: on a 184 GiB host, s3fs serves its warm reads from the Linux page cache over its own cache files, and after dropping caches it falls to about 148 MiB/s on the same data. The large-file 4 KiB results reflect a real per-operation FUSE ceiling — at one or two threads only one or two FUSE operations are in flight, so neither additional readers nor cache geometry help. ``--direct`` forces strict 4 KiB operations with no kernel read-ahead, a deliberately pessimistic operating point; workloads that do not use ``O_DIRECT`` benefit from page-cache assistance and reach roughly 2.6 GiB/s on the same data.

Reproducing These Numbers
-------------------------

Defaults are not benchmark settings. The values below are what the results above were measured with; a run that differs on any of them is not comparable.

.. list-table:: Benchmark configuration
   :widths: 26 24 50
   :header-rows: 1

   * - Setting
     - Value
     - Why
   * - ``fuse_fd_per_worker``
     - ``false``
     - Shared ``/dev/fuse`` descriptor. Cloned per-worker descriptors measured 18-25% slower.
   * - ``fuse_workers``
     - ``50``
     - On a 72-vCPU host. The default of ``0`` uses ``runtime.NumCPU()``, which is too many readers on large hosts.
   * - ``cache_line_size``
     - ``10485760``
     - 10 MiB fetch and residency granularity.
   * - ``cache_lines``
     - sized to the working set
     - ``10000`` held the whole 88 GiB dataset.
   * - ``cache_lines_to_prefetch``
     - ``4``
     - Read-ahead depth. Higher values can help backends with a different latency knee.
   * - ``process_memory_limit``
     - ``68719476736``
     - The 4 GiB default causes sustained garbage collection on large ingests.
   * - ``GOMAXPROCS``
     - unset on a 72-vCPU host
     - Left unset so Go uses every vCPU. On a 256-vCPU host, pinning it to 72 matched the 72-vCPU result; leaving it at 32 throttled the read and GC path.

Two settings live outside the configuration file and must be re-applied after every mount, because the FUSE connection id changes each time:

.. code-block:: bash
   :caption: Post-mount tuning, required after every mount

   ulimit -n 131072

   sudo sh -c 'for d in /sys/fs/fuse/connections/*/; do
       echo 144 > "${d}max_background"
       echo 108 > "${d}congestion_threshold"
   done'

The kernel defaults, ``max_background=12`` and ``congestion_threshold=9``, throttle in-flight background and read-ahead requests and cap read concurrency well below what the thread count suggests. Leaving them at their defaults is the most common reason a run appears to stop scaling after a few threads.

.. _msfs-scale-boundary:

Qualified Scale Boundary
========================

What the measurements above establish, and what they do not:

.. list-table:: Scale boundary
   :widths: 22 34 44
   :header-rows: 1

   * - Dimension
     - Measured
     - Not established
   * - Clients
     - 1 MSFS process per test
     - Many concurrent clients against one backend, including request fan-out and cache-hit behavior under aggregate load
   * - Application threads
     - 1 - 8
     - Higher concurrency per mount
   * - Objects
     - ~100M in one namespace
     - Substantially larger namespaces
   * - Working set
     - ~88 GiB, cache-resident
     - Datasets far larger than cache, where only a small fraction is resident
   * - Latency
     - Mean throughput per cell
     - Tail-latency targets
   * - Failure handling
     - Clean runs
     - Backend failure and recovery under load
   * - Access pattern
     - Read path and namespace bootstrap
     - Write throughput and write durability at scale

The companion run in which the dataset deliberately exceeded cache capacity, so that reads had to keep returning to the backend, stopped after 18 of 24 cases and was never completed. Sustained-eviction behavior is therefore not characterized.

Authentication
==============

Direct mounts take credentials from the configuration file, or from the standard AWS configuration and credentials files through ``use_config_env`` and ``use_credentials_env``. Environment variable references such as ``${AWS_ACCESS_KEY_ID}`` keep literal secrets out of the configuration file.

Under Kubernetes, the CSI node plugin additionally supports a static Secret referenced by ``nodePublishSecretRef``, a driver-level workload identity (IRSA on EKS), and a per-workload role assumed from ``volumeAttributes.roleArn``.

.. note::

   Credential rotation and multi-tenant isolation have not been qualified end-to-end. Because authorization is per mount rather than per user, a mount exposes everything its credentials can reach to every reader of the mount point.

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

   COPY msfs-linux-amd64 /usr/bin/msfs
   COPY mount.msfs /usr/bin/mount.msfs

   RUN chmod +x /usr/bin/msfs /usr/bin/mount.msfs

   CMD ["/usr/bin/msfs"]

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

- **Writes are S3-only:** Writable mounts are supported for S3 backends; GCS and AIStore backends remain read-only
- **Backend modifications:** Existing backends cannot be modified via SIGHUP; only additions and removals are supported
- **Node-local cache:** Each MSFS process owns an independent cache. It is not shared or coordinated across mounts or nodes, even when ``cache_dir_path`` points at a shared filesystem
- **Cache lifetime:** The cache is discarded on unmount; a remount starts cold
- **No pre-warm API:** Data can only be warmed by reading it through a mount that stays running
- **Mount-level authorization:** Access is granted per mount, not per UID/GID
- **Qualified scale:** Measurements cover a single client at 1-8 threads; see :ref:`msfs-scale-boundary`

Use-Case Suggestions
====================

Fronting a Remote Bucket with a Fast Tier (AIStore)
------------------------------------------------------------

MSFS caching is node-local and bounded, so the cost of a first touch is paid per node and again after any remount. Where that cost dominates — many nodes reading the same dataset once, or working sets too large to stay cache-resident — a shared cache tier in front of the remote bucket can absorb it. MSFS does not implement such a tier, but it can read one as a backend.

We measured this with AIStore. The 24-cell read matrix was re-run with MSFS pointed at an AIStore cluster (3 proxies, 3 targets) fronting the same S3 bucket, with the 88 GiB dataset prefetched into the cluster, from a 256 vCPU / 1.5 TiB client:

- **First-touch reads were 2.2x to 37x faster** than MSFS reading S3 directly, peaking near 1.7 GiB/s. The gain was largest on small files (23x - 37x at 64 KiB, 8.7x - 19x at 4 KiB) and smallest on large files (2.2x - 6.9x), because a first touch lands on in-datacenter targets instead of crossing the network to the remote bucket.
- **Cache-resident reads were unchanged**, which is the expected result: ``backend_read_file_successes_total`` stayed flat across the cache-resident pass on all 24 cells, so those reads never reached any backend. Cache-resident throughput is a property of the MSFS cache and the host, not of what sits behind it. A fast tier improves the first touch, not the cached steady state.
- The 100M-object namespace on the same cluster generated in 1m 27.7s and ingested in about 3m 19s, but only with listing delegated to the underlying store through ``manifest_gen_backend``. Listing the fronted bucket through AIStore timed out at this scale, while listing the store directly did not. Object reads still went through AIStore.

The practical reading: a fast tier is worth evaluating when first-touch cost dominates, and ``manifest_gen_backend`` should point at whichever backend lists the namespace fastest, which is not necessarily the one serving reads. This was one client at 1-8 threads and does not establish behavior for many concurrent clients; see :ref:`msfs-scale-boundary`.

See Also
========

- :doc:`/user_guide/quickstart` - Getting started with MSC configuration
- :doc:`/references/configuration` - Complete configuration schema
- :doc:`/user_guide/telemetry` - Observability and metrics configuration
- :doc:`/user_guide/concepts` - Core MSC concepts

