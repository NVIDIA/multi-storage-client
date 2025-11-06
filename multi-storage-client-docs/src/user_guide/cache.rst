#####
Cache
#####

The MSC provides a caching system designed to improve performance when accessing remote storage objects.

************
How It Works
************

When a developer calls ``msc.open("msc://profile/path/to/data.bin")``, MSC automatically handles caching behind the scenes 
if cache is configured. On the first access, the remote object is downloaded and written to the configured cache location, 
making all subsequent accesses significantly faster since the data is served directly from local storage.

The cache location should be configured as an absolute filesystem path, and for optimal performance, it's recommended to point it to 
high-performance storage such as NVMe drives or parallel file systems like Lustre.

To ensure data integrity in multi-process or multi-threaded environments, MSC employs filelock mechanisms that prevent 
concurrent writes to the same cached object. When multiple processes attempt to cache the same file simultaneously, an exclusive 
lock ensures only one process proceeds with the write operation while others wait, eliminating redundant downloads and potential corruption.

.. note:: If the profile uses a POSIX filesystem (e.g. ``type: file``) as the storage provider, the cache won't be used for that profile.

*************************
Basic Cache Configuration
*************************

To enable cache, you need to configure the cache location and size in the configuration file. The cache location should be 
an absolute filesystem path, and the size should be a positive integer with a unit suffix (e.g. ``"100M"``, ``"100G"``).

.. code-block:: yaml
   :caption: Example configuration to enable basic cache.

   cache:
     size: 500G
     location: /path/to/msc_cache
     check_source_version: true   # validate remote object version before serving cached copy
     cache_line_size: 64M         # size of chunks for partial file caching

``check_source_version`` replaces the older ``use_etag`` flag (still accepted
for backward-compatibility).  The default is ``true`` (MSC validates the
remote object’s current version — for example via ETag — before serving a
cached copy).  Set it to ``false`` only if you want to skip that validation
for performance reasons or when the storage backend lacks a versioning
mechanism.

For detailed configuration options, see :doc:`/references/configuration`

********************
Partial File Caching
********************

MSC supports partial file caching, which allows efficient caching of large files by downloading and storing them in smaller chunks. This is particularly useful for large files where you only need to access specific portions.

**Key Features:**

* **Chunk-based Storage**: Large files are automatically split into configurable chunks (default 64MB) and stored separately in the cache.
* **Range Request Optimization**: When reading specific byte ranges, MSC only downloads the necessary chunks, not the entire file.

**Configuration:**

The ``cache_line_size`` parameter controls the size of each chunk. This should be set based on your typical access patterns:

.. code-block:: yaml
   :caption: Example configuration for partial file caching.

   cache:
     size: 500G
     location: /path/to/msc_cache
     cache_line_size: 64M    # 64MB chunks
     check_source_version: true

**Usage:**

Partial file caching is automatically enabled when using ``client.read()`` with range:

.. code-block:: python
   :caption: Using partial file caching with range reads.

   from multistorageclient import StorageClient, StorageClientConfig
   from multistorageclient.types import Range
   
   # Create a storage client with configuration
   config = StorageClientConfig.from_file()  # or from_dict()
   client = StorageClient(config=config, profile="your_profile")
   
   # This will only download the necessary chunks for the specified range
   data = client.read("path/to/large_file.bin", 
                     byte_range=Range(offset=1024*1024, size=512*1024))  # 512KB read from 1MB offset
   
   # For full file access, use client.open() with prefetch_file parameter
   with client.open("path/to/large_file.bin", prefetch_file=False) as f:
       # This will use partial file caching instead of downloading the entire file
       data = f.read(1024*1024)  # Read 1MB

**Source Version Validation:**

The ``client.read()`` method supports source version validation through the ``check_source_version`` parameter. For most use cases, you can control source version checking entirely through the cache configuration:

.. code-block:: yaml
   :caption: Controlling source version validation via cache config.

   cache:
     check_source_version: false  # Disable validation globally for performance
     # or
     check_source_version: true   # Enable validation globally for data consistency

When using ``SourceVersionCheckMode.INHERIT`` (the default), the cache configuration setting is used. You only need to explicitly specify ``ENABLE`` or ``DISABLE`` when you want to override the cache config for specific calls.

.. code-block:: python
   :caption: Using source version validation in range reads.

   from multistorageclient.types import SourceVersionCheckMode
   
   # Use cache configuration setting (default - recommended for most cases)
   data = client.read("path/to/file.bin", 
                     byte_range=Range(offset=0, size=1024))
   # Equivalent to: check_source_version=SourceVersionCheckMode.INHERIT
   
   # Override cache config for specific calls when needed
   # Always validate source version (may require HEAD request)
   data = client.read("path/to/file.bin", 
                     byte_range=Range(offset=0, size=1024),
                     check_source_version=SourceVersionCheckMode.ENABLE)
   
   # Skip source version validation for performance
   data = client.read("path/to/file.bin", 
                     byte_range=Range(offset=0, size=1024),
                     check_source_version=SourceVersionCheckMode.DISABLE)

**Prefetch File Control:**

The ``prefetch_file`` parameter in ``client.open()`` controls whether the entire file is downloaded upfront or uses partial file caching:

.. code-block:: python
   :caption: Controlling file prefetch behavior.

   # Download entire file upfront (default behavior)
   with client.open("path/to/large_file.bin", prefetch_file=True) as f:
       data = f.read(1024*1024)  # Fast local read after download
   
   # Use partial file caching - only download chunks as needed
   with client.open("path/to/large_file.bin", prefetch_file=False) as f:
       data = f.read(1024*1024)  # Downloads only necessary chunks
       f.seek(1024*1024*100)     # Seek to different position
       more_data = f.read(1024*1024)  # Downloads additional chunks as needed

**When to use each approach:**

* **prefetch_file=True** (default): Use when you expect to read most or all of the file. The entire file is downloaded once, then all subsequent reads are served from local cache at maximum speed.

* **prefetch_file=False**: Use for large files where you only need specific portions, or when you want to start processing data immediately without waiting for the full download. This enables partial file caching with chunk-based downloads.

*************************
Cache Directory Structure
*************************

The cache directory structure is mirrored to the remote storage object hierarchy. Because the cache location is shared by all 
profiles, the MSC automatically prefixes the profile name to the cache directory to avoid conflicts.

.. code-block:: text
   :caption: Directory structure on S3.
   
   s3://bucketA/
       └── datasets/
           ├── parts_0001/
           │   ├── data-0001.tar
           │   └── data-0002.tar
           └── parts_0002/
               ├── data-0001.tar
               └── data-0002.tar

.. code-block:: text
   :caption: Directory structure in cache location (/tmp/msc_cache).
   
   /tmp/msc_cache/{profile_name}/
       └── datasets/
           ├── parts_0001/
           │   ├── data-0001.tar
           │   └── data-0002.tar
           └── parts_0002/
               ├── data-0001.tar
               └── data-0002.tar

**************
Cache Eviction
**************

The cache eviction procedure is a background thread that runs periodically to remove the cached files from the cache directory to ensure the 
cache size is within the configured limit. Since all profiles share the same cache directory, the cache eviction procedure is synchronized 
across all profiles.

.. note:: When running in a multi-process environment, only one process will run the cache eviction procedure at a time (protected by an exclusive filelock).

The cache eviction policy is configured using the ``eviction_policy`` key in the cache configuration. By default, the cache eviction policy 
is set to ``fifo`` with a refresh interval of 300 seconds.

Available eviction policies:

* **FIFO (First In First Out)**: Evicts the oldest cached files first, regardless of access patterns. This is the default policy and works well for most workloads.

* **LRU (Least Recently Used)**: Evicts files that haven't been accessed recently, keeping frequently accessed files in cache. Best for workloads with temporal locality where recent files are likely to be accessed again.

* **MRU (Most Recently Used)** (**Experimental**): Evicts the most recently accessed files first, keeping older files in cache. Useful for workloads that scan through datasets sequentially and are unlikely to re-access recently read files. Requires ``experimental_features: {cache_mru_eviction: true}`` in config.

* **RANDOM**: Randomly selects files for eviction (except the most recently added file). Provides unpredictable but fair eviction behavior.

The ``purge_factor`` parameter (**Experimental**) controls how aggressively the cache is cleaned during eviction. It specifies the percentage of maximum cache size to delete (0-100) when eviction is triggered, reducing the frequency of future eviction cycles. Requires ``experimental_features: {cache_purge_factor: true}`` in config.

* ``purge_factor = 0`` (default): Delete only what's needed to stay just under the cache limit. This provides minimal cleanup and may trigger frequent evictions.
* ``purge_factor = 20``: Delete 20% of max cache size. For a 100GB cache, this keeps 80GB after eviction, providing 20GB of free space.
* ``purge_factor = 50``: Delete 50% of max cache size. Provides substantial free space but reduces cache hit rate.
* ``purge_factor = 100``: Delete everything (clear entire cache). Useful for workloads that benefit from completely fresh cache periodically.

Use higher purge factors (20-50%) when you want to reduce eviction frequency at the cost of some cache hits. Use ``purge_factor=0`` (default) when cache hit rate is more important than eviction frequency. Consider your workload patterns: if cache fills up quickly and frequently, a moderate purge factor (20-30%) can improve performance by reducing lock contention during eviction.

.. code-block:: yaml
   :caption: Example configuration with purge_factor for aggressive cleanup (experimental).

   experimental_features:
     cache_mru_eviction: true      # Enable MRU policy
     cache_purge_factor: true      # Enable purge_factor

   cache:
     size: 500G
     location: /path/to/msc_cache
     eviction_policy:
       policy: lru
       refresh_interval: 300
       purge_factor: 20  # Delete 20% (100GB) during eviction, keeping 400GB


**************
Best Practices
**************

**Full File Caching:**

Configure MSC cache when your workload performs **frequent** small range-reads on objects. This is particularly common in:

* **ML Training Workloads**: Machine learning training often involves reading large amount of data files and selecting random samples from different parts of the file, resulting in many small range-read operations. By caching the entire object upfront, these expensive network round-trips are eliminated, and subsequent sample reads are served directly from local storage at much higher speeds.

* **Checkpoint Loading**: When loading large model checkpoints (often several GB in size), frameworks like PyTorch may perform multiple small reads to load different parts of the model. Rather than allowing these small reads to hit remote storage repeatedly, it's much more performant to download the entire checkpoint file using multi-threaded downloads to the cache first, then let PyTorch load from the local cached file.

* **Random Access Patterns**: Any workload that requires random access to different parts of large files frequently will benefit significantly from caching, as the alternative would be numerous individual range requests to remote storage.

**Partial File Caching:**

For large files where you only need to access specific portions, use partial file caching with range reads:

* **Large Dataset Access**: When working with very large datasets (hundreds of GB or TB), use ``client.read()`` with ``byte_range`` to only download the chunks you need.

* **Sparse File Access**: For files where you only read small portions scattered throughout the file, partial file caching is more efficient than downloading the entire file.

* **Streaming Workloads**: When processing large files sequentially in chunks, partial file caching allows you to process data as it's downloaded without waiting for the entire file.

**Performance Considerations:**

* **Full File Caching** (``prefetch_file=True`` with ``client.open()``):  The cache transforms what would be hundreds or thousands of small, high-latency network requests into a single bulk download followed by fast local file system access.

* **Partial File Caching** (``prefetch_file=False`` with ``client.open()``): Use for large files with sparse access patterns. Set ``cache_line_size`` based on your typical read sizes - smaller chunks (e.g., 16MB) for fine-grained access, larger chunks (e.g., 128MB) for coarser access patterns.

* **Source Version Validation**: This feature can be turned on and off via the cache config using ``check_source_version: true/false``. For fine-grained tuning, use ``SourceVersionCheckMode.DISABLE`` for maximum performance when you can tolerate potentially stale data, or when the data doesn't change (gives best performance). Use ``SourceVersionCheckMode.ENABLE`` when data consistency is critical.

* **Prefetch File Strategy**: 
  - Use ``prefetch_file=True`` for sequential reads or when you need the entire file
  - Use ``prefetch_file=False`` for random access patterns or when you want to start processing immediately
  - Combine with appropriate ``cache_line_size`` for optimal performance


***********
Limitations
***********

* **Full file caching inefficiency**: Traditional full file caching downloads entire files, which can be inefficient for large files when your workload only reads small portions at a time. For such cases, use partial file caching with ``client.read()`` and ``byte_range``, or use ``client.open(..., prefetch_file=False)`` to enable chunk-based caching.

* **Chunk size tuning**: The effectiveness of partial file caching depends on choosing an appropriate ``cache_line_size``. Too small chunks increase overhead, while too large chunks reduce the benefits of partial caching.

* **Network dependency**: Initial chunk downloads still require network access. For completely offline scenarios, ensure all required chunks are pre-cached.