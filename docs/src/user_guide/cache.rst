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

``check_source_version`` replaces the older ``use_etag`` flag (still accepted
for backward-compatibility).  The default is ``true`` (MSC validates the
remote object’s current version — for example via ETag — before serving a
cached copy).  Set it to ``false`` only if you want to skip that validation
for performance reasons or when the storage backend lacks a versioning
mechanism.

For detailed configuration options, see :doc:`/references/configuration`

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

.. code-block:: yaml
   :caption: Example configuration to override the default cache eviction.

   cache:
     size: 500G
     location: /path/to/msc_cache
     eviction_policy:
       policy: random
       refresh_interval: 300


**************
Best Practices
**************

Configure MSC cache when your workload performs **frequent** small range-reads on objects. This is particularly common in:

* **ML Training Workloads**: Machine learning training often involves reading large amount of data files and selecting random samples from different parts of the file, resulting in many small range-read operations. By caching the entire object upfront, these expensive network round-trips are eliminated, and subsequent sample reads are served directly from local storage at much higher speeds.

* **Checkpoint Loading**: When loading large model checkpoints (often several GB in size), frameworks like PyTorch may perform multiple small reads to load different parts of the model. Rather than allowing these small reads to hit remote storage repeatedly, it's much more performant to download the entire checkpoint file using multi-threaded downloads to the cache first, then let PyTorch load from the local cached file.

* **Random Access Patterns**: Any workload that requires random access to different parts of large files frequently will benefit significantly from caching, as the alternative would be numerous individual range requests to remote storage.

The cache transforms what would be hundreds or thousands of small, high-latency network requests into a single bulk download followed by fast local file system access.

.. note:: Cache the file if you anticipate reading more than 50% of it, as this threshold ensures that the benefits of local access outweigh the cost of downloading the entire file upfront.


***********
Limitations
***********

* **Inefficient for large files with sparse access**: The cache always downloads entire files, making it inefficient for large files when your workload only reads small portions at a time. For such cases, you can disable the read cache for specific files using ``msc.open(..., disable_read_cache=True)``.