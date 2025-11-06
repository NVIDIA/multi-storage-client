###########
Rust Client
###########

The MSC Rust Client is an experimental feature that provides enhanced performance for storage operations by leveraging Rust.

.. warning::
   The Rust Client is an experimental feature starting from v0.24 and is subject to change in future releases.

********
Overview
********

Due to Python's Global Interpreter Lock (GIL), achieving optimal multi-threading performance within a single Python process can be challenging, especially for I/O-intensive storage operations.
The MSC Rust Client addresses this limitation by implementing critical storage operations in Rust, which can run concurrently without GIL restrictions.


***************************
Supported Storage Providers
***************************

Currently, the Rust Client supports the following storage providers:

* **s3**: AWS S3 and S3-compatible storage
* **s8k**: SwiftStack storage
* **gcs_s3**: Google Cloud Storage via S3 interface
* **gcs**: Google Cloud Storage (native)

*************
Configuration
*************

To enable the Rust Client, simply add the ``rust_client`` option to your current storage provider configuration.
The Rust client shares the same configuration parameters as the Python client, including ``base_path``, ``region_name``, ``credentials_provider``, etc.

.. code-block:: yaml
   :caption: Basic S3 configuration with Rust Client.

   profiles:
     my-profile:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
           region_name: us-east-1
           rust_client: {}

Rust Client supports different chunk sizes and concurrency levels than the Python client, for the multipart upload and download operations.
You can configure the chunk size and concurrency level for the Rust client independently.

.. code-block:: yaml
   :caption: Advanced configuration with both Python and Rust client settings.

   profiles:
     my-profile:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
           region_name: us-east-1
           # Python client settings
           multipart_threshold: 16777216    # 16MiB
           multipart_chunksize: 4194304     # 4MiB
           io_chunksize: 4194304           # 4MiB
           max_concurrency: 8
           # Rust client settings
           rust_client:
             multipart_chunksize: 2097152   # 2MiB
             max_concurrency: 16

For more details of configuration options, please refer to :ref:`rust-client-reference`.

********************
Supported Operations
********************

When the Rust Client is enabled, it automatically replaces Python implementations for the following storage provider operations:

**Core I/O Operations:**

* :py:class:`multistorageclient.types.StorageProvider.put_object`
* :py:class:`multistorageclient.types.StorageProvider.get_object`
* :py:class:`multistorageclient.types.StorageProvider.upload_file`
* :py:class:`multistorageclient.types.StorageProvider.download_file`

These operations cover most of the I/O operations when using MSC, regardless of how you interact with MSC (:ref:`operations`). You can continue using your existing code without any changes.

**Operations that continue using Python implementation:**

* :py:class:`multistorageclient.types.StorageProvider.list_objects`
* :py:class:`multistorageclient.types.StorageProvider.copy_object`
* :py:class:`multistorageclient.types.StorageProvider.delete_object`
* :py:class:`multistorageclient.types.StorageProvider.get_object_metadata`
* :py:class:`multistorageclient.types.StorageProvider.glob`
* :py:class:`multistorageclient.types.StorageProvider.is_file`

.. note::
   For `put_object()` and `upload_file()` operations, if `attributes` are provided, the Rust client will not be used and will fall back to the Python implementation.


********************
Performance Benefits
********************

The Rust Client is particularly beneficial for multi-threaded operations. We tested the performance of the Rust client in the following setup:

* **VM**: AWS EC2 instance with 72 vCPUs
* **Object Storage**: AWS S3 bucket in same region
* **Test Files**: 1000 files and 64MB each

**Write Throughput (MB/s)**

.. table:: 
   :align: left

   +----------+----------+---------------+------------+
   | Threads  | fsspec   | MSC (Python)  | MSC (Rust) |
   +==========+==========+===============+============+
   | 1        | 95.4     | 95.3          | 77.6       |
   +----------+----------+---------------+------------+
   | 32       | 522.0    | 982.2         | 2,210.0    |
   +----------+----------+---------------+------------+
   | 128      | 554.4    | 976.9         | 5,440.0    |
   +----------+----------+---------------+------------+

**Read Throughput (MB/s)**

.. table:: 
   :align: left

   +----------+----------+---------------+------------+
   | Threads  | fsspec   | MSC (Python)  | MSC (Rust) |
   +==========+==========+===============+============+
   | 1        | 60.8     | 51.9          | 94.2       |
   +----------+----------+---------------+------------+
   | 32       | 376.6    | 604.2         | 2,530.0    |
   +----------+----------+---------------+------------+
   | 128      | 367.5    | 395.2         | 4,990.0    |
   +----------+----------+---------------+------------+

**Upload File Throughput (MB/s)**

.. table:: 
   :align: left

   +----------+----------+---------------+------------+
   | Threads  | fsspec   | MSC (Python)  | MSC (Rust) |
   +==========+==========+===============+============+
   | 1        | 90.4     | 90.9          | 70.8       |
   +----------+----------+---------------+------------+
   | 32       | 402.8    | 925.0         | 1,784.0    |
   +----------+----------+---------------+------------+
   | 128      | 420.4    | 894.1         | 2,827.0    |
   +----------+----------+---------------+------------+

**Download File Throughput (MB/s)**

.. table:: 
   :align: left

   +----------+----------+---------------+------------+
   | Threads  | fsspec   | MSC (Python)  | MSC (Rust) |
   +==========+==========+===============+============+
   | 1        | 68.2     | 54.3          | 55.7       |
   +----------+----------+---------------+------------+
   | 32       | 398.9    | 539.6         | 1,536.0    |
   +----------+----------+---------------+------------+
   | 128      | 411.9    | 404.0         | 3,189.0    |
   +----------+----------+---------------+------------+

.. note::
   ``Write``, ``Read`` tests are uploading/downloading from/to memory, whereas ``Upload File``, ``Download File`` tests are uploading/downloading from/to local file system.
   
   A ramdisk was used for ``Upload File``, ``Download File`` tests to mitigate local storage I/O bottlenecks.

The results demonstrate that with the Rust Client, MSC performance scales almost linearly with the number of threads, which indicates that the GIL was the bottleneck for the Python implementation.

By leveraging native CSP SDKs, MSC(Python) delivers up to 2x better performance than fsspec.

The Rust client for MSC, on top of that, provides an additional up to 12x performance improvement.
