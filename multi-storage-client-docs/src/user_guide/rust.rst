###########
Rust Client
###########

The MSC Rust Client provides enhanced performance for storage operations by leveraging Rust.

********
Overview
********

Due to Python's Global Interpreter Lock (GIL), achieving optimal multi-threading performance within a single Python process can be challenging, especially for I/O-intensive storage operations.
The MSC Rust Client addresses this limitation by implementing critical storage operations in Rust, which can run concurrently without GIL restrictions.

For S3 providers with Rust enabled, MSC creates the boto3 client only when an
operation needs it. Object metadata lookups and file reads use Rust, preserving
user metadata and symbolic links without initializing boto3. Directory checks and
missing-file probes also use Rust. Ordinary listings, copies, deletes, and other
Python fallback operations, including large downloads into file-like objects, can still
initialize boto3. The first such operation pays the client initialization cost;
subsequent operations reuse that client. S3 providers without Rust enabled still
initialize boto3 immediately.

When a profile explicitly sets ``verify``, object metadata and directory checks
continue using boto3 to preserve that verification policy. Metadata lookups for
non-S3 providers retain their existing implementation.
Keys that Rust cannot represent unchanged, including empty, ``.`` and ``..``
path segments, also use boto3 for metadata and directory probes. Directory probes
also fall back if Rust cannot represent a child key returned by the listing.
Probes for a bucket different from the Rust client's configured bucket use boto3
so the request continues targeting the requested bucket.
S3 Express directory buckets and JWT-authenticated AIStore S3 providers also keep
metadata and directory probes on boto3 to preserve their endpoint and authentication
requirements.


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

The Rust client's ``read_timeout`` limits how long an individual read can wait for
data and resets after each successful read. Its default is 60 seconds, matching
boto3; an explicit storage provider ``read_timeout`` is inherited by the Rust client.
Each HTTP request also has a separate total timeout of 600 seconds, including
connection establishment and response body transfer. For multipart downloads, this
total timeout applies to each part request, rather than the complete file.

The retry ``timeout`` (180 seconds by default) controls how long request retries
remain eligible. It is separate from both HTTP timeouts. Batch transfers retry files
that fail with a retryable Rust error according to the MSC retry configuration.
Single-file uploads and downloads also translate retryable Rust failures into MSC
retryable errors, including transfers below the multipart threshold. Missing
objects and denied access raise ``FileNotFoundError`` and ``PermissionError``.
For GCS, permission failures during metadata lookup also raise ``PermissionError``.

Batch downloads follow symbolic links and use the target object's size to select
single-part or multipart transfer, matching individual downloads.

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
* :py:class:`multistorageclient.types.StorageProvider.glob`
* :py:class:`multistorageclient.types.StorageProvider.is_file`

For ``put_object()`` and ``upload_file()``, user-defined ``attributes`` are uploaded by the Rust client
as object metadata when the Rust path is otherwise supported. Operations requiring features such as
conditional writes or a custom ``Content-Type`` still use the Python implementation. MSC passes
metadata to the selected upload implementation without changing keys or values or switching paths
based on metadata content. HTTP-backed uploads may lowercase metadata keys or reject values according
to the underlying client and service. If Rust cannot construct a metadata header, it reports the
offending metadata key instead of panicking. A direct ``RustClient`` call raises ``ValueError``;
provider calls include the cause in their error message.


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


*******************
Resumable Downloads
*******************

Rust multipart downloads to local files resume automatically after interruption.
Retry the same destination against the same backend and bucket to reuse completed
chunks when the source object is unchanged. For ``/data/model.bin``, progress is
saved in ``/data/.model.bin.msc-dl/``. No additional configuration is needed.

After downloading all chunks, MSC merges them, atomically replaces the destination,
and removes the state directory. An existing destination remains intact until
publication. Each merge runs in one blocking task with an 8 MiB buffered reader
to reduce filesystem I/O overhead. Merging requires approximately twice the object
size in staging space, plus any existing destination. To discard progress, stop the
download and remove its state directory.

Resume requires a usable ETag or object version; changed objects start fresh.
Missing state metadata restarts the download in the same directory; invalid or
unreadable metadata reports an error and preserves its contents. Disposable cache
and append staging is cleaned up even on failure and is not reused by later calls.

Automatic sync excludes reserved staging paths; explicit ``source_files`` can
select them for copying. See :doc:`/user_guide/cli` for the sync exclusion rules.
