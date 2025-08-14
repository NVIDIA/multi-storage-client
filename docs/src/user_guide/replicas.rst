########
Replicas
########

MSC replicas provide automatic data mirroring across multiple storage backends for improved performance and availability. 
When you configure replicas, MSC reads from replicas based on configured priority and provides mechanisms to populate 
replicas from the source without requiring code changes.

*************
Configuration
*************

To enable replicas, add a ``replicas`` list to your profile configuration. Each replica entry specifies a profile name 
and a required ``read_priority`` (lower numbers = higher priority).

.. code-block:: yaml
   :caption: Configuration example with source and replicas.
   :linenos:

   profiles:
     my-dataset:
       storage_provider:
         type: s3
         options:
           base_path: my-dataset-bucket
       # Configure two replicas
       replicas:
         - replica_profile: my-dataset-s3-express
           read_priority: 1   # First choice
         - replica_profile: my-dataset-lustre
           read_priority: 2   # Second choice and so on...

     my-dataset-s3-express:
       storage_provider:
         type: s3
         options:
           base_path: my-dataset-bucket--x-s3

     my-dataset-lustre:
       storage_provider:
         type: file
         options:
           base_path: /lustre/datasets/my-dataset

In the above example, the ``my-dataset`` profile is the source, and the ``my-dataset-s3-express`` and ``my-dataset-lustre`` 
profiles are replicas. When you use the ``my-dataset`` profile, MSC will read from the ``my-dataset-s3-express`` profile first, 
and if that is not available, it will read from the ``my-dataset-lustre`` profile. If both replicas are unavailable, MSC will 
fall back and read from the source profile ``my-dataset``.

.. note::

  The ``read_priority`` is **required** and must be a positive integer (``1`` = highest priority), with replicas of the 
  same priority being tried in the order they are listed in the configuration.

.. _mirror-data-to-replicas:

***********************
Mirror Data to Replicas
***********************

There are two ways to mirror data to replicas: using the command-line interface (CLI) or Python code.

**Command Line Interface**

The easiest way to mirror data is using the MSC CLI. See :ref:`msc-sync-replicas-cli` for detailed information about the command.

**Python Code**

To populate replicas from the source using Python, you can use the :py:meth:`multistorageclient.StorageClient.sync_replicas` method:

.. code-block:: python
   :caption: Mirror data to replicas using Python.
   :linenos:

   from multistorageclient import StorageClient, StorageClientConfig

   # Initialize the client
   client = StorageClient(StorageClientConfig.from_file(profile="my-dataset"))

   # Mirror data from source to replica
   client.sync_replicas(source_path="", num_worker_processes=8)

The ``sync_replicas`` will spawn a number of worker processes to copy data from the source to the replicas. By default, it uses the local mode 
that runs the worker processes on the same machine as the client. You can also use the Ray mode to run the worker processes on a Ray cluster to 
take advantage of the distributed computing capabilities of Ray.

.. code-block:: python
   :caption: Mirror data to replicas using Ray.
   :linenos:

   from multistorageclient import StorageClient, StorageClientConfig
   from multistorageclient.types import ExecutionMode

   import ray

   # Connect to the Ray cluster
   ray.init(address="auto")

   # Initialize the client
   client = StorageClient(StorageClientConfig.from_file(profile="my-dataset"))

   # Mirror data from source to replica
   client.sync_replicas(source_path="", execution_mode=ExecutionMode.RAY)

   # Shutdown the Ray cluster
   ray.shutdown()

******************
Read from Replicas
******************

When you read from the source, MSC will automatically read from the replicas based on the configured priority.

.. code-block:: python
   :caption: Read from replicas using Python.
   :linenos:

   from multistorageclient import StorageClient, StorageClientConfig

   # Initialize the client
   client = StorageClient(StorageClientConfig.from_file(profile="my-dataset"))

   # Read object from the replicas
   # It will read from the replicas based on the read priority, 
   # so first from the my-dataset-s3-express profile, then from 
   # the my-dataset-lustre profile.
   client.read("files/my-file.txt")

   # Supported methods:
   # client.download_file("files/my-file.txt", "/local/path/to/my-file.txt")
   # client.copy("files/my-file.txt", "files/my-file-copy.txt")
   # client.open("files/my-file.txt", mode="rb")

If replicas are not populated (i.e., the data doesn't exist in the replica storage), MSC will automatically 
fall back to the source profile. This ensures that your application continues to work even if the replica 
synchronization hasn't been completed or if some replicas are unavailable.

The fallback mechanism works seamlessly in the background. When you attempt to read from a replica that 
doesn't contain the requested data, MSC will automatically try the next replica in priority order, and if 
all replicas fail, it will ultimately read from the source profile.

Additionally, MSC implements an **async upload-on-miss** strategy. When a read operation misses on any 
replica, MSC automatically uploads the object to replicas that are missing the data. This happens in 
background threads, so the caller doesn't block.

This provides a robust, fault-tolerant system where your applications can continue operating normally 
regardless of the replica population status, while keeping replicas up-to-date opportunistically.

**************
Best Practices
**************

**Latency-Sensitive Workloads**

If your workload is latency-sensitive and the data source is geographically distant, choose a replica 
that is close to your compute resources. For optimal performance, prefer a parallel filesystem such as 
Lustre, which provides high-throughput, low-latency access for compute-intensive applications.

**Network Throughput Optimization**

To maximize network throughput when copying data from object storage to local shared filesystems or 
to other object storage systems, consider using the :ref:`rust-client-reference` Rust client in MSC. 
The Rust client offers significant performance improvements over Python clients such as boto3, making it 
ideal for large-scale data transfer operations.

**Replica Synchronization Strategy**

Populate replicas first using :ref:`mirror-data-to-replicas` before relying on the **async upload-on-miss** mechanism. 
While async upload-on-miss provides automatic fallback, it is not optimal compared to :ref:`mirror-data-to-replicas`, 
which leverages multiprocessing for significantly better performance during **bulk data transfers**.
