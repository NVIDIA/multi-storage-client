########
Replicas
########

MSC supports *replica profiles* – secondary storage locations that mirror the
contents of a primary profile.  Replicas improve read-throughput and
availability by letting the client fetch data from whichever backend is
closest or healthiest.

Overview
********

A **replica** is simply another MSC profile that points to a different storage
bucket / container / folder but stores the *same* objects as the primary
profile.  When you configure replicas, MSC automatically:

* Looks for objects in replicas before falling back to the primary.
* If the object is **only found on the primary**, MSC copies it to any missing
  replicas *asynchronously* in the background – this is **read-through
  back-fill**, not a write-through replication.

All coordination is handled by :py:class:`multistorageclient.replica_manager.ReplicaManager` – your
application code continues to use the familiar
:py:meth:`multistorageclient.StorageClient.read` and
:py:func:`multistorageclient.open` APIs.

How reads work (fetch-on-demand)
--------------------------------

1. Your code calls :py:meth:`multistorageclient.StorageClient.read` or :py:meth:`multistorageclient.StorageClient.download_file`.
2. The replica manager iterates over replicas in *increasing priority order*
   (lowest number first) and checks
   :py:meth:`multistorageclient.StorageClient.is_file`.
3. If the file exists in a replica, it is downloaded from that replica and
   returned immediately.
4. If no replica has the file, MSC downloads from the primary provider.
5. The file is uploaded to any replicas that were missing it – this
   happens in a background thread so the caller does not block **(async upload-on-miss)**.

This strategy minimises latency on cache-misses while keeping replicas
up-to-date opportunistically.

How writes/copies work
----------------------

* :py:func:`multistorageclient.upload_file` and
  :py:meth:`multistorageclient.StorageClient.copy` still send data to the
  *primary* provider synchronously.
* After the primary write succeeds MSC schedules background uploads/copies to
  all configured replicas.
* To replicate **existing** objects proactively (e.g. migrating a dataset) use
  :py:meth:`multistorageclient.StorageClient.sync_replicas` – this performs a bulk copy from the
  primary to the selected replicas as described in the next section.

Configuration
*************

Add a ``replicas`` list to any profile that should *use* replicas.  Each entry
specifies the *profile name* of a replica and a mandatory ``read_priority``.
Lower numbers are tried first.  Example:

.. code-block:: yaml

   profiles:
     primary-s3:
       storage_provider:
         type: s3
         options:
           base_path: primary-bucket
       # enable two replicas
       replicas:
         - replica_profile: backup-gcs
           read_priority: 1   # first choice
         - replica_profile: cold-azure
           read_priority: 2   # second choice and so on...

     backup-gcs:
       storage_provider:
         type: gcs
         options:
           base_path: backup-bucket

     cold-azure:
       storage_provider:
         type: azure
         options:
           base_path: cold-container

Notes:

* Replicas can themselves point to *any* provider type (S3, GCS, Azure…).
* ``read_priority`` is **required** and must be a positive integer (``1`` = highest
  priority).  Replicas with the same priority are tried in the order they are
  listed in the configuration.
* Replica profiles can also have caching, telemetry, etc. – normal profiles.

Prefetching replicas with :py:meth:`multistorageclient.StorageClient.sync_replicas`
-----------------------------------------

While this read-through mechanism eventually brings frequently accessed
objects into replicas, you may want
to **pre-populate** a replica *before* running a workload (for example to warm
up a new cache in another region or cluster).  The helper
:py:meth:`multistorageclient.StorageClient.sync_replicas` copies objects in bulk from the primary to the
replicas ahead of time:

.. code-block:: python
   :linenos:

   from multistorageclient import StorageClient, StorageClientConfig

   # Load config and create a client for the primary profile
   config = StorageClientConfig.from_file(profile="my-test-profile")
   client = StorageClient(config=config)

   # Prefetch an entire dataset into all configured replicas
   client.sync_replicas(source_path="datasets/2024/", delete_unmatched_files=False)

This performs a **prefetch** – it traverses the given prefix and ensures every
object is present in the target replicas.  You can restrict the destination
set with the ``replica_indices`` parameter.  Transfers run in parallel (local
threads or Ray workers depending on ``execution_mode``).

After prefetching, read operations will hit the closest replica immediately,
avoiding the first-access latency and reducing load on the primary store.

Environment variables
---------------------

.. glossary::
   :sorted:

   :envvar:`MSC_REPLICA_UPLOAD_THREADS`
       Override the default background thread pool size (default: ``8``).

Best practices
--------------

* Prefer *low-latency* replicas (same region/zone) for the lowest
  ``read_priority``.
* Combine with the local cache feature to avoid repeated network transfers when
  re-reading the same objects.

Limitations
-----------

* Replicas are **eventually** consistent – background uploads can lag behind
  your writes.
* Object deletions are *not* propagated automatically. 
  Use :py:meth:`multistorageclient.StorageClient.sync_replicas` with ``delete_unmatched_files=True`` or clean up manually.


Further reading
---------------

* :doc:`/references/configuration` – full configuration reference.
* :doc:`/user_guide/cache` – local caching strategy you can layer on top of
  replicas for maximum performance.

