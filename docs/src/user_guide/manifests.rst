#########
Manifests
#########

A manifest is a file (or group of files) describing the objects in a dataset, such as names, sizes, last-modified timestamps, and custom metadata tags. Manifests are optional but can greatly accelerate object listing and metadata retrieval for large datasets in object stores.

A common approach is to prepare a manifest that includes metadata (e.g. object/file paths, sizes, custom tags) to speed up data loading and parallel processing of very large datasets. By reading a manifest, MSC can quickly discover (list) or filter (glob) objects without having to iterate over every object in the bucket or prefix.

***************
Manifest Format
***************

The MSC supports a **manifest index** (JSON) that references one or more **parts manifests**. The main manifest or manifest index:

* Declares a version.
* Lists each part manifest, including its path.
* Specifies the format of the manifest parts (JSONL or Parquet).

The parts manifests can be stored in either:

* **JSON Lines (JSONL)** format (``.jsonl``): Each line is a separate object's metadata. JSONL is more scalable than a single JSON array for large manifests because each line can be processed incrementally, avoiding excessive memory usage.
* **Parquet** format (``.parquet``): A columnar storage format that provides efficient compression and faster read performance for large datasets. Requires the ``pyarrow`` package.

.. code-block:: json
   :caption: Example Main Manifest (JSON)

   {
     "version": "1.0",
     "format": "jsonl",
     "parts": [
       {
         "path": "parts/msc_manifest_part000001.jsonl"
       },
       {
         "path": "parts/msc_manifest_part000002.jsonl"
       }
     ]
   }

.. code-block:: json
   :caption: Example Parts Manifest (JSONL)

   {
     "key": "train/cat-pic001.jpg",
     "size_bytes": 1048576,
     "last_modified": "2024-09-05T15:45:00Z"
   }
   {
     "key": "train/cat-pic002.jpg",
     "size_bytes": 2097152,
     "last_modified": "2024-09-05T15:46:00Z"
   }

Manifest Storage Organization
=============================

This example demonstrates how manifests are organized. Here, we assume that manifests are stored alongside the data in the same bucket. However, this is not strictly required, as MSC also supports placing manifests in a different location.

.. code-block:: text

   s3://bucketA/
       └── .msc_manifests/
           ├── 2024-09-06T14:55:29Z/
           │   ├── msc_manifest_index.json                   # Main manifest file
           │   └── parts/
           │       ├── msc_manifest_part000001.jsonl         # Split part of the manifest
           │       ├── msc_manifest_part000002.jsonl
           │       └── msc_manifest_part000003.jsonl
           └── 2024-10-01T10:21:42Z/                         # New version of the manifest
               ├── msc_manifest_index.json
               └── parts/
                   ├── msc_manifest_part000001.jsonl
                   ├── msc_manifest_part000002.jsonl
                   └── msc_manifest_part000003.jsonl

Writing and Using Manifests Programmatically
============================================

MSC provides a :py:class:`multistorageclient.providers.ManifestMetadataProvider` to read from and write to manifests, and a :py:class:`multistorageclient.generators.ManifestMetadataGenerator` to generate the manifests. When manifests are configured as a “metadata provider,” MSC can utilize them for efficient object metadata retrieval.

Generating Manifests
--------------------

Using the :py:class:`multistorageclient.generators.ManifestMetadataGenerator` is straightforward. For example:

.. code-block:: python
   :linenos:

   from multistorageclient import StorageClient
   from multistorageclient.generators import ManifestMetadataGenerator
   from multistorageclient.providers.manifest_formats import ManifestFormat

   # Suppose we have two clients:
   # data_storage_client: Reads the data files we want to include in the manifest.
   # manifest_storage_client: Writes the manifest to the desired path (bucket/folder).

   # Generate a JSONL manifest (default)
   ManifestMetadataGenerator.generate_and_write_manifest(
       data_storage_client=data_storage_client,
       manifest_storage_client=manifest_storage_client
   )

   # Generate a Parquet manifest (requires pyarrow)
   ManifestMetadataGenerator.generate_and_write_manifest(
       data_storage_client=data_storage_client,
       manifest_storage_client=manifest_storage_client,
       manifest_format=ManifestFormat.PARQUET
   )


To use Parquet format, install the ``pyarrow`` package:

.. code-block:: bash

   pip install multi-storage-client[parquet]

Referencing Manifests in Configuration
--------------------------------------

When you set a profile's ``metadata_provider`` to ``type: manifest``, you must also provide the ``manifest_path`` option, which refers to manifest path relative to the storage profile's ``base_path``.

You can also specify the ``format`` option to control the format used when writing new manifests (defaults to ``jsonl``):

.. code-block:: yaml
   :linenos:

   profiles:
     my-profile:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
       metadata_provider:
         type: manifest
         options:
           manifest_path: .msc_manifests
           format: parquet  # Optional: jsonl (default) or parquet

You can also store manifests in a **different** profile than your data. In that case, the ``metadata_provider`` will refer to storage profile using the ``storage_provider_profile`` option. Here's an example:

.. code-block:: yaml
   :linenos:

   profiles:
     my-manifest-profile:
       storage_provider:
         type: s3
         options:
           base_path: manifest-bucket

     my-profile:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
         metadata_provider:
           type: manifest
           options:
             # Refer to the storage profile for the manifests
             storage_provider_profile: my-manifest-profile
             # The real path of manifests in this will be manifest-bucket/.msc_manifests
             manifest_path: .msc_manifests

Once configured, MSC automatically uses the manifests to speed up listing or retrieving metadata for objects whenever you perform MSC operations on that profile.
