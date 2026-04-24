###########################
Checksum Support Reference
###########################

This page summarizes the end-to-end data-integrity guarantees MSC provides for
each supported storage backend. *Integrity* here means that a checksum is
computed on one side of the wire and verified on the other:

- On **upload**, the client computes a checksum, sends it alongside the object,
  and the server rejects the request if the received bytes do not match.
- On **download**, the server returns a checksum with the object, and the
  client rejects the response if the bytes it read do not match.

*******
Summary
*******

.. list-table::
   :header-rows: 1
   :widths: 14 14 14 34

   * - Provider
     - Upload integrity
     - Download integrity
     - Notes
   * - ``s3``
     - Yes
     - Yes
     - Default upload algorithm is ``CRC32``. Full GETs validate when the server returns a checksum; ranged GETs are not validated.
   * - ``s8k``
     - Yes
     - No
     - Default upload algorithm is ``CRC32``. SwiftStack does not return checksum headers on GET / HEAD.
   * - ``gcs``
     - Yes
     - Yes
     - SDK ``auto`` uses ``CRC32C``, falling back to ``MD5``. Ranged GETs skip checksum validation by SDK design.
   * - ``gcs_s3``
     - No
     - No
     - Flexible checksum support is disabled in MSC because GCS's S3-compatible API does not support S3-style checksum headers.
   * - ``ais``
     - No
     - No
     - No transfer checksum controls in the AIStore Python SDK.
   * - ``ais_s3``
     - No
     - No
     - AIStore S3 silently ignores checksum headers and does not validate or persist them.
   * - ``azure``
     - Yes
     - Yes
     - ``MD5`` only. Keep download ranges / ``io_chunksize`` at ``4 MiB`` or less when ``validate_content`` is enabled.
   * - ``oci``
     - No
     - No
     - SDK supports opt-in checksum parameters, but MSC does not pass them.
   * - ``huggingface``
     - Yes
     - Partial (size check)
     - Uploads use ``SHA256`` via Git LFS verify.
   * - ``file``
     - N/A
     - N/A
     - Integrity is delegated to the operating system / filesystem.
   * - Rust client (``s3`` / ``s8k``)
     - Partial
     - No
     - Upload only; ``SHA256`` only.

*************
How to Enable
*************

``s3``
======

Enabled by default. See :py:class:`S3StorageProvider <multistorageclient.providers.s3.S3StorageProvider>` for supported checksum algorithms.

.. code-block:: yaml

   profiles:
     my-aws-s3:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
           region_name: us-east-1
           checksum_algorithm: SHA256

``s8k``
=======

Enabled by default. See :py:class:`S8KStorageProvider <multistorageclient.providers.s8k.S8KStorageProvider>` for supported checksum algorithms.

.. code-block:: yaml

   profiles:
     my-swiftstack:
       storage_provider:
         type: s8k
         options:
           base_path: my-bucket
           endpoint_url: https://swiftstack.example.com
           checksum_algorithm: SHA256

``gcs``
=======

Enabled by default. There is no MSC checksum configuration option for the
native GCS provider.

``azure``
=========

Set the
:py:class:`AzureBlobStorageProvider <multistorageclient.providers.azure.AzureBlobStorageProvider>`
``validate_content`` option to ``true``. Keep download ranges and
``io_chunksize`` at ``4 MiB`` or less.

.. code-block:: yaml

   profiles:
     my-azure:
       storage_provider:
         type: azure
         options:
           base_path: my-container
           endpoint_url: https://my-storage-account.blob.core.windows.net
           validate_content: true
           io_chunksize: 4194304


``rust client``
===============

For the Rust client on ``s3`` and ``s8k``, set
``rust_client.checksum_algorithm: SHA256``.

.. code-block:: yaml

   profiles:
     my-aws-s3-rust:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
           region_name: us-east-1
           rust_client:
             checksum_algorithm: SHA256
