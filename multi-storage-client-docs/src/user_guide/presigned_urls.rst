##############
Presigned URLs
##############

Presigned URLs are time-limited URLs that grant temporary access to private objects without requiring the
requester to hold credentials. MSC can generate presigned URLs using the same credentials it already manages
for storage operations.

Usage
=====

The simplest way to generate a presigned URL is via the ``msc.generate_presigned_url()`` shortcut:

.. code-block:: python

   import multistorageclient as msc

   url = msc.generate_presigned_url("msc://my-s3-profile/datasets/model.bin")
   # => https://my-bucket.s3.us-east-1.amazonaws.com/datasets/model.bin?X-Amz-Algorithm=...

You can also call ``generate_presigned_url`` directly on a :py:class:`~multistorageclient.StorageClient`:

.. code-block:: python

   from multistorageclient import StorageClient, StorageClientConfig

   client = StorageClient(StorageClientConfig.from_file("my-s3-profile"))
   url = client.generate_presigned_url("datasets/model.bin")

To generate a URL that allows the recipient to **upload** an object, pass ``method="PUT"``:

.. code-block:: python

   url = msc.generate_presigned_url("msc://my-s3-profile/uploads/data.tar", method="PUT")

Signer Types
============

The ``signer_type`` parameter selects which signing backend to use. When omitted (or ``None``),
the storage provider's native signer is used.

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - ``SignerType``
     - Description
     - Requires
   * - ``SignerType.S3`` (default for S3 providers)
     - Uses the boto3 S3 client's native ``generate_presigned_url``.
     - ``boto3``
   * - ``SignerType.CLOUDFRONT``
     - Generates CloudFront signed URLs using an RSA key pair.
     - ``boto3``, ``cryptography``

S3 Native Signing
=================

When using an S3 storage provider, presigned URLs are generated via the S3 SDK by default.
No extra ``signer_type`` is needed:

.. code-block:: python

   url = client.generate_presigned_url("datasets/model.bin")

You can pass ``expires_in`` (seconds) via ``signer_options``:

.. code-block:: python

   url = client.generate_presigned_url(
       "datasets/model.bin",
       signer_options={"expires_in": 900},  # 15 minutes
   )

CloudFront Signing
==================

For CloudFront signed URLs (e.g. wildcard-signed URLs for Zarr datasets), pass
``signer_type=SignerType.CLOUDFRONT`` with the required options:

.. code-block:: python

   from multistorageclient import SignerType

   url = client.generate_presigned_url(
       "results/experiment.zarr/*",
       signer_type=SignerType.CLOUDFRONT,
       signer_options={
           "key_pair_id": "K2JCJMDEHXQW5F",
           "private_key_path": "/secrets/cloudfront.pem",
           "domain": "d111111abcdef8.cloudfront.net",
           "expires_in": 7200,
       },
   )

Supported Providers
===================

Presigned URL generation is currently supported for:

- **S3** and S3-compatible providers (S3, S8K, GCS via S3, AIStore via S3)

Other providers (Azure, GCS native, OCI, POSIX) will raise ``NotImplementedError``.
Support for additional providers can be added by implementing a ``URLSigner``
subclass in the corresponding provider module.
