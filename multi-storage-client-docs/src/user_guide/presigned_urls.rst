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
   * - ``SignerType.AZURE``
     - Generates Azure Blob Storage SAS tokens. Uses account-key signing with ``AzureCredentials`` and user-delegation-key signing with ``DefaultAzureCredentials``.
     - ``azure-storage-blob``

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

Azure SAS Signing
=================

For Azure Blob Storage, presigned URLs are generated as
`Shared Access Signature (SAS) <https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview>`_
tokens. Pass ``signer_type=SignerType.AZURE`` (or omit it, since it is the
default for Azure providers):

.. code-block:: python

   from multistorageclient import SignerType

   url = client.generate_presigned_url(
       "datasets/model.bin",
       signer_type=SignerType.AZURE,
       signer_options={"expires_in": 3600},  # 1 hour (default)
   )

To generate a URL that allows uploading:

.. code-block:: python

   url = client.generate_presigned_url(
       "uploads/data.tar",
       method="PUT",
       signer_options={"expires_in": 900},  # 15 minutes
   )

**Signing method** depends on the configured credentials provider:

- ``AzureCredentials`` (connection string / account key) — signs with the
  storage account key directly.
- ``DefaultAzureCredentials`` (managed identity, Azure CLI, etc.) — obtains a
  `user delegation key <https://learn.microsoft.com/en-us/rest/api/storageservices/get-user-delegation-key>`_
  from Azure AD and uses it to sign the SAS token. The delegation key is cached
  and refreshed automatically before it expires.

Credential Lifetime and URL Expiration
=======================================

When the underlying credentials are temporary (STS, IAM roles, EC2 instance
profiles, or Azure user delegation keys), the effective URL lifetime is the
**shorter** of ``expires_in`` and the remaining credential lifetime.  For
example, if the session token expires in 10 minutes but ``expires_in`` is set
to 3600 (1 hour), the URL will stop working after 10 minutes when the token
expires.

For more details, see the
`AWS presigned URL documentation <https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html>`_
or the
`Azure SAS documentation <https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview>`_.

Supported Providers
===================

Presigned URL generation is currently supported for:

- **S3** and S3-compatible providers (S3, S8K, GCS via S3, AIStore via S3)
- **Azure** Blob Storage (via SAS tokens)

Other providers (GCS native, OCI, POSIX) will raise ``NotImplementedError``.
Support for additional providers can be added by implementing a ``URLSigner``
subclass in the corresponding provider module.
