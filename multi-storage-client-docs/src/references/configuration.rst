#######################
Configuration Reference
#######################

This page documents the configuration schema for the Multi-Storage Client (MSC). The configuration file allows you to define
storage profiles, caching behavior, and observability settings. Each profile can be configured to work with different storage
providers like S3, Azure Blob Storage, Google Cloud Storage, and others.

*********
Top-Level
*********

The top-level configuration schema consists of six main sections:

* ``include``

  * Optional list of configuration file paths to include and merge. This enables modular configuration management by splitting settings across multiple files.

* ``experimental_features``

  * Optional dictionary to enable experimental features. When omitted, all experimental features are disabled.

* ``profiles``

  * Dictionary containing profile configurations. Each profile defines storage, metadata, and credentials providers.

* ``cache``

  * Configuration for local caching of remote objects.

* ``opentelemetry``

  * Configuration for OpenTelemetry metrics and tracing exporters.

* ``path_mapping``

  * Configuration for mapping existing non-MSC URLs to existing MSC profiles.

.. code-block:: yaml
   :caption: Top-level schema.

   # Optional. List of config files to include
   include: <list_of_config_paths>

   # Optional. Experimental features flags
   experimental_features: <experimental_features_config>

   # Optional. Dictionary of profile configurations
   profiles: <profile_config>

   # Optional. Cache configuration
   cache: <cache_config>

   # Optional. OpenTelemetry configuration
   opentelemetry: <opentelemetry_config>

   # Optional. Path mapping configuration
   path_mapping: <path_mapping_config>

*******************
Multi-Configuration
*******************

MSC supports splitting configuration across multiple files using the ``include`` keyword. This enables modular configuration management by sharing common settings (e.g., opentelemetry, cache) across multiple configurations.

The ``include`` field accepts a list of configuration file paths. Paths can be absolute (``/etc/msc/shared.yaml``) or relative to the main configuration file (``./shared/profiles.yaml``, ``../common/cache.yaml``).

.. note::

  * **Include order matters** for fields that concatenate lists (e.g., ``opentelemetry.metrics.attributes``). Files are processed in the order listed, and later entries can override or extend earlier ones.
  * **No nested includes**: included files cannot contain ``include`` section.
  * All included files must exist and be valid MSC configurations.

.. code-block:: yaml
   :caption: Example: Main configuration including shared settings

   include:
     - ./team-profiles.yaml
     - /etc/msc/shared-cache.yaml
     - /etc/msc/shared-opentelemetry.yaml

   profiles:
     my-profile:
       storage_provider:
         type: file
         options:
           base_path: /tmp

.. code-block:: yaml
   :caption: Example: ``team-profiles.yaml``

   profiles:
     team-s3:
       storage_provider:
         type: s3
         options:
           base_path: team-bucket

.. code-block:: yaml
   :caption: Example: ``shared-cache.yaml``

   cache:
     size: 500G
     location: /scratch/msc_cache

.. code-block:: yaml
   :caption: Example: ``shared-opentelemetry.yaml``

   opentelemetry:
     metrics:
       attributes:
         - type: environment_variables
           options:
             attributes:
               msc.cluster: CLUSTER
       reader:
         options:
           collect_interval_millis: 10
           export_interval_millis: 1000
       exporter:
         type: otlp
         options:
           endpoint: http://localhost:4318/v1/metrics

When merged, the main configuration will contain all profiles from both the main file and ``team-profiles.yaml``, plus the cache configuration from ``shared-cache.yaml``.

Merge Rules
===========

* **Profiles**: Combined from all files. Identical profile definitions are allowed (idempotent). Different definitions for the same profile name raise an error.
* **OpenTelemetry**: Metrics attributes are concatenated. Reader and exporter must be identical across all files or defined in only one file.
* **Path Mapping / Experimental Features**: Entries are merged. Identical entries are allowed. Conflicting entries raise an error.
* **Cache / POSIX**: Must be identical across all files or defined in only one file.

*********************
Experimental Features
*********************

The ``experimental_features`` section allows you to enable experimental features that are under active development.
These features may have breaking changes in future releases.

.. warning::
   Experimental features are not guaranteed to be stable and may change or be removed in future versions.
   Use with caution in production environments.

Currently available experimental features:

* ``cache_mru_eviction``

  * Enables the MRU (Most Recently Used) eviction policy for cache (boolean, default: not enabled)

* ``cache_purge_factor``

  * Enables the purge_factor parameter for controlling cache eviction aggressiveness (boolean, default: not enabled)

.. code-block:: yaml
   :caption: Example: Enable specific experimental features

   experimental_features:
     cache_mru_eviction: true
     cache_purge_factor: true

   cache:
     size: "10G"
     eviction_policy:
       policy: mru           # Requires cache_mru_eviction: true
       purge_factor: 50      # Requires cache_purge_factor: true

If you attempt to use an experimental feature without enabling it, you'll receive a clear error message:

.. code-block:: text

   ValueError: MRU eviction policy is experimental and not enabled.
   Enable it by adding to config:
     experimental_features:
       cache_mru_eviction: true

*******
Profile
*******

Each profile in the configuration defines how to interact with storage services through four main sections:

* ``storage_provider``

  * Configures which storage service to use and how to connect to it.

* ``metadata_provider``

  * Configures metadata services that provide additional object information.

* ``credentials_provider``

  * Configures authentication credentials for the storage service.

* ``provider_bundle``

  * Configures a custom provider implementation that bundles the above providers together.

* ``replicas``

  * Configure one or more *replica profiles* that the current profile can
    read from and write to opportunistically (see :doc:`/user_guide/replicas`).

* ``retry``

  * Configures the retry strategy for the profile.

.. code-block:: yaml
   :caption: Profile schema.

   # Required. Configuration for the storage provider
   storage_provider:
     # Required. Provider type
     type: <string>
     # Required. Provider-specific options
     options: <provider_options>

   # Optional. Configuration for the metadata provider
   metadata_provider:
     # Required. Provider type (e.g. "manifest")
     type: <string>
     # Required. Provider-specific options
     options: <provider_options>

   # Optional. Configuration for the credentials provider
   credentials_provider:
     # Required. Provider type
     type: <string>
     # Required. Provider-specific options
     options: <provider_options>

   # Optional.
   provider_bundle:
     # Required. Fully-qualified class name for a custom provider bundle
     type: <string>
     # Required. Provider-specific options
     options: <provider_options>

   # Optional. Enable caching for this profile (default: false)
   caching_enabled: <boolean>

   # Optional. List of replica configurations that this profile can use
   # for fetch-on-demand reads and background read-through backfill.
   replicas:
     - replica_profile: <string>   # Name of another profile acting as replica
       read_priority: <int>        # Required. Lower = preferred (1 = highest)

   # Optional. Retry configuration
   retry:
     # Optional. Number of attempts before giving up. Must be at least 1.
     attempts: <int>
     # Optional. Base delay (in seconds) for exponential backoff. Must be a non-negative value.
     delay: <float>
     # Optional. Backoff multiplier for exponential backoff. Must be at least 1.0.
     backoff_multiplier: <float>

.. note::
   The configuration follows a consistent pattern across different providers:

   * The ``type`` field specifies which provider implementation to use. This can be:

     * A predefined name (e.g. "s3", "azure", "file") that maps to built-in providers
     * A fully-qualified class name for custom provider implementations

   * The ``options`` field contains provider-specific configuration that will be passed to the provider's constructor. The available options depend on the specific provider implementation being used.

   * Profile names must not start with an underscore (_) to prevent collision with :ref:`implicit profiles <implicit-profiles>`.

   * The ``caching_enabled`` field controls whether caching is enabled for this specific profile. When set to ``true``, the profile will use the global cache configuration if provided. When set to ``false`` or omitted, caching is disabled for this profile regardless of global cache settings.

Storage Providers
=================

The following storage provider types are supported:

``file``
--------

The POSIX filesystem provider.

Options: See parameters in :py:class:`multistorageclient.providers.posix_file.PosixFileStorageProvider`.

MSC includes a default POSIX filesystem profile that is used when no configuration file is found. This profile provides basic local filesystem access:

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     default:
       storage_provider:
         type: file
         options:
           base_path: /

``s3``
------

AWS S3 and S3-compatible storage provider.

Options: See parameters in :py:class:`multistorageclient.providers.s3.S3StorageProvider`.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
           region_name: us-east-1

``s8k``
-------

SwiftStack provider.

Options: See parameters in :py:class:`multistorageclient.providers.s8k.S8KStorageProvider`.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       storage_provider:
         type: s8k
         options:
           base_path: my-bucket
           region_name: us-east-1
           endpoint_url: https://s8k.example.com

Content Type Inference
^^^^^^^^^^^^^^^^^^^^^^

The S8K storage provider supports automatic MIME type inference from file extensions through the ``infer_content_type`` option.
When enabled, files are uploaded with appropriate ``Content-Type`` headers based on their extensions (e.g., ``.wav`` → ``audio/x-wav``,
``.mp3`` → ``audio/mpeg``, ``.json`` → ``application/json``).

This is particularly useful for serving media files directly from object storage, as browsers can play audio/video files inline rather
than downloading them when the correct content type is set.

.. code-block:: yaml
   :caption: Example configuration with content type inference enabled.

   profiles:
     my-profile:
       storage_provider:
         type: s8k
         options:
           base_path: my-bucket
           region_name: us-east-1
           endpoint_url: https://s8k.example.com
           infer_content_type: true  # Enable automatic MIME type inference

.. note::
   Content type inference is **disabled by default** (``infer_content_type: false``). When disabled, boto3's default
   behavior applies, which typically results in ``application/octet-stream`` for most files.

.. note::
   **Performance Considerations**: Content type inference uses Python's built-in ``mimetypes`` module, which is fast
   (dictionary lookup). However, the inference only occurs during write operations (``upload_file``, ``write``, ``put_object``),
   so there is no impact on read performance.

If a file extension is not recognized, no ``Content-Type`` header is explicitly set, and boto3 will use its default behavior
which typically results in ``application/octet-stream``.

``azure``
---------

Azure Blob Storage provider.

Options: See parameters in :py:class:`multistorageclient.providers.azure.AzureBlobStorageProvider`.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       storage_provider:
         type: azure
         options:
           base_path: my-container
           account_url: https://my-storage-account.blob.core.windows.net

``gcs``
-------

Google Cloud Storage provider.

Options: See parameters in :py:class:`multistorageclient.providers.gcs.GoogleStorageProvider`.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       storage_provider:
         type: gcs
         options:
           base_path: my-bucket
           project_id: my-project-id

``gcs_s3``
----------

Google Cloud Storage provider using the GCS S3 interface.

Options: See parameters in :py:class:`multistorageclient.providers.gcs_s3.GoogleS3StorageProvider`.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       storage_provider:
         type: gcs_s3
         options:
           base_path: my-bucket
           endpoint_url: https://storage.googleapis.com

``oci``
-------

OCI Object Storage provider.

Options: See parameters in :py:class:`multistorageclient.providers.oci.OracleStorageProvider`.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       storage_provider:
         type: oci
         options:
           base_path: my-bucket
           namespace: my-namespace

``aistore``
-----------

NVIDIA AIStore provider using the native SDK.

Options: See parameters in :py:class:`multistorageclient.providers.ais.AIStoreStorageProvider`.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       storage_provider:
         type: ais
         options:
           endpoint: https://ais.example.com
           base_path: my-bucket

``ais_s3``
----------

NVIDIA AIStore provider using the S3-compatible API.

Options: See parameters in :py:class:`multistorageclient.providers.ais_s3.AIStoreS3StorageProvider`.

.. code-block:: yaml
   :caption: Example 1: Local deployment (no authentication, no SSL).

   profiles:
     local-aistore:
       storage_provider:
         type: ais_s3
         options:
           endpoint_url: http://localhost:51080/s3
           base_path: my-bucket

.. code-block:: yaml
   :caption: Example 2: Production with self-signed certificate (JWT token provided, skip SSL verification).

   profiles:
     prod-aistore:
       storage_provider:
         type: ais_s3
         options:
           endpoint_url: https://aistore.example.com/s3
           base_path: my-bucket
           verify: false  # Skip SSL verification for self-signed certificates
       credentials_provider:
         type: AISCredentials
         options:
           token: ${AIS_TOKEN}  # Pre-generated JWT token

.. code-block:: yaml
   :caption: Example 3: Production with CA certificate (obtain JWT token with username/password).

   profiles:
     prod-aistore:
       storage_provider:
         type: ais_s3
         options:
           endpoint_url: https://aistore.example.com/s3
           base_path: my-bucket
           verify: /path/to/aistore-ca.crt  # CA certificate for S3 API endpoint
       credentials_provider:
         type: AISCredentials
         options:
           username: ${AIS_USERNAME}
           password: ${AIS_PASSWORD}
           authn_endpoint: https://authn.example.com:52001
           ca_cert: /path/to/authn-ca.crt  # CA certificate for AuthN server (often same as above)
.. _rust-client-reference:

``huggingface``
---------------

HuggingFace Storage Provider.

Options: See parameters in :py:class:`multistorageclient.providers.huggingface.HuggingFaceStorageProvider`.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       storage_provider:
         type: huggingface
         options:
           repository_id: my-repository-id
           repo_type: my-repo-type
           repo_revision: my-repo-revision
           base_path: base-path

.. note::

   The HuggingFace provider leverages HuggingFace Hub's built-in transfer mechanisms
   for optimal performance. The HuggingFace SDK (0.34.4) does not provide API-level
   control over the underlying data transfer mechanisms, instead allowing configuration
   through environment variables. MSC does not manipulate these variables to maintain
   debuggability and avoid conflicts in multi-threaded/multi-processing setups.

   As of May 23rd, 2025, XET-enabled repositories are the default for all new users
   and organizations. When the HuggingFace provider is used with XET-enabled repositories,
   it will automatically utilize `hf_xet <https://github.com/huggingface/xet-core>`_
   for efficient data transfer. Users can disable this behavior by setting
   ``HF_HUB_DISABLE_XET=1``.

   Alternatively, users can set ``HF_HUB_ENABLE_HF_TRANSFER=1`` to use
   `hf_transfer <https://github.com/huggingface/hf_transfer>`_. Based on our
   performance evaluation, ``hf_xet`` provides optimal performance for download
   operations, while ``hf_transfer`` provides optimal performance for upload
   operations.

   For detailed configuration instructions, see the `HuggingFace documentation <https://huggingface.co/docs/huggingface_hub/en/guides/download#faster-downloads>`_.

``rust_client`` (experimental)
------------------------------

.. warning::
   The Rust client is an experimental feature starting from v0.24 and is subject to change in future releases.

Due to Python's Global Interpreter Lock (GIL), achieving optimal multi-threading performance within a single Python process is challenging.
To address this limitation, MSC introduces an experimental Rust client, which aims to improve performance in multi-threaded scenarios.

To enable the Rust client, add the ``rust_client`` option to your storage provider configuration.

.. note::
   Currently, the Rust client is supported for the following storage providers: ``s3``, ``s8k``, ``gcs_s3``, and ``gcs``.

.. code-block:: yaml
   :caption: Example S3 storage provider configuration with Rust client.

   profiles:
     my-profile:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
           region_name: us-east-1
           multipart_threshold: 16777216 # 16MiB
           multipart_chunksize: 4194304 # 4MiB
           io_chunksize: 4194304 # 4MiB
           max_concurrency: 8
           rust_client:
             multipart_chunksize: 2097152 # 2MiB, Rust client supports a different multipart chunksize than the Python client
             max_concurrency: 16 # Rust client supports a different multipart concurrency level than the Python client

When the Rust client is enabled, it will replace Python implementations for the following storage provider operations:

* :py:class:`multistorageclient.types.StorageProvider.put_object`
* :py:class:`multistorageclient.types.StorageProvider.get_object`
* :py:class:`multistorageclient.types.StorageProvider.upload_file`
* :py:class:`multistorageclient.types.StorageProvider.download_file`

.. note::
   For `put_object()` and `upload_file()`, if `attributes` is provided, the Rust client will not be used.

Other storage provider operations continue to use the Python implementation:

* :py:class:`multistorageclient.types.StorageProvider.list_objects`
* :py:class:`multistorageclient.types.StorageProvider.copy_object`
* :py:class:`multistorageclient.types.StorageProvider.delete_object`
* :py:class:`multistorageclient.types.StorageProvider.get_object_metadata`
* :py:class:`multistorageclient.types.StorageProvider.glob`
* :py:class:`multistorageclient.types.StorageProvider.is_file`

Metadata Providers
==================

``manifest``
------------
The manifest-based metadata provider for accelerated object listing and metadata retrieval. See :doc:`/user_guide/manifests` for more details.

Options: See parameters in :py:class:`multistorageclient.providers.manifest_metadata.ManifestMetadataProvider`.

.. code-block:: yaml
   :caption: Example configuration.

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

Credentials Providers
=====================

Credentials providers vary by storage service. When running in a cloud service provider's (CSP) managed environment
(like AWS EC2, Azure VMs, or Google Cloud Compute Engine), credentials are automatically handled through instance
metadata services. Similarly, when running locally, credentials are typically handled through environment variables
or configuration files (e.g., AWS credentials file).

Therefore, it's recommended to omit the credentials provider and let the storage service use its default
authentication mechanism. This approach is more secure than storing credentials in the MSC configuration file
and ensures credentials are properly rotated when running in cloud environments.

If you need to provide static credentials, it's strongly recommended to pass them through environment variables rather
than hardcoding them directly in configuration files. See `Environment Variables`_ for more details.

``S3Credentials``
-----------------
Static credentials provider for Amazon S3 and S3-compatible storage services.

Options: See parameters in :py:class:`multistorageclient.providers.s3,StaticS3CredentialsProvider`.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       credentials_provider:
         type: S3Credentials
         options:
           access_key: ${AWS_ACCESS_KEY}
           secret_key: ${AWS_SECRET_KEY}

``AzureCredentials``
---------------------
Static credentials provider for Azure Blob Storage.

Options: See parameters in :py:class:`multistorageclient.providers.azure.StaticAzureCredentialsProvider`.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       credentials_provider:
         type: AzureCredentials
         options:
           connection: ${AZURE_CONNECTION_STRING}

``AISCredentials``
-------------------
Static credentials provider for NVIDIA AIStore.

Options: See parameters in :py:class:`multistorageclient.providers.ais.StaticAISCredentialProvider`.

.. code-block:: yaml
   :caption: Example 1: Production with pre-generated JWT token.

   profiles:
     my-profile:
       credentials_provider:
         type: AISCredentials
         options:
           token: ${AIS_TOKEN}  # Pre-generated JWT token

.. code-block:: yaml
   :caption: Example 2: Production with username/password (obtains JWT token automatically).

   profiles:
     my-profile:
       credentials_provider:
         type: AISCredentials
         options:
           username: ${AIS_USERNAME}
           password: ${AIS_PASSWORD}
           authn_endpoint: https://authn.example.com:52001

``GoogleIdentityPoolCredentialsProvider``
------------------------------------------
Workload Identity Federation (WIF) credentials provider for Google Cloud Storage.

Options: See parameters in :py:class:`multistorageclient.providers.gcs.GoogleIdentityPoolCredentialsProvider`.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       credentials_provider:
         type: GoogleIdentityPoolCredentialsProvider
         options:
           audience: https://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_ID/providers/PROVIDER_ID
           token: token

``GoogleServiceAccountCredentialsProvider``
--------------------------------------------
Service account credentials provider for Google Cloud Storage.

Options: See parameters in :py:class:`multistorageclient.providers.gcs.GoogleServiceAccountCredentialsProvider`.

.. code-block:: yaml
   :caption: Example 1: Service account private key file.

   profiles:
     my-profile:
       credentials_provider:
         type: GoogleServiceAccountCredentialsProvider
         options:
           file: /path/to/application_default_credentials.json

.. code-block:: yaml
   :caption: Example 2: Service account private key file contents.

   profiles:
     my-profile:
       credentials_provider:
         type: GoogleServiceAccountCredentialsProvider
         options:
           info:
             type: service_account
             project_id: project_id
             private_key_id: private_key_id
             private_key: |
               -----BEGIN PRIVATE KEY-----
               {private key}
               -----END PRIVATE KEY-----
             client_email: email@example.com
             client_id: client_id
             auth_uri: https://accounts.google.com/o/oauth2/auth
             token_uri: https://oauth2.googleapis.com/token
             auth_provider_x509_cert_url: https://www.googleapis.com/oauth2/v1/certs
             client_x509_cert_url: https://www.googleapis.com/robot/v1/metadata/x509/{key}%40{project}.iam.gserviceaccount.com
             universe_domain: googleapis.com

``FileBasedCredentials``
-------------------------
File-based credentials provider that reads credentials from a JSON file following the AWS external process credential provider format.

This provider is designed for scenarios where credentials are managed by an external process that periodically updates
a JSON file with fresh credentials. The credentials file can be updated by external tools, and MSC will read the latest
credentials when :py:meth:`refresh_credentials` is called.

Options: See parameters in :py:class:`multistorageclient.providers.file_credentials.FileBasedCredentialsProvider`.

The JSON file must follow this schema:

.. code-block:: json

   {
     "Version": 1,
     "AccessKeyId": "your-access-key-id",
     "SecretAccessKey": "your-secret-access-key",
     "SessionToken": "your-session-token",
     "Expiration": "2024-12-31T23:59:59Z"
   }

Where:

* ``Version``: Must be 1 (required)
* ``AccessKeyId``: The access key for authentication (required)
* ``SecretAccessKey``: The secret key for authentication (required)
* ``SessionToken``: An optional session token for temporary credentials
* ``Expiration``: An optional ISO 8601 formatted timestamp indicating when the credentials expire

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my-profile:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
       credentials_provider:
         type: FileBasedCredentials
         options:
           credential_file_path: /path/to/credentials.json

.. code-block:: yaml
   :caption: Example with environment variable for path.

   profiles:
     my-profile:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
       credentials_provider:
         type: FileBasedCredentials
         options:
           credential_file_path: ${CRED_FILE_PATH}

.. note::
   The credential file must exist and contain valid JSON when the provider is initialized. The provider
   will validate the file format and schema at startup. If the file is updated by an external process,
   call :py:meth:`refresh_credentials` to reload the credentials from the file.

Retry
=====

MSC will retry on errors classified as ``RetryableError`` (see :py:class:`multistorageclient.types.RetryableError`) in addition to the retry logic of the underlying CSP native SDKs.

Options: See parameters in :py:class:`multistorageclient.types.RetryConfig`.

The retry strategy uses exponential backoff: the delay is multiplied by the backoff multiplier raised to the power of the attempt number for each subsequent attempt, and a random jitter of 0 to 1 second is added to the delay.

.. code-block:: yaml
   :caption: Example configuration.

    profiles:
      my_profile:
        storage_provider:
          type: s3
          options:
            base_path: my-bucket
        retry:
          attempts: 3
          delay: 1.0
          backoff_multiplier: 2.0

In the example above, the retry will wait for 1.0, 2.0, 4.0 seconds before giving up, with a jitter of 0-1 second added to the delay each time.

The exponential backoff delay calculation is: ``delay * (backoff_multiplier ** attempt)`` where attempt starts at 0. The ``backoff_multiplier`` defaults to 2.0 if not specified.

*****
Cache
*****

The MSC cache configuration allows you to specify caching behavior for improved performance. The cache stores
files locally for faster access on subsequent reads. The cache is shared across all profiles.

.. note::
   Caching can be controlled at the profile level using the ``caching_enabled`` field in the profile configuration.
   When ``caching_enabled`` is set to ``true`` for a profile, that profile will use the global cache configuration.
   When set to ``false`` or omitted, caching is disabled for that profile regardless of global cache settings.

Options:

* ``size``

  * Maximum cache size with unit (e.g. ``"100M"``, ``"1G"``) (optional, default: ``"10G"``)

* ``location``

  * Absolute filesystem path for storing cached files (optional, default: system temporary directory + ``"/msc_cache"``)

* ``use_etag``

  * Use ETag for cache validation, it introduces a small overhead by checking the Etag agains the remote object on every read (optional, default: ``true``)

* ``eviction_policy``: Cache eviction policy configuration (optional, default policy is ``"fifo"``)

  * ``policy``: Eviction policy type

    * ``"fifo"``: First In, First Out (stable)
    * ``"lru"``: Least Recently Used (stable)
    * ``"mru"``: Most Recently Used (**experimental** - requires ``cache_mru_eviction: true``)
    * ``"random"``: Random eviction (stable)

  * ``refresh_interval``: Interval in seconds to trigger cache eviction (optional, default: ``"300"``)

  * ``purge_factor``: (**experimental** - requires ``cache_purge_factor: true``) Percentage of cache to delete during eviction (0-100, optional, default: ``"0"``)

    * ``0`` = Delete only what's needed to stay under limit (default behavior)
    * ``20`` = Delete 20% of max cache size (keep 80%)
    * ``50`` = Delete 50% of max cache size (keep 50%)
    * ``100`` = Delete everything (clear entire cache)


.. code-block:: yaml
   :caption: Example configuration to enable basic cache.

   cache:
     size: 500G
     location: /path/to/msc_cache

.. code-block:: yaml
   :caption: Example configuration to configure cache eviction policy.

   cache:
     size: 500G
     location: /path/to/msc_cache
     eviction_policy:
       policy: lru
       refresh_interval: 3600

.. code-block:: yaml
   :caption: Example configuration with purge_factor to reduce eviction frequency (experimental).

   experimental_features:
     cache_purge_factor: true  # Enable experimental feature

   cache:
     size: 500G
     location: /path/to/msc_cache
     eviction_policy:
       policy: lru
       refresh_interval: 3600
       purge_factor: 20  # Delete 20% during eviction (keep 400GB free space)

.. code-block:: yaml
   :caption: Example configuration with MRU eviction policy (experimental).

   experimental_features:
     cache_mru_eviction: true    # Enable experimental feature
     cache_purge_factor: true    # Enable experimental feature

   cache:
     size: 500G
     location: /path/to/msc_cache
     eviction_policy:
       policy: mru
       purge_factor: 50  # Delete 50% during eviction

.. code-block:: yaml
   :caption: Example configuration with profile-level caching control.

   cache:
     size: 500G
     location: /path/to/msc_cache

   profiles:
     s3-profile:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
       caching_enabled: true  # This profile will use caching

     azure-profile:
       storage_provider:
         type: azure
         options:
           base_path: my-container
       caching_enabled: false  # This profile will not use caching

*************
OpenTelemetry
*************

MSC supports OpenTelemetry for collecting client-side metrics and traces to help monitor and debug your application's
storage operations. This includes:

* Metrics about storage operations.
* Traces showing the flow of storage operations and their timing.

The OpenTelemetry configuration schema consists of these sections:

* ``metrics``

  * Metrics configuration dictionary.

* ``traces``

  * Traces configuration dictionary.

.. code-block:: yaml
   :caption: OpenTelemetry schema.

   # Optional. Metrics configuration.
   metrics: <metrics_config>

   # Optional. Traces configuration.
   traces: <traces_config>

.. code-block:: yaml
   :caption: Example configuration.

   opentelemetry:
     metrics:
       attributes:
         - type: static
           options:
             attributes:
               organization: NVIDIA
               cluster: DGX SuperPOD 1
         - type: host
           options:
             attributes:
               node: name
         - type: process
           options:
             attributes:
               process: pid
       reader:
         options:
           # ≤ 100 Hz collect frequency.
           collect_interval_millis: 10
           collect_interval_timeout: 100
           # ≤ 1 Hz export frequency.
           export_interval_millis: 1000
           export_timeout_millis: 500
       exporter:
         type: otlp
         options:
           # OpenTelemetry Collector default local HTTP endpoint.
           endpoint: http://localhost:4318/v1/traces
     traces:
       exporter:
         type: otlp
         options:
           # OpenTelemetry Collector default local HTTP endpoint.
           endpoint: http://localhost:4318/v1/traces

Metrics
=======

The metrics configuration schema consists of these sections:

* ``attributes``

  * Additional attributes to add to metrics.

* ``reader``

  * Metrics reader configuration.

* ``exporter``

  * Metric exporter configuration.

.. code-block:: yaml
   :caption: Metrics schema.

   # Optional. Attributes provider configurations.
   attributes:
     - # Required. Attributes provider type or fully-qualified class name.
       type: <string>
       # Optional. Constructor keyword parameters.
       options: <provider_options>

   # Optional. Metric reader configuration.
   reader:
     # Optional. Constructor keyword parameters.
     options: <reader_options>

   # Optional. Metric exporter configuration.
   exporter:
     # Required. Attributes provider type ("console", "otlp") or fully-qualified class name.
     type: <string>
     # Optional. Constructor keyword parameters.
     options: <exporter_options>

Attributes
----------

The attributes configuration schema is a list of attributes provider configurations. Attributes providers implement :py:class:`multistorageclient.telemetry.attributes.base.AttributesProvider`.

If multiple attributes providers return an attribute with the same key, the value from the latest attribute provider is kept.

The following attributes provider types are provided:

.. list-table:: Attributes Provider Types
   :header-rows: 1

   * - Type
     - Fully-Qualified Class Name
   * - ``environment_variables``
     - :py:class:`multistorageclient.telemetry.attributes.environment_variables.EnvironmentVariablesAttributesProvider`
   * - ``host``
     - :py:class:`multistorageclient.telemetry.attributes.host.HostAttributesProvider`
   * - ``msc_config``
     - :py:class:`multistorageclient.telemetry.attributes.msc_config.MSCConfigAttributesProvider`
   * - ``process``
     - :py:class:`multistorageclient.telemetry.attributes.process.ProcessAttributesProvider`
   * - ``static``
     - :py:class:`multistorageclient.telemetry.attributes.static.StaticAttributesProvider`
   * - ``thread``
     - :py:class:`multistorageclient.telemetry.attributes.thread.ThreadAttributesProvider`

.. code-block:: yaml
   :caption: Example configuration.

   opentelemetry:
     metrics:
       attributes:
         - type: static
           options:
             attributes:
               organization: NVIDIA
               cluster: DGX SuperPOD 1
         - type: host
           options:
             attributes:
               node: name
         - type: process
           options:
             attributes:
               process: pid
         - type: my_library.MyAttributesProvider
           options:
             # ...

Reader
------

The reader configuration schema is a metrics reader configuration. This configures a :py:class:`multistorageclient.telemetry.metrics.readers.diperiodic_exporting.DiperiodicExportingMetricReader`.

.. code-block:: yaml
   :caption: Example configuration.

   opentelemetry:
     metrics:
       reader:
         options:
           # ≤ 100 Hz collect frequency.
           collect_interval_millis: 10
           collect_interval_timeout: 100
           # ≤ 1 Hz export frequency.
           export_interval_millis: 1000
           export_timeout_millis: 500

Distributed object stores typically have latencies on the order of 10-100 milliseconds, so a metric reader collect interval of 10 milliseconds is recommended.

.. note::

   The ratio between the collect and export intervals shouldn't be too high. Otherwise, export payloads may exceed the payload size limit for telemetry backends.

Exporter
--------

The exporter configuration schema is a metric exporter configuration. Metric exporters implement :py:class:`opentelemetry.sdk.metrics.export.MetricExporter`.

The following exporter types are provided:

.. list-table:: Metric Exporter Types
   :header-rows: 1

   * - Type
     - Fully-Qualified Class Name
   * - ``console``
     - :py:class:`opentelemetry.sdk.metrics.export.ConsoleMetricExporter`
   * - ``otlp``
     - :py:class:`opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter`

.. note::

   These need additional dependencies to be present (provided as an extra dependencies).

.. code-block:: yaml
   :caption: Example configuration.

   opentelemetry:
     metrics:
       exporter:
         type: otlp
         options:
           # OpenTelemetry Collector default local HTTP endpoint.
           endpoint: http://localhost:4318/v1/metrics

************
Path Mapping
************

The ``path_mapping`` section allows mapping non-MSC URLs to MSC URLs.
This enables users to use their existing URLs with MSC without having to change their code/config.

.. code-block:: yaml

   path_mapping:
     /lustrefs/a/b/: msc://profile-for-file-a-b/
     /lustrefs/a/: msc://profile-for-file-a/
     s3://bucket1/: msc://profile-for-s3-bucket1/
     s3://bucket1/a/b/: msc://profile-for-s3-bucket1-a-b/
     gs://bucket1/: msc://profile-for-gcs-bucket1/
     s3://old-bucket-123/: msc://profile-for-gcs-new-bucket-456/  # pointing existing s3 urls to gcs profile with different bucket name

Each key-value pair maps a source path to a destination MSC URL. MSC
will automatically convert paths that match the source prefix to use the
corresponding MSC URI when accessing files.  The storage provider of the
specified destination profile doesn't need to match the type of the source
protocol, which allows users to point existing URLs to different storage providers.

.. note::
   Path mapping must adhere to the following constraints:

   **Source Path:**

   * Must end with ``/`` to prevent unintended partial name conflicts and ensure clear mapping of prefixes
   * The protocol can be anything as long as it points to a valid storage provider
   * No duplicate protocol + bucket + prefix combinations are allowed

   **Destination Path:**

   * Must start with ``msc://``
   * Must end with ``/``
   * Must reference a profile that is defined in the MSC configuration

   While processing non-MSC URLs, If multiple source paths match a given input path, the longest matching prefix takes precedence.

*****************
Implicit Profiles
*****************

.. _implicit-profiles:

Implicit profiles are automatically created by MSC when users provide non-MSC URLs directly to MSC functions. Unlike explicitly defined profiles in the configuration file, implicit profiles are inferred dynamically from URL patterns.

This feature enables users to:

* Continue using existing URLs without modification.
* Use MSC without managing a separate MSC configuration file.

When a non-MSC URL is provided to functions like :py:func:`multistorageclient.open` or
:py:func:`multistorageclient.resolve_storage_client`, MSC will first check if there is an existing profile applicable through path mapping. If not, MSC will create an implicit profile:

1. Infer the storage provider based on the URL protocol (currently supported: ``s3``, ``gcs``, ``ais``, ``file``) and construct an implicit profile name with the convention ``_protocol-bucket`` (e.g., ``_s3-bucket1``, ``_gs-bucket1``) or ``_file`` for file system paths.  If the derived protocol is not supported, an exception will be thrown.
2. Configure the storage provider and credential provider with default settings, i.e. credentials will the same as that native SDKs look for (aws credentials file, azure credentials file, etc.)
3. If MSC config is present, inherit global settings like observability and file cache; otherwise, only default settings for file system based cache.

Here are examples of non-MSC URLs that are automatically translated to MSC URIs:

* ``s3://bucket1/path/to/object`` → ``msc://_s3-bucket1/path/to/object``
* ``/path/to/another/file`` → ``msc://_file/path/to/another/file``

Implicit profiles are identified by their leading underscore prefix, which is why user-defined profile names cannot start with an underscore.

*********************
Environment Variables
*********************

The MSC configuration file supports environment variable expansion in string values. Environment variables
can be referenced using either ``${VAR}`` or ``$VAR`` syntax.

.. code-block:: yaml
   :caption: Example configuration.

   profiles:
     my_profile:
       storage_provider:
         type: s3
         options:
           base_path: ${BUCKET_NAME}
       credentials_provider:
         type: S3Credentials
         options:
           access_key: ${AWS_ACCESS_KEY}
           secret_key: ${AWS_SECRET_KEY}

In this example, the values will be replaced with the corresponding environment variables at runtime. If an
environment variable is not set, the original string will be preserved.

The environment variable expansion works for any string value in the configuration file, including:

* Storage provider options
* Credentials provider options
* Metadata provider options
* Cache configuration
* OpenTelemetry configuration

This allows sensitive information like credentials to be passed securely through environment variables rather
than being hardcoded in the configuration file.
