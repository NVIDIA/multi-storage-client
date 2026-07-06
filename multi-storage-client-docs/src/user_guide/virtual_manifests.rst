#####################################
Single-Parquet Virtual Manifests (v2)
#####################################

Virtual manifests expose an immutable logical dataset from one Parquet file. Each logical file is reconstructed from ordered byte ranges in configured object-storage profiles and, optionally, deterministic HTTP services. MSC validates the complete manifest before exposing any file.

This is a storage provider (``storage_provider.type: manifest``), not the legacy metadata-provider feature. The existing :doc:`/user_guide/manifests` guide documents **v1 metadata manifests**: an index and parts used to accelerate listing and metadata lookup for another storage provider. A v2 virtual manifest supplies the file bytes itself.

Installation
============

Install the virtual-manifest extra, which provides PyArrow and Requests:

.. code-block:: shell

   pip install "multi-storage-client[virtual-manifest]"

Concepts
========

A v2 manifest has three kinds of configured dependency:

* a direct profile holding the one immutable Parquet manifest;
* zero or more direct object-storage profiles, referenced by aliases in object chunks; and
* zero or more allowlisted HTTP services, referenced by aliases in service chunks.

The virtual profile does not receive cloud credentials itself. The referenced direct profiles own their credentials. Referenced manifest and object-source profiles must be direct storage profiles; they cannot be composite profiles, provider bundles, replica profiles, metadata-provider profiles, or other virtual-manifest profiles.

Referenced-profile restrictions
===============================

The manifest-storage profile and every object-source profile must be suitable for exact byte-range reads. MSC rejects a referenced profile with ``autocommit`` configured, because a virtual manifest is immutable and does not participate in metadata commits. Hugging Face direct profiles are also rejected because they do not provide this exact range-read contract.

The manifest-storage profile cannot enable ``rust_client``: MSC uses its Python range reader to validate the one Parquet manifest before exposing a dataset. This restriction does not apply to direct object-source profiles. An object source may use ``rust_client`` when that direct provider supports the required exact byte-range reads; MSC will use it for the source ranges after manifest validation.

Configuration
=============

The following example has both an object chunk source and an HTTP service source. ``binding_revision`` is an application-owned, non-secret revision label. Change it whenever the configured dependency can produce different bytes for the same logical manifest row.

.. code-block:: yaml
   :caption: A virtual-manifest storage profile.

   profiles:
     manifest-store:
       storage_provider:
         type: s3
         options:
           base_path: dataset-control
           region_name: us-east-1

     raw-objects:
       storage_provider:
         type: s3
         options:
           base_path: dataset-objects
           region_name: us-east-1

     rendered-dataset:
       storage_provider:
         type: manifest
         options:
           # A normalized relative .parquet path in manifest-store.
           manifest_storage_profile: manifest-store
           manifest_path: releases/2026-07-02/catalog.parquet
           max_workers: 8

           source_profiles:
             raw:
               profile: raw-objects
               binding_revision: raw-objects-2026-07-02

           services:
             render:
               type: http
               options:
                 base_url: https://renderer.example.com/v1
                 binding_revision: renderer-v4
                 allowed_path_prefixes:
                   - render
                 allowed_query_parameters:
                   - format
                   - frame
                 headers:
                   Authorization: "Bearer ${RENDERER_TOKEN}"
                 connect_timeout_seconds: 10
                 read_timeout_seconds: 60
                 verify_tls: true

``manifest_path`` must be a normalized relative Parquet path: it cannot be absolute, contain backslashes or ``.``/``..`` segments, or escape the configured manifest profile. It names exactly one file, not a directory, index, or collection of parts.

.. important::

   Do not put ``metadata_provider: {type: manifest}`` on this profile. That is the separate v1 metadata-manifest feature described in :doc:`/user_guide/manifests`. A v2 virtual manifest uses ``storage_provider.type: manifest`` and owns its logical bytes.

Parquet v2 Wire Format
======================

The Parquet footer must contain exactly these reserved values:

.. code-block:: text

   msc.manifest.version = "2"
   msc.manifest.kind = "virtual-file-chunks"

Unrelated writer metadata is allowed. An unknown ``msc.manifest.*`` footer key, a missing key, or a conflicting value is rejected.

The Arrow schema is exact. Column order, types, nullability, nested field names, and timestamp unit/timezone are part of the v2 contract. The following language-neutral table describes the required schema.

.. list-table:: v2 Parquet columns
   :header-rows: 1
   :widths: 26 34 40

   * - Column
     - Type and nullability
     - Meaning
   * - ``key``
     - required UTF-8 string
     - Normalized relative logical file path.
   * - ``size_bytes``
     - required signed 64-bit integer
     - Total reconstructed logical-file length.
   * - ``last_modified``
     - required timestamp, microseconds, UTC
     - Logical-file timestamp.
   * - ``content_type``
     - optional UTF-8 string
     - Logical-file content type.
   * - ``storage_class``
     - optional UTF-8 string
     - Logical-file storage class.
   * - ``metadata``
     - optional UTF-8 string
     - Strict JSON object; canonicalized before comparison and ETag calculation.
   * - ``chunk_index``
     - required signed 32-bit integer
     - Zero-based reconstruction order within ``key``.
   * - ``chunk_size_bytes``
     - required signed 64-bit integer
     - Bytes contributed by this chunk.
   * - ``chunk_kind``
     - required UTF-8 string
     - One of ``empty``, ``object``, or ``service``.
   * - ``source_profile``
     - optional UTF-8 string
     - Object binding alias; required only for ``object`` chunks.
   * - ``source_path``
     - optional UTF-8 string
     - Object path; required only for ``object`` chunks.
   * - ``source_offset``
     - optional signed 64-bit integer
     - Object byte offset; required only for ``object`` chunks.
   * - ``service_id``
     - optional UTF-8 string
     - HTTP service alias; required only for ``service`` chunks.
   * - ``service_path``
     - optional UTF-8 string
     - Relative service path; required only for ``service`` chunks.
   * - ``service_query``
     - optional list of required ``element`` structs with required UTF-8 ``name`` and ``value`` fields
     - Ordered service query pairs; required, but allowed to be empty, for ``service`` chunks.

Rows may occur in any Parquet row group and in any physical order. MSC groups by ``key`` and orders chunks by ``chunk_index``. Non-empty files require contiguous, unique indexes beginning at zero, positive chunk sizes, and a checked signed-64-bit chunk sum equal to ``size_bytes``. Every row for a key must agree on the file-level columns after metadata normalization. Logical keys are prefix-free: a manifest cannot contain both a file ``a`` and a descendant file ``a/b``.

An empty logical file is exactly one ``empty`` row: index ``0``, zero file/chunk size, and all object/service fields null. Object rows require only their three ``source_*`` fields; service rows require only their three ``service_*`` fields. Cross-kind fields are rejected.

Reading Files
=============

Use the virtual profile exactly as any other MSC storage profile. Reads, metadata lookup, listing, globbing, and streaming ``open`` calls operate on the logical keys in the Parquet manifest.

.. code-block:: python

   from multistorageclient import StorageClient, StorageClientConfig

   config = StorageClientConfig.from_file(profile="rendered-dataset")
   client = StorageClient(config=config)

   prefix = client.read("samples/first.bin", byte_range=None)
   with client.open("samples/first.bin", "rb") as stream:
       first_kib = stream.read(1024)

The standard MSC fsspec adapter also uses the same profile and streaming behavior:

.. code-block:: python

   import multistorageclient as msc

   fs = msc.async_fs.MultiStorageAsyncFileSystem()
   with fs.open("msc://rendered-dataset/samples/first.bin", "rb") as stream:
       first_kib = stream.read(1024)

The reader plans only touched chunks for a requested range, clips reads at logical EOF, and returns an empty result at or beyond EOF. Negative logical offsets or sizes are invalid.

Virtual-manifest profiles default ``open(..., prefetch_file=None)`` to streamed reads (the effective value is ``False``). A streamed ``ObjectFile`` requests only the logical ranges consumed by the caller. Set ``prefetch_file=True`` to materialize the complete virtual file through MSC's normal cache or in-memory download policy instead. The synthetic manifest ETag participates in the normal MSC source-version cache check, so an enabled source-version check invalidates cached logical bytes when the decoded manifest identity changes. Set ``disable_read_cache=True`` to bypass the MSC read cache even for streamed range requests; logical-path and metadata resolution still occur before the direct source read.

Path and HTTP Contract
======================

Logical keys and object ``source_path`` values are normalized relative POSIX paths. They must not be empty, absolute, URI-like, use backslashes or NUL bytes, contain repeated/trailing separators, or contain ``.`` or ``..`` segments.

Service paths are stricter: they are unescaped relative Unicode paths. They also reject percent escapes, queries, fragments, authorities, and paths outside an allowlisted prefix. Prefix matching is segment-aware: an allowed ``render`` prefix accepts ``render`` and ``render/...``, but not ``rendered/...``. Service query names must be non-empty and appear in ``allowed_query_parameters``; query values may be empty, and duplicate name/value pairs retain their order. The configured HTTP base URL rejects controls, backslashes, encoded or userinfo authorities, and ambiguous separators before MSC rebuilds its canonical transport origin. Configured header names must be RFC HTTP tokens; protocol-controlled headers such as ``Range`` and ``Host`` cannot be supplied, including whitespace lookalikes.

For every service chunk MSC sends a single ``GET`` with an exact ``Range`` request and ``Accept-Encoding: identity``. Redirects are disabled. A service must return ``206`` with matching ``Content-Range`` (including the total), matching ``Content-Length``, and no content encoding other than absent or ``identity``. MSC reads at most the requested byte count plus one sentinel byte and rejects short or oversized bodies. Non-TLS connection failures, timeouts, and HTTP ``408``, ``429``, ``500``, ``502``, ``503``, and ``504`` are retryable. TLS, certificate, and protocol-verification failures fail closed and are nonretryable, as are redirects, authentication failures, malformed range metadata, and other statuses.

ETags, Caching, and Immutability
================================

Each logical file receives a synthetic ETag beginning with ``msc-v2-sha256:``. It is a SHA-256 digest over canonical file metadata, chunks in index order, ordered service query pairs, and the used binding alias, non-secret binding identity, and ``binding_revision``. Credential material and configured secret headers are excluded.

Changing file metadata, chunk paths/offsets/boundaries, query order, a used binding identity, or a used binding revision changes the ETag. Changing an unused binding does not. This gives normal MSC caches a stable, dependency-aware identity without exposing credentials.

Decoded manifest files, chunks, range plans, and ordered query pairs are immutable. An initialized provider exposes one immutable snapshot; callers cannot mutate the decoded plan in place.

Publishing and Reloading
========================

Publish a complete, validated Parquet file at a new immutable path, such as a release- or content-addressed path. Do not overwrite a manifest that existing clients may use. After publication, update ``manifest_path`` in configuration and construct a new client (or reload its configuration) to adopt the release.

There is no live manifest reload. Existing manifest providers deliberately continue using the snapshot loaded at initialization, including its synthetic ETags and binding revisions.

Limitations
===========

Virtual manifests are intentionally narrow in v2:

* The provider is read-only. It has no manifest writer and does not support writes, deletes, copies, uploads, appends, or symlinks.
* There are no chunk checksums, source version pins, manifest version pins, live reload, or multipart write support.
* The Parquet file is a single immutable v2 manifest; v1 indexes and parts are not accepted as virtual manifests.
* The Multi-Storage File System (MSFS/FUSE) does not support virtual-manifest profiles.
* HTTP services must be deterministic for the configured path/query/range contract. MSC does not follow redirects or infer service authorization rules.

For v1 metadata manifests that accelerate discovery while another provider still supplies the bytes, see :doc:`/user_guide/manifests`.
