#####################################
Single-Parquet Virtual Manifests (v2)
#####################################

Virtual manifests expose an immutable logical dataset from one Parquet file. Each logical file is reconstructed by concatenating ordered byte ranges from configured object-storage profiles and, optionally, deterministic HTTP services. MSC does not interpret the resulting bytes: MP4, Parquet, JSONL, and other formats all use the same concatenation path.

With respect to the manifest itself, construction validates only the Parquet footer and exact schema together with the structural metadata needed to index row groups, including ``key`` statistics when present. It does not decode manifest data rows at construction time. Row-level and logical-file semantic validation is lazy; construction also does not read referenced object bytes or call HTTP transports. This separation is what makes the manifest object itself part of the publication contract described below.

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

The manifest-storage profile cannot enable ``rust_client``: MSC uses its Python range reader to inspect the Parquet footer and lazily read manifest row groups. This restriction does not apply to direct object-source profiles. An object source may use ``rust_client`` when that direct provider supports the required exact byte-range reads; MSC will use it for the source ranges after manifest validation.

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
           # Defaults to 64 MiB; 0 disables decoded row-group retention.
           manifest_row_group_cache_size_bytes: 67108864

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

Python producers can obtain the exact authoring schema without importing PyArrow until the helper is called:

.. code-block:: python

   from multistorageclient.manifest import virtual_manifest_v2_schema

   schema = virtual_manifest_v2_schema()

The helper is a conformance aid, not a production manifest writer. In the physical Parquet schema, ``metadata`` is encoded as an optional ``MAP`` with required UTF-8 ``key`` and ``value`` leaves:

.. code-block:: text

   optional group metadata (MAP) {
     repeated group key_value {
       required binary key (STRING);
       required binary value (STRING);
     }
   }

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
     - optional map of required UTF-8 string keys to required UTF-8 string values
     - Logical-file custom metadata. Duplicate keys are rejected; map order is ignored. Null and an empty map are distinct.
   * - ``chunk_index``
     - required signed 32-bit integer
     - Zero-based reconstruction order within ``key``.
   * - ``chunk_size_bytes``
     - required signed 64-bit integer
     - Bytes contributed by this chunk.
   * - ``chunk_kind``
     - required UTF-8 string
     - One of ``object`` or ``service``.
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

Rows must be strictly sorted in ascending ``(key, chunk_index)`` order. Keys use unsigned lexicographic order over their valid UTF-8 bytes (equivalent to Unicode scalar-value order), and ``chunk_index`` uses numeric order. MSC rejects duplicate, descending, or noncontiguous key/chunk positions instead of regrouping an unsorted manifest in memory. Each logical file requires contiguous indexes beginning at zero, positive file and chunk sizes, and a checked signed-64-bit chunk sum equal to ``size_bytes``. Every row for a key must agree on the file-level columns after native-map normalization. Logical keys are not prefix-free: a file ``a`` may coexist with descendants such as ``a/b``.

Sorted rows for one key may cross batches, data pages, and row groups, and row groups remain physical batching rather than semantic partitions. Producers should nevertheless keep all rows for a key in one row group when practical, and should avoid splitting a key across data pages, because key-local layout improves manifest scan locality. A key must never be interleaved with another key.

A zero-row Parquet file with the complete schema represents an empty dataset. V2 does not represent zero-byte logical files and does not define an ``empty`` chunk kind. Object rows require only their three ``source_*`` fields; service rows require only their three ``service_*`` fields. Cross-kind fields are rejected. ``source_offset + chunk_size_bytes`` must also remain representable as a signed 64-bit integer.

Lazy Validation, Lookup, and Retention
======================================

The footer/schema check establishes the manifest artifact and its row-group layout, but it is not an eager validation pass over every row. Path normalization, row ordering, chunk semantics, binding references, file metadata consistency, and file-size totals are validated when the relevant rows are decoded. An exact-key operation, including ``info``, ``is_file``, ``read``, or ``open``, decodes and validates the whole selected logical file before it plans or performs object-source or service I/O. A ranged logical read therefore still validates every chunk row for its file, not merely the chunks that overlap the requested range.

``list_objects_recursive`` is intentionally streaming. It traverses manifest/key order, validates each complete logical file as it is reached, and can yield valid earlier files before discovering a malformed later row or file. Callers that require an all-or-nothing validation result must force a complete traversal before treating the listing as authoritative.

For exact-key lookup, complete authoritative ``key`` minimum/maximum statistics permit a binary search to the candidate row groups. If any nonempty row group lacks usable ``key`` statistics, MSC emits a warning at construction and exact lookups fall back to a potentially slow linear row-group scan. Statistics that are structurally inconsistent (for example, invalid key values, inverted bounds, reported null keys, or globally unordered intervals) fail construction. Structurally valid but incorrect bounds cannot be detected by the reader; they are a producer contract violation and can make lookup results incorrect.

PyArrow presently reports only whether the ``key`` column advertises both column and offset page indexes; it does not read or use their contents. A future Rust Arrow backend could consume those indexes. We strongly recommend that producers write complete and exact ``key`` statistics, use reasonable row-group sizes, keep one logical key in one row group and, where practical, in one data page, and write page indexes. Those choices preserve both the current binary row-group search and a future page-level lookup path.

``manifest_row_group_cache_size_bytes`` bounds retained decoded Parquet row groups. It defaults to ``67108864`` bytes (64 MiB), and ``0`` disables row-group retention. The cache is an LRU over complete immutable row groups, accounts Arrow buffers conservatively, and bypasses retention for a row group that cannot fit as a whole; it never retains only part of an oversized row group. This cache is independent of MSC's logical-content read cache. The provider retains no decoded logical-file plans, chunks, or range plans outside the bounded row-group cache.

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

Virtual-manifest profiles default ``open(..., prefetch_file=None)`` to streamed reads (the effective value is ``False``). A streamed ``ObjectFile`` asks for only the logical ranges consumed by the caller, but those ranges may be satisfied from MSC's logical read cache. Set ``prefetch_file=True`` to materialize the complete virtual file through MSC's normal cache or in-memory download policy instead. The synthetic manifest ETag participates in the normal MSC source-version cache check, so an enabled source-version check invalidates cached logical bytes when the decoded manifest identity changes. Set ``disable_read_cache=True`` to bypass the logical read cache; logical-path and metadata resolution still occur before the direct source read. Fsspec ``get_file`` uses MSC's native optimized transfer for a path destination, writing to a sibling temporary file and atomically replacing the destination after success. A file-like or ``outfile`` destination instead receives uncached streamed bytes.

``download_file`` reconstructs the file in 8 MiB logical windows. A path destination is written to a sibling temporary file and atomically replaced only after success; a failed download cleans the temporary file. Filesystem materialization uses actual reconstructed bytes rather than a source locator.

Capabilities
============

.. list-table:: Virtual-manifest provider capabilities
   :header-rows: 1
   :widths: 35 18 47

   * - Operation
     - Supported
     - Notes
   * - full and ranged ``read``
     - yes
     - Ranges are clipped at EOF and planned across only touched chunks.
   * - binary/text ``open`` and seek
     - yes
     - Includes UTF-8 and line records crossing chunk boundaries.
   * - ``info``, ``is_file``, ``is_empty``, list, recursive list, and glob
     - yes
     - Directories are synthesized from sorted logical keys.
   * - ``download_file(s)`` and filesystem materialization
     - yes
     - Path publication is atomic.
   * - fsspec ``open``, ``cat_file``, and ``get_file``
     - yes
     - ``cat_file`` uses standard end-exclusive slicing, including negative endpoints.
   * - sync source
     - yes
     - The manifest can supply bytes to a writable target.
   * - sync target or any mutation
     - no
     - Rejected before worker construction or file I/O.
   * - presigned URLs
     - no
     - A logical file may span unrelated dependencies.
   * - MSFS/FUSE mount
     - no
     - Python is the only supported implementation layer for v2.

Path and HTTP Contract
======================

Logical keys and object ``source_path`` values are normalized relative POSIX paths. They must not be empty, absolute, URI-like, use backslashes or NUL bytes, contain repeated/trailing separators, or contain ``.`` or ``..`` segments. A bare path ``a`` resolves an exact logical file ``a`` when it exists, even if ``a/b`` also exists. A path ending in ``a/`` explicitly selects descendants. In a shallow listing where the file ``a`` and descendants such as ``a/b`` would otherwise map to the same visible entry, the direct file has precedence over a synthesized directory.

Service paths are stricter: they are unescaped relative Unicode paths. They also reject percent escapes, queries, fragments, authorities, and paths outside an allowlisted prefix. Prefix matching is segment-aware: an allowed ``render`` prefix accepts ``render`` and ``render/...``, but not ``rendered/...``. Service query names must be non-empty and appear in ``allowed_query_parameters``; query values may be empty, and duplicate name/value pairs retain their order. The configured HTTP base URL rejects controls, backslashes, encoded or userinfo authorities, and ambiguous separators before MSC rebuilds its canonical transport origin. Configured header names must be RFC HTTP tokens; protocol-controlled headers such as ``Range`` and ``Host`` cannot be supplied, including whitespace lookalikes.

Each touched service subrange sends one ``GET`` with an exact ``Range`` request and ``Accept-Encoding: identity``. Distinct service calls are not coalesced, and repeated logical reads may touch the same service chunk more than once. Redirects are disabled. A service must return ``206`` with matching ``Content-Range`` (including the total), matching ``Content-Length``, and no content encoding other than absent or ``identity``. MSC reads at most the requested byte count plus one sentinel byte and rejects short or oversized bodies. Non-TLS connection failures, timeouts, and HTTP ``408``, ``429``, ``500``, ``502``, ``503``, and ``504`` are retryable. TLS, certificate, and protocol-verification failures fail closed and are nonretryable, as are redirects, authentication failures, malformed range metadata, and other statuses.

A retryable subread failure is surfaced to the enclosing MSC logical-read retry. The retry therefore repeats the complete requested logical range, including any sibling chunks that already succeeded; it is not an independent per-service-call retry. Before surfacing a failed attempt, MSC cancels work that has not started and waits for already-running sibling reads, preventing abandoned requests from overlapping the next attempt.

ETags, Caching, and Immutability
================================

Each logical file receives a synthetic ETag beginning with ``msc-v2-sha256:``. It is a SHA-256 digest over canonical file metadata, chunks in index order, ordered service query pairs, and the used binding alias, non-secret binding identity, and ``binding_revision``. Credential material and configured secret headers are excluded.

Changing file metadata, chunk paths/offsets/boundaries, query order, a used binding identity, or a used binding revision changes the ETag. Changing an unused binding does not. This gives normal MSC caches a stable, dependency-aware identity without exposing credentials.

The ETag identifies the decoded file plan and configured bindings; it is not a checksum of reconstructed file bytes. The manifest object, referenced source objects, deterministic service outputs, and binding locations must remain immutable for the manifest lifetime. In particular, the manifest object must never be replaced at its configured path: MSC reads the footer at construction, then opens independent streams for later lazy row-group reads. Replacing it can combine one artifact's footer with another artifact's rows. If any dependency can produce new bytes, publish a new manifest and/or change its ``binding_revision``. MSC verifies only the exact length of each fetched range. Same-length byte drift is undetectable and can produce mixed-version reads or apparently valid stale cache hits under an unchanged ETag. Disabling source-version checking intentionally permits cached logical bytes to remain stale.

The cache belongs to the virtual logical profile. Object-source aliases invoke their configured direct provider range API and bypass that source profile's logical routing, replicas, metadata provider, and MSC read cache.

Decoded logical-file plans, chunks, range plans, and ordered query pairs are immutable for their use, but an initialized provider does not retain a whole decoded manifest snapshot. It retains footer metadata and, subject to ``manifest_row_group_cache_size_bytes``, decoded row groups; later file lookups decode their plans from those rows or from new row-group reads.

Publishing and Reloading
========================

Publish a complete, validated Parquet file at a new immutable path, such as a release- or content-addressed path. Do not overwrite or replace a manifest that existing clients may use, even if the replacement has the same schema or length: footer and later row-group reads are separate operations. After publication, update ``manifest_path`` in configuration and construct a new client (or reload its configuration) to adopt the release.

There is no live manifest reload. Existing providers continue using their construction-time footer metadata and configured binding revisions, while later lookups read row groups lazily from the same immutable manifest object.

Limitations
===========

Virtual manifests are intentionally narrow in v2:

* The provider is read-only. It has no manifest writer and does not support writes, deletes, copies, uploads, appends, or symlinks.
* Zero-byte logical files and zero-byte chunks are not representable. A zero-row manifest represents an empty dataset.
* There are no chunk checksums, source version pins, conditional range reads, live reload, or multipart manifest support.
* The Parquet file is a single immutable v2 manifest; v1 indexes and parts are not accepted as virtual manifests.
* The Multi-Storage File System (MSFS/FUSE) does not support virtual-manifest profiles.
* HTTP services must be deterministic for the configured path/query/range contract. MSC does not follow redirects or infer service authorization rules.

For v1 metadata manifests that accelerate discovery while another provider still supplies the bytes, see :doc:`/user_guide/manifests`.
