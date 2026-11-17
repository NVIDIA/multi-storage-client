# 001: Native Rust MVP

## Implementation boundary

The independent `multi-storage-client-rust` package implements an async
`StorageClient`, POSIX and S3 storage providers, configuration/credentials, and
an `msc-benchmark` binary. `s8k` is an alias using the S3 implementation.
This spec records the implementation in this PR; further features get their
own specs when implemented.

The package has its own Cargo.lock and pinned Rust toolchain and uses the root
Nix environment. The default `s3` feature enables S3/S8K. Without default features,
core types and POSIX are available on Unix. Python public APIs, PyO3, and the
accelerator dependency graph are unchanged.

Public operations live in [client/api.rs](../src/client/api.rs), shared types in
[types.rs](../src/types.rs), the public provider contract in
[provider/traits.rs](../src/provider/traits.rs), and backend extension hooks in
[provider/backend.rs](../src/provider/backend.rs). Shared policy and transfers
live in [provider/base/](../src/provider/base/mod.rs); `StorageClient` delegates
to that provider contract. Applications own Tokio. Client and base-provider
clones share immutable state, backend connection pools, and cleanup.

## StorageClient API contract

All instance operations are async and borrow `&self`; `builder` is an associated,
synchronous constructor. Remote paths accept `impl AsRef<str>`, local paths accept
`impl AsRef<Path>`, and option arguments are explicit. `Result` uses `MscError`.

| Method | Return type |
|---|---|
| `builder(config: ResolvedConfig, profile: impl Into<String>)` | `StorageClientBuilder` |
| `info(path, InfoOptions)` | `Result<ObjectMetadata>` |
| `is_file(path)` | `Result<bool>` |
| `is_empty(prefix)` | `Result<bool>` |
| `read(path, ReadOptions)` | `Result<Bytes>` |
| `write(path, Bytes, WriteOptions)` | `Result<()>` |
| `upload_file(remote, local, WriteOptions)` | `Result<()>` |
| `download_file(remote, local, ReadOptions)` | `Result<()>` |
| `copy(source, destination, CopyOptions)` | `Result<()>` |
| `make_symlink(path, target: &str)` | `Result<()>` |
| `delete(path, recursive: bool, DeleteOptions)` | `Result<()>` |
| `delete_many(paths: &[ObjectPath], DeleteOptions)` | `Result<BatchResult>` |
| `list(ListOptions)` | `Result<MetadataStream>` |
| `list_recursive(ListOptions)` | `Result<MetadataStream>` |
| `shutdown()` | `Result<()>` |

`StorageClientBuilder::from_provider(Arc<dyn StorageBackend>)` provides direct
backend injection, automatically wrapped in `BaseStorageProvider`. The builder selects provider factories, retry policy, and the
in-memory read limit before `build().await`. Construction validates one selected
profile and does not install a global client or runtime.

- Keys are relative to the profile root; providers apply `base_path` once.
  `ResolvedConfig::resolve` handles MSC URLs and absolute local paths separately.
  `ObjectPath` normalizes duplicate separators and rejects absolute keys, dot
  segments, backslashes, and NULs. Source versions are opaque and backend-scoped.
- `ReadOptions` selects full, bounded, or suffix byte reads and optional source
  conditions. Bounded ranges are half-open and clip at EOF. The default read
  memory limit is 64 MiB; an option or builder setting overrides it. File
  downloads transfer whole objects without applying that byte-read size limit.
- `InfoOptions.strict` defaults to true. S3 can recognize virtual directories
  after a missing exact HEAD; false disables that fallback. An explicit trailing
  `/` queries a remote directory for either value. Symlinks resolve internally
  with cycle detection and an eight-hop limit.
- Write, upload, download, copy, and symlink creation return unit after validation.
  `info` retrieves stored metadata when needed. File methods use remote key first,
  local path second. Applications compose bounded concurrency across files.
- Copy inherits source attributes when destination attributes are empty; nonempty
  attributes replace them. An omitted content type inherits the source type.
- `make_symlink(path, target)` takes two profile-relative keys, matching Python.
  For example, `("links/alias", "data/file")` stores `../data/file` and resolves
  to `data/file`. POSIX encodes the target after resolving the link's physical
  parent, preserving this meaning through directory aliases.
- Single deletion checks the exact key or requested version and returns
  `NotFound` if missing. It deletes a link itself. Recursive deletion returns
  `Unsupported`. Batch deletion treats missing keys as success, retains original
  input indices and item errors, and retries only failed retryable items.
- File-only listing is lexical. Directory-inclusive listing retains provider
  order. Options support prefix, recursion, exclusive `start_after`, inclusive
  `end_at`, attributes, attribute filters, and skipping unsupported listed keys.
  `MetadataStream` can fail after opening. Only file-only streams resume after
  emitted entries, using the last scanned key.
- `shutdown` drains provider-owned cleanup, including cancelled multipart uploads,
  while the application's runtime is alive. It does not close the client to
  later operations; Rust destructors cannot await this cleanup.

## Provider contract and extension boundary

### Public StorageProvider

`StorageProvider` is an object-safe async trait with Python's operation names.
Paths are `&str` keys relative to the profile root; local paths are `&Path`.
Options are explicit Rust structs. Listing returns a fallible asynchronous
metadata stream rather than Python's synchronous iterator.

| Method | Return type |
|---|---|
| `get_object_metadata(path, InfoOptions)` | `Result<ObjectMetadata>` |
| `is_file(path)` | `Result<bool>` |
| `get_object(path, ReadOptions)` | `Result<Bytes>` |
| `put_object(path, Bytes, WriteOptions)` | `Result<()>` |
| `upload_file(remote, local, WriteOptions)` | `Result<()>` |
| `download_file(remote, local, ReadOptions)` | `Result<()>` |
| `copy_object(source, destination, CopyOptions)` | `Result<()>` |
| `make_symlink(path, target)` | `Result<()>` |
| `delete_object(path, DeleteOptions)` | `Result<()>` |
| `delete_objects(paths: &[ObjectPath], DeleteOptions)` | `Result<BatchResult>` |
| `list_objects(ListOptions)` | `Result<MetadataStream>` |
| `list_objects_recursive(ListOptions)` | `Result<MetadataStream>` |
| `shutdown()` | `Result<()>` |

Provider metadata inspects the entry without following its final symlink,
matching Python's provider boundary. `StorageClient.info` resolves links through
a single shared, bounded resolver, also used by reads and `is_file`.
`get_object` follows links and enforces byte-read limits. The trait supplies
`is_file` and recursive-listing defaults. Mutations return unit; batch deletion
retains indexed outcomes as the previously agreed Rust difference. `shutdown`
is necessary because Rust destructors cannot await cleanup. Multi-file upload
and download wrappers, glob, and presigned URLs remain outside this API.

### BaseStorageProvider and StorageBackend

`BaseStorageProvider<B>` implements the public contract for any
`B: StorageBackend`, including `dyn StorageBackend`. It owns an `Arc<B>`, retry
policy, and byte-read limit. `new(Arc<B>)` selects defaults;
`with_retry_policy` validates a replacement budget, and
`with_max_in_memory_bytes` changes the default byte-read limit. Clones share the
same state; separately configured wrappers may share one backend without sharing
retry budgets or limits.

The base struct reuses the implementation formerly in `StorageClient`: key
validation, symlink resolution, strict directory detection, bounded byte reads,
attribute filtering, listing retry cursors, publication validation, native-copy
selection and streaming fallback, batch-delete fallback and item retries, and
local-file staging, multipart download, and checkpoint recovery. Client methods
delegate without an additional retry loop or duplicated transfer implementation.

An external backend implements eight required hooks: `name`, `capabilities`,
`get_object_metadata`, `get_object`, `get_object_range`, `put_object`,
`delete_object`, and `list_objects`. The core operation names match Python.
These hooks perform one SDK attempt using validated `ObjectPath` keys; reads and
writes exchange `ByteStream`, and publication returns `PutResult` for internal
generation validation. Backend-specific physical prefixes are applied once by
the adapter, retaining POSIX descriptor anchoring and cloud key conventions.

Optional hooks have defaults for generation lookup, suffix reads, batch deletion,
native copy, virtual directories, symlinks, metadata preservation, multipart
parts, backend identity, and cleanup. Capabilities must accurately advertise
support. Native copy defaults to `Unsupported` before mutation so the shared
provider selects streaming; batch deletion can use a native backend override.
SDK retries must be disabled. Safe multipart part retries use the wrapper's
operation policy without adding a whole-publication retry budget.

To extend MSC from another crate:

1. Define an SDK-owning struct and implement public `StorageBackend` hooks.
2. Use `BaseStorageProvider::new(Arc::new(backend))` for direct Python-named
   provider operations, or inject the backend through the client builder.
3. For configuration-based construction, register a `ProviderFactory` returning
   `Arc<dyn StorageBackend>` under a custom tag. Supply that registry to both
   `ConfigLoader::with_registry` and `StorageClientBuilder::with_provider_registry`.
   The builder applies the selected policy and wraps the backend automatically.

Registration uses the existing process-local registry. No core dispatch edits,
new runtime, Python imports, or dynamic-library loading are required. External
Rust providers are compiled and linked into the application.

This replaces the former primitive `StorageProvider` extension API. Existing
native adapters migrate to `StorageBackend`; `head`, `get`, `put`, `copy`,
`delete`, `delete_many`, and `list` become `get_object_metadata`, `get_object`,
`put_object`, `copy_object`, `delete_object`, `delete_objects`, and `list_objects`.
Range/suffix/version hook names use the same `get_object` vocabulary. Direct
injection and factory trait objects change to `Arc<dyn StorageBackend>`.
`StorageClient` data-operation signatures, configuration schema, Python, and
PyO3 remain unchanged. No dependencies are added.

## Built-in backend implementations

POSIX and S3 implement `StorageBackend` and receive the same shared provider
behavior. Unsupported conditions fail before mutation; failed conditions return
`PreconditionFailed`. Errors retain retry classification and redacted context
without exposing SDK types or credentials.

### POSIX

[PosixProvider](../src/provider/posix/unix.rs) anchors operations to an existing
absolute root using directory descriptors and rejects symlink/root escapes.
Filesystem work uses bounded blocking execution; concurrency defaults to 32.
Raw metadata inspects the entry itself, while shared provider reads resolve safe internal
symlinks. File writes publish atomically through same-filesystem staging.
POSIX retains JSON attributes and optionally preserves permissions and mtime.
Upload sources are regular files opened without following a final symlink;
retries reuse the handle and detect source size/modification changes.
Directory ordering sorts each directory with a trailing `/` on directory names.

### S3 and S3-compatible storage

[S3Provider](../src/provider/s3/mod.rs) uses `object_store` streaming I/O plus
native ListObjectsV2, COPY, and batch DELETE requests. It supports configurable
endpoints, addressing, TLS, timeouts, metadata, and static/file/default AWS
credentials with refresh and rotation. `s8k` uses the same adapter and feature.

File-only listings are lexical across pages; shallow pages expose CommonPrefixes
before Contents. Zero-byte entries may require HEAD to recognize emulated
symlinks. Raw unsupported keys fail or are skipped according to listing options.
PUT/COPY retain publication-response metadata without adding a completion HEAD.
Native batch deletion sends at most 1,000 keys per request and validates item
correspondence. Unsupported conditional multipart publication fails explicitly.

The shared base provider owns retry policy; SDK retries are disabled. Retryable reads and
replayable writes, uploads, copies, and deletes use the configured budget,
including unconditional mutations. Defaults are three attempts, one-second
initial delay, multiplier 2, a 30-second delay ceiling, and jitter. A conditional
conflict after a retried mutation returns `AmbiguousOutcome`; cancellation and
uncertain multipart completion are not replayed.

## File transfers and restart recovery

Uploads stream from their opened source without a whole-file temporary copy.
S3 shares transfer settings between uploads and file downloads: an 8 MiB
`multipart_threshold`, 8 MiB `part_size`, and `multipart_concurrency` of 4 by
default. Python-compatible aliases include `multipart_chunksize` and
`max_concurrency`. Objects at or below the threshold use streaming; larger
eligible objects use bounded multipart transfers.

Downloads obtain metadata for the requested historical version or current
object. Large S3 downloads pin every range to that version or observed strong
ETag, validate range/total/generation/streamed length, and avoid per-part HEAD.
Missing usable validators select streaming. Each failed part retries its own
range with one provider budget, without SDK retries, adaptive resumes, or an
outer whole-file retry.

[Checkpoint state](../src/provider/base/download/state.rs) uses
`.<destination>.msc-dl/` with an exclusive OS lock, `state.json`, and validated
chunk files. The `msc-download-v1` marker records key, size, ETag, version,
chunk size, and an optional backend fingerprint. Fresh matching metadata allows
completed chunks to survive failure and process restart; recovery retains saved
partitioning and uses current concurrency. Conflicting markers fail safely;
lock contention uses independent transient staging.

Parts stream into separate files. A bounded merge validates total length and
synchronizes staging before the active caller atomically publishes the destination.
Failure/cancellation preserves an existing destination and completed checkpoints.
Outstanding blocking work retains staging and lock ownership; the final directory
owner explicitly releases the lock, even if a child inherited a file descriptor.
Cleanup removes recognized entries only; cleanup failure after publication is
logged without failing the download. Unix staging uses private permissions.
Peak staging space is about twice the object size, plus any existing destination.

## Configuration and validation

The loader supports YAML/JSON discovery, ordered one-level includes, environment
expansion, provenance, profile/option validation, and redacted diagnostics.
Shared Python configuration can retain deferred settings and unsupported
profiles; selecting unsupported profile behavior fails before storage I/O.

The independent [justfile](../justfile) keeps three separate gates:

- `just build`: formatting, all-target/all-feature Clippy, compilation, unit/
  doctests, and POSIX/HTTP/S3-simulator integration. Unit and POSIX/client tests
  cover both feature configurations; S3-specific checks use the S3 feature.
- `just run-e2e-tests`: a real backend selected by explicit `MSC_E2E_PROFILE`
  configuration and credentials.
- `just run-benchmarks`: POSIX/S3 CLI smoke checks, with setup excluded from
  transfer timing, validated successful byte counts, and cleanup of owned keys.

The shared public-provider contract suite runs directly against POSIX, S3, and
an integration-test backend implemented outside the library. It covers byte and
file APIs, range/suffix reads, metadata, attributes/filtering, native and fallback
copy/batch deletion, and symlink semantics where supported. Factory tests verify
external registration, independent wrapper budgets, exact retry counts, and
byte-read limits without restricting file downloads.

Existing tests also cover client API contracts, backend conditions and listing order,
threshold/part boundaries, bounded concurrency, out-of-order parts, retry
exhaustion, historical versions, restart recovery, cancellation, destination
preservation, and cleanup ownership. Emulator success is not live-cloud or
performance qualification. User documentation, examples, and quickstarts are
outside this MVP implementation.
