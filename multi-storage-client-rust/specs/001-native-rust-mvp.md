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
[types.rs](../src/types.rs), and backend contracts in
[provider/traits.rs](../src/provider/traits.rs). Implementation modules contain
client policy, transfers, factories, and backend I/O. Applications own Tokio;
client clones share immutable state, providers, connection pools, and cleanup.

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

`StorageClientBuilder::from_provider(Arc<dyn StorageProvider>)` provides direct
injection. The builder selects provider factories, retry policy, and the
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

## StorageProvider implementations

Both providers implement typed metadata, byte streams, ranged reads, writes, deletion,
copy, listing, and symlinks through `StorageProvider`. Capabilities describe
optional operations. Unsupported conditions fail before mutation; failed
conditions return `PreconditionFailed`. Errors retain retry classification and
redacted context without exposing SDK types or credentials.

### POSIX

[PosixProvider](../src/provider/posix/unix.rs) anchors operations to an existing
absolute root using directory descriptors and rejects symlink/root escapes.
Filesystem work uses bounded blocking execution; concurrency defaults to 32.
Raw metadata inspects the entry itself, while client reads resolve safe internal
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

The client owns retry policy; SDK retries are disabled. Retryable reads and
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
range with one client budget, without SDK retries, adaptive resumes, or an
outer whole-file retry.

[Checkpoint state](../src/client/download/state.rs) uses
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

Existing tests cover API contracts, provider conditions and listing order,
threshold/part boundaries, bounded concurrency, out-of-order parts, retry
exhaustion, historical versions, restart recovery, cancellation, destination
preservation, and cleanup ownership. Emulator success is not live-cloud or
performance qualification. User documentation, examples, and quickstarts are
outside this MVP implementation.
