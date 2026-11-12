# 003: Storage client

- Status: Draft for review
- Depends on: 001, 002
- Integration dependency: at least one adapter from 004

## Summary

`StorageClient` is the public async facade for a resolved single-backend
profile. It owns path translation, retry policy, metadata-provider coordination,
cache integration, replica hooks, and consistent convenience operations. It
does not expose cloud SDK types.

This spec covers the single-backend client. Composite routing is spec 010.

## Construction and ownership

```rust
let client = StorageClient::builder(resolved_config, "profile")
    .with_provider_registry(registry)
    .build()
    .await?;
```

Construction resolves and validates exactly one profile, builds credentials
and provider instances, attaches an optional metadata provider, creates one
retry policy, and conditionally wraps reads with the local cache. The resulting
client is cheap to clone and internally shares immutable state with `Arc`.

`StorageClient` is `Send + Sync`. Dropping a clone does not cancel operations
owned by other clones. Explicit background services, such as cache maintenance,
have cancellation and join handles owned by the final shared state.

## Public async API

### Read and metadata

- `read(path) -> Bytes` for explicitly bounded/small-object use;
- `read_range(path, ByteRange, ReadOptions) -> Bytes`;
- `reader(path, ReadOptions) -> ObjectReader` implementing
  `tokio::io::AsyncRead + AsyncSeek` for sequential and random-access reads;
- `download_to(remote, local, options)`;
- `info(path, options) -> ObjectMetadata`;
- `is_file(path)` and `is_empty(prefix)`;
- `list(options) -> Stream<Result<ObjectMetadata>>`;
- `list_recursive(options)` and `glob(pattern, options)` convenience streams;
- `presigned_url` only when the provider capability exists.

`read` enforces a configurable maximum in-memory object size or requires an
explicit opt-out. Large objects use `reader` or `download_to`.

`ObjectReader` is the Rust-native file-read handle. It supports Tokio's
`AsyncReadExt` and `AsyncSeekExt` helpers without text modes or Python file
emulation. It uses bounded range requests, with cache chunks when caching is
enabled and direct provider ranges otherwise. Seek itself changes the cursor
without downloading data; construction obtains the size/version needed for
seek-from-end and consistent reads. Providers must advertise ranged reads for
this seekable handle; the underlying byte stream remains available through the
provider contract. Reader behavior is detailed in 005.

### Write and mutation

- `write(path, Bytes, WriteOptions) -> ObjectMetadata`;
- `writer(path, size_hint, WriteOptions) -> ObjectWriter` implementing
  `tokio::io::AsyncWrite` when streaming writes are supported;
- `upload_from(remote, local, options)`;
- `copy(source, destination, options)`;
- `delete(path, recursive, options)`;
- `delete_many(paths, options) -> BatchResult`;
- `make_symlink(path, target)` when supported; and
- `commit_metadata()`.

Batch upload/download conveniences use bounded concurrency and preserve item
indices in partial errors. `delete_many` treats a missing object as success,
matching the idempotent MSC behavior.

`ObjectWriter` is sequential, not `AsyncSeek`. `write_all` accepts bytes under
bounded backpressure; `flush` drains buffered bytes without publishing the
object. Trait `shutdown` ends byte input but does not commit. Explicit async
`finish(self) -> ObjectMetadata` drains, completes the provider upload, stages
metadata, and invalidates cache before returning success. `abort(self)` cancels
and awaits cleanup. Dropping an unfinished writer never commits; temporary
files/multipart state receive best-effort cleanup, with owned cleanup tasks
joined on client shutdown. No async operation runs implicitly in `Drop`.

`sync_from` and `sync_replicas` may be convenience methods, but their
implementation and types live in specs 006 and 009.

## Path semantics

Client methods accept profile-relative object paths, not URLs. A separate
resolver returns `(client, ObjectPath)` for URLs and absolute POSIX paths.

The client's configured `base_path` is applied exactly once in the provider
adapter. Returned metadata keys are relative to the profile root. The empty
path is the root; `info("")` returns synthetic directory metadata. `info()` is
the public client name; it may call the internal provider `head` primitive.

Normalization collapses duplicate separators and rejects traversal. Object
store keys remain case-sensitive. POSIX adapters enforce filesystem root
containment after symlink resolution according to the selected symlink policy.

## Metadata-provider coordination

Without a metadata provider, logical and physical keys are identical.

With a metadata provider:

### Reads

1. Resolve the logical path.
2. Require `Exists`.
3. Route to the resolved physical key.
4. Merge logical metadata/attributes where the API requires them.

### Writes

1. Check writability and overwrite policy.
2. Allocate a physical key.
3. Write the physical object.
4. Read authoritative physical metadata when required.
5. Stage the logical add/update with merged user attributes.
6. Commit only when requested by call options or autocommit policy.

### Deletes

Resolve the logical object, perform hard or soft deletion according to metadata
capabilities, stage the logical removal, and commit according to policy.

Metadata updates are not distributed transactions with object storage. Errors
identify the completed stage and return enough information for repair. A
physical write followed by a metadata failure is never reported as a clean
success.

Autocommit is an explicit task owned by the client, not a process-exit hook.
Library users can await shutdown/flush. The CLI flushes before exit.

## Cache integration

The cache is a read-through decorator used only when both global cache config
exists and `caching_enabled` is true. The POSIX provider bypasses caching.

Reads give the cache the resolved physical identity and logical/profile
namespace. Every successful mutation invalidates the full object and every
chunk for the logical/physical identity, even when source-version validation is
disabled. Cache failure normally degrades to direct provider I/O as specified
in 005.

## Retry and idempotency

The client is the single owner of the retry budget from 001. Adapters return
typed errors and do not add an unaccounted retry loop.

- Info/list/read and conditional delete are idempotent.
- A put is retryable only when the provider supports an idempotency token,
  multipart upload ID, or conditional create/update that prevents duplicates.
- Batch retries include only failed retryable items.
- Cancellation is never retried.

## Listing and globbing

All listing variants preserve the globally sorted provider contract.
`start_after` is exclusive and `end_at` inclusive. `include_directories`
controls immediate directory synthesis for delimited listings.

Glob matching uses normalized profile-relative keys and an AWS-S3-compatible
syntax for `*`, `?`, character classes, and negated classes. Ordered include/
exclude selection is shared with sync and CLI rather than separately
reimplemented.

`ListOptions.attribute_filter_expression` evaluates the Python attribute
expression grammar in the client. Comparisons, parentheses, `AND` and `OR`
work across POSIX, manifests, and native cloud attributes. The client fetches
attributes internally even when the result omits them. Invalid or excessively
large/deep expressions fail before provider I/O. Filtering retains the scanned
resume cursor so retries do not repeat rejected records.

## File transfers

`download_to` writes a temporary file in the destination filesystem, validates
the expected length/version when available, optionally applies mtime and
attributes, then atomically renames. Cancellation and errors remove the temp
file.

`upload_from` streams from the file and uses multipart upload when warranted.
It never reads the whole file into memory. Symlink behavior is explicit in
options and defaults to following only safe internal links.

## Provider metrics

Provider-level OpenTelemetry metrics are required in the MVP as defined in
011. Client construction decorates provider instances with a shared meter;
applications can inject their own meter without MSC owning or shutting down
their SDK. The CLI can initialize an owned SDK from configuration.

Instrumentation is inside the retry boundary and outside adapter internals.
Each provider attempt is counted once, including stream completion/error/drop.
A cache hit emits no provider read; cache validation and misses count only the
provider operations they actually invoke. Client conveniences do not duplicate
provider measurements. Without an injected meter, library clients use a no-op
meter; telemetry configuration is initialized explicitly through the helper in
011. Exporter failure never changes storage results. Traces are a follow-up.

## Intentional differences from Python

- No Python file object or temp-file-backed `open()` facade.
- No process-exit autocommit.
- No dynamic method forwarding into provider SDKs.
- One retry budget rather than Python plus Rust retries.
- Range reads through replicas must preserve the requested range.
- Cache invalidation occurs on all successful mutations.

## Acceptance criteria

- A fake provider exercises every method without adapter-specific branching in
  client code.
- Construction tests cover provider, credential, metadata, retry, and cache
  wiring plus redacted failures.
- Path tests prove base paths are added/removed once and traversal is rejected.
- Read tests cover bytes, streaming, range, EOF, metadata routing, conditional
  failure, and in-memory size protection.
- Reader tests use Tokio read/seek helpers with and without caching, including
  backward seeks, seek-from-end, bounded buffers, and generation changes.
- Write tests cover create, overwrite, conditions, attributes, cancellation,
  metadata staging/commit failure, and cache invalidation.
- Writer tests prove flush/shutdown/drop never publish, finish publishes once,
  and abort awaits cleanup without leaking transfer permits.
- Listing tests cover root, ordering, pagination boundaries, directories, glob,
  and partial stream errors.
- Batch tests verify bounded concurrency, stable indices, partial success, and
  retry-only-failed behavior.
- File transfer tests prove bounded memory, atomic destination publication, and
  temp cleanup.
- Cloned clients can perform concurrent operations without a global lock.
- `info()` returns object/synthetic-root metadata; no public `head()` alias is
  required.
- Metrics integration tests cover retries, streams, cache hits/misses, no-op
  meters, and borrowed versus owned exporter lifecycle as specified in 011.

## Source evidence

- `src/multistorageclient/client/{client,types,single}.py`
- `src/multistorageclient/types.py`
- `src/multistorageclient/shortcuts.py`
- `src/multistorageclient/file.py`
- `tests/test_multistorageclient/unit/client/test_client.py`
- `tests/test_multistorageclient/unit/{test_file,test_shortcuts,test_retry}.py`
- Tokio's [AsyncRead](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html),
  [AsyncSeek](https://docs.rs/tokio/latest/tokio/io/trait.AsyncSeek.html), and
  [AsyncWrite](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html) contracts
