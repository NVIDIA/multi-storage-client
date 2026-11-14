# 001: Core types and async provider contract

- Status: Draft for review
- Depends on: 000

## Summary

This spec defines the provider-neutral types, error taxonomy, capability model,
and async traits on which all adapters and clients depend. The public contract
must express streaming, conditional operations, stable listing, and partial
batch failure without leaking a cloud SDK.

## Data model

### Paths

`ObjectPath` is a validated provider-relative UTF-8 key. It uses `/` separators,
has no leading slash, removes repeated separators, and rejects `.` and `..`
segments where they could escape a POSIX root. The empty path represents the
profile root.

Raw POSIX absolute paths and `msc://` URLs are resolved before constructing an
`ObjectPath`. Providers never receive a URL or profile name.

### Ranges

`ByteRange { offset: u64, length: u64 }` is half-open:
`offset..offset + length`. Construction checks overflow. A zero-length range
returns an empty buffer without calling the provider. A range beginning beyond
EOF returns empty; a range crossing EOF returns the available prefix.

### Metadata

`ObjectMetadata` contains:

- normalized key;
- `ObjectKind` (`File`, `Directory`, or `Symlink`);
- content length;
- UTC last-modified timestamp when known;
- opaque provider ETag and normalized `SourceVersion` when known;
- optional symlink target;
- user attributes as a deterministic JSON value map; and
- optional provider fields needed for diagnostics, not comparison.

Directory size is zero. The profile root can be represented as a synthetic
directory. Provider adapters must not invent an ETag from an mtime.

`SourceVersion` is opaque and scoped to a provider/backend. It can be used for
conditional reads only by the backend that produced it. Cross-provider sync
must not assume two ETags are comparable.

### Listing

`ListOptions` includes prefix, recursive/delimited mode, exclusive
`start_after`, inclusive `end_at`, whether directories are synthesized, and
whether attributes are required and a client-side attribute filter expression.
Cloud-native user metadata requires string values; manifests and POSIX retain
JSON types. Destination write and delete conditions may carry opaque, scoped
source versions.

The returned stream must be globally ordered by normalized key in ascending
bytewise lexical order. It must not yield duplicates. If a provider cannot
natively meet this contract, the adapter is responsible for external sorting
or a bounded prefix merge. `start_after >= end_at` is invalid.

## Error model

`MscError` has a stable `ErrorKind` and contextual fields:

- `NotFound`
- `PermissionDenied`
- `Unauthenticated`
- `AlreadyExists`
- `PreconditionFailed`
- `NotModified`
- `RateLimited`
- `Timeout`
- `Unavailable`
- `InvalidConfig`
- `InvalidPath`
- `Unsupported`
- `Cancelled`
- `Integrity`
- `Provider`
- `Io`

It may include operation, provider, redacted endpoint, key, HTTP status,
request ID, retry-after, and a source error chain. Secrets are never included.

Retryability is typed metadata derived by the adapter, not string matching.
`NotFound`, authentication/authorization failures, invalid input, and failed
preconditions are not retried by default. Timeouts, rate limiting, and service
unavailability may be retried when the operation is idempotent.

`BatchError` preserves every failed item's original index, source key,
destination key when applicable, and `MscError`. Successful items remain
successful; callers can retry only failed retryable items.

## Capability model

Providers expose immutable `ProviderCapabilities`, including:

- ranged reads;
- streaming reads and writes;
- conditional get/put/delete;
- batch delete;
- server-side copy and the scope in which it works;
- multipart upload;
- native lexical listing;
- symlink representation;
- presigned GET/PUT;
- user metadata; and
- stable source versions.

Unsupported optional operations return `ErrorKind::Unsupported`. Callers use
capabilities to select a safe fallback and never infer support from provider
type strings.

## Async storage provider

The object-safe `StorageProvider: Send + Sync` contract supports:

- `head(key)`;
- `get(key, conditions) -> byte stream`;
- `get_range(key, range, conditions) -> Bytes`;
- `put(key, byte stream, size, options) -> PutResult`;
- `delete(key, conditions)` and `delete_many`;
- `copy(source, destination, options)` when supported;
- `list(options) -> ordered metadata stream`;
- `make_symlink` when supported; and
- presigned URL generation when supported.

`GetConditions` and write/delete options carry `if_match`, `if_none_match`, or
equivalent normalized conditions. A provider must fail with
`PreconditionFailed`, not silently ignore an unsupported requested condition.

The primitive `get`/`put` streams are the canonical data path. Convenience
upload/download helpers belong in `StorageClient` so all providers inherit
consistent temporary-file, retry, and progress behavior.

The internal metadata primitive remains `head`; the public client names this
operation `info()`. A provider decorator from 011 maps it to the metric
operation `info`. The decorator sits inside the client retry loop so each
attempt is recorded once, and retains a completion guard for streaming
operations. It does not add a retry policy or depend on a cloud SDK.

## Credentials provider

`CredentialsProvider: Send + Sync` asynchronously returns credentials plus an
optional expiration. Concurrent refresh is single-flight. Refresh happens
before expiration with skew, and a refresh error never exposes credential
material.

Credentials are provider-specific tagged values behind a secret-safe wrapper.
Their `Debug` output is always redacted. File-backed credentials detect file
replacement and support atomic external rotation.

## Metadata provider

`MetadataProvider: Send + Sync` is separate from object storage. It provides:

- logical list, info, glob, and path resolution;
- physical path allocation for writes;
- staged add/remove updates;
- commit;
- writable/overwrite/soft-delete capabilities; and
- optional child profile routing for composite reads.

`ResolvedPath` has `Exists`, `Deleted`, or `Untracked` state, physical key, and
optional child profile. Implementations own their consistency and commit
contract; the storage provider must not know about manifests.

## Retry policy

One `RetryPolicy` applies at the outer provider call boundary. Defaults mirror
the user-visible Python client intent: three total attempts, one-second initial
delay, multiplier 2, capped jitter. Exact defaults remain configurable.

An operation is retried only when both conditions hold:

1. the adapter marks the error retryable; and
2. the operation is idempotent or protected by a stable idempotency token or
   conditional request.

Provider SDK retries must be disabled or charged against the same total budget.
Batch retries contain only failed retryable items.

## Symlinks

Symlink resolution supports at most eight hops and detects cycles. Targets are
normalized relative to the link's parent unless absolute provider semantics
are explicitly supported. `head(..., follow=false)` returns symlink metadata;
normal reads follow links unless the caller selects another policy.

## Acceptance criteria

- A deterministic fake provider passes the complete provider contract suite.
- Listing tests cover ordering, duplicate rejection, pagination, boundaries,
  directories, and a failure after partial output.
- Range tests cover zero length, overflow, EOF, and conditional reads.
- Error translation tests prove that no retry decision depends on text.
- Batch tests preserve original indices and partial successes.
- Secret values never appear in formatted errors or debug snapshots.
- Symlink tests cover relative links, cycles, eight-hop limit, broken links,
  and path escape attempts.
- Cancellation drops streams and releases all permits without publishing a
  partial write.

## Source evidence

- `src/multistorageclient/types.py`
- `src/multistorageclient/providers/base.py`
- `src/multistorageclient/retry.py`
- `tests/test_multistorageclient/unit/providers/test_base_storage_provider.py`
- `tests/test_multistorageclient/unit/providers/test_storage_providers.py`
- `tests/test_multistorageclient/unit/providers/test_s3_error_translation.py`
- `tests/test_multistorageclient/unit/test_retry.py`
