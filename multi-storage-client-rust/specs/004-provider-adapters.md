# 004: Provider adapters

- Status: Draft for review
- Depends on: 001, 002

## Summary

This spec defines the provider delivery matrix and adapter-specific behavior.
Every adapter implements the same contract from 001; adapters can be developed
in parallel and selected with Cargo features.

## Delivery matrix

| Provider tag | Backend | Milestone | Notes |
|---|---|---|---|
| `file` | POSIX filesystem | MVP | Always enabled |
| `s3` | AWS S3 and S3-compatible services | MVP | Configurable endpoint, addressing, and AWS-compatible auth |
| `gcs` | Native Google Cloud Storage | MVP | Native GCS auth and generation semantics |
| `azure` | Azure Blob Storage | MVP | Account keys, SAS, identity auth, version/ETag conditions |

MVP means the library, cache, sync, and CLI are production-capable for POSIX
and S3/S3-compatible endpoints, native GCS, and Azure Blob. Other remote adapters are future
extensions, not additional delivery commitments in this spec. POSIX remains
the local transfer/cache baseline. Later adapters cannot change the core trait.

## Dependency policy

Prefer maintained, released Rust crates. Do not depend on an unpinned Git
branch for core behavior. `object_store` may provide common S3/GCS/local
data paths, but each adapter must close MSC semantic gaps such as credentials,
conditional requests, ordered listing, error translation, and presigning.

Provider SDK types remain private. SDK retry loops are disabled or configured
to participate in the single MSC retry budget.

Each heavy native provider has an independent Cargo feature. Config validation can
recognize supported but disabled tags and explain which feature is required.
The default library and CLI enable S3, GCS, and Azure. Unsupported future adapters
must not be advertised as merely disabled features.

All adapters use the provider metrics decorator from 011. SDK-specific HTTP,
pagination, and multipart calls do not duplicate MSC operation measurements.

## POSIX adapter

### Configuration

- absolute `base_path`;
- symlink policy and optional permission/mtime preservation;
- optional blocking pool/concurrency limit.

### Behavior

- Resolve every key beneath `base_path` without allowing `..`, absolute-key,
  or symlink escape.
- Stream files with Tokio filesystem APIs; isolate unavoidable blocking calls.
- Produce globally lexical recursive listings and synthesize immediate
  directories for delimited mode.
- Use lstat when the caller asks not to follow symlinks.
- Preserve exact file length and UTC mtime.
- Implement atomic download/write publication with same-filesystem temp files.
- Map `NotFound`, permission, already-exists, and I/O errors into stable kinds.
- `delete(recursive=false)` refuses a non-empty directory.
- Batch delete is idempotent for missing paths.
- Server-side copy is a filesystem copy/reflink optimization only when it
  preserves requested metadata semantics.

Tests run on real temporary directories and include symlink attacks and races.

## S3 and S3-compatible adapter

### Configuration

Typed options include:

- bucket/base path;
- region;
- endpoint URL;
- virtual-hosted versus path-style addressing;
- TLS verification/CA settings;
- multipart threshold, part size, and concurrency;
- connect/read/pool timeouts;
- checksum algorithm; and
- user agent.

Secrets live in a credentials provider, not ordinary options. Standard AWS
credential discovery remains available when no explicit credentials provider
is configured.

### Behavior

- GET and exact range GET stream data and validate response lengths.
- PUT selects single-part or multipart while respecting S3's 5 MiB minimum
  non-final part, 10,000-part maximum, and maximum part size.
- Aborted/cancelled multipart uploads are cleaned up.
- HEAD exposes ETag without assuming it is a content checksum.
- Listing is globally lexical across pagination and strips base path once.
- Conditional operations map 304/409/412 distinctly.
- 401/403/404/408/429/5xx and transport failures receive typed categories.
- Batch delete reports individual failures.
- Server-side copy is used only within a compatible endpoint/account scope;
  otherwise the caller falls back to streaming.
- Presigned GET/PUT is advertised only when credentials support signing.
- Native copy preserves source metadata by default, validates completion XML,
  and returns the generation from its response. Configured encryption uses
  streaming SDK copy to preserve encryption settings.
- Native batch DELETE validates all per-key results and never treats omitted,
  duplicated, or unknown results as success.
- SDK requester-pays and payload-signing settings apply to native requests too.
- Conditional multipart publication and S3 Express are explicitly unsupported.
- S3-compatible services use the same `s3` tag with endpoint/addressing
  options. Service-specific authentication beyond AWS-compatible credentials
  is a separately scoped extension, not an implicit preset.

## Native GCS adapter

Native GCS is part of the MVP, not just access through its S3-compatible API.
Typed options include bucket/base path, optional project and endpoint,
timeouts, and multipart upload thresholds/concurrency. Credentials support
service accounts, workload identity, and application-default discovery without
exposing credential documents in errors.

Generation/metageneration become opaque source/conditional versions; adapters
translate normalized conditions into native GCS preconditions. Full/range
reads, XML multipart upload cancellation/cleanup, JSON lexical pagination and
rewrite copy, user
metadata, and typed error translation must pass the common contract suite.
Signed URLs are advertised only when the selected credentials can sign.

## Azure Blob adapter

Typed options include container/base path, account endpoint, account key,
existing SAS token, bearer token, service-principal/workload identity settings,
timeouts, block size, and listing page size. Python connection-string credential
profiles are accepted for account keys and SAS.

- Full/range reads use source versions and validate response sizes.
- Single PUT supports ETag conditions; block uploads remain bounded.
- Version IDs are pinned for reads; deletes resolve version tokens to a current
  ETag and apply an atomic conditional DELETE.
- Listing pages are spooled and externally sorted into global lexical order.
- Native 256-item batch deletion verifies response IDs and preserves input indices.
- Copy uses the bounded facade streaming path; native copy is not advertised.
- Account keys can generate SAS URLs. Existing SAS/anonymous profiles cannot
  generate new signed URLs.
- User metadata and stored logical links round-trip; malformed link metadata
  and case-colliding metadata headers are rejected.
- Conditional block-list publication remains explicitly unsupported before
  block upload begins.

## Future adapters

OCI, native AIStore, Hugging Face, cuObject, and dedicated S3-service
presets are outside the initial scope. Add each through a future adapter spec
and registry entry after the initial library/CLI contracts are stable; do not
reserve built-in configuration tags or promise feature flags before then.

## Provider registry

Built-ins register a tag, typed option decoder, async factory, feature name,
and capability expectations. Applications can register custom factories before
loading providers. Duplicate registrations fail unless an explicit override is
allowed by a builder API.

The registry is process-local code configuration, not a mechanism for loading
arbitrary dynamic libraries from YAML.

## Common contract test suite

Every writable adapter is tested for:

- create, overwrite, head, full read, range read, copy, and delete;
- zero-byte and multipart-size objects;
- Unicode, spaces, normalized separators, and prefix-collision keys;
- rejection of raw noncanonical listed keys that could alias another key;
- lexical recursive/delimited listing across pagination;
- conditional success and precondition failures;
- missing and idempotent batch deletion;
- user metadata round-trip;
- cancellation and cleanup;
- concurrency limits and retry classification; and
- no credential leakage.

Read-only adapters declare unsupported operations and run the remaining suite.

## Integration environments

- POSIX: temporary local filesystem plus targeted NFS/Lustre validation later.
- S3-compatible: a reproducible emulator in Nix and optional AWS end-to-end
  tests.
- Native GCS: native XML/JSON HTTP fixtures cover credentials, generation
  conditions, multipart cleanup, rewrite continuations, listing, and signing.
- Azure Blob: native HTTP fixtures cover credentials, ranges, block uploads,
  versions, metadata, pagination, SAS, batch correspondence, and error paths.
- Credentialed AWS/GCS/Azure release validation is separate from the local gate;
  cloud identity-service deployment and account policy require real accounts.

Emulator differences are documented; an emulator passing is not proof of
cloud conditional or multipart behavior.

## Acceptance criteria

- POSIX, S3/S3-compatible, native GCS, and Azure adapters pass the same
  provider-neutral suite.
- S3 multipart tests cover exact boundary sizes, 10,000-part resizing,
  cancellation, and partial failure cleanup.
- Provider capability flags match observed behavior in contract tests.
- Config options reject invalid combinations before network I/O.
- Disabled providers fail with an actionable feature message.
- Unsupported future adapters fail without suggesting nonexistent features.
- GCS tests cover native credentials, generation preconditions, ranges,
  multipart cleanup, pagination, and credential-dependent signing support.
- Every adapter passes the provider metrics integration contract from 011.
- Error tests assert stable kinds/status/request IDs without matching a whole
  SDK error string.
- No adapter starts a runtime or an unbounded thread pool.

## Source evidence

- `src/multistorageclient/providers/`
- `src/multistorageclient/types.py`
- `multi-storage-client/rust/src/{lib,credentials,types}.rs`
- `multi-storage-client-docs/src/references/configuration.rst`
- `multi-storage-client-docs/src/references/checksum_support.rst`
- `tests/test_multistorageclient/unit/providers/`
- `tests/test_multistorageclient/e2e/`
