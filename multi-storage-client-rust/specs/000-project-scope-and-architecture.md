# 000: Project scope and architecture

- Status: Draft for review
- Baseline: Python MSC commit `442bb07531f832df8b7f6946675a53403649c9e5`
- Depends on: None

## Summary

Multi-Storage Client Rust is an async-first Rust library and CLI that presents
one object/file interface over multiple storage systems. It ports behavioral
contracts from MSC rather than reproducing Python implementation mechanisms.

The central architecture is a typed configuration loader, a provider-neutral
async backend contract, a `StorageClient` facade, and independent decorators or
services for caching, synchronization, provider metrics, manifests, replicas,
and composite routing.

## Goals

- Preserve the useful MSC configuration/profile model.
- Make provider I/O natively asynchronous and cancellation-safe.
- Support library and CLI users from the same implementation.
- Stream large objects and listings with bounded memory.
- Treat local caching and `msc sync` as first-class capabilities.
- Include Storage Provider-level OpenTelemetry metrics in the initial release.
- Start with S3/S3-compatible, native GCS, and Azure Blob remote providers, plus POSIX for
  local transfers and cache storage.
- Normalize provider errors and retry decisions.
- Make provider-specific capabilities discoverable instead of inferred.
- Keep high-level features separable so they can be implemented and tested in
  parallel.

## Non-goals

- Python APIs, PyO3, file-like Python objects, or Python extension loading.
- `contrib`, fsspec, NumPy, PyTorch, Ray, Hydra, or pathlib emulation.
- Explorer, MCP server, and OpenTelemetry traces/log export in the initial
  project. Provider metrics are in scope.
- Remote providers beyond S3/S3-compatible, native GCS, and Azure Blob;
  extensions are deferred. S3 RDMA is out of scope. Composite-client
  expansion is a later priority.
- Bit-for-bit compatibility with the Python on-disk cache.
- Reproducing Python multiprocessing; Rust concurrency is in-process async I/O.
- A FUSE mount or transparent operating-system filesystem.

## Architectural decisions

### Async is the native API

All operations that may perform provider, filesystem, credential, or metadata
I/O are async. The library never creates or owns a hidden Tokio runtime. The
`msc` binary owns its multi-thread Tokio runtime.

A future `blocking` feature may expose a separately named
`BlockingStorageClient`. It must own an explicit runtime and must reject calls
from an existing Tokio runtime instead of nesting `block_on`. It is not part of
the MVP.

Configuration discovery and parsing may remain synchronous because it reads a
small number of startup files. Provider construction may be async when it needs
credential discovery.

### Rust-native file I/O

The Rust equivalent of file I/O is an object handle implementing Tokio I/O
traits, not a port of Python's `open()` API. `StorageClient::reader` returns an
`ObjectReader` implementing `AsyncRead + AsyncSeek`, using bounded range reads
with optional local caching. `StorageClient::writer` returns an `ObjectWriter`
implementing sequential `AsyncWrite`, with explicit async `finish()` to publish
the object and return metadata. Random-access writes, text modes, and Python
mode strings are not supported. These contracts are detailed in 003 and 005.

### Provider metrics

Every provider instance is instrumented at the provider-operation boundary as
defined in 011. Request/response counts, latency, data size, and data rate are
available for POSIX, S3/S3-compatible, native GCS, Azure Blob, and registered providers.
Async streams remain instrumented until completion, failure, or cancellation.
Metrics use an injected OpenTelemetry meter or an explicitly owned SDK; the
library does not replace the application's global telemetry provider.

### Library/CLI boundary

The library returns typed values, progress events, reports, and errors. It does
not print, detect terminal capabilities, select colors, or terminate the
process. The CLI owns:

- argument parsing and mutual-exclusion checks;
- stdout, stderr, and progress rendering;
- confirmation prompts;
- signal-to-cancellation wiring; and
- process exit codes.

### Runtime and concurrency

- Tokio is the async runtime.
- Streams and byte buffers are bounded; whole objects are not implicitly loaded
  unless an API explicitly returns `Bytes`.
- Bounded channels and semaphores provide backpressure.
- Blocking filesystem locks, xattrs, large directory scans, or synchronous SDK
  calls run through a bounded blocking pool.
- Child tasks are structured and joined. Cancellation cannot detach a writer
  that later publishes stale data.

### Object safety and extensibility

Provider instances are heterogeneous and shared as `Arc<dyn StorageProvider>`.
The object-safe trait uses explicit boxed futures/streams or an equivalent
well-audited abstraction. The high-level `StorageClient` exposes ergonomic
`async fn` methods and does not leak the boxing choice.

Built-in providers are selected by tagged configuration. Rust users extend the
system with a `ProviderRegistry` or builder that registers factories. Python
fully-qualified class names are rejected with an actionable error.

### Crate structure

Start as one package with a library and `msc` binary. Modules follow behavioral
boundaries:

```text
src/
  config/
  error.rs
  path.rs
  types.rs
  provider/
  telemetry/
  client/
  cache/
  sync/
  manifest/
  replica/
  composite/
  bin/msc.rs
```

Splitting a workspace is deferred until independent versioning, optional heavy
SDKs, or compile-time isolation provides a demonstrated benefit. Provider SDKs
may use Cargo features without changing the public core contracts.

## Compatibility decisions

| Area | Decision |
|---|---|
| `msc://profile/key` | Preserve, including duplicate slash normalization in object keys. |
| Default POSIX profile | Preserve `__filesystem__` with base path `/`. |
| YAML/JSON config discovery | Preserve search order and first-existing-file behavior. |
| File I/O | Rust-native `AsyncRead + AsyncSeek` reader and sequential `AsyncWrite` writer with explicit `finish()`; no Python `open()` compatibility. |
| OpenTelemetry | Provider metrics in the MVP; preserve operation metric names and define async lifecycle semantics in 011. |
| Remote providers | S3/S3-compatible and native GCS initially; other adapters are future extensions. |
| Dynamic provider class names | Replace with an explicit Rust registry. |
| Retry | One policy and budget; no nested provider/client multiplication. |
| Local cache | Preserve behavior, not unsafe Python disk layout or races. |
| Sync filters | Apply to copies and deletions; do not reproduce delete bypass. |
| CLI ignored flags | Reject invalid combinations instead of silently ignoring them. |
| Composite direct config | Implement as a first-class typed profile, fixing the Python loader gap. |

## Security and reliability invariants

- A provider-relative key cannot escape a configured POSIX or cache root.
- Credentials and resolved secret values never appear in `Debug`, logs, or CLI
  validation output unless the user explicitly requests unsafe disclosure in a
  future design.
- Writes use conditional operations when the caller requests them.
- Cache entries are never considered valid while payload and metadata disagree.
- A failed sync source listing cannot trigger the destructive delete phase.
- Retries require a typed retry classification and an idempotency decision.
- Metric attributes exclude secrets, object keys, endpoints, and request IDs;
  exporter failures never change storage operation results.

## Performance expectations

No fixed throughput target is imposed before representative benchmarks exist.
The design must nevertheless guarantee:

- O(concurrency) transfer buffers rather than O(object count or object size);
- a bounded sync work queue;
- streaming listings;
- multipart transfer for large remote objects;
- no executor blocking on advisory file locks or directory walks;
- configurable per-provider and global concurrency; and
- no network export or per-operation telemetry task on the provider hot path.

## Project quality gates

Every change must pass:

```shell
just format-check
just analyze
just test
```

The same gates must pass in `nix develop`. Public behavior requires rustdoc and
tests. Provider implementations require common contract tests plus emulator or
cloud integration tests where practical.

## Source evidence

- `multi-storage-client/AGENTS.md`
- `multi-storage-client/src/multistorageclient/{config,types,shortcuts,retry}.py`
- `multi-storage-client/src/multistorageclient/client/`
- `multi-storage-client/src/multistorageclient/providers/`
- `multi-storage-client/src/multistorageclient/sync/`
- `multi-storage-client/src/multistorageclient/telemetry/`
- `multi-storage-client-docs/src/user_guide/`
- `multi-storage-client/rust/Cargo.toml` and `rust/src/lib.rs`
