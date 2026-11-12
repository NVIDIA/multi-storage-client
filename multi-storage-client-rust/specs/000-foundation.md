# Native Rust MSC foundation

## Summary and provenance

Import the POSIX/S3 subset of the Rust reference at
`134e2669c36595415f4a65f46b14bbf7ca3e3a2f` (Shunjia Ding,
`feat: close MSC 1.4 core parity gaps`). The source checkout is read-only.
This package follows the parent repository's Apache-2.0 license.

## API and architecture contract

Applications own and configure their Tokio runtime. `StorageProvider` is the
asynchronous, provider-neutral I/O boundary. `ProviderRegistry` constructs
built-in `file`, `s3`, and `s8k` adapters or application-owned factories.
`StorageClient::builder(config, profile).build().await` selects a named profile.
Client clones share the provider, connection pools, and cleanup owners.

The client provides info, byte/range/suffix/streaming reads, seekable readers,
byte writes, spooled writers, upload_from, download_to, ordered listings,
exact-key deletion, copy, symlinks, presigning, and bounded batch file operations.
Optional provider operations retain documented Unsupported defaults. Capabilities
describe optional behavior; requested conditions are enforced or explicitly
rejected before mutation. Errors carry typed kinds and retry eligibility.

The client owns operation retry policy. SDK retries are disabled. Reads retry
safe operations; replayable writes, uploads, copies and deletes use the configured
budget, as Python does. A conflict after a retried conditional mutation returns
`AmbiguousOutcome` because an earlier attempt may have committed. Multipart
parts use the current client budget, without replaying ambiguous publication.
S3 transport recovery, timeout handling, connection reuse, and abort ownership
are retained from the reference. Returned streams can fail after opening;
consumers must reach EOF and propagate each item error. Streams are not replayed
by the client after being returned to the application.

Keys are profile-relative. Providers apply base_path once. ObjectPath rejects
traversal. POSIX uses anchored directory descriptors and rejects root escapes,
including symlink escapes. Large file transfers have bounded memory. Uploads
read from a retained source file handle, seek on retries, and validate source
size/modification without making a whole-file temporary copy;
downloads stream to a temporary sibling, validate length, synchronize the file,
and atomically publish. Failed downloads preserve existing destinations.
Cancellation must retain required cleanup ownership. Await explicit shutdown
while the runtime is alive, on both success and failure, to drain multipart
cleanup and report failures. Shutdown does not prevent subsequent operations.

Explicit source-version reads obtain metadata for that generation, including
the size used by seekable readers and downloads. S3 uses `head_version` to
address historical versions. POSIX resolves symlinks and requires the current
target generation to match. POSIX rejects symlink targets containing
parent traversal after a named component rather than resolve them lexically
and risk returning a different object from the filesystem.

## Scope and intentional adaptations

S3 is the only default Cargo feature. No-default-features supports core and POSIX
on Unix. One independent package, lockfile, and pinned toolchain reuse the parent
Nix environment. There is no Cargo workspace or Python binding change.

Client cache, metadata, replicas, composites, sync, GCS, Azure, general msc CLI,
telemetry, RDMA, bindings, and release publication are deferred. Configurations
may retain those settings for other profiles. Unsupported behavior fails when
the affected profile is selected, while supported physical profiles remain usable. Factory and capability boundaries
allow future adapters without importing those runtimes now.

The reference client parallel-download checkpoint implementation depends on
cache filesystem state. This subset uses its bounded streaming atomic-download
path for all file downloads, without persistent checkpoints. This is an
intentional behavior change. Provider-level S3 range recovery is retained.
Cache read overrides and staged metadata options are removed from public types.
Recursive client deletion is explicitly unsupported; exact-key deletion remains
idempotent. Benchmark metadata commit and telemetry phases are removed.

## Backward compatibility and cross-language impact

This is additive to the monorepo. Python APIs, configuration, packaging, PyO3
accelerator dependencies, and .pyi stubs do not change. Native configuration
shares supported profile field spellings with Python and defers unsupported-profile
validation until selection. Top-level cache and telemetry settings are retained
without activating those unimplemented services.
Real-cloud E2E and performance qualification remain subsequent work.

## File selection and adaptation plan

| Files | Selection/adaptation |
|---|---|
| src/{error,path,types,credentials}.rs | Import neutral contracts and credentials; trim deferred read options |
| src/provider/{mod.rs,posix/,s3/} | Import adapters and reliability code; remove other backend factories and telemetry file wrapper |
| src/provider/{posix,s3}/listing.rs | Match Python directory ordering; use native S3 pagination and directory-local POSIX sorting |
| src/config/ | Retain discovery, includes, expansion, provenance, strict schemas, URL resolution, redaction; validate deferred features at profile selection |
| src/client/ | Keep retry policy and self-contained byte/file/listing helpers; remove decorators and checkpoint state |
| src/bin/msc-benchmark/ | Keep process protocol, bounded concurrency, phases and reporting; remove commits/exporters |
| tests/ | Keep relevant contracts, HTTP fixtures, and source/request-count regression coverage |
| Cargo.*, rust-toolchain.toml, justfile | Independent package, S3 feature, locked checks and shared storage simulator lifecycle |
| parent justfile, CI workflows, AGENTS.md | Dispatch Rust checks once per pipeline and distinguish accelerator from native library |

## Performance and benchmark contract

Files use bounded chunks, and downloads use atomic destination staging; byte APIs and benchmark payloads materialize configured objects.
Benchmark setup/payload generation is outside transfer timing. Successful bytes
are counted after completion only. Reads reach EOF and validate length. File
phases measure source reads and destination synchronization as API behavior.
Process counts mean child processes; per-process operations are bounded by
threads. Unique run namespaces own exact cleanup keys; unrelated data survives.
Failures, including cleanup failures, yield nonzero exit status.

## Acceptance and validation

`just build` runs formatting, all-target/all-feature clippy, unit tests,
doctests, integration tests with local simulators, both feature configurations,
and compilation. Integration tests reuse the Python package's
`start-storage-systems` and `stop-storage-systems` recipes, including Versity.
The Source pipeline invokes this build gate for main and the repository's
trusted `pull-request/*` push workflow.

Real backend tests run only through `just run-e2e-tests`, using the selected
`MSC_E2E_PROFILE` from normal MSC configuration and credentials. They use unique
object keys and clean up only the objects they own. `just run-benchmarks` runs
the POSIX/S3 benchmark smoke checks. Neither command is a build dependency.
Examples, quickstarts and user documentation are outside this PR.
Emulator results do not imply live-cloud qualification.

## Python behavior alignment

Reads issue GET first; empty results may require metadata to resolve symbolic
links. Range reads validate their GET response without a preliminary HEAD.
Zero-byte listing entries retain Python's metadata probe, and normal listings
do not fetch per-object attributes unless requested. S3 shallow pages expose
CommonPrefixes before Contents, as Python does. POSIX sorts each directory
using the trailing delimiter for directory order, without external sort spools.
File-only recursive listings remain lexically ordered. Directory-inclusive
streams retain provider order and are not automatically resumed after emission.

`ObjectPath` retains the foundation's canonical-key safety boundary. Native S3
LIST does not pass keys through object_store normalization. Callers can select
`skip_unsupported_keys` to continue past keys outside this subset; default
behavior reports the unsupported key class without exposing its contents.
PUT and COPY return publication metadata without an extra HEAD. Fields absent
from the publication response, including a PUT modification timestamp, remain
`None`; an explicit info call fetches complete stored metadata.
