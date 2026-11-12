# Multi-Storage Client Rust specifications

These specifications define a Rust-native port of the core Multi-Storage
Client behavior. They are based on the Python implementation and User Guide at
upstream commit `442bb07531f832df8b7f6946675a53403649c9e5` (2026-09-21).

Subsequent core compatibility updates track MSC 1.4.0 at
`7d97e3888569838d8e255f489ad9235b92f1fea2`, including suffix reads,
resumable downloads, transfer controls and progress, and manifest interoperability.

The documents describe behavioral contracts, not a file-for-file translation.
Every spec is independently reviewable and implementable once its declared
dependencies are complete.

This directory contains the full design roadmap for review. Importing a
specification does not imply that its features are implemented in this PR.
The foundation covers POSIX, S3/S3-compatible storage, core configuration,
client operations, credentials, and benchmarks. GCS, Azure, cache, sync, the
CLI, manifests, replicas, composites, and telemetry remain planned work.
See [Foundation delivery scope](000-foundation.md) for this PR's boundary.

## Scope

The planned MVP milestone includes:

- an async Rust library;
- MSC YAML and JSON configuration;
- S3/S3-compatible, native GCS, and Azure Blob storage, plus POSIX;
- a provider-independent `StorageClient` facade;
- full-object and partial local caching;
- a bounded, async synchronization engine;
- Storage Provider-level OpenTelemetry metrics; and
- the `msc config validate`, `msc ls`, `msc rm`, and `msc sync` commands.

The following are intentionally outside the port: Python bindings, all Python
`contrib` integrations, Ray execution, Explorer, and MCP. Other remote storage
providers and OpenTelemetry traces/log export are follow-ups, not initial
requirements. S3 RDMA is out of scope, and composite-client expansion is lower priority.

## Documents and dependency graph

| ID | Specification | Direct dependencies | Current delivery |
|---|---|---|---|
| 000 | [Project scope and architecture](000-project-scope-and-architecture.md) | None | Design roadmap |
| 001 | [Core types and async provider contract](001-core-types-and-provider-contract.md) | 000 | Foundation subset |
| 002 | [Configuration and URL resolution](002-configuration-and-url-resolution.md) | 001 | Foundation subset |
| 003 | [Storage client](003-storage-client.md) | 001, 002; one 004 adapter | Foundation subset |
| 004 | [Provider adapters](004-provider-adapters.md) | 001, 002 | POSIX and S3 implemented; other adapters planned |
| 005 | [Local cache](005-local-cache.md) | 001, 002, 003 | Planned |
| 006 | [Synchronization engine](006-sync-engine.md) | 001, 003 | Planned |
| 007 | [Command-line interface](007-cli.md) | 002, 003, 006 | Planned |
| 008 | [Manifest metadata](008-manifest-metadata.md) | 001, 003 | Planned |
| 009 | [Replicas](009-replicas.md) | 003; 006 for bulk sync | Planned |
| 010 | [Composite read-only client](010-composite-client.md) | 001, 003, metadata routing | Planned |
| 011 | [Provider metrics and OpenTelemetry](011-provider-metrics-and-opentelemetry.md) | 001, 002 | Planned |

```text
000 Architecture
  └─ 001 Core types + provider traits
       ├─ 002 Config + URL resolution
       │    ├─ 004 Provider adapters ───────┐
       │    ├─ 011 Provider metrics         │
       │    └───────────────────────────────┤
       └───────────────────────────── 003 StorageClient
                                             ├─ 005 Local cache
                                             ├─ 006 Sync engine ── 007 CLI
                                             ├─ 008 Manifest metadata
                                             ├─ 009 Replicas
                                             └─ 010 Composite client
```

Specs 005 and 006 deliberately do not depend on each other and can be
implemented in parallel. Specs 008, 009, and 010 are also independent siblings;
the composite client depends on a routing contract, not on the concrete v1
manifest format. Spec 011 can be implemented against a fake provider in
parallel with adapters and the client. Its integration decorates provider
instances and supplies exporter lifecycle management to the CLI; it does not
depend on cache, sync, or any particular cloud adapter.

## Delivery sequence

1. Foundation: 000–002, fake provider contract tests, and configuration tests.
2. Usable library: POSIX and S3/S3-compatible adapters from 004,
   plus 003 and provider metrics from 011. Native GCS and Azure Blob are included in the MVP.
3. Parallel features: 005 local cache and 006 synchronization.
4. User interface: 007 CLI.
5. Higher-level capabilities: 008–010 independently.

Each implementation must add unit tests for its contract and provider-neutral
integration tests using a deterministic fake backend. Cloud end-to-end tests
are additional gates, not substitutes for contract tests.

## Compatibility policy

User-visible configuration and path semantics should remain compatible where
they are safe and meaningful in Rust. Known Python defects are recorded as
intentional differences in the relevant spec. In particular, this port does
not preserve unsafe cache publication, filter-bypassing sync deletions,
silently ignored CLI arguments, nested retry multiplication, or Python dynamic
class loading.
