# 006: Synchronization engine

- Status: Draft for review
- Depends on: 001, 003
- Independent of: 005 local cache

## Summary

The sync engine compares two client/path trees, creates a deterministic plan,
and executes copies, updates, symlink operations, and optional deletions with
bounded async concurrency. It powers both the Rust library and `msc sync`.

The Rust implementation keeps MSC's useful size/mtime compatibility behavior
but fixes unsafe deletion filtering, unbounded or process-oriented execution,
silent partial state, and delete-before-source-enumeration risks.

## Public model

Conceptual types:

```rust
struct SyncOptions {
    delete_unmatched: bool,
    selection: OrderedSelection,
    ignore_hidden: bool,
    symlink_handling: SymlinkHandling,
    compare: ComparePolicy,
    concurrency: SyncConcurrency,
    preserve_source_attributes: bool,
    source_files: Option<Vec<ObjectPath>>,
    commit_metadata: bool,
    shutdown_clients: bool,
}

enum PlannedOperation {
    Copy { source: ObjectMetadata, target_key: ObjectPath },
    Delete { target: ObjectMetadata },
    Symlink { source: ObjectMetadata, target_key: ObjectPath },
}

struct SyncReport {
    planned: OperationCounts,
    completed: OperationCounts,
    skipped: u64,
    bytes_copied: u64,
    failures: Vec<SyncItemFailure>,
    metadata_committed: bool,
    cancelled: bool,
}
```

`SyncError` contains the partial report and a stable cause. The library also
emits optional `SyncEvent` values (`Planned`, `Started`, `Completed`, `Failed`,
`PhaseChanged`) through a bounded observer interface. It never prints.

The primary API separates planning and execution:

- `SyncPlanner::plan(source, target, options) -> SyncPlan`;
- `SyncExecutor::execute(plan, options) -> SyncReport`; and
- `sync(...)` as a convenience composition.

The plan is consumable once and may use an on-disk spool; it need not retain
millions of operations in memory.

## Path mapping and overlap

Source and target roots are normalized client-relative keys. For every source
object, remove the source root and append the relative key to the target root.
A single source file targeting a directory uses its basename.

When source and target refer to the same backend identity, ambiguous overlapping
prefixes are rejected before execution. Compatibility treats raw prefix
overlap conservatively, so `data` and `data2` overlap while `data/a` and
`data/b` do not. A later path-segment policy can be added only as an explicit
breaking decision.

The same object may not be both source and target through profile aliases or
path mappings; backend fingerprint plus physical root is used for detection.
For metadata namespaces, planning also resolves physical source objects and
planned target mutations. A shared physical object rejects the plan before
execution, including when the source alias is hidden or excluded. Disjoint
manifest mappings on the same backend remain valid.

## Selection semantics

Selection uses an ordered list of include/exclude glob rules evaluated against
the key relative to the sync source/target root. Later matching rules override
earlier rules.

- With only include rules, the default is exclude.
- If any exclude rule exists, the default is include.
- Hidden means any path component begins with `.`.
- Hidden paths are excluded by default and included with
  `ignore_hidden=false`.

One selection predicate applies symmetrically to source comparison, copy/update
planning, and target-only deletion. A target outside the selected namespace is
never deleted. This intentionally fixes Python's behavior where patterns were
applied to ADD/UPDATE but not DELETE.

Selection and hidden behavior live in one reusable module shared with `msc ls`.
`source_files` bypasses source enumeration and retrieves only those exact keys,
sorted and deduplicated; missing files are skipped. An empty list selects no
source files. Exact file lists cannot be combined with glob selection rules.
Supplying `source_files`, including an empty list, with `delete_unmatched=true`
is rejected before listing or mutation. An exact source list does not define an
authoritative namespace for deleting target-only files.

## Listing and planning

The planner consumes globally lexical source and target metadata streams and
performs a merge join:

- source-only selected entry -> `Copy` or `Symlink`;
- target-only selected entry -> `Delete` only when enabled;
- both -> compare and either skip or update;
- unselected/hidden -> no operation.

A provider stream that violates ordering is an integrity error. Listing errors
stop planning and identify which side failed.

Copy/update operations may stream to workers while planning continues. Delete
operations are spooled but cannot execute until the source listing reaches a
successful end and the entire plan is valid. This ensures a truncated or failed
source listing cannot erase target data.

The spool has a versioned record format, bounded flush behavior, restrictive
permissions, and RAII cleanup. Dry-run can reuse it.

## Compare policy

MVP default `SizeAndModified` matches effective Python behavior:

1. If either entry is a symlink, compare normalized symlink target.
2. Otherwise sizes must match.
3. Truncate timestamps to seconds.
4. Treat the target as current when `target_mtime >= source_mtime`.
5. If source attributes are explicitly preserved into a writable metadata
   provider, compare those attributes too.

ETag is not part of this default. Cross-provider ETags are not generally
comparable, and the Python implementation did not actually use them despite
documentation suggesting otherwise.

The interface permits later policies:

- always copy;
- exact size/mtime;
- trusted same-backend source version; and
- explicit checksum.

Policies must state what happens when metadata is missing. A `--checksum`
future feature cannot silently fall back to ETag.

## Symlink handling

For POSIX sources:

- `Follow`: dereference internal links, skip external/broken links;
- `FollowStrict`: dereference internal links, error on external/broken links;
- `Skip`: omit all links;
- `Preserve`: recreate internal links, skip external/broken links;
- `PreserveStrict`: recreate internal links, error on external/broken links.

Internal means resolution remains under the source root. Directory links,
cycles, and the global eight-hop limit are handled consistently with 001.
Object-store sources use stored symlink metadata and do not reinterpret the CLI
POSIX follow policy.

Symlink operations appear in the add plan with `symlink_target` and have their
own completion/failure counts.

## Transfer strategies

The executor selects the safest efficient path from capabilities:

1. same compatible backend: server-side copy;
2. POSIX to POSIX: bounded streaming copy with optional metadata preservation;
3. POSIX to remote: streaming/multipart upload;
4. remote to POSIX: atomic `download_to`;
5. remote to remote: bounded producer/consumer stream or multipart pipe;
6. temporary-file fallback only when retry/SDK constraints require replay.

Remote-to-remote transfer must not buffer a whole large object in memory. Temp
space use is bounded by configured concurrency and per-transfer limits, and all
temps are cleaned after success, error, or cancellation.

Transfers are grouped or scheduled by size only as an optimization. Correctness
does not depend on Python's exact size buckets or batch size.

## Concurrency and backpressure

Use Tokio structured concurrency:

- bounded channels between planner and workers;
- a global transfer semaphore;
- optional per-provider read/write semaphores;
- bounded multipart part concurrency;
- cancellation token observed by planner, workers, and metadata owner; and
- all spawned tasks joined before returning.

The first fatal item failure stops admission of new work. In-flight operations
may complete and are reflected in the partial report. No detached task may
continue mutating storage after `sync` returns.

Typed options are canonical. Compatibility environment variables such as
`MSC_SYNC_BATCH_SIZE` can be read by the CLI layer later, but invalid values are
errors rather than silently clamped.

## Delete phase

Deletes start only after:

- both listings completed successfully;
- copy/update planning completed;
- selection was applied; and
- by default, all required copy/update operations succeeded.

This "copy before delete" default minimizes destructive partial states. A
future aggressive mode requires an explicit option.

Delete uses batch capability when available and preserves individual failures.
Missing targets are successful. Cancellation between delete batches returns a
partial report.

## Metadata-provider integration

Workers report completed object mutations to one async metadata owner. It
serially stages add/remove operations and commits after execution by default.
`commit_metadata=false` leaves completed mutations staged so multiple syncs can
share one later commit. Endpoint clients remain reusable by default;
`shutdown_clients=true` explicitly closes both endpoints after execution.
Deferred commit cannot be combined with automatic shutdown. There is no
cross-process queue proxy.

Completed physical work is committed even if a later item fails so the metadata
view does not knowingly omit completed objects. The error report states whether
commit succeeded. There is no rollback guarantee.

For resume, an existing physical object that satisfies compare/overwrite rules
may be added to metadata without retransferring. Reports distinguish
`bytes_transferred` from `objects_tracked` so metadata repair is not counted as
network transfer.

Sync temporarily disables foreground replica fallback/backfill on the source;
otherwise a missing primary object could be read from a replica and mutate
replica state during planning.

## Dry-run and plan export

Dry-run performs full listing, selection, comparison, and validation but no
copy, delete, cache mutation, or metadata commit. It always creates:

- `files_to_add.jsonl`; and
- `files_to_delete.jsonl`.

Each line uses a versioned schema with operation, source metadata, target key,
comparison reason, and symlink target when applicable. Empty files still exist.
UTC timestamps round-trip without losing precision.

When no output directory is supplied, dry-run creates a temporary directory and
returns its path; ownership and cleanup responsibility are explicit in the
result. A planning error returns paths and a partial plan count when safe.

## Progress and reporting

Reports distinguish planned, completed, skipped, failed, metadata-only,
deleted, symlinked, and bytes actually transferred. Progress totals may grow
while planning, but completion never displays 100% before all required phases
and metadata commit finish.

The CLI may throttle terminal rendering; event production itself remains
bounded and nonblocking for workers.

## Intentional differences from Python

- One async process instead of multiprocessing, monitor threads, and Ray.
- Patterns protect target-only deletes.
- Delete is a second safe phase after successful source enumeration/copies.
- Remote-to-remote prefers server copy/streaming rather than always staging a
  complete batch in a temp directory.
- Reports separate transfer bytes from metadata tracking.
- Invalid option combinations and values are errors.
- All tasks are joined and cancellation has a defined partial result.

## MVP and follow-ups

MVP includes direct source-to-target sync, all POSIX/remote transfer matrix
paths, default comparison, filters/hidden selection, safe delete, bounded
concurrency, dry-run JSONL, partial reports, metadata integration, SIGINT-ready
cancellation, and the five symlink modes for POSIX.

Follow-ups include replica fan-out, checksum comparison, persistent resumable
plans, multipart resume, richer machine-readable progress, and cross-host
distributed execution. Ray is not planned.

## Acceptance criteria

- Every source/target matrix supports initial copy, idempotent rerun, update,
  and optional delete.
- Size/mtime tests cover target newer, source newer, same-second differences,
  missing times, and symlinks.
- Single-file-to-directory and unequal root lengths map correctly.
- Same-backend overlap, including `data`/`data2`, is rejected.
- Ordered include/exclude combinations behave deterministically.
- Delete never touches unselected or hidden targets.
- A source listing failure executes zero deletes.
- Dry-run changes no backend/metadata/cache state and both JSONL files
  round-trip even when empty.
- First worker failure stops new admission, waits for in-flight work, commits
  completed metadata, and returns an accurate partial report.
- Fake-provider tests prove configured concurrency is never exceeded and queues
  remain bounded for millions of generated entries.
- Large remote-to-remote objects do not reside wholly in memory and temp usage
  stays within the documented bound.
- Metadata resume distinguishes tracking from transfer.
- All five symlink modes cover internal, external, broken, cyclic, file, and
  directory links.
- Cancellation between each phase leaves no background tasks or temp files.

## Source evidence

- `src/multistorageclient/sync/{manager,producer,worker,monitors,metadata_proxy,types}.py`
- `src/multistorageclient/client/single.py`
- `src/multistorageclient/types.py`
- `src/multistorageclient/utils.py`
- `multi-storage-client-docs/src/user_guide/cli.rst`
- `tests/test_multistorageclient/unit/sync/`
- `tests/test_multistorageclient/unit/test_sync.py`
