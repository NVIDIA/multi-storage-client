# 009: Replicas

- Status: Draft for review
- Depends on: 003; 006 only for bulk replica synchronization
- Independent of: 008 manifests, 010 composite client

## Summary

Replicas give a single-backend profile ordered read failover, asynchronous
read-through backfill, best-effort cleanup, and explicit bulk synchronization.
They do not turn ordinary writes into synchronous multi-backend transactions.

## Configuration

```yaml
profiles:
  primary:
    storage_provider: { ... }
    replicas:
      - replica_profile: fast-local
        read_priority: 1
      - replica_profile: secondary-cloud
        read_priority: 2
```

Validation is defined in 002. At runtime replicas are stably sorted by ascending
priority, preserving config order for ties. Profiles are built once and shared;
constructing the primary cannot recursively build a replica graph.

## Foreground reads

For full, range, stream, and download reads:

1. Try replicas in priority order.
2. A definite not-found records a miss and proceeds.
3. A retry-exhausted or other provider error is recorded and proceeds according
   to the read policy.
4. The first successful replica result is returned.
5. If all replicas miss/fail, read the primary.

Default availability policy continues past replica errors and returns the
primary result. A strict policy may fail after selected categories, but must be
explicit.

Range reads remain range reads. A replica implementation must not download a
whole object and ignore the requested range, which is a known Python defect.
Streaming reads do not probe with a separate HEAD unless required for
conditions/cache validation.

Metadata returned to the caller uses the primary profile's logical namespace.
Provider-specific versions remain scoped to the backend that produced them.

## Backfill

After a successful primary read, replicas that were definite misses may be
backfilled in the background. Replicas that failed with permission, integrity,
or transient errors are not treated as safe empty targets unless policy allows
it.

Backfill properties:

- fire-and-observe, not fire-and-forget: tasks are owned, cancellable, and
  drained at client shutdown;
- single-flight per `(replica, key, source version)`;
- failure is emitted as an event/warning but does not fail the completed
  foreground read;
- content streams or a completed local/cache file are reused when safe rather
  than issuing an unnecessary second primary download;
- conditional create prevents overwriting a newer replica object;
- delete fences and drains pending backfills for the key before replica cleanup,
  so a backfill from before the delete cannot publish deleted content afterward; and
- range reads do not backfill a corrupt partial object as a full object. They
  either schedule an independent full copy or skip backfill.

Backfill concurrency and bandwidth are separately bounded so it cannot starve
foreground reads.

## Writes and deletes

Normal write/upload/copy targets the primary only. Applications use bulk
replica sync for deliberate mirroring.

In Python-compatible `Unchecked` mode, an existing replica may continue serving
its older bytes after a primary write until explicit reconciliation. This mode
does not guarantee read-your-writes. Applications needing that guarantee must
read the primary or use a freshness-checked mode that verifies the replica's
generation against authoritative primary metadata before returning its bytes.

After primary delete succeeds, cleanup is attempted for each replica. Replica
failures do not roll back the primary delete. They are surfaced in a typed
`ReplicaCleanupReport` and warning event rather than silently discarded. The
simple client delete remains successful when the primary mutation succeeded,
but callers can opt into strict cleanup or inspect the report-capable API.

If cleanup fails, `Unchecked` reads may still return the undeleted replica.
A successful best-effort primary delete does not promise read-after-delete
consistency in that mode. Freshness-checked reads must consult authoritative
primary state and reject a replica hit when the primary is deleted, even if
replica cleanup failed. No durable replica tombstone mechanism is implemented
by this foundation PR.

All affected child caches are invalidated according to spec 005.

## Bulk synchronization

`sync_replicas` invokes the sync engine from 006 with the primary as a direct
source and each selected replica as a target. The source bypasses replica
failover and backfill to prevent circular or self-mutating behavior.

Replica indices are zero-based after configuration order, not sorted priority
order, so CLI references are stable when priorities change. Indices must be
unique and in range.

Jobs run with bounded parallelism or sequentially according to options. Default
failure policy is best-effort-all: every selected replica is attempted and an
aggregate report contains each success/failure. CLI returns nonzero if any
selected job fails.

```rust
struct ReplicaSyncReport {
    replicas: Vec<ReplicaJobReport>,
}
```

Dry-run, delete, filters, hidden files, symlink handling, cancellation, and
partial results have exactly the same semantics as direct sync. No option is
silently dropped.

## Lifecycle and consistency

Replica reads provide availability, not linearizability. The primary is the
authority. A successful replica hit may be stale if the backend has no
comparable source version and policy permits it. The recommended policy requires
a freshness signal for mutable datasets. `Unchecked` mode can remain stale
indefinitely until explicit reconciliation, matching Python's availability
behavior rather than promising automatic convergence.

The API documents whether freshness is `Unchecked`, `VersionMatched`, or
`ManifestSnapshot`. No cross-provider ETag equality is assumed.

Client shutdown awaits or cancels owned backfills with a configurable grace
period and returns a report. Dropping the last client cannot leave detached
Tokio tasks mutating replicas indefinitely.

## Acceptance criteria

- Stable priority order and config-order ties are tested.
- An `Unchecked` replica hit avoids primary I/O; miss/error falls through as configured.
- Full, range, stream, and download reads preserve requested semantics.
- All-replica failure returns the primary result/error with diagnostic context.
- Concurrent identical misses schedule one backfill per replica/version.
- Backfill failure never changes an already returned foreground result.
- Conditional backfill cannot overwrite a newer replica object.
- Range reads never publish a partial object as full backfill.
- Primary writes do not synchronously mirror.
- `Unchecked` reads explicitly permit stale replicas after writes or failed
  delete cleanup; freshness-checked reads reject stale or deleted generations.
- Delete returns a complete cleanup report and supports strict/best-effort mode.
- Bulk sync covers all/subset/invalid indices, filters, dry-run, partial failure,
  cancellation, and best-effort-all aggregation.
- Sync source performs no replica fallback or read-triggered backfill.
- Shutdown leaves no detached tasks.

## Source evidence

- `src/multistorageclient/replica_manager.py`
- `src/multistorageclient/client/single.py`
- `multi-storage-client-docs/src/user_guide/replicas.rst`
- `tests/test_multistorageclient/unit/test_replica_manager.py`
- `tests/test_multistorageclient/unit/test_sync.py`
