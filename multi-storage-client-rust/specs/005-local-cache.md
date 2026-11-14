# 005: Local cache

- Status: Draft for review
- Depends on: 001, 002, 003
- Independent of: 006 synchronization

## Summary

The local cache is a safe, async read-through layer for remote objects. It
supports whole-object caching, fixed-size range chunks, cross-task and
cross-process download deduplication, source-version validation, mutation
invalidation, and a global capacity policy shared by profiles.

The Rust implementation preserves user-visible MSC caching behavior while
intentionally replacing the Python cache's path, locking, publication, and
invalidation hazards.

## Configuration

Caching is active only when a global `cache` block exists and the selected
single-backend profile has `caching_enabled: true`. POSIX providers bypass the
cache. Composite caching is out of scope.

Resolved defaults:

| Setting | Default |
|---|---|
| capacity | 10 GiB |
| location | `$TMPDIR/msc_cache` |
| cache line size | 64 MiB |
| source-version check | enabled |
| reader prefetch | whole object |
| eviction policy | FIFO |
| refresh interval | 300 seconds |
| purge factor | 0% |

Capacity and line size accept positive integer or fractional binary units `M`, `G`, `T`, `P`, and `E` and are
stored as bytes. Location must be absolute; line size must be positive and no
larger than capacity. Refresh interval is at least one second and purge factor
is 0–100.

Deprecated `size_mb`, scalar `eviction_policy`, and `use_etag` are rejected with
migration messages. A legacy `cache_backend.cache_path` alias may be read for
one compatibility release, but `location` is canonical and conflicts fail.

## Cache identity and layout

An entry identity includes:

- schema version;
- profile name;
- a non-secret backend fingerprint;
- resolved physical object key; and
- representation kind (whole object or chunk line size/index).

Logical aliases that resolve to the same physical object within one profile
may share payload only when their mutation/invalidation relationship is known.
Different profiles never collide even if keys are identical.

Use a versioned hashed layout rather than interpolating untrusted object keys:

```text
<root>/v1/
  profiles/<profile-hash>/
    objects/<key-hash-prefix>/<key-hash>/
      identity.json
      object.lock
      full.data
      full.meta.json
      chunks/<line-size>/<index>.data
      chunks/<line-size>/<index>.meta.json
  tmp/<process-id>/...
  eviction.lock
```

`identity.json` records the original redacted identity and prevents a hash
collision from becoming a false hit. Hashing uses a stable cryptographic hash.
Lock files are persistent, excluded from capacity, and never unlinked while
held.

Every entry path is derived only by the safe mapper. Absolute keys, `..`,
symlinks inside the cache tree, names ending in `.lock`, Unicode, and very long
keys cannot escape or collide with control files.

## Metadata

Sidecar JSON is canonical so the cache works on macOS, Linux, NFS/Lustre
variants, and filesystems without xattrs. Xattrs may be written as an optional
diagnostic/compatibility mirror but are never required for a hit.

Entry metadata includes:

- cache schema version;
- complete identity/hash;
- source version and its provider scope;
- known object size;
- payload length;
- line size and chunk index for chunks;
- generation ID;
- created and last-access timestamps; and
- optional integrity checksum.

Structural metadata is validated on every hit even when source-version checks
are disabled. Changing `cache_line_size` can never reuse chunks created with an
old size.

## Locking and single-flight

The cache combines in-process single-flight with cross-process advisory locks.
Blocking lock acquisition runs outside async executor workers.

Lock order is fixed:

1. object shared/exclusive lock;
2. chunk exclusive lock.

A caller must never acquire object lock after chunk lock. Reads hold a shared
object lock while opening and validating an entry. Publication, replacement,
mutation invalidation, and whole-family cleanup hold the exclusive object lock.
Different chunks can fill concurrently after the shared object state check.

After waiting for another producer, a caller rechecks the cache before
downloading. Lock timeout degrades to direct provider I/O; it must not deadlock
the request. OS lock release on process death plus persistent lock files avoids
the Python inode race caused by unlinking a held lock.

Eviction takes one root-level `eviction.lock`, then uses nonblocking entry locks.
It skips in-use entries and revisits them later. This fixes the upstream
per-profile eviction-lock bug while retaining a global capacity.

## Atomic publication

Payload and validity metadata are one observable publication unit:

1. create same-filesystem temporary payload and metadata files under the cache root;
2. stream payload, verify length/conditions, flush as configured;
3. write complete temp metadata;
4. obtain the exclusive object/entry lock;
5. finish immutable payload and metadata generation files, then atomically replace
   one manifest referencing both files from that generation; and
6. release only after the manifest is published, with durability as configured.

Readers resolve both files through the same manifest under a shared lock.
Missing, malformed, or mismatched generations are misses, including when source
version checking is disabled. A crash exposes either the previous generation or
the complete new generation. RAII removes temps after cancellation, panic,
provider failure, disk full, or metadata failure. Maintenance removes orphaned
temps and unpublished generations older than a grace period.

No caller can observe a new payload with old/missing metadata, which was
possible when the Python full-file cache set xattrs after rename.

## Whole-object reads

On read:

1. map identity and inspect a whole-object entry;
2. validate structure;
3. if source checking is enabled, HEAD and compare the stored scoped version;
4. serve the hit, otherwise single-flight a provider GET;
5. admit and atomically publish only if the final size is within capacity; and
6. remove obsolete chunks after publishing the full object.

When validation is disabled, a structurally valid hit performs no HEAD. A
whole object without an ETag is valid in this mode. Objects larger than the
capacity bypass whole-object publication; they still stream to the caller.

`SourceVersionCheck` is `Inherit`, `Enable`, or `Disable`, with per-call choice
overriding global config.

## Partial/range reads

Range reads first try a valid whole object. Otherwise they map the half-open
request to fixed-size chunks:

```text
first = offset / line_size
last  = (offset + length - 1) / line_size
```

A zero-length request returns immediately and avoids underflow. Missing chunks
are fetched with exact provider ranges, independently locked, structurally
validated, and concatenated only across the requested overlap.

Rules:

- A final short chunk is valid only when known object size proves EOF.
- A truncated or unverifiable short read is discarded and the request falls
  back to direct provider I/O.
- Entirely beyond EOF returns empty; crossing EOF returns the available prefix.
- A multi-chunk response must use one source generation.
- Prefer conditional range GET with `if_match=source_version`.
- Without conditional range support, HEAD before and after a fill; discard and
  retry a bounded number of times if the version changed.
- Multi-chunk reads require a usable source version or an equivalent provider
  mechanism that pins every chunk to one generation. Missing versions from both
  HEAD responses do not prove consistency. Without either mechanism, reject the
  consistency-sensitive read as unsupported rather than combine unpinned chunks.
- If a request from offset zero covers the known complete object, it may publish
  a whole entry instead of chunks.
- A small chunk zero becomes a whole object only when completeness is proven.
- A whole-object publication removes the same key's chunks.

Validation-disabled reads still check line size, index, generation, payload
length, and known object size.

## Reader behavior

`prefetch_file=true` obtains a validated whole object before opening the local
reader. `false` exposes a binary async seekable reader backed by range chunks.
An explicit read option overrides config. Both paths return the `ObjectReader`
from 003 implementing `tokio::io::AsyncRead + AsyncSeek`; this is an MVP API,
not a later Python `open()` compatibility layer. With caching disabled, that
same handle obtains ranges directly from the provider.

The seekable reader:

- performs no I/O on seek alone;
- fetches only chunks intersecting reads;
- returns stable bytes from one source generation per read call;
- supports repeated and backward seeks; and
- does not promise text decoding or line buffering.

Text-mode behavior is outside the Rust API.

## Mutation invalidation

After a successful remote write, upload, copy-to, delete, or writer `finish`, the
client invalidates the full object and all chunk sizes/generations under the
exclusive object lock. Successful idempotent delete of an already-missing
object also invalidates. Outcomes that may have committed, including
`AmbiguousOutcome`, invalidate the same entries even when the operation reports
failure.

Invalidation increments or replaces the local generation so a download that
started before the mutation cannot later publish stale bytes. Only a failure
established not to have committed keeps an older valid cache entry. When a
provider returns the new version and complete payload, a future write-through
optimization may publish it; MVP uses
invalidation.

Provider metrics from 011 observe only real backend calls. A cache hit does not
increment provider read counters; a validation HEAD is an `info` operation and
each missing-chunk fetch is a `read`. Cache hit/miss/eviction metrics remain a
separate follow-up and must not be mislabeled as provider operations.

This rule applies even when external source-version checking is disabled.

## Capacity and eviction

Capacity is a soft global limit across profiles. Concurrent publication may
temporarily exceed it. A successful publication schedules maintenance when the
refresh interval has elapsed; maintenance is never required for foreground
success.

If current payload bytes exceed capacity, eviction targets:

```text
capacity * (1 - purge_factor / 100)
```

Payload files count; locks, sidecars, identity files, and live temps do not.
Whole objects and chunks are individual eviction units, but evicting an entry
always leaves its metadata consistent.

MVP supports FIFO using creation timestamps, LRU and MRU using explicit access
metadata rather than filesystem atime, RANDOM selection, and NO_EVICTION.
NO_EVICTION intentionally permits growth beyond configured capacity.

Maintenance tolerates individual stat/unlink failures, skips locked entries,
and converges on later passes. `purge_factor` does not trigger early eviction
when the cache is within capacity.

## Failure policy

The cache is an optimization:

- invalid configuration or an unusable root fails client construction;
- corrupt entries, lock timeout, disk full, metadata write failure, or
  maintenance failure degrade to provider reads and may quarantine the entry;
- failed validation HEAD cannot use an unvalidated entry, but a direct GET may
  still succeed;
- a known stale entry never masks a provider error;
- eviction failure never fails a foreground read; and
- cancellation releases locks and never publishes a partial entry.

## Intentional differences from Python

- Hashed safe layout instead of raw mirrored keys.
- Root-level eviction lock instead of a lock inside each profile.
- Persistent lockfiles rather than unlinking a held chunk lock.
- Sidecar metadata works without xattrs.
- Full-object payload and metadata publish atomically.
- Structural checks remain active without ETag/version checks.
- All successful mutations invalidate both full and partial cache families.
- Multi-chunk reads cannot combine object generations.
- LRU/MRU do not rely on unreliable filesystem atime.

## MVP and follow-ups

MVP includes whole-object and range cache, seekable binary reader, version
tristate, no-HEAD hit path, safe layout, atomic publication, single-flight,
cross-process locks, mutation invalidation, global eviction policies, capacity,
purge factor, and crash recovery.

Follow-ups include LRU/MRU/RANDOM/NO_EVICTION parity, Python cache migration,
persistent eviction index, checksums, read-ahead, adjacent-chunk parallelism,
metrics, inspection/purge CLI, composite caching, and Windows-specific locks.

## Acceptance criteria

### Configuration

- Defaults and every invalid bound/unit/policy are tested.
- Disabled profiles and POSIX providers perform no cache I/O.
- Profiles share capacity but never collide on identical keys.

### Whole objects

- First read performs one GET; repeats hit.
- Disabled validation produces zero HEAD calls on a hit.
- All `Inherit`/`Enable`/`Disable` combinations are covered.
- Equal version hits; changed/missing/corrupt metadata misses safely.
- Oversized objects are served but not admitted.
- Concurrent tasks/processes on one miss publish one valid download.

### Ranges

- Tests cover one/multiple chunks, boundaries, EOF cases, zero length, custom
  line size, repeated reads, and seek patterns.
- Old-line-size chunks never hit after config changes.
- Source change cannot yield a mixed-generation result.
- Valid whole files satisfy ranges without creating chunks.
- Short reads are accepted only with proven EOF.

### Races and recovery

- Mutation racing an old fill cannot republish stale content.
- Reader/eviction/replacement races see a complete old or new entry only.
- Task cancellation, process kill, and panic release locks.
- Orphan temps are cleaned after the grace period.
- Absolute, traversal, symlink, and collision attacks cannot affect files
  outside the cache root.

### Eviction

- Global lock serializes eviction across profiles/processes.
- FIFO/LRU/MRU ordering, RANDOM selection, NO_EVICTION, and purge 0/20/50/100 reach the expected target.
- Locked entries are skipped and a later pass converges.
- Full and chunk bytes count; locks/metadata do not.

### Mutations

- Write/upload/copy/delete invalidate full and all chunks.
- A mutation failure known not to have committed preserves a valid older entry.
- `AmbiguousOutcome` and other possibly committed mutations invalidate old entries,
  including when source-version checking is disabled.
- A client write is immediately visible even with version checking disabled.

## Source evidence

- `src/multistorageclient/cache.py`
- `src/multistorageclient/caching/`
- `src/multistorageclient/{_xattr,file,config,schema,types}.py`
- `src/multistorageclient/client/single.py`
- `multi-storage-client-docs/src/user_guide/cache.rst`
- `tests/test_multistorageclient/unit/{test_cache,test_cache_mp,test_partial_file_caching,test_file}.py`
