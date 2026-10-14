# Resumable Large-File Downloads

**Status:** Implemented on `shunjiad/feat-resumable-downloads`

**Issue:** [NGCDP-9343](https://nvidia.atlassian.net/browse/NGCDP-9343)

## Scope and Compatibility

Automatically resume Rust multipart downloads to local files through the existing S3-compatible and GCS backends. This includes single-file, batch, and remote-to-POSIX sync downloads. Public signatures and configuration stay unchanged; existing thresholds, chunk size, concurrency, and retries still apply. The Rust method returns the full object size, including reused bytes.

Python-only transfers, small-file downloads, file-like outputs, uploads, and persistent remote-to-remote staging retain their existing behavior. No legacy-state migration or special long-filename fallback is added. Power-loss recovery and detection of external edits to chunks are outside scope.

## Persistent State

Downloading `/data/model.bin` uses the deterministic directory `/data/.model.bin.msc-dl/`:

```text
lock                         # Exclusive OS lock
state.json                   # Object revision and chunk partitioning
000000.chunk                 # Completed chunk, zero-based index
000001.chunk.<id>.tmp         # Incomplete write
merged.<id>.tmp               # Output being assembled
```

Create directories with mode `0700` and files with mode `0600`, subject to umask. The published output retains mode `0600`. The destination basename must leave room for the leading dot and `.msc-dl` suffix.

Write `state.json` atomically before downloading chunks:

```json
{
  "format": "msc-download-v1",
  "key": "models/model.bin",
  "size": 104857600,
  "etag": "object-revision",
  "version": null,
  "chunk_size": 33554432
}
```

Retries to the same local destination assume the same backend and bucket. Compare key, size, ETag, and version to identify matching state. Require the complete schema with valid types, an unsigned 64-bit size, and a positive unsigned 64-bit chunk size. ETags are opaque validators; weak/empty ETags and empty/S3 `null` versions do not enable resume.

Chunk `i` covers `[i * chunk_size, min((i + 1) * chunk_size, size))`. A completed chunk is reusable only when it is a regular `.chunk` file of the expected length. Chunk files record progress; no bitmap or per-chunk metadata updates are needed. Atomic metadata/chunk writes use unique temporary files. Temporary IDs are six ASCII alphanumeric characters; chunk indices have at least six digits.

## Download Lifecycle

1. **Inspect and lock.** Fetch fresh remote metadata. Validate existing state before locking, then always read it under an exclusive `lock` file lock, even if this call created the directory. Verify that the locked file still matches its directory entry; retain the lock through publication and cleanup.
2. **Prepare state.** Matching state keeps its saved chunk size and uses current concurrency. Changed keys, sizes, or revisions discard recognized download files and install fresh metadata. A missing marker reinitializes the same directory under its lock, discarding old chunks before publishing fresh state. Invalid/unreadable markers, symlink markers/directories, and conflicting paths fail without deleting unrelated contents.
3. **Download missing chunks.** Remove recognized interrupted temporary files, then request missing or wrong-length chunks with bounded concurrency. Pin Range requests to the captured ETag/version and validate the response revision, range, and length before atomically publishing each chunk. On failure, stop and await outstanding tasks, retain completed chunks, and propagate the error.
4. **Merge and publish.** Stream chunks in order into a merge temporary file, verify total length, flush and `sync_all`, then atomically replace the destination. Keep chunks and any existing destination until publication succeeds. Merge/publication failure retains chunks for retry.
5. **Clean up.** Remove recognized download files, then the marker, lock, and empty directory while retaining the lock handle. Preserve unrelated entries; restore the marker if removing the lock or directory fails. Cleanup failure after publication logs a warning and still returns success.

Without a usable revision validator, or when the persistent lock is busy/replaced, use independent transient staging such as `.msc-dl-<id>.msc-dl/`. It is not reused across calls. Concurrent downloads publish complete files atomically; the last successful publication wins. Metadata/authentication errors preserve existing state.

`ObjectFile` cache fills and append operations own random temporary paths that later operations will not reuse. After their download call and retries finish, remove the temporary file and its entire adjacent state directory, on success or failure, without inspecting markers. Removal does not follow symlinks outside the directory; cleanup errors do not replace transfer errors. Fixed-path downloads retain interrupted state for resume.

## Sync Exclusion

Dot-prefixed POSIX directories ending in `.msc-dl` are reserved staging directories. Automatic sync excludes their contents from copying and unmatched-file deletion regardless of `ignore_hidden` or marker validity. Copies into reserved POSIX destinations are also excluded. Explicit `source_files` bypass source and destination staging checks for copying; existing hidden-file, pattern, and symlink settings still apply. Target staging files remain excluded from unmatched-file deletion.

Reuse the existing `list`/`list_recursive` selection and filter paths after traversal. The predicate checks directory names only, without resolving metadata aliases or filesystem symlinks. Ordinary listing, object-store keys, composite listings, other hidden directories, and standalone files such as `.model.msc-dl` retain their usual behavior. Ordinary-named aliases into staging follow existing sync settings and may expose its contents.

## Implementation and Cost

Paths below are relative to `multi-storage-client/` unless noted.

| Files | Change |
|---|---|
| `rust/src/download.rs`, `rust/src/lib.rs`, Rust Cargo manifests/lockfile | Private state, locking, chunk transfer, merge, and cleanup; delegate the existing Rust entry point. |
| `src/multistorageclient/providers/s3.py` | Pass the stable final destination to Rust. GCS and batch routing remain unchanged. |
| `src/multistorageclient/file.py` | Clean up disposable cache/append paths and their state. |
| `src/multistorageclient/sync/producer.py` | Filter reserved POSIX paths from existing listings. |
| `src/multistorageclient_rust/multistorageclient_rust.pyi` | Document resume and full-size return semantics; signatures unchanged. |
| Existing Rust/provider/file/sync tests; repository `multi-storage-client-docs/src/user_guide/{rust,cli}.rst` | Verify recovery and document staging, exclusions, and storage cost. |

Each completed chunk adds one file; a 1 TiB object at 32 MiB per chunk creates 32,768 chunk files. Merging adds a full sequential read and write. Peak staging space is approximately twice the object size, plus any existing destination. Chunk buffers remain bounded by chunk size and concurrency.

## Validation

Acceptance coverage:

- Interrupted downloads resume across calls/processes; reuse only complete chunks, accept a short final chunk, and retain saved partitioning after chunk-size changes.
- Changed keys/revisions reset state; conditional requests prevent mixed revisions. Merge/publication failures preserve chunks and the existing destination.
- Missing markers recover in place; invalid markers and unrelated persistent contents are preserved. A directory creator must recheck state written by another downloader before it acquired the lock. Same-/cross-process contention uses independent staging; cleanup interruptions remain recoverable.
- Single-file, batch, and remote-to-POSIX sync share the transfer path. Disposable cache/append failures clean up fully without masking errors.
- Sync filtering covers persistent/transient leftovers, marker states, hidden settings, attributes, explicit files, ordinary hidden directories, object-store keys, metadata aliases, and existing symlink modes.

Verification includes 32 download cases, 90 sync producer cases, 37 local file/lifecycle cases, and 17 Rust unit tests, plus existing provider and bridge coverage. Client lint/type checks, Python 3.10 compatibility checking, and documentation build passed. Full unit execution is blocked on this macOS host by GNU `mkdir --parents`; `just build` is blocked by missing `treefmt`. Real-cloud integration and large-file throughput remain unmeasured.
