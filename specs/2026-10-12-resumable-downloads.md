# Resumable Large-File Downloads

**Status:** Single-file staging and bounded response batches implemented; functional checks and repeated Linux throughput verification pass on `shunjiad/fix-transfer-throughput-gap`

**Issue:** [NGCDP-9343](https://nvidia.atlassian.net/browse/NGCDP-9343)

## Scope and Compatibility

Automatically resume Rust multipart downloads to local files through the existing S3-compatible and GCS backends. This includes single-file, batch, and remote-to-POSIX sync downloads. Public signatures and configuration stay unchanged; existing thresholds, chunk size, concurrency, and retries still apply. The Rust method returns the full object size, including reused bytes.

Python-only transfers, small-file downloads, file-like outputs, uploads, and persistent remote-to-remote staging retain their existing behavior. No special long-filename fallback is added. Power-loss recovery and detection of external edits to staging data are outside scope.

The original implementation stored each completed range in a separate file, then read every range again and wrote the full object during final assembly. The revised implementation writes ranges directly into one staging file and publishes that file without a merge or full-data reread.

## Persistent State

Downloading `/data/model.bin` uses the deterministic directory `/data/.model.bin.msc-dl/`:

```text
lock                         # Exclusive OS lock
state.json                   # Object revision, partitioning, completed ranges
download.part                # One staging file, sized to the complete object
state.json.<id>.tmp           # Interrupted atomic metadata update
data.<id>.tmp                 # Interrupted staging-file initialization
```

Create directories with mode `0700` and files with mode `0600`, subject to umask. The published output retains mode `0600`. The destination basename must leave room for the leading dot and `.msc-dl` suffix.

On Unix, reject existing state directories that are not owned by the current effective user or grant access to other users. Reopen saved data without following symlinks. Retain the validated data handle for all range writes and final sync; reject a replaced data pathname before publication. The caller-selected destination parent remains a trusted namespace.

Write `state.json` atomically before downloading ranges:

```json
{
  "format": "msc-download-v2",
  "key": "models/model.bin",
  "size": 104857600,
  "etag": "object-revision",
  "version": null,
  "chunk_size": 33554432,
  "completed": [[0, 1], [2, 3]]
}
```

Retries to the same local destination assume the same backend and bucket. Compare key, size, ETag, and version to identify matching state. Require valid types, an unsigned 64-bit size, and a positive unsigned 64-bit chunk size. `completed` contains disjoint, strictly ascending, non-adjacent half-open `[start, end)` ranges of unsigned chunk indices ending at or below the chunk count, so each checkpoint write stays proportional to the number of gaps rather than the chunk count; omitted progress means no reusable ranges. ETags are opaque validators; weak/empty ETags and empty/S3 `null` versions do not enable resume.

Chunk `i` covers `[i * chunk_size, min((i + 1) * chunk_size, size))`. Reuse a range only when its index is covered by `completed` and `download.part` is a regular file of the expected object size. File length alone does not establish completion: a newly sized file may contain unwritten ranges. Missing or wrong-length data clears all progress before a replacement staging file is published. Non-regular staging paths fail without following symlinks.

Recognize valid `msc-download-v1` state, discard its recognized chunk/merge files under the same directory lock, and restart using v2 state. Do not merge or migrate v1 data. Preserve unrelated contents and any existing destination. Malformed state and unsupported formats still fail without cleanup. Atomic metadata/data initialization uses unique temporary files with six ASCII alphanumeric characters.

Older binaries reject v2 state. Before downgrading, finish active downloads or stop them and remove their state directories. Transparent downgrade of partial progress is not supported.

## Download Lifecycle

1. **Inspect and lock.** Fetch fresh remote metadata. Validate existing state before locking, then always read it under an exclusive `lock` file lock, even if this call created the directory. Verify that the locked file still matches its directory entry; retain the lock through publication and cleanup.
2. **Prepare state.** Matching v2 state keeps its saved chunk size and uses current concurrency. Changed keys, sizes, revisions, or v1 state discard recognized download files and install fresh metadata. A missing marker reinitializes the same directory under its lock, discarding old data before publishing fresh state. Invalid/unreadable markers, symlink markers/directories, invalid completion indices, and conflicting paths fail without deleting unrelated contents. Initialize one private staging file at the complete object size; validate saved data before reusing progress.
3. **Download missing ranges.** Remove recognized interrupted temporary files, then request indices not covered by `completed` with bounded concurrency. Pin Range requests to the captured ETag/version and validate the response revision, range, and length. Stream each response into at most 2 MiB write batches at its fixed offset through the retained data handle, using positional writes on the supported Linux and macOS targets. Do not retain a complete multipart response per active request. The write batch size is independent of the configured multipart chunk size; concurrency still bounds active range requests. A blocking write retains the data handle, directory, and lock until it finishes, even if its async waiter is cancelled. Only after the entire range body passes its length check and all of its writes complete does the coordinator atomically checkpoint its completion in `state.json`; concurrent workers never overwrite each other's progress. A failed or interrupted body may leave bytes in an uncheckpointed range. A retry must overwrite that whole range, including any bytes written before the failure. On failure, stop outstanding async tasks, retain checkpointed ranges, and propagate the error. There is no per-range `fsync`; this preserves the existing process-interruption guarantee without introducing a power-loss guarantee.
4. **Sync and publish.** After every range completes, atomically clear persistent completion progress before calling `sync_all` on the retained data handle. Restore the full completed set only after sync succeeds. Checkpoint invalidation, sync, and checkpoint restoration run inside the same non-cancellable blocking task while retaining the data handle, directory, and lock, so cancellation cannot restore progress after a failed sync. A second non-cancellable blocking task then re-verifies that the retained data handle still matches the staging pathname, atomically renames the staging file over the destination, and retains the directory and lock, so a cancelled caller cannot release the lock to a concurrent download before a pending rename runs. A replaced staging pathname clears persistent completion records and fails before the rename, so a same-length replacement is not reused on retry; a pathname rename narrows this window but cannot fully close it. A failed or interrupted final sync can require downloading every range again, even if the staging file retains its full length. A publication-only failure after successful sync keeps the restored checkpoints and retries without data GET requests. Do not create a merged output, read staging data back, or write a second full copy. Preserve any existing destination until publication succeeds.
5. **Clean up.** Remove recognized download files, then the marker, lock, and empty directory while retaining the lock handle. Preserve unrelated entries; restore the marker if removing the lock or directory fails. Cleanup failure after publication logs a warning and still returns success.

Without a usable revision validator, or when the persistent lock is busy/replaced, use independent transient staging such as `.msc-dl-<id>.msc-dl/`. Write directly to one staging file without persistent progress updates or a final merge. It is not reused across calls. Concurrent downloads publish complete files atomically; the last successful publication wins. Metadata/authentication errors preserve existing state.

`ObjectFile` cache fills and append operations own random temporary paths that later operations will not reuse. After their download call and retries finish, remove the temporary file and its entire adjacent state directory, on success or failure, without inspecting markers. Removal does not follow symlinks outside the directory; cleanup errors do not replace transfer errors. Fixed-path downloads retain interrupted state for resume.

## Sync Exclusion

Dot-prefixed POSIX directories ending in `.msc-dl` are reserved staging directories. Automatic sync excludes their contents from copying and unmatched-file deletion regardless of `ignore_hidden` or marker validity. Copies into reserved POSIX destinations are also excluded. Explicit `source_files` bypass source and destination staging checks for copying; existing hidden-file, pattern, and symlink settings still apply. Target staging files remain excluded from unmatched-file deletion.

Reuse the existing `list`/`list_recursive` selection and filter paths after traversal. The predicate checks directory names only, without resolving metadata aliases or filesystem symlinks. Ordinary listing, object-store keys, composite listings, other hidden directories, and standalone files such as `.model.msc-dl` retain their usual behavior. Ordinary-named aliases into staging follow existing sync settings and may expose its contents.

## Implementation and Cost

Paths below are relative to `multi-storage-client/` unless noted.

| Files | Change |
|---|---|
| `rust/src/download.rs`, `rust/Cargo.toml` | Versioned state, locking, retained data handles, range writes, completion checkpoints, Unix staging protections, atomic publication, and cleanup; retain the existing Rust entry point. |
| `src/multistorageclient/providers/s3.py` | Pass the stable final destination to Rust. GCS and batch routing remain unchanged. |
| `src/multistorageclient/file.py` | Clean up disposable cache/append paths and their state. |
| `src/multistorageclient/sync/producer.py` | Filter reserved POSIX paths from existing listings. |
| `src/multistorageclient_rust/multistorageclient_rust.pyi` | Document resume and full-size return semantics; signatures unchanged. |
| Existing Rust/provider/file/sync tests; repository `multi-storage-client-docs/src/user_guide/{rust,cli}.rst` | Verify recovery and document staging, exclusions, and storage cost. |

Each download uses one data file, a lock, and JSON progress proportional to the number of completed ranges. For N fresh object bytes, the data path writes approximately N bytes with no full-file reread or second N-byte output. Peak staging space is approximately the object size, plus any existing destination and small metadata files. Application response buffers scale with small write batches and active request count rather than retaining each complete multipart range. HTTP/TLS buffers and allocation overhead remain additional memory costs. Saved v1 state is restarted once after upgrade.

## Validation

Acceptance coverage:

- A response that stalls after writing part of a range leaves that range uncheckpointed and preserves the old destination. A retry rewrites it and publishes exact contents.
- Interrupted downloads resume across calls/processes using checkpointed ranges; accept a short final range and retain saved partitioning after chunk-size changes. Out-of-order concurrent writes produce exact contents without shared-cursor races or lost progress.
- Changed keys/revisions, valid v1 state, and missing/wrong-length data reset progress safely; conditional requests prevent mixed revisions. Invalid completion indices, unsafe state directories, non-regular data paths, and replaced staging identities fail safely. Failed final sync invalidates progress before retry; publication-only failure preserves synced progress. Both preserve the existing destination.
- Fault-injected writeback failure with full-length corrupted staging data forces the real downloader to rewrite ranges and produce correct final contents. Path replacement during a paused transfer cannot redirect writes into an external file, and cannot publish replacement data.
- An interrupted fresh download contains one complete-size staging file and progress metadata, with no chunk or merged-output files. A fully checkpointed retry publishes without data GET requests. Missing markers never reuse untracked preallocated ranges.
- Missing markers recover in place; invalid markers and unrelated persistent contents are preserved. A directory creator must recheck state written by another downloader before it acquired the lock. Same-/cross-process contention uses independent staging; cleanup interruptions remain recoverable.
- Single-file, batch, and remote-to-POSIX sync share the transfer path. Disposable cache/append failures clean up fully without masking errors.
- Sync filtering covers persistent/transient leftovers, marker states, hidden settings, attributes, explicit files, ordinary hidden directories, object-store keys, metadata aliases, and existing symlink modes.

The original v1 verification covered 32 download cases, 90 sync producer cases, 37 local file/lifecycle cases, and 17 Rust unit tests, plus provider and bridge coverage. Client lint/type checks, Python 3.10 compatibility checking, and documentation build passed. Full unit execution was blocked on the original macOS host by GNU `mkdir --parents`; `just build` was blocked by missing `treefmt`.

For v2, run the existing recovery, concurrency, process-kill, provider, file-lifecycle, and sync suites, extending only uncovered cases. Rebuild the Python extension before testing the Rust bridge. Repeat a Linux download benchmark with data larger than RAM, holding backend, object set, chunk size, and concurrency fixed. Record elapsed time and process I/O, verify final contents outside the timed interval, and confirm elimination of the extra full read and write. Record completed verification and any remaining limitations with the change.

### V2 Verification Results (2026-11-17)

- `just multi-storage-client/run-unit-tests`: 1,818 Python tests passed, 3 skipped, with 7 reruns; all 18 native tests passed. This includes single/batch transfers, sync, disposable files, interrupted-process recovery, concurrent destinations, permissions, and publication failures.
- Extended native cancellation coverage passes for both range writes and final sync, using persistent and transient staging. Blocking operations retain their lock and staging resources after their async waiter is cancelled.
- After correcting the interrupted data-temporary filename, the rebuilt extension passed all 39 focused recovery cases and all 18 native tests again. Final client analysis, changed-file Rustfmt, Clippy, and whitespace checks also pass.
- After rebasing onto main at `832c9ca2`, the full suite passed again with 1,818 Python tests, 3 skipped, 5 reruns, and 18 native tests. Client lint and type checks also pass with the updated AIStore dependency.
- New bridge coverage verifies one staging file, checkpointed out-of-order ranges, missing/truncated/non-regular data, invalid completion indices, v1 restart, and publication by renaming the same staging inode.
- `just multi-storage-client/analyze` and `just multi-storage-client-docs/build` pass, including lint, type checks, and the Sphinx build with warnings treated as errors.
- Rustfmt passes for the modified Rust file. `cargo clippy --all-targets --no-deps` succeeds, with existing warnings in untouched credential/binding code. Repository-wide rustfmt also reports pre-existing differences in untouched files.
- Matched Linux throughput verification passed against PDX with 32 × 2 GiB objects, 32 MiB chunks, and 16 concurrent files, exceeding the host's 23 GiB RAM. Baseline and candidate use source revision `7ff940bcbccd80037fff3619f07610f473231650`, the same Cargo lock and Python dependencies, and the same objects and settings; only the candidate download implementation differs. Included runs alternate baseline/candidate twice. All 32 destination files in every included run passed full SHA256 comparison with their source files outside the transfer timer.

| Implementation | Elapsed seconds, two runs | Aggregate MiB/s | Process read GiB per run | Process write GiB per run |
|---|---|---|---|---|
| Baseline chunk files and merge | 148.82, 144.78 | 446.43 | 63.54, 63.53 | 128.00, 128.00 |
| Single staging file and checkpoints | 51.57, 49.34 | 1298.84 | 0.0030, 0.0034 | 64.0081, 64.0081 |

Aggregate throughput improved by 2.91× and mean elapsed time fell by 65.6% on this workload. Counters are Linux process I/O accounting, not direct physical-device measurements; these two pairs do not establish a universal speedup. Metadata checkpoint writes are included. Final verification confirms elimination of the full staging-data reread and second full data write while retaining resume behavior.

The preliminary candidate sample predates the interrupted-temporary cleanup correction and is excluded from these figures. Both included candidate runs use the corrected native-library SHA256 `5b5073c48b5db309faab4f268ff22cb69c125567ae52aec51a80e91e9ce4c321`, built from the exact verified `download.rs` source (SHA256 `1dcf3d9a4bd212652c105cf6f4f33c552e2a3e40a8dfcabeb73a88e997be3430`). Test objects, local data fixtures, the copied credential configuration, and task-owned build caches were removed after verification.

These measurements precede the review follow-up that retains one data handle, validates private staging ownership, and invalidates progress before final sync. That follow-up preserves the single full data write and adds only metadata checkpoint updates; the recorded throughput is a measurement of the identified source revision, not a new measurement of later revisions.

Review follow-up verification: all 19 native tests and 1,822 Python unit tests passed, with 3 skips and one AIStore idempotent-delete retry. The 43 focused download/sync tests also passed. Fault injection leaves full-length corrupted staging data after an EIO and verifies that a real retry rewrites the ranges and publishes exact contents. Existing rename-failure coverage confirms retry without data GET requests; new paused-transfer cases reject symlink and regular-file replacement without changing external files or the existing destination.

### Regression Follow-up (2026-11-18)

The investigation compared the last good release before proposing further changes. On the same Linux host, 32 × 2 GiB with the same Python dependencies and transfer settings reached 1,275 and 1,170 MiB/s with MSC 1.2.0, then 403 MiB/s with MSC 1.3.0. The first regressing implementation is commit `ddd557415e87005531f1e4f1a9ca0d9c88f0ba4c` (resumable downloads, PR #207). The failing run read approximately 64 GiB and wrote 128 GiB. Restoring only the MSC 1.5 download module under the QA revision's compiler and dependency lock reproduced the extra read/write and reached 557 MiB/s. The exact QA PR wheel reached 1,303 MiB/s on the same 64 GiB fixture. This supports the original merge regression and the single-staging-file fix on this host.

The reported subsequent slowdown of the QA PR relative to MSC 1.5 has not reproduced under pinned conditions. Completed 100 × 2 GiB runs of the exact QA wheel reached 1,360 MiB/s, while s5cmd 2.2.2 with its defaults reached 1,242 and 1,351 MiB/s. All destination bytes passed SHA256 checks outside the transfer timer. An unchanged rebuild of the QA source varied from 1,370 to 1,020 MiB/s across two runs. System samples show substantial local-device writeback activity and I/O wait, but do not establish an exclusive cause of that variation. These results do not establish that QA's original environment or SDK timing boundaries have been reproduced.

The follow-up candidate replaces complete-range response collection with batches of at most 2 MiB. Conditional requests, response validation, range checkpointing, retained handles and locks, final sync, and atomic publication keep their existing contracts. A full range is checkpointed only after its entire response has been validated and every batch write has completed. The existing timeout test now covers a response that writes a batch and then stalls: the destination survives, the range remains uncheckpointed, and retry publishes exact contents with different data across batch boundaries.

The earlier streaming prototype reached 1,283 MiB/s on 100 × 2 GiB with 1.48 GiB peak process RSS, versus 12.66 GiB for the exact QA wheel. This is a memory improvement, not proof of a throughput improvement. Offloading checkpoint JSON alone and combining it with streaming did not show a consistent throughput advantage and were excluded from the candidate. No upload implementation changed.

The current candidate splits incoming buffers to keep each write batch at most 2 MiB. Its `download.rs` SHA256 is `a8d5e132fed5a4ba45ac5061da2f9b1e543de4859f343f1e550e4f873bdb8fc2`; the Linux native-library SHA256 is `d620e0a53f36e09af6f6718359f4193f93229bb6f24bbcbf20674b20849e3f6c`. The actual unit-test entrypoint passed 1,824 Python tests with 3 skips and 2 reruns, plus all 20 native tests. Client analysis, changed-file Rustfmt, Clippy, and the documentation build pass. The first concurrent documentation build exited 137; its isolated retry passed. Clippy retains warnings in unchanged code.

Final Linux verification of that exact source and native library completed with 100 × 2 GiB objects, exceeding the host's 23.45 GiB RAM. All files in every included run passed full SHA256 comparison outside the transfer timer. The standard-library-only controller used less than 6 MiB RSS during these final runs. Transfer settings remained at 32 MiB ranges, 8 requests per file, batch size 32 and 16 files per batch. The initial 32 independently generated file contents repeat across 100 distinct remote keys and local destination inodes; each 2 GiB file repeats its own random 16 MiB block.

| Implementation | Elapsed seconds | MiB/s | Peak RSS GiB |
|---|---:|---:|---:|
| Exact bounded-response candidate, r17 | 139.19 | 1,471.34 | 1.38 |
| s5cmd 2.2.2 defaults, r17 | 159.27 | 1,285.86 | 0.12 |
| Exact bounded-response candidate, r18 | 147.85 | 1,385.15 | 1.35 |
| s5cmd 2.2.2 defaults, r18 | 151.52 | 1,351.67 | 0.14 |
| Unchanged QA source rebuild, r18 | 147.98 | 1,383.97 | 12.74 |

The two candidate runs aggregate to 1,426.95 MiB/s versus 1,317.94 MiB/s for s5cmd, an 8.3% advantage. Including s5cmd's separately measured file-sync tail yields 1,426.80 versus 1,304.67 MiB/s. Candidate minor faults are 1.38 and 1.51 million, versus approximately 53 million for the whole-response controls. All candidate runs retain one payload write and no full payload reread. The write-size-only diagnostic retained approximately 13 GiB peak RSS and higher CPU cost, so it was excluded.

These runs establish parity with s5cmd for the measured host and fixture. The unchanged control also reaches parity, and earlier unchanged runs varied substantially. The evidence therefore supports a repeatable memory reduction more strongly than a causal throughput improvement over the existing fix. It does not explain or disprove QA's newer report. Offloaded checkpoints, added writer queues, conditional-header removal and upload changes remain excluded.

All original result files, scripts, hashes and validation logs are preserved in the local investigation artifact. Early 100-file trials whose fixture parent retained substantial memory are excluded. The final controller reports completion, all 340 task-owned objects were removed from the unique test prefix, current-object and multipart-upload listings are empty, the copied credential configuration was removed, and the task-owned system sampler exited. Bucket versioning was reported as not enabled. The user's original configuration and unrelated data were preserved.
