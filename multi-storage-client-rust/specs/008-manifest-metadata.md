# 008: Manifest metadata

- Status: Draft for review
- Depends on: 001, 003
- Independent of: 009 replicas, 010 composite routing

## Summary

A manifest metadata provider maps logical keys to physical objects and serves
fast metadata/listing operations from versioned manifest snapshots. This spec
first delivers read-only JSONL v1, then adds writable snapshots and Parquet
without changing the `MetadataProvider` contract.

This provider describes one data backend. Multi-backend profile routing and
extent assembly belong to spec 010 or later work.

## Configuration

```yaml
metadata_provider:
  type: manifest
  options:
    manifest_path: .msc_manifests
    storage_provider_profile: optional-separate-profile
    writable: false
    allow_overwrites: false
    manifest_format: jsonl
```

`manifest_path` is relative to the selected manifest-storage profile unless it
is already a valid provider-relative key. The default storage profile is the
data profile. Referencing another profile cannot introduce a provider-build
cycle.

`writable`, overwrite, and output format options are rejected until their
delivery phase is compiled/enabled; they are never silently ignored.

## Discovery

The configured path can identify:

1. a concrete `msc_manifest_index.json`;
2. a directory containing that filename; or
3. a base directory under which `.msc_manifests/*/msc_manifest_index.json`
   snapshots exist.

Case 3 uses the Rust publication pointer while also recognizing newer Python
publications. Timestamp comparison accepts canonical Python RFC 3339 names and
legacy compact Rust names. Empty discovery yields an explicit
`ManifestNotFound` result; callers decide whether this is fatal. It does not
silently fall back to live provider listing when a manifest profile was
explicitly configured.

New Rust snapshots use Python MSC 1.4.0's whole-second RFC 3339 directory names.
A writer reserves an unused second, waiting for the next second if needed.
`msc_manifest_rust.json` records the index digest before index publication, so
unpublished Rust indexes cannot supersede the committed pointer. A Python rewrite
in the same directory changes that digest and becomes visible. Commits compare
the observed snapshot contents as well as the pointer to detect foreign writes.
Concrete index paths remain pinned. Python writers do not participate in Rust's
commit lease; applications must serialize writers across languages.
Python readers also ignore Rust's publication pointer and marker. They can
observe a complete index before Rust finishes advancing its pointer, and Python
can overwrite an index within the same second. Shared bases therefore inherit
Python's publication and historical-snapshot limits. Rust readers retain the
stronger pointer recovery protocol.

Python 1.4.0 sorts old compact Rust directory names incorrectly. When an existing
base contains those names, generate the current snapshot into a fresh shared
manifest base and point both clients at that base. `ManifestGenerator::generate_source`
preserves logical-to-physical mappings, so data objects do not need to move.
Keep the old base for readers pinned to historical indexes. Rust can continue
reading the legacy base and can discover newer Python snapshots within it.

For example, with the old mapped data client and the manifest-storage client:

```rust,ignore
let options = ManifestOptions {
    manifest_path: ObjectPath::new("shared/.msc_manifests")?,
    ..ManifestOptions::default()
};
let generator = ManifestGenerator::for_client(options, &manifest_storage_client)?;
let index = generator.generate_source(&old_data_client, None).await?;
generator.shutdown().await?;
```

Set both clients' `manifest_path` to `shared/.msc_manifests` after generation.

Index and part paths are provider keys, not host paths. Relative part paths
resolve beneath the index directory and cannot traverse above it. Absolute host
path semantics are forbidden for remote manifests.

## V1 index format

```json
{
  "version": "1",
  "format": "jsonl",
  "parts": [
    {"path": "parts/msc_manifest_part000001.jsonl"}
  ]
}
```

The reader accepts version `"1"` and the documented legacy spelling `"1.0"`,
normalizing both to v1. Writers emit `"1"`. Unknown versions/formats/fields
needed for interpretation fail with a typed schema error.

Each JSONL record contains at least:

- `key`;
- `size_bytes`;
- `last_modified`;
- optional `physical_path`, defaulting to `key`;
- optional ETag/source version;
- object kind and symlink target; and
- optional string-valued custom metadata.

Unknown record fields are retained only in a namespaced extension map or
rejected according to a documented forward-compatibility flag; they never
overwrite core fields.

Blank lines may be skipped. Invalid JSON, duplicate keys, invalid paths,
negative sizes, unsorted data when sorting is required, and malformed times are
errors with part path and line number.

## Loading and indexes

The first milestone may load entries into memory for parity, but parsing is
streaming and enforces configurable limits for record size, count, and total
metadata bytes. Parts are processed in index order.

The provider builds a sorted key index and directory-prefix index. Logical
directories are synthesized from keys. Listing, head, glob, and symlink
resolution never need to enumerate the underlying data backend.

Duplicate logical keys across parts are an error rather than last-writer-wins.
This avoids snapshot behavior depending on part order.

A later on-disk or mmap index can replace the in-memory implementation without
changing behavior.

## Read-only behavior

- `info(logical)` returns manifest metadata.
- `resolve(logical)` returns `Exists` plus `physical_path` and no child profile.
- Missing entries return `Untracked`/not found according to the metadata trait.
- Recursive and delimited listings are globally lexical.
- Symlink resolution follows the common eight-hop/cycle contract.
- `add`, `remove`, and `commit` return `Unsupported` in read-only mode.
- `is_writable=false`, `allow_overwrites=false`, and `soft_delete=false`.

The client reads bytes from the data storage provider at `physical_path` while
returning the logical `key` and manifest attributes to users.

## Writable snapshots

Writable manifests are a second delivery phase. Adds/removes remain staged in
memory (or a local journal) until commit. Reads can opt into seeing pending
updates; normal committed views remain stable.

Commit creates a new immutable timestamped snapshot:

```text
.msc_manifests/<UTC timestamp>/
  msc_manifest_index.json
  parts/msc_manifest_part000001.jsonl
```

Part files are written and validated before the index is published last. A
reader therefore sees either the previous complete snapshot or the new complete
snapshot. Concurrent writers use a conditional latest-pointer/index publish or
fail with a conflict; they do not silently overwrite each other.

The first writer may emit one part. Later partitioning is deterministic and
bounded by record/byte limits. Old snapshots are retained; retention is a
separate maintenance feature.

Overwrite behavior:

- `allow_overwrites=false` rejects an existing logical key, including pending
  additions that collide with committed keys after conflict recovery and refresh;
- metadata updates to the same pending allocation staged during this writer's
  publication can be finalized by a later commit;
- true allocates/uses the correct physical path and records the replacement;
- physical write success plus manifest commit failure returns a repairable
  partial error; and
- no soft-delete claim is made unless tombstones are separately specified.

## Parquet follow-up

Parquet is an optional Cargo feature that maps the same logical schema and
validations. It cannot change timestamp, metadata, duplicate-key, or ordering
semantics. Mixed part formats in one v1 index are unsupported because v1 has
one index-level `format`.

## Manifest generation

A generator consumes an ordered `StorageClient` listing and writes a snapshot
through the same writer. It preserves logical key, physical path, size, mtime,
kind, symlink target, source version, and selected attributes. Generation is
streaming and bounded rather than collecting a full bucket before writing.

## Intentional differences from Python

- Both documented `"1.0"` and implemented `"1"` are accepted on read.
- Duplicate entries fail rather than silently replacing earlier parts.
- Path traversal in part references is rejected.
- Snapshot publication is index-last and concurrency-aware.
- Streaming parser/generator limits prevent unbounded record input.
- Multi-backend routing is not overloaded onto the single-backend format.

## Acceptance criteria

- Discovery tests cover direct index, containing directory, base directory,
  lexical latest selection, missing snapshot, and separate storage profile.
- Index tests cover versions 1/1.0, format default, unsupported values, missing
  fields, invalid part paths, and secret-safe errors.
- JSONL tests cover round-trip, blank lines, Unicode, physical path default,
  metadata, symlinks, duplicates, malformed lines, negative size, and limits.
- Head/list/glob/directory/symlink results match provider-neutral semantics.
- Read-only mutation methods fail explicitly.
- Writable tests prove pending visibility, overwrite policy, index-last atomic
  publish, concurrent conflict, recovery from part/index failure, and old
  snapshot retention.
- Generator tests remain bounded for a synthetic multi-million-entry listing.
- Parquet feature, when present, passes the same logical golden cases.

## Source evidence

- `src/multistorageclient/providers/{manifest_metadata,manifest_formats,manifest_object_metadata}.py`
- `src/multistorageclient/generators/manifest_metadata.py`
- `multi-storage-client-docs/src/user_guide/manifests.rst`
- `tests/test_multistorageclient/unit/providers/{test_manifest_metadata,test_manifest_formats}.py`
- `tests/test_multistorageclient/unit/generators/test_manifest_metadata.py`
