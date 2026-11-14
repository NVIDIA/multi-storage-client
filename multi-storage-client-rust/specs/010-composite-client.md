# 010: Composite read-only client

- Status: Draft for review
- Depends on: 001, 003, and the metadata routing contract
- Independent of: the concrete manifest format in 008

## Summary

A composite client exposes one logical read-only namespace whose objects live
across multiple child profiles. A metadata provider resolves each logical key
to `(child profile, physical key)`. The parent can list and read but cannot be a
write/sync target.

This makes the configuration shape already accepted by the Python schema a
first-class Rust feature and fixes the Python loader path that could not build
it without a custom provider bundle.

## Configuration

```yaml
profiles:
  dataset:
    storage_provider_profiles:
      - us-backend
      - eu-backend
    metadata_provider:
      type: routing-manifest
      options:
        manifest_path: ...
```

The profile cannot also define `storage_provider`, credentials, cache,
replicas, or a single-backend provider bundle. Child profile names must exist,
be unique, and resolve to single-backend clients. Cycles are rejected with the
full profile chain.

The metadata provider advertises a routing capability and returns a child
profile for every `Exists` object. A plain single-backend manifest that returns
no profile is not sufficient unless the composite config names an explicit
default child.

## Construction

The parent owns:

- its profile name;
- the routing metadata provider; and
- an immutable map of child name to `Arc<StorageClient>`.

Children retain their own credentials, retries, provider limits, and optional
local caches. The parent has no storage provider, retry loop, replica manager,
or cache of its own.

Construction validates every route target known at metadata-load time. Dynamic
routes to undeclared children fail on resolution and never fall back to another
child silently.

## Read behavior

For `read`, `read_range`, `reader`, `download_to`, and presigned URL:

1. resolve the logical path in parent metadata;
2. require state `Exists`, child profile, and physical key;
3. locate the declared child;
4. call the matching child operation on the physical key; and
5. expose the logical key/attributes in parent-facing metadata.

Range, conditions, cancellation, and streaming semantics are preserved. The
parent does not materialize a whole object simply because it routed across a
child boundary.

Batch downloads group resolved items by child to use child concurrency while
preserving original result indices and aggregate failures.

Presigned URLs come from the resolved child and are supported only when that
child can sign the requested operation.

## Metadata and listing

`info`, `list`, `list_recursive`, `glob`, `is_file`, and `is_empty` operate on
the logical metadata provider. They do not enumerate every child backend.
Results are globally lexical logical keys and never expose physical keys or
child names unless a diagnostic option explicitly requests routing details.

The routing provider is the source of truth for directories, attributes,
symlinks, and tombstones. Following a logical symlink re-enters parent routing
and observes the common cycle/depth rules.

## Write restrictions

The following return `Unsupported` before child I/O:

- write/writer/upload;
- copy and make-symlink;
- delete/delete-many;
- metadata commit;
- sync as target; and
- replica mutation.

A composite client can be a sync source. Sync receives direct routed reads and
logical ordered listing; it cannot use replica fallback beyond what a child
explicitly owns.

The public type may expose only a read-only trait to make invalid operations
unrepresentable where ergonomic. The general `StorageClient` enum must still
return a clear typed error when a runtime-resolved composite is used as target.

## Assembly/extent extension

Future assembly manifests may resolve one logical file to ordered extents from
multiple children. That is not part of this spec. A future `ReadPlan` sum type
can add `SingleObject` and `Extents` without changing basic list metadata.

Parent-level caching of assembled data needs a logical version covering every
extent and is explicitly deferred. Child caches may still cache individual
physical objects.

## Failure behavior

- Missing logical key -> parent `NotFound` without child probing.
- Missing child route -> `Integrity` with logical key and redacted route.
- Child physical not found for an existing route -> `Integrity`/stale metadata,
  not a normal logical miss.
- Child provider errors retain child/profile context while public paths remain
  logical.
- Partial batch results preserve original logical indices and child causes.

## Intentional differences from Python

- Direct `storage_provider_profiles + metadata_provider` config is buildable.
- Parent and child roles are statically validated instead of relying on a
  Python `ProviderBundleV2` class.
- Parent caching/replicas are rejected rather than ignored.
- Batch routing preserves typed per-item errors.
- Assembly/extents are a separate extension, not an accidental behavior of a
  single-object manifest.

## Acceptance criteria

- Construction covers valid multiple children, missing/duplicate names,
  single/composite cycles, incompatible parent fields, and provider-build
  failures.
- Read/range/stream/download route to the exact child/key once; `info()` reads
  logical metadata from the parent routing provider.
- Listing/glob use only parent metadata and remain globally lexical.
- Logical metadata never leaks physical key in normal output.
- Batch downloads group by child while preserving input order and failures.
- Child cache/retry/credentials are used; no parent duplicate layer appears.
- Every mutation fails before any child call.
- Composite works as sync source and is rejected as sync target.
- Missing route, unknown child, and stale physical object are distinct errors.
- Symlink routing covers same-child and cross-child logical targets, cycles,
  and depth limit.
- A fake routing provider proves the client does not depend on manifest v1.

## Source evidence

- `src/multistorageclient/client/composite.py`
- `src/multistorageclient/client/types.py`
- `src/multistorageclient/types.py` (`ProviderBundleV2`, `ResolvedPath`)
- `src/multistorageclient/config.py`
- `multi-storage-client-docs/src/references/configuration.rst`
- `tests/test_multistorageclient/unit/client/test_client.py`
