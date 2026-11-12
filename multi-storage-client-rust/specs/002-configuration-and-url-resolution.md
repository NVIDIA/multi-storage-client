# 002: Configuration and URL resolution

- Status: Draft for review
- Depends on: 001

## Summary

This spec defines how MSC YAML/JSON files become typed profiles and how user
paths resolve to a profile and provider-relative key. Core MSC configurations
should remain portable from Python while validation becomes stricter and error
messages become source-aware.

## Discovery

When no explicit paths are supplied, search in this order and load only the
first existing file:

1. `MSC_CONFIG`;
2. `$XDG_CONFIG_HOME/msc/config.yaml`, then `.json`;
3. `$HOME/.msc_config.yaml`, then `.json`;
4. `$HOME/.config/msc/config.yaml`, then `.json`;
5. each `$XDG_CONFIG_DIRS/msc/config.yaml`, then `.json`, defaulting the base to
   `/etc/xdg`; and
6. `/etc/msc_config.yaml`, then `.json`.

An invalid first existing file is an error; discovery does not fall through.
Explicit paths replace the default search list. Paths are made absolute before
loading. `.json` selects JSON; all supported YAML extensions select YAML;
unknown extensions are rejected rather than guessed.

No config is valid and yields only the implicit `__filesystem__` profile.

## Includes and merge rules

The root `include` field is an ordered list. Relative include paths resolve
against the containing primary file. Includes are one level deep; an included
file containing `include` is rejected with its source path.

The primary document is the base, followed by includes in listed order:

- `profiles`: merge by whole profile. Identical duplicates are idempotent;
  different definitions conflict. Fields of the same profile are not combined.
- `path_mapping` and `experimental_features`: identical entries are idempotent;
  different values conflict.
- `cache` and `posix`: may appear once or be exactly identical.
- `opentelemetry`: concatenate `metrics.attributes` in document order. Merge
  other fields additively; overlapping values must be identical or conflict.
- unsupported top-level sections fail validation.

The resolved representation omits `include` and records every contributing
source for diagnostics.

OpenTelemetry provider metrics are part of the initial release. The typed
`opentelemetry.metrics` schema and runtime behavior are defined in 011. Later
attribute providers override earlier values; reserved MSC attributes cannot be
overridden. Conflicts identify both source files and the field path.

## Environment expansion

String values recursively support `$NAME` and `${NAME}`. Mapping keys are not
expanded. Missing variables fail closed with the variable name and config
location. Values designated as secret are redacted in errors.

Expansion occurs on parsed string nodes before typed deserialization. It does
not coerce a substituted string into a number or boolean; typed non-string
settings must be expressed as native YAML/JSON values. This preserves the
effective Python behavior while making the restriction explicit.

## Typed schema

Unknown fields are rejected at every typed MSC level. Only provider,
credential, metadata, and extension `options` maps remain open for
provider-specific keys.

Top-level MVP fields:

- `profiles`
- `cache`
- `path_mapping`
- `experimental_features`
- `opentelemetry` for provider metrics
- `posix` only when its behavior is implemented

`retry` is explicitly declared in the profile schema, fixing the upstream
schema omission.

Each profile is exactly one of:

1. single backend: `storage_provider` or a registered `provider_bundle`;
2. composite: `storage_provider_profiles` plus `metadata_provider`.

Common fields are `credentials_provider`, `metadata_provider`, `replicas`,
`retry`, `autocommit`, `comment`, and `caching_enabled` where valid for that
profile kind. Incompatible combinations are rejected.

Profile names cannot begin with `_`, except the reserved `__filesystem__`.
Replica references must exist, cannot reference self, cannot duplicate a
profile even with a different priority, and cannot reference a profile that
itself has replicas. Priorities are positive integers; ties preserve config
order.

Provider `options.base_path` is required for storage providers and may be an
empty string for root-scoped object stores. Adapter-specific typed validation
runs when the provider is built.

## Built-in provider tags

The initial built-in schema supports only:

- `file`: POSIX for local transfers and cache storage;
- `s3`: AWS S3 and S3-compatible endpoints through endpoint/addressing options;
- `s8k`: Python-compatible SwiftStack tag using the same S3 adapter and feature;
- `gcs`: native Google Cloud Storage; and
- `azure`: native Azure Blob Storage.

A supported but disabled adapter reports the required Cargo feature. Other
tags are unsupported unless an application explicitly registers a factory;
they must not suggest enabling a Cargo feature that does not exist. Python
tags such as `gcs_s3` and `ais_s3` are not initial built-ins; those compatible
services use `s3` with appropriate options instead of dedicated presets.
`s8k` retains the Python profile tag and accepts the same S3 options and static
credentials, including `S3Credentials` with `access_key` and `secret_key`.
HTTP 429 uses the common client retry budget. MIME type inference is enabled by default for `s8k` and can be configured
through `infer_content_type`. Metrics retain the `s8k` provider identity.
Other native adapters may be introduced by future specs without expanding the
initial scope.

Python fully-qualified class names are never imported. They produce an error
that directs Rust callers to `ProviderRegistry`.

## OpenTelemetry configuration

`opentelemetry.metrics` supports typed `attributes`, `reader`, and `exporter`
settings from 011. OTLP endpoint/authentication and reader timing are validated
without starting an exporter. Exporter headers and resolved environment
secrets are redacted in config display and diagnostics. Python-only telemetry
process/IPC settings and unsupported signals produce actionable errors rather
than being ignored.

## Cache configuration

The global `cache` block is typed as defined in spec 005. A profile uses it only
when `caching_enabled: true`. Important validation performed during config load:

- location is absolute;
- capacity and line size use positive integer or fractional binary units `M` through `E`;
- line size does not exceed capacity;
- refresh interval is at least one second;
- purge factor is 0–100; and
- removed keys `size_mb` and `use_etag` produce migration errors.

The canonical default policy is FIFO, not the stale LRU comment found in one
upstream type.

## Default and implicit profiles

The resolved config always contains an implicit profile when the user did not
define it:

```yaml
profiles:
  __filesystem__:
    storage_provider:
      type: file
      options:
        base_path: /
```

A conflicting user definition is rejected.

URL resolver behavior:

- `msc://profile/path` selects `profile` and normalizes repeated `/` in path.
- Absolute POSIX paths select `__filesystem__`.
- `file://`, `s3://`, and `gs://` may synthesize an implicit profile;
  `gs` maps to `gcs`.
- Azure uses configured `msc://profile/key` URLs; other remote schemes are
  unsupported in the initial resolver.
- `path_mapping` is evaluated before implicit profile synthesis, using the
  longest matching source prefix.
- Relative raw POSIX paths are rejected unless an API explicitly supplies a
  base directory.

URL parsing must not decode a key into `.` or `..` traversal. Profile names and
keys are reported separately in errors.

## Secret handling and validation output

The resolved in-memory config retains secret wrappers. `msc config validate`
prints a redacted resolved document by default. A future explicit unsafe option
would require a separate security review; it is not part of this spec.

## Rclone compatibility

Rclone config import is a follow-up. It must not block core config or sync.
When implemented, preserve upstream search order and map supported remotes into
profiles. Merge is additive and conflicts with MSC profiles are errors. The
`rclone` binary is never used for transfer.

## Public API

The configuration module exposes concepts equivalent to:

```rust
ConfigLoader::new()
    .with_explicit_paths(...)
    .with_registry(...)
    .load() -> Result<ResolvedConfig, ConfigError>;

ResolvedConfig::profile(name) -> Result<&ProfileConfig, ConfigError>;
ResolvedConfig::resolve(input) -> Result<ResolvedUrl, ConfigError>;
```

Errors include a JSON-pointer-like field path and source file when available.

## Acceptance criteria

- Golden tests cover every discovery position and prove first-existing wins.
- Explicit paths suppress environment/default discovery.
- YAML and JSON produce identical typed configs.
- Include tests cover relative paths, order, identical values, conflicts,
  missing files, and nested include rejection.
- Unknown fields fail except inside registered option maps.
- Environment tests cover both syntaxes, nesting, missing values, redaction,
  keys not expanded, and no scalar coercion.
- Default POSIX access works with no config.
- URL tests cover normalization, longest mapping, implicit providers, invalid
  profiles, encoded traversal, and duplicate slashes.
- Replica graph validation catches self-reference, cycles through forbidden
  nested replicas, duplicates, missing profiles, and invalid priority.
- `retry` is accepted and validated as a declared field.
- Disabled provider tags explain the required Cargo feature.
- Unsupported tags/schemes do not imply an unimplemented adapter is available.
- S3-compatible endpoints use `s3`; native `gcs` and `azure` are accepted in the MVP.
- Telemetry include tests cover ordered attribute concatenation, overrides,
  additive settings, conflicts, timing validation, and secret redaction.
- Config display never prints test secret values.

## Source evidence

- `src/multistorageclient/config.py`
- `src/multistorageclient/schema.py`
- `src/multistorageclient/utils.py`
- `src/multistorageclient/rclone.py`
- `src/multistorageclient/shortcuts.py`
- `multi-storage-client-docs/src/references/configuration.rst`
- `multi-storage-client-docs/src/user_guide/quickstart.rst`
- `tests/test_multistorageclient/unit/{test_config,test_schema,test_utils,test_rclone}.py`
