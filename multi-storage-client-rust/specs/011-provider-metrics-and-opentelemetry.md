# 011: Provider metrics and OpenTelemetry

- Status: Planned, not implemented in this foundation PR
- Depends on: 001, 002
- Integration: 003 client construction, 004 adapters, 007 CLI lifecycle

## Instrumentation boundary

A provider-neutral decorator instruments POSIX, S3/S8K, GCS, Azure and registered
providers beneath client retries and caching, outside adapter internals. Backend
pagination and multipart parts remain one MSC attempt; a retry is another
attempt. Cache hits add no synthetic read or byte measurements.

| Provider method | Operation label |
|---|---|
| `head` | `info` |
| `get`, `get_range` | `read` |
| `put`, `make_symlink` | `write` |
| `copy`, `delete`, `delete_many`, `list` | Matching method name |
| `presigned_url` | No measurements, including failed signing |

Required preparatory metadata, cache admission sizing and fill consistency
HEADs still execute without additional `info` measurements. A preparatory
lookup records each failed HEAD retry under the requested `read` operation;
successful preparatory HEADs add no read event. Exhausted retries do not add a
duplicate terminal error. Cancelling a pending preparatory HEAD records one
cancelled read, while cancelling during retry backoff adds no new attempt.
Bookkeeping failures cannot invent read events, and preparation that performed
an instrumented native read cannot count that read twice. Bounded reads,
downloads, writes and native copies defer successful native attempts until
client validation/publication passes; a failed check records the transfer error.
Native completion timestamps freeze before that deferred settlement, preserving
provider latency and rate. A returned cached stream retains its logical scope
through EOF length validation. Seekable readers likewise validate range results
before success settles. Local validation errors that abandon an open stream
record the read error; externally abandoned streams remain cancelled.

Explicit `info()` and `is_file()` queries remain visible, including queries on
objects whose cached reads disable source checks. Enabled cache version
validation emits `info`. Mapped writes retain their physical registration
`info`, matching Python's `_register_written_file()`; queries for pending logical
metadata do not invent a physical operation. Native copy records `copy`, while
a streaming fallback records actual read and write attempts. Sync, replicas,
manifests and composite routing inherit provider measurements.

Synthetic root metadata, zero-length ranges, unpolled futures and rejected
arguments/capabilities that never enter a provider emit no attempt.

## Metric schema

The instrumentation scope is `multistorageclient`, with the Rust package version.
The six Python operation metric names, instrument types and units are preserved:

| Metric | Instrument | Unit | Recorded when |
|---|---|---|---|
| `multistorageclient.request.sum` | Counter | `{request}` | Attempt settles, without terminal status |
| `multistorageclient.response.sum` | Counter | `{response}` | Attempt settles, including error/cancellation |
| `multistorageclient.latency` | Gauge | `s` | Attempt settles; monotonic provider duration |
| `multistorageclient.data_size` | Gauge | `By` | Successful read/write/copy with known size |
| `multistorageclient.data_rate` | Gauge | `By/s` | Known successful size divided by positive latency |
| `multistorageclient.data_size.sum` | Counter | `By` | Successful read/write/copy with known size |

Request and response counters settle together using one attribute snapshot,
as in Python's `_record_metrics()`. Counters retain totals; gauges represent
the latest value for an attribute set. Gauge samples update independently: an
unknown-size operation updates latency without replacing an earlier size/rate
sample. Export does not guarantee every raw sample or a percentile distribution.

Read size counts bytes yielded by a successful read, including only the requested
range. Write size counts successful upload bytes. Native copy counts logical
object bytes, rather than network traffic. Unknown sizes omit byte/rate values;
empty successes record zero. Zero duration omits rate. Failed/cancelled transfers
add no successful bytes, even if partial bytes streamed or a PUT completed before
client verification failed.

### Real file descriptors

| Metric | Instrument | Unit | Recorded when |
|---|---|---|---|
| `multistorageclient.file_descriptor.open` | Up/down counter | `{file_descriptor}` | `+1` for a returned handle, `-1` on drop |
| `multistorageclient.file_descriptor.duration` | Gauge | `s` | Owned handle is released |

On Unix, `StorageClient::open_posix_file()` returns an anchored, generation
pinned `TrackedFile<tokio::fs::File>` supporting asynchronous read/seek, with mode
`rb`. `FileDescriptorMetrics::track()` can wrap another real `AsFd` handle.
The guard owns the handle; layered decorators do not count it twice.
File-mode/version/provider/configured attributes freeze at open so close
decrements the same series. Descriptor metrics have no operation/status and add
no request/response/byte measurements. Client shutdown does not close a handle
owned by its caller. Internal roots, cache files, spools and temporary files
are excluded.

## Lifecycle, status and compatibility limits

Attempts begin when provider futures are first polled. Value operations settle
on success/error; returned read/list streams settle on EOF, first terminal error
or unfinished drop. Completion guards cannot count EOF followed by drop twice.
Upload success follows publication and validation, never writer flush or
acceptance of a buffered chunk. Unfinished public stream/future drops emit
`cancelled`; early list termination, including a CLI limit, is cancellation.
Mixed-success batches emit one `error.partial_failure`; retrying failed items
creates another attempt. These two labels are explicit Rust extensions to
Python's blocking API, which has no matching returned stream cancellation or
mixed-success batch value.

Error labels use `error.<Python exception class>`, including `FileNotFoundError`,
`PermissionError`, `PreconditionFailedError`, `NotModifiedError`, `RetryableError`,
`NotImplementedError`, `ValueError` and `RuntimeError`. Native Python provider HTTP
translation takes precedence over generic typed-error translation: native S3
HTTP 304 is `error.RuntimeError`, GCS HTTP 304 is `error.NotModifiedError`, and
Azure HTTP 403 is `error.RuntimeError`. Rust typed errors/retry classification
remain unchanged. Python's optional Rust transport may translate HTTP failures
differently from its native SDK path.

Metric schema and deterministic fixtures are compatible, but complete numerical
parity is not claimed. Language package versions and actual latency/rate values
differ. Preparatory HEAD failures and actual GET/PUT retries each emit their
attempt, preserving read retry counts without changing storage retry budgets.
Returned listing metrics include consumption time,
where Python may finish its measurement when a lazy iterator is constructed.

## Attributes and safety

Operation records reserve `multistorageclient.version`,
`multistorageclient.provider`, `multistorageclient.operation` and terminal-only
`multistorageclient.status`. Descriptor records additionally reserve
`multistorageclient.file_mode`. Provider values are registered tags, never buckets
or endpoints. Object keys, URLs, credentials and arbitrary error messages are
never automatic labels.

Ordered providers are `static`, `environment_variables`, `host`, `process`,
`thread` and `msc_config`. Static values may be scalars or homogeneous arrays;
host maps to `name`, process to `pid`, and thread to `ident`/`native_id`.
Static/configuration/host/process values resolve at construction; environment
and thread values resolve once at operation settlement, or descriptor open.
Missing environment values are omitted; later providers override earlier values.
Thread `ident` is a stable Rust thread identifier; OS `native_id` is available
on Linux and omitted on other targets.

`msc_config` maps labels to `{expression: <JMESPath>}` against a redacted
configuration document. Missing/null results are omitted; results must be OTel
scalars or homogeneous arrays. `hash(algorithm, string)` returns hexadecimal
MD5, `sha1`, SHA-2 (`sha224`, `sha256`, `sha384`, `sha512`, `sha512_224`,
`sha512_256`) or SHA-3 (`sha3_224`, `sha3_256`, `sha3_384`, `sha3_512`) digests;
supported dashed SHA-2/SHA-3 spellings are normalized. Additional platform-specific
Python `hashlib` algorithms are not all implemented. Secret-designated keys are
redacted before expression evaluation and hashing. Invalid expressions omit
secret text from diagnostics.

Applications may inject a `Send + Sync` `MetricAttributeProvider` or closure
through `with_metric_attribute_provider()`, or append one to `AttributeProviders`.
Callbacks run on the recording thread and must finish promptly without blocking
or network I/O. Arbitrary Python import paths are not loaded. Configured reserved
or secret-designated labels are rejected; unsafe dynamic values are discarded.
At most 64 configured labels are allowed, with 256-byte keys and 1,024-byte string
values. Owned SDK streams cap cardinality at 512 series.

## Configuration and reader

The existing `opentelemetry.metrics` configuration shape is supported:

```yaml
opentelemetry:
  metrics:
    attributes:
      - type: static
        options:
          attributes:
            service: training-data
    reader:
      async: true
      options:
        collect_interval_millis: 1000
        collect_timeout_millis: 10000
        export_interval_millis: 60000
        export_timeout_millis: 30000
        lifecycle_timeout_millis: 10000
    exporter:
      type: otlp
      options:
        endpoint: http://localhost:4318/v1/metrics
```

The four collect/export timings shown are Python-compatible defaults and
independent positive integers; timeout may exceed interval. Attribute lists
concatenate across includes. Reader/exporter fields merge additively, with
conflicting overlaps rejected. `reader.async` accepts either boolean: both
modes record to SDK memory, without Python per-operation queues or multiprocessing.

Independent collection/export workers retain at most 64 owned SDK snapshots;
overflow discards the oldest with a redacted, rate-limited diagnostic. Collection
and enqueue are serialized across periodic/flush/shutdown paths to preserve order;
late periodic/flush collections are rejected after shutdown begins, while final
collection is permitted. Export uses a separate lock and does not block storage
I/O. Cumulative counters retain totals if an intermediate snapshot is discarded;
intermediate gauge samples may be lost. Collection timeout discards a late
snapshot. Synchronous application SDK callbacks cannot be interrupted safely
and may finish after their deadline.

The separate lifecycle timeout bounds awaited flush/shutdown to ten seconds.
When omitted it is `min(3 × export_timeout + 250 ms, 10 s)`; explicit values
are capped at ten seconds. Timed-out blocking work may finish later.

Exporters are `otlp` over HTTP/protobuf and `console` to an explicit sink (stderr
in the CLI). OTLP supports redacted headers, custom CA `certificate_file` and
the required mTLS pair `client_certificate_file`/`client_key_file`. TLS verification
remains enabled. Endpoint validation requires HTTPS whenever OTLP headers are
configured. Plain HTTP is allowed only for literal loopback endpoints without
headers. Unknown exporters, traces/log sections and unsupported options
fail validation. Loading configuration and `msc config validate` start no exporter.
OTLP requests carrying configured headers do not follow redirects, so headers
cannot be forwarded to a different authority or an HTTP endpoint.

## Ownership and failure behavior

The builder accepts a borrowed `Meter` and leaves SDK lifecycle with its caller.
The default meter is no-op. `OwnedTelemetry::from_resolved_config()` evaluates
configuration attributes against the redacted resolved document. Applications
use `with_telemetry()` or `OwnedTelemetry::build_client()` to share its meter and
dynamic attributes. No global SDK or hidden Tokio runtime is installed.
`shutdown_client()` completes storage cleanup, then awaits SDK shutdown even if
cleanup fails, preserving the storage result.

The CLI initializes one owned handle per command and awaits bounded shutdown
after cleanup on success, failure or Ctrl-C. Blocking SDK work runs outside Tokio
executor workers. Explicit shutdown attempts delivery; dropping an owned handle
requests background cleanup without guaranteeing delivery. Recording performs no
exporter I/O and creates no per-operation task. Export failure/backpressure does
not retry storage or replace results/exit codes. Warnings use fixed redacted text
and are limited to one per 30 seconds. Invalid configuration fails before storage
I/O; exporter initialization failure degrades to no-op metrics with a diagnostic.

## Verification and source evidence

`tests/telemetry_metrics.rs` contains Python-source schema/status/operation
fixtures and verifies actual exported names, kinds, units, labels, counts and
bytes through configured POSIX, fake-provider and emulator S3 clients. Coverage
includes each failed preparation retry, recovery and backoff/HEAD cancellation,
cache validation fallback, native copy, failed post-PUT/copy validation,
cached/seekable stream integrity, frozen provider duration, cache overrides,
explicit metadata queries, mapped registration, descriptor balance, cancellation,
partial batches, dynamic attributes, JMESPath/hash evaluation and redaction.

`tests/telemetry_export.rs` verifies independent cadence, bounded snapshots,
concurrent collection/flush order, lifecycle deadlines, stdout isolation, actual
OTLP protobuf/headers, failures/timeouts, CA verification and mTLS. Tests requiring
local collectors or the S3 emulator are explicitly ignored in normal runs and
exercised by the external-service recipe.

Python source evidence: `providers/base.py` (`_record_metrics`, operation wrappers,
file-descriptor hooks), `client/single.py` (`read`, `info`, `_register_written_file`),
`telemetry/__init__.py` (schema), `telemetry/attributes/msc_config.py` (hash expressions)
and `telemetry/metrics/readers/diperiodic_exporting.py` (cadence defaults).
Rust implementations are in `src/telemetry/`, `src/config/telemetry.rs`,
`src/client/read.rs`, `src/client/reader.rs` and `src/client/write.rs`.
