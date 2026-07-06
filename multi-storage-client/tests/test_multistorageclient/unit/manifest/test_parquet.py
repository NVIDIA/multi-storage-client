# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Strict decoding tests for virtual-manifest Parquet rows."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from multistorageclient.manifest import ManifestValidationError
from multistorageclient.manifest.bindings import ServiceBinding, SourceBinding
from multistorageclient.manifest.models import EmptyChunk, ObjectChunk, QueryParameter, ServiceChunk
from multistorageclient.manifest.parquet import load_virtual_manifest
from multistorageclient.manifest.schema import virtual_manifest_v2_schema
from multistorageclient.types import Range

from .helpers import (
    StubServiceReader,
    StubSourceReader,
    empty_row,
    object_row,
    service_bindings,
    service_row,
    source_bindings,
    write_manifest,
)

MAX_I64 = (1 << 63) - 1


def _decode(rows, *, sources=None, services=None, row_group_size=None, schema=None):
    return load_virtual_manifest(
        write_manifest(rows, row_group_size=row_group_size, schema=schema),
        source_bindings() if sources is None else sources,
        service_bindings() if services is None else services,
    )


def test_load_virtual_manifest_accepts_an_empty_dataset() -> None:
    """A valid single-Parquet manifest may deliberately expose no logical files."""
    assert _decode([]) == {}


def test_load_virtual_manifest_rejects_a_file_key_that_is_another_file_key_prefix() -> None:
    """Logical files cannot collide with the synthetic directory required by a descendant key."""
    with pytest.raises(ManifestValidationError, match="ancestor"):
        _decode(
            [
                object_row(key="a", source_path="objects/a.bin"),
                object_row(key="a/b", source_path="objects/a-b.bin"),
            ]
        )


@pytest.mark.parametrize("physical_schema_mutation", ["chunk_index_type", "extra_column"])
def test_load_virtual_manifest_rejects_a_spoofed_arrow_schema_in_the_footer(
    physical_schema_mutation: str,
) -> None:
    """The physical Parquet schema cannot be hidden behind a canonical ARROW:schema footer."""
    expected_schema = virtual_manifest_v2_schema()
    canonical_manifest = pq.ParquetFile(write_manifest([object_row()]))
    canonical_arrow_schema = (canonical_manifest.metadata.metadata or {})[b"ARROW:schema"]
    footer_metadata = dict(expected_schema.metadata or {}) | {b"ARROW:schema": canonical_arrow_schema}

    if physical_schema_mutation == "chunk_index_type":
        chunk_index = expected_schema.get_field_index("chunk_index")
        physical_schema = expected_schema.set(chunk_index, pa.field("chunk_index", pa.int64(), nullable=False))
        rows = [object_row()]
    else:
        physical_schema = pa.schema(
            [*expected_schema, pa.field("injected", pa.string())],
            metadata=footer_metadata,
        )
        rows = [object_row(injected=None)]

    if physical_schema_mutation == "chunk_index_type":
        physical_schema = physical_schema.with_metadata(footer_metadata)

    stream = BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=physical_schema), stream)
    stream.seek(0)
    parquet_file = pq.ParquetFile(stream)
    assert parquet_file.schema_arrow != expected_schema
    assert (parquet_file.metadata.metadata or {})[b"ARROW:schema"] == canonical_arrow_schema
    stream.seek(0)

    with pytest.raises(ManifestValidationError, match="Parquet schema does not exactly match"):
        load_virtual_manifest(stream, source_bindings(), service_bindings())


def test_load_virtual_manifest_decodes_object_rows_across_row_groups_out_of_order() -> None:
    """Chunk index, rather than physical Parquet row position, defines file reconstruction order."""
    second_chunk = object_row(
        key="logical/alpha.bin",
        size_bytes=5,
        metadata='{"b":2, "a":1}',
        chunk_index=1,
        chunk_size_bytes=2,
        source_path="objects/alpha-tail.bin",
        source_offset=3,
    )
    first_chunk = object_row(
        key="logical/alpha.bin",
        size_bytes=5,
        metadata='{"a":1,"b":2}',
        chunk_index=0,
        chunk_size_bytes=3,
        source_path="objects/alpha-head.bin",
        source_offset=0,
    )
    independent_file = service_row(
        key="logical/beta.bin",
        size_bytes=2,
        chunk_size_bytes=2,
        service_path="clips/beta.bin",
        service_query=[
            {"name": "format", "value": "raw"},
            {"name": "tag", "value": ""},
            {"name": "format", "value": "derived"},
        ],
    )

    files = _decode([second_chunk, independent_file, first_chunk], row_group_size=1)

    alpha = files["logical/alpha.bin"]
    assert alpha.size_bytes == 5
    assert alpha.metadata_json == '{"a":1,"b":2}'
    assert alpha.cumulative_ends == (3, 5)
    assert alpha.chunks == (
        ObjectChunk(0, 3, "source", "objects/alpha-head.bin", 0),
        ObjectChunk(1, 2, "source", "objects/alpha-tail.bin", 3),
    )
    assert files["logical/beta.bin"].chunks == (
        ServiceChunk(
            0,
            2,
            "service",
            "clips/beta.bin",
            (
                QueryParameter("format", "raw"),
                QueryParameter("tag", ""),
                QueryParameter("format", "derived"),
            ),
        ),
    )


def test_load_virtual_manifest_decodes_the_single_empty_file_representation() -> None:
    """A zero-byte file is represented by exactly one explicit empty chunk."""
    files = _decode([empty_row(key="logical/empty.bin")])

    empty_file = files["logical/empty.bin"]
    assert empty_file.size_bytes == 0
    assert empty_file.chunks == (EmptyChunk(),)
    assert empty_file.cumulative_ends == (0,)


@pytest.mark.parametrize(
    ("case", "rows"),
    [
        ("negative file size", [object_row(size_bytes=-1)]),
        ("negative chunk index", [object_row(chunk_index=-1)]),
        ("negative chunk size", [object_row(chunk_size_bytes=-1)]),
        ("negative source offset", [object_row(source_offset=-1)]),
        (
            "chunk index gap",
            [
                object_row(size_bytes=6, chunk_size_bytes=3, chunk_index=0),
                object_row(size_bytes=6, chunk_size_bytes=3, chunk_index=2, source_path="objects/tail.bin"),
            ],
        ),
        (
            "duplicate key and chunk index",
            [
                object_row(size_bytes=6, chunk_size_bytes=3, chunk_index=0),
                object_row(size_bytes=6, chunk_size_bytes=3, chunk_index=0, source_path="objects/duplicate.bin"),
            ],
        ),
        ("zero-sized nonempty chunk", [object_row(size_bytes=0, chunk_size_bytes=0)]),
        ("chunk sum differs from file size", [object_row(size_bytes=4, chunk_size_bytes=3)]),
        ("unknown chunk kind", [object_row(chunk_kind="inline")]),
        ("object without source profile", [object_row(source_profile=None)]),
        ("object without source path", [object_row(source_path=None)]),
        ("object without source offset", [object_row(source_offset=None)]),
        ("object with service fields", [object_row(service_id="service", service_path="clips/x", service_query=[])]),
        ("service without identifier", [service_row(service_id=None)]),
        ("service without path", [service_row(service_path=None)]),
        ("service without query list", [service_row(service_query=None)]),
        ("service with source fields", [service_row(source_profile="source")]),
        ("empty row has object fields", [empty_row(source_path="objects/not-empty.bin")]),
        ("empty row has nonzero index", [empty_row(chunk_index=1)]),
        ("empty row has nonzero chunk", [empty_row(chunk_size_bytes=1)]),
        ("empty row has nonzero file size", [empty_row(size_bytes=1)]),
    ],
)
def test_load_virtual_manifest_rejects_invalid_chunk_rows(case: str, rows: list[dict[str, object]]) -> None:
    """Every row kind has mutually exclusive, fully specified source fields."""
    with pytest.raises(ManifestValidationError):
        _decode(rows)


@pytest.mark.parametrize(
    ("case", "row"),
    [
        ("object has service identifier", object_row(service_id="service")),
        ("object has service path", object_row(service_path="clips/unexpected.bin")),
        ("object has service query", object_row(service_query=[])),
        ("service has source profile", service_row(source_profile="source")),
        ("service has source path", service_row(source_path="objects/unexpected.bin")),
        ("service has source offset", service_row(source_offset=0)),
        ("empty has source profile", empty_row(source_profile="source")),
        ("empty has source path", empty_row(source_path="objects/unexpected.bin")),
        ("empty has source offset", empty_row(source_offset=0)),
        ("empty has service identifier", empty_row(service_id="service")),
        ("empty has service path", empty_row(service_path="clips/unexpected.bin")),
        ("empty has service query", empty_row(service_query=[])),
    ],
)
def test_load_virtual_manifest_rejects_each_cross_kind_field_independently(case: str, row: dict[str, object]) -> None:
    """A field from another chunk kind is invalid even when every other field is canonical."""
    with pytest.raises(ManifestValidationError):
        _decode([row])


@pytest.mark.parametrize(
    ("case", "rows"),
    [
        ("two empty rows for one key", [empty_row(), empty_row()]),
        ("empty row and object row for one key", [empty_row(), object_row()]),
        (
            "empty row with a second index",
            [empty_row(), empty_row(chunk_index=1)],
        ),
    ],
)
def test_load_virtual_manifest_rejects_multiple_or_mixed_empty_row_representations(
    case: str, rows: list[dict[str, object]]
) -> None:
    """A logical empty file has one and only one all-null empty row."""
    with pytest.raises(ManifestValidationError):
        _decode(rows)


@pytest.mark.parametrize(
    "required_field", ["key", "size_bytes", "last_modified", "chunk_index", "chunk_size_bytes", "chunk_kind"]
)
def test_load_virtual_manifest_rejects_nulls_in_physical_required_fields(required_field: str) -> None:
    """Arrow nullability is rechecked rather than trusted from a malformed writer."""
    schema = virtual_manifest_v2_schema()
    field_index = schema.get_field_index(required_field)
    field = schema.field(field_index)
    nullable_schema = schema.set(field_index, field.with_nullable(True))

    with pytest.raises(ManifestValidationError):
        _decode([object_row(**{required_field: None})], schema=nullable_schema)


@pytest.mark.parametrize(
    ("field", "different_value"),
    [
        ("size_bytes", 7),
        ("last_modified", datetime(2026, 1, 2, 3, 4, 6, tzinfo=timezone.utc)),
        ("content_type", "text/plain"),
        ("storage_class", "ARCHIVE"),
        ("metadata", '{"different":true}'),
    ],
)
def test_load_virtual_manifest_rejects_conflicting_file_metadata(field: str, different_value: object) -> None:
    """All rows for a logical file must agree on its file-level metadata."""
    first = object_row(size_bytes=6, chunk_size_bytes=3, chunk_index=0)
    second = object_row(
        size_bytes=6,
        chunk_size_bytes=3,
        chunk_index=1,
        source_path="objects/second.bin",
        source_offset=0,
    )
    second[field] = different_value

    with pytest.raises(ManifestValidationError):
        _decode([first, second])


@pytest.mark.parametrize(
    "invalid_metadata",
    [
        "{",
        "[]",
        "null",
        '"not-an-object"',
        '{"number":NaN}',
        '{"number":Infinity}',
        '{"number":-Infinity}',
        '{"key":1,"key":2}',
    ],
)
def test_load_virtual_manifest_rejects_metadata_that_is_not_a_json_object(invalid_metadata: str) -> None:
    """Metadata remains a JSON object even though Parquet carries it as a string."""
    with pytest.raises(ManifestValidationError):
        _decode([object_row(metadata=invalid_metadata)])


@pytest.mark.parametrize(
    ("field", "invalid_path"),
    [
        ("key", ""),
        ("key", "/absolute/file"),
        ("key", "https://example.test/file"),
        ("key", "scheme:file"),
        ("key", "folder\\file"),
        ("key", "folder\x00file"),
        ("key", "folder//file"),
        ("key", "folder/"),
        ("key", "./file"),
        ("key", "folder/./file"),
        ("key", "../file"),
        ("key", "folder/../file"),
        ("source_path", ""),
        ("source_path", "/absolute/file"),
        ("source_path", "s3://bucket/file"),
        ("source_path", "scheme:file"),
        ("source_path", "folder\\file"),
        ("source_path", "folder\x00file"),
        ("source_path", "folder//file"),
        ("source_path", "folder/"),
        ("source_path", "./file"),
        ("source_path", "folder/./file"),
        ("source_path", "../file"),
        ("source_path", "folder/../file"),
    ],
)
def test_load_virtual_manifest_rejects_noncanonical_logical_and_object_paths(field: str, invalid_path: str) -> None:
    """Logical keys and direct-object paths are normalized relative POSIX paths."""
    with pytest.raises(ManifestValidationError):
        _decode([object_row(**{field: invalid_path})])


@pytest.mark.parametrize(
    "invalid_path",
    [
        "",
        "/clips/file",
        "https://service.example.test/clips/file",
        "%2Fclips/file",
        "clips/file?variant=preview",
        "clips/file#fragment",
        "//other-authority/file",
        "clips//file",
        "./clips/file",
        "../clips/file",
        "clips/../file",
        "clips\\file",
        "clips\x00file",
        "clips/",
        "clips/%2e%2e/file",
    ],
)
def test_load_virtual_manifest_rejects_noncanonical_service_paths(invalid_path: str) -> None:
    """Service paths are unescaped relative Unicode paths, not URL fragments."""
    with pytest.raises(ManifestValidationError):
        _decode([service_row(service_path=invalid_path)])


def test_load_virtual_manifest_accepts_unicode_paths_and_empty_query_values() -> None:
    """Unicode path text and intentionally empty query values are retained exactly."""
    row = service_row(
        key="logical/日本語.bin",
        service_path="clips/日本語 👋.bin",
        service_query=[{"name": "label", "value": ""}],
    )

    file = _decode([row])["logical/日本語.bin"]

    assert file.chunks == (ServiceChunk(0, 3, "service", "clips/日本語 👋.bin", (QueryParameter("label", ""),)),)


def test_load_virtual_manifest_rejects_an_empty_service_query_name() -> None:
    """A query parameter may have an empty value but never an empty name."""
    with pytest.raises(ManifestValidationError):
        _decode([service_row(service_query=[{"name": "", "value": "allowed"}])])


def test_load_virtual_manifest_rejects_unknown_source_and_service_aliases() -> None:
    """Rows can only refer to bindings injected by the manifest profile."""
    with pytest.raises(ManifestValidationError):
        _decode([object_row(source_profile="not-configured")])

    with pytest.raises(ManifestValidationError):
        _decode([service_row(service_id="not-configured")])


@pytest.mark.parametrize(
    "invalid_binding",
    [
        "empty source alias",
        "empty service alias",
        "empty source identity",
        "empty service identity",
        "empty source revision",
        "empty service revision",
        "nonstring source alias",
        "nonstring service alias",
        "nonstring source identity",
        "nonstring service identity",
        "nonstring source revision",
        "nonstring service revision",
    ],
)
def test_load_virtual_manifest_rejects_invalid_injected_bindings_before_using_rows(invalid_binding: str) -> None:
    """Injected bindings are validated as configuration, including unused malformed entries."""
    sources = source_bindings()
    services = service_bindings()

    if invalid_binding == "empty source alias":
        sources[""] = SourceBinding(StubSourceReader(), "source-revision-2")
    elif invalid_binding == "empty service alias":
        services[""] = ServiceBinding(StubServiceReader(), "service-revision-2")
    elif invalid_binding == "empty source identity":
        sources["source"] = SourceBinding(StubSourceReader(""), "source-revision-1")
    elif invalid_binding == "empty service identity":
        services["service"] = ServiceBinding(StubServiceReader(""), "service-revision-1")
    elif invalid_binding == "empty source revision":
        sources["source"] = SourceBinding(StubSourceReader(), "")
    elif invalid_binding == "empty service revision":
        services["service"] = ServiceBinding(StubServiceReader(), "")
    elif invalid_binding == "nonstring source alias":
        sources[cast(str, 1)] = SourceBinding(StubSourceReader(), "source-revision-2")
    elif invalid_binding == "nonstring service alias":
        services[cast(str, 1)] = ServiceBinding(StubServiceReader(), "service-revision-2")
    elif invalid_binding == "nonstring source identity":
        sources["source"] = SourceBinding(StubSourceReader(cast(str, 1)), "source-revision-1")
    elif invalid_binding == "nonstring service identity":
        services["service"] = ServiceBinding(StubServiceReader(cast(str, 1)), "service-revision-1")
    elif invalid_binding == "nonstring source revision":
        sources["source"] = SourceBinding(StubSourceReader(), cast(str, 1))
    else:
        services["service"] = ServiceBinding(StubServiceReader(), cast(str, 1))

    row = service_row() if "service" in invalid_binding and "source" not in invalid_binding else object_row()
    with pytest.raises(ManifestValidationError):
        _decode([row], sources=sources, services=services)


def test_load_virtual_manifest_validates_service_paths_and_queries_against_the_bound_reader() -> None:
    """The service allowlist is checked while loading, before a virtual file is exposed."""

    class RecordingServiceReader:
        binding_identity = "https://service.example.test/v1"

        def __init__(self) -> None:
            self.validated: list[tuple[str, tuple[QueryParameter, ...]]] = []

        def validate(self, path: str, query: Sequence[QueryParameter]) -> None:
            self.validated.append((path, tuple(query)))

        def read(self, path: str, query: Sequence[QueryParameter], byte_range: Range, total_size: int) -> bytes:
            raise AssertionError("manifest decoding must not fetch service bytes")

    reader = RecordingServiceReader()
    row = service_row(service_query=[{"name": "format", "value": "raw"}])

    _decode([row], services={"service": ServiceBinding(reader, "service-revision-1")})

    assert reader.validated == [("clips/rendered.bin", (QueryParameter("format", "raw"),))]


def test_load_virtual_manifest_preserves_service_validation_rejection_reason() -> None:
    """A rejected service row identifies both its binding and the reader's reason."""

    class RejectingServiceReader:
        binding_identity = "https://service.example.test/v1"

        def validate(self, path: str, query: Sequence[QueryParameter]) -> None:
            raise ValueError("service path is not allowlisted")

        def read(self, path: str, query: Sequence[QueryParameter], byte_range: Range, total_size: int) -> bytes:
            raise AssertionError("manifest decoding must not fetch service bytes")

    with pytest.raises(ManifestValidationError) as error:
        _decode([service_row()], services={"service": ServiceBinding(RejectingServiceReader(), "service-revision-1")})

    assert "service binding 'service' rejected manifest request: service path is not allowlisted" in str(error.value)


def test_load_virtual_manifest_checks_i64_chunk_sums_without_overflow() -> None:
    """Large valid values work, while a checked running total cannot wrap around."""
    valid = _decode([object_row(size_bytes=MAX_I64, chunk_size_bytes=MAX_I64)])
    assert valid["logical/file.bin"].size_bytes == MAX_I64

    overflowing_rows = [
        object_row(size_bytes=MAX_I64, chunk_index=0, chunk_size_bytes=MAX_I64),
        object_row(
            size_bytes=MAX_I64,
            chunk_index=1,
            chunk_size_bytes=1,
            source_path="objects/overflow.bin",
            source_offset=0,
        ),
    ]
    with pytest.raises(ManifestValidationError):
        _decode(overflowing_rows)


def test_synthetic_etag_is_stable_for_canonical_metadata_and_has_the_v2_shape() -> None:
    """Equivalent JSON metadata cannot perturb a logical file's cache identity."""
    first = _decode([object_row(metadata='{"b":2, "a":1}')])["logical/file.bin"].etag
    second = _decode([object_row(metadata='{"a":1,"b":2}')])["logical/file.bin"].etag

    assert first == second
    assert re.fullmatch(r"msc-v2-sha256:[0-9a-f]{64}", first)


def test_synthetic_etag_tracks_each_used_object_binding_identity_alias_and_revision() -> None:
    """Changing any used object-binding identity component invalidates the logical ETag."""
    row = object_row()
    original = _decode([row], sources=source_bindings())["logical/file.bin"].etag
    changed_revision = _decode([row], sources=source_bindings(binding_revision="source-revision-2"))[
        "logical/file.bin"
    ].etag
    changed_location = _decode([row], sources=source_bindings(binding_identity="s3://other-bucket/objects"))[
        "logical/file.bin"
    ].etag
    renamed_alias = _decode([object_row(source_profile="renamed")], sources=source_bindings(alias="renamed"))[
        "logical/file.bin"
    ].etag

    assert original != changed_revision
    assert original != changed_location
    assert original != renamed_alias


@pytest.mark.parametrize(
    ("case", "changed_row"),
    [
        ("file size", object_row(size_bytes=4, chunk_size_bytes=4)),
        ("last modified", object_row(last_modified=datetime(2026, 1, 2, 3, 4, 6, tzinfo=timezone.utc))),
        ("content type", object_row(content_type="text/plain")),
        ("storage class", object_row(storage_class="ARCHIVE")),
        ("metadata", object_row(metadata='{"source":"different"}')),
    ],
)
def test_synthetic_etag_tracks_each_file_metadata_field(case: str, changed_row: dict[str, object]) -> None:
    """Every normalized file-metadata field is part of the cache identity input."""
    original = _decode([object_row()])["logical/file.bin"].etag
    changed = _decode([changed_row])["logical/file.bin"].etag

    assert original != changed


def test_synthetic_etag_tracks_object_locator_and_chunk_boundary_mutations() -> None:
    """Chunk source locations and partition boundaries cannot be changed under the same ETag."""
    original = _decode([object_row()])["logical/file.bin"].etag
    changed_path = _decode([object_row(source_path="objects/other.bin")])["logical/file.bin"].etag
    changed_offset = _decode([object_row(source_offset=8)])["logical/file.bin"].etag

    original_boundaries = _decode(
        [
            object_row(
                size_bytes=6, chunk_index=0, chunk_size_bytes=3, source_path="objects/head.bin", source_offset=0
            ),
            object_row(
                size_bytes=6, chunk_index=1, chunk_size_bytes=3, source_path="objects/tail.bin", source_offset=0
            ),
        ]
    )["logical/file.bin"].etag
    changed_boundaries = _decode(
        [
            object_row(
                size_bytes=6, chunk_index=0, chunk_size_bytes=2, source_path="objects/head.bin", source_offset=0
            ),
            object_row(
                size_bytes=6, chunk_index=1, chunk_size_bytes=4, source_path="objects/tail.bin", source_offset=2
            ),
        ]
    )["logical/file.bin"].etag

    assert original != changed_path
    assert original != changed_offset
    assert original_boundaries != changed_boundaries


def test_synthetic_etag_tracks_used_service_binding_alias_location_revision_path_and_query_order() -> None:
    """All service dependency components, including ordered query pairs, participate in the ETag."""
    service_file = service_row()
    original_service_etag = _decode([service_file])["logical/file.bin"].etag
    changed_revision = _decode([service_file], services=service_bindings(binding_revision="service-revision-2"))[
        "logical/file.bin"
    ].etag
    changed_location = _decode(
        [service_file], services=service_bindings(binding_identity="https://other.example.test/v1")
    )["logical/file.bin"].etag
    changed_alias = _decode([service_row(service_id="alternate")], services=service_bindings(alias="alternate"))[
        "logical/file.bin"
    ].etag
    changed_path = _decode([service_row(service_path="clips/other.bin")])["logical/file.bin"].etag
    query_in_original_order = _decode(
        [service_row(service_query=[{"name": "format", "value": "raw"}, {"name": "tag", "value": "a"}])]
    )["logical/file.bin"].etag
    query_in_changed_order = _decode(
        [service_row(service_query=[{"name": "tag", "value": "a"}, {"name": "format", "value": "raw"}])]
    )["logical/file.bin"].etag

    assert original_service_etag != changed_revision
    assert original_service_etag != changed_location
    assert original_service_etag != changed_alias
    assert original_service_etag != changed_path
    assert query_in_original_order != query_in_changed_order


def test_synthetic_etag_excludes_unused_source_and_service_bindings() -> None:
    """Only dependencies reachable from a file can change its cache identity."""
    sources_with_unused_binding = source_bindings()
    sources_with_unused_binding["unused"] = SourceBinding(
        StubSourceReader("s3://unused-bucket/objects"), "unused-revision-1"
    )
    services_with_unused_binding = service_bindings()
    services_with_unused_binding["unused"] = ServiceBinding(
        StubServiceReader("https://unused.example.test/v1"), "unused-revision-1"
    )

    source_file = object_row()
    original_source_etag = _decode([source_file])["logical/file.bin"].etag
    unused_binding_changed_etag = _decode([source_file], sources=sources_with_unused_binding)["logical/file.bin"].etag
    service_file = service_row()
    original_service_etag = _decode([service_file])["logical/file.bin"].etag
    unused_service_changed_etag = _decode([service_file], services=services_with_unused_binding)[
        "logical/file.bin"
    ].etag

    assert original_source_etag == unused_binding_changed_etag
    assert original_service_etag == unused_service_changed_etag


def test_load_virtual_manifest_normalizes_timestamp_identity_to_utc() -> None:
    """Decoded file metadata is normalized to UTC before it participates in ETag input."""
    last_modified = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=2)))

    file = _decode([object_row(last_modified=last_modified)])["logical/file.bin"]

    assert file.last_modified == datetime(2026, 1, 2, 1, 4, 5, tzinfo=timezone.utc)


def test_synthetic_etag_is_equal_for_timestamps_that_describe_the_same_instant() -> None:
    """Equivalent timezone offsets normalize to one canonical UTC ETag representation."""
    utc_etag = _decode([object_row(last_modified=datetime(2026, 1, 2, 1, 4, 5, tzinfo=timezone.utc))])[
        "logical/file.bin"
    ].etag
    offset_etag = _decode(
        [object_row(last_modified=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=2))))]
    )["logical/file.bin"].etag

    assert utc_etag == offset_etag


def test_decoded_manifest_models_are_immutable_and_use_tuples_for_ordered_data() -> None:
    """Consumers cannot mutate decoded files, chunks, cumulative ends, or query ordering in place."""
    object_file = _decode([object_row()])["logical/file.bin"]
    service_file = _decode([service_row(service_query=[{"name": "format", "value": "raw"}])])["logical/file.bin"]

    assert type(object_file.chunks) is tuple
    assert type(object_file.cumulative_ends) is tuple
    assert type(service_file.chunks) is tuple
    assert isinstance(service_file.chunks[0], ServiceChunk)
    assert type(service_file.chunks[0].service_query) is tuple
    with pytest.raises(FrozenInstanceError):
        setattr(object_file, "key", "logical/replaced.bin")
    with pytest.raises(FrozenInstanceError):
        setattr(object_file.chunks[0], "source_path", "objects/replaced.bin")
    with pytest.raises(FrozenInstanceError):
        setattr(service_file.chunks[0].service_query[0], "value", "replaced")
