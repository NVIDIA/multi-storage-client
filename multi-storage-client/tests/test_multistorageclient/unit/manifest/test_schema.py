# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the single-Parquet virtual-manifest v2 schema."""

from __future__ import annotations

import base64
from collections.abc import Callable
from io import BytesIO

import pyarrow as pa
import pytest

from multistorageclient.manifest import ManifestValidationError, QueryParameter, ServiceRangeReader
from multistorageclient.manifest.parquet import load_virtual_manifest
from multistorageclient.manifest.schema import (
    MANIFEST_KIND,
    MANIFEST_KIND_METADATA_KEY,
    MANIFEST_VERSION,
    MANIFEST_VERSION_METADATA_KEY,
    virtual_manifest_v2_schema,
)

from .helpers import source_bindings, write_manifest


def test_virtual_manifest_v2_schema_has_the_exact_column_contract() -> None:
    """The physical Arrow shape is deliberately a strict, versioned wire contract."""
    schema = virtual_manifest_v2_schema()
    query_item = pa.field(
        "element",
        pa.struct(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("value", pa.string(), nullable=False),
            ]
        ),
        nullable=False,
    )

    assert [(field.name, field.type, field.nullable) for field in schema] == [
        ("key", pa.string(), False),
        ("size_bytes", pa.int64(), False),
        ("last_modified", pa.timestamp("us", tz="UTC"), False),
        ("content_type", pa.string(), True),
        ("storage_class", pa.string(), True),
        ("metadata", pa.string(), True),
        ("chunk_index", pa.int32(), False),
        ("chunk_size_bytes", pa.int64(), False),
        ("chunk_kind", pa.string(), False),
        ("source_profile", pa.string(), True),
        ("source_path", pa.string(), True),
        ("source_offset", pa.int64(), True),
        ("service_id", pa.string(), True),
        ("service_path", pa.string(), True),
        ("service_query", pa.list_(query_item), True),
    ]
    assert schema.metadata == {
        b"msc.manifest.version": b"2",
        b"msc.manifest.kind": b"virtual-file-chunks",
    }


def test_virtual_manifest_public_api_exports_query_parameter() -> None:
    """Consumers can construct ordered service query pairs from the public manifest namespace."""
    parameter = QueryParameter("format", "raw")

    assert (parameter.name, parameter.value) == ("format", "raw")
    assert ServiceRangeReader.__name__ == "ServiceRangeReader"


def test_load_virtual_manifest_accepts_unrelated_writer_footer_metadata() -> None:
    """Writers may retain their own footer metadata without expanding the MSC namespace."""
    footer_metadata = {
        MANIFEST_VERSION_METADATA_KEY: MANIFEST_VERSION,
        MANIFEST_KIND_METADATA_KEY: MANIFEST_KIND,
        b"writer.application": b"manifest-fixture",
    }

    assert load_virtual_manifest(write_manifest([], footer_metadata=footer_metadata), source_bindings(), {}) == {}


def _encoded_embedded_schema(metadata: dict[bytes, bytes]) -> bytes:
    """Serialize an Arrow schema whose metadata differs from the Parquet footer."""
    schema = virtual_manifest_v2_schema().with_metadata(metadata)
    return base64.b64encode(schema.serialize().to_pybytes())


def test_load_virtual_manifest_accepts_unrelated_writer_embedded_schema_metadata() -> None:
    """Writer metadata remains permitted in the embedded Arrow schema as well as the footer."""
    schema = virtual_manifest_v2_schema()
    embedded_metadata = dict(schema.metadata or {}) | {b"writer.application": b"manifest-fixture"}
    footer_metadata = dict(schema.metadata or {}) | {b"ARROW:schema": _encoded_embedded_schema(embedded_metadata)}

    assert load_virtual_manifest(write_manifest([], footer_metadata=footer_metadata), source_bindings(), {}) == {}


@pytest.mark.parametrize(
    "embedded_metadata",
    [
        pytest.param(
            {
                MANIFEST_VERSION_METADATA_KEY: b"3",
                MANIFEST_KIND_METADATA_KEY: MANIFEST_KIND,
            },
            id="conflicting-required-version",
        ),
        pytest.param(
            {
                MANIFEST_VERSION_METADATA_KEY: MANIFEST_VERSION,
                MANIFEST_KIND_METADATA_KEY: MANIFEST_KIND,
                b"msc.manifest.future": b"1",
            },
            id="unknown-reserved-key",
        ),
    ],
)
def test_load_virtual_manifest_rejects_noncanonical_embedded_schema_metadata(
    embedded_metadata: dict[bytes, bytes],
) -> None:
    """The embedded Arrow schema cannot disagree with the outer valid v2 footer."""
    schema = virtual_manifest_v2_schema()
    footer_metadata = dict(schema.metadata or {}) | {b"ARROW:schema": _encoded_embedded_schema(embedded_metadata)}

    with pytest.raises(ManifestValidationError):
        load_virtual_manifest(write_manifest([], footer_metadata=footer_metadata), source_bindings(), {})


def test_load_virtual_manifest_rejects_noncanonical_embedded_arrow_schema_base64() -> None:
    """The embedded Arrow schema must use a canonical strict Base64 representation."""
    schema = virtual_manifest_v2_schema()
    canonical = _encoded_embedded_schema(dict(schema.metadata or {}))
    footer_metadata = dict(schema.metadata or {}) | {b"ARROW:schema": canonical + b"\n"}

    with pytest.raises(ManifestValidationError, match="embedded Arrow schema is malformed"):
        load_virtual_manifest(write_manifest([], footer_metadata=footer_metadata), source_bindings(), {})


def test_load_virtual_manifest_rejects_embedded_arrow_schema_trailing_bytes() -> None:
    """The strict embedded schema payload must end exactly where Arrow schema decoding ends."""
    schema = virtual_manifest_v2_schema()
    encoded = base64.b64encode(schema.serialize().to_pybytes() + b"unexpected-trailing-bytes")
    footer_metadata = dict(schema.metadata or {}) | {b"ARROW:schema": encoded}

    with pytest.raises(ManifestValidationError, match="embedded Arrow schema is malformed"):
        load_virtual_manifest(write_manifest([], footer_metadata=footer_metadata), source_bindings(), {})


@pytest.mark.parametrize(
    ("case", "footer_metadata"),
    [
        (
            "missing version",
            {MANIFEST_KIND_METADATA_KEY: MANIFEST_KIND},
        ),
        (
            "missing kind",
            {MANIFEST_VERSION_METADATA_KEY: MANIFEST_VERSION},
        ),
        (
            "conflicting version",
            {
                MANIFEST_VERSION_METADATA_KEY: b"3",
                MANIFEST_KIND_METADATA_KEY: MANIFEST_KIND,
            },
        ),
        (
            "conflicting kind",
            {
                MANIFEST_VERSION_METADATA_KEY: MANIFEST_VERSION,
                MANIFEST_KIND_METADATA_KEY: b"other-kind",
            },
        ),
        (
            "unknown MSC namespace key",
            {
                MANIFEST_VERSION_METADATA_KEY: MANIFEST_VERSION,
                MANIFEST_KIND_METADATA_KEY: MANIFEST_KIND,
                b"msc.manifest.future": b"1",
            },
        ),
    ],
)
def test_load_virtual_manifest_rejects_noncanonical_footer_metadata(
    case: str, footer_metadata: dict[bytes, bytes]
) -> None:
    """Only the two v2 MSC footer keys are accepted in the reserved namespace."""
    with pytest.raises(ManifestValidationError):
        load_virtual_manifest(write_manifest([], footer_metadata=footer_metadata), source_bindings(), {})


def _without_first_field(schema: pa.Schema) -> pa.Schema:
    return schema.remove(0)


def _with_extra_field(schema: pa.Schema) -> pa.Schema:
    return schema.append(pa.field("unexpected", pa.string()))


def _with_reordered_columns(schema: pa.Schema) -> pa.Schema:
    return pa.schema(list(reversed(schema)), metadata=schema.metadata)


def _with_nullable_key(schema: pa.Schema) -> pa.Schema:
    return schema.set(0, pa.field("key", pa.string(), nullable=True))


def _with_wrong_key_type(schema: pa.Schema) -> pa.Schema:
    return schema.set(0, pa.field("key", pa.large_string(), nullable=False))


def _with_wrong_chunk_index_type(schema: pa.Schema) -> pa.Schema:
    return schema.set(6, pa.field("chunk_index", pa.int64(), nullable=False))


def _with_required_metadata(schema: pa.Schema) -> pa.Schema:
    return schema.set(5, pa.field("metadata", pa.string(), nullable=False))


def _with_required_source_path(schema: pa.Schema) -> pa.Schema:
    return schema.set(10, pa.field("source_path", pa.string(), nullable=False))


def _with_wrong_timestamp_unit(schema: pa.Schema) -> pa.Schema:
    return schema.set(2, pa.field("last_modified", pa.timestamp("ms", tz="UTC"), nullable=False))


def _with_wrong_timestamp_timezone(schema: pa.Schema) -> pa.Schema:
    return schema.set(2, pa.field("last_modified", pa.timestamp("us", tz="America/Chicago"), nullable=False))


def _with_changed_field_type(schema: pa.Schema, field_index: int, changed_type: pa.DataType) -> pa.Schema:
    field = schema.field(field_index)
    return schema.set(field_index, pa.field(field.name, changed_type, nullable=field.nullable))


def _with_toggled_field_nullability(schema: pa.Schema, field_index: int) -> pa.Schema:
    field = schema.field(field_index)
    return schema.set(field_index, field.with_nullable(not field.nullable))


def _with_wrong_query_child_name(schema: pa.Schema) -> pa.Schema:
    query_item = pa.field(
        "item",
        pa.struct(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("value", pa.string(), nullable=False),
            ]
        ),
        nullable=False,
    )
    return schema.set(14, pa.field("service_query", pa.list_(query_item), nullable=True))


def _with_nullable_query_name(schema: pa.Schema) -> pa.Schema:
    query_item = pa.field(
        "element",
        pa.struct(
            [
                pa.field("name", pa.string(), nullable=True),
                pa.field("value", pa.string(), nullable=False),
            ]
        ),
        nullable=False,
    )
    return schema.set(14, pa.field("service_query", pa.list_(query_item), nullable=True))


def _with_binary_query_value(schema: pa.Schema) -> pa.Schema:
    query_item = pa.field(
        "element",
        pa.struct(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("value", pa.binary(), nullable=False),
            ]
        ),
        nullable=False,
    )
    return schema.set(14, pa.field("service_query", pa.list_(query_item), nullable=True))


def _with_nullable_query_element(schema: pa.Schema) -> pa.Schema:
    query_item = pa.field(
        "element",
        pa.struct(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("value", pa.string(), nullable=False),
            ]
        ),
        nullable=True,
    )
    return schema.set(14, pa.field("service_query", pa.list_(query_item), nullable=True))


def _with_wrong_query_name_field_name(schema: pa.Schema) -> pa.Schema:
    query_item = pa.field(
        "element",
        pa.struct(
            [
                pa.field("query_name", pa.string(), nullable=False),
                pa.field("value", pa.string(), nullable=False),
            ]
        ),
        nullable=False,
    )
    return schema.set(14, pa.field("service_query", pa.list_(query_item), nullable=True))


def _with_wrong_query_value_field_name(schema: pa.Schema) -> pa.Schema:
    query_item = pa.field(
        "element",
        pa.struct(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("query_value", pa.string(), nullable=False),
            ]
        ),
        nullable=False,
    )
    return schema.set(14, pa.field("service_query", pa.list_(query_item), nullable=True))


def _with_binary_query_name(schema: pa.Schema) -> pa.Schema:
    query_item = pa.field(
        "element",
        pa.struct(
            [
                pa.field("name", pa.binary(), nullable=False),
                pa.field("value", pa.string(), nullable=False),
            ]
        ),
        nullable=False,
    )
    return schema.set(14, pa.field("service_query", pa.list_(query_item), nullable=True))


def _with_nullable_query_value(schema: pa.Schema) -> pa.Schema:
    query_item = pa.field(
        "element",
        pa.struct(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("value", pa.string(), nullable=True),
            ]
        ),
        nullable=False,
    )
    return schema.set(14, pa.field("service_query", pa.list_(query_item), nullable=True))


@pytest.mark.parametrize(
    ("case", "alter_schema"),
    [
        ("missing required column", _without_first_field),
        ("unexpected extra column", _with_extra_field),
        ("different column order", _with_reordered_columns),
        ("key nullable", _with_nullable_key),
        ("key type changed", _with_wrong_key_type),
        ("chunk index type changed", _with_wrong_chunk_index_type),
        ("metadata made required", _with_required_metadata),
        ("source path made required", _with_required_source_path),
        ("timestamp precision changed", _with_wrong_timestamp_unit),
        ("timestamp timezone changed", _with_wrong_timestamp_timezone),
        ("query list child name changed", _with_wrong_query_child_name),
        ("query list element made nullable", _with_nullable_query_element),
        ("query name child name changed", _with_wrong_query_name_field_name),
        ("query value child name changed", _with_wrong_query_value_field_name),
        ("query name type changed", _with_binary_query_name),
        ("query name made nullable", _with_nullable_query_name),
        ("query value type changed", _with_binary_query_value),
        ("query value made nullable", _with_nullable_query_value),
    ],
)
def test_load_virtual_manifest_rejects_any_schema_deviation(
    case: str, alter_schema: Callable[[pa.Schema], pa.Schema]
) -> None:
    """The decoder does not coerce near-miss schemas into the v2 representation."""
    with pytest.raises(ManifestValidationError):
        load_virtual_manifest(
            write_manifest([], schema=alter_schema(virtual_manifest_v2_schema())), source_bindings(), {}
        )


@pytest.mark.parametrize(
    ("field_name", "field_index", "changed_type"),
    [
        ("key", 0, pa.binary()),
        ("size_bytes", 1, pa.int32()),
        ("last_modified", 2, pa.int64()),
        ("content_type", 3, pa.binary()),
        ("storage_class", 4, pa.binary()),
        ("metadata", 5, pa.binary()),
        ("chunk_index", 6, pa.int64()),
        ("chunk_size_bytes", 7, pa.int32()),
        ("chunk_kind", 8, pa.binary()),
        ("source_profile", 9, pa.binary()),
        ("source_path", 10, pa.binary()),
        ("source_offset", 11, pa.int32()),
        ("service_id", 12, pa.binary()),
        ("service_path", 13, pa.binary()),
        ("service_query", 14, pa.string()),
    ],
)
def test_load_virtual_manifest_rejects_a_type_mutation_for_every_physical_column(
    field_name: str, field_index: int, changed_type: pa.DataType
) -> None:
    """Every v2 column is checked exactly; no field is permissively coerced by the loader."""
    schema = _with_changed_field_type(virtual_manifest_v2_schema(), field_index, changed_type)

    with pytest.raises(ManifestValidationError):
        load_virtual_manifest(write_manifest([], schema=schema), source_bindings(), {})


@pytest.mark.parametrize(
    ("field_name", "field_index"),
    [
        ("key", 0),
        ("size_bytes", 1),
        ("last_modified", 2),
        ("content_type", 3),
        ("storage_class", 4),
        ("metadata", 5),
        ("chunk_index", 6),
        ("chunk_size_bytes", 7),
        ("chunk_kind", 8),
        ("source_profile", 9),
        ("source_path", 10),
        ("source_offset", 11),
        ("service_id", 12),
        ("service_path", 13),
        ("service_query", 14),
    ],
)
def test_load_virtual_manifest_rejects_a_nullability_mutation_for_every_physical_column(
    field_name: str, field_index: int
) -> None:
    """Both required and optional v2 columns retain their exact nullability contract."""
    schema = _with_toggled_field_nullability(virtual_manifest_v2_schema(), field_index)

    with pytest.raises(ManifestValidationError):
        load_virtual_manifest(write_manifest([], schema=schema), source_bindings(), {})


@pytest.mark.parametrize("payload", [b"not-a-parquet-file", b"PAR1corrupt"])
def test_load_virtual_manifest_wraps_nonparquet_bytes_as_manifest_validation_errors(payload: bytes) -> None:
    """Invalid container bytes cannot leak a PyArrow-specific exception through the API."""
    with pytest.raises(ManifestValidationError):
        load_virtual_manifest(BytesIO(payload), source_bindings(), {})


def test_load_virtual_manifest_wraps_a_truncated_parquet_stream_as_a_manifest_validation_error() -> None:
    """Footer and row reads fail closed when a manifest stream ends prematurely."""
    truncated = write_manifest([]).getvalue()[:-8]

    with pytest.raises(ManifestValidationError):
        load_virtual_manifest(BytesIO(truncated), source_bindings(), {})
