# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Small fixtures shared by virtual-manifest contract tests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from multistorageclient.manifest.bindings import ServiceBinding, SourceBinding
from multistorageclient.manifest.models import QueryParameter
from multistorageclient.manifest.schema import virtual_manifest_v2_schema
from multistorageclient.types import Range


@dataclass(frozen=True)
class StubSourceReader:
    """A decode-only source binding; tests must not fetch its bytes."""

    binding_identity: str = "s3://source-bucket/objects"

    def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        raise AssertionError(f"unexpected source read for {path!r} at {byte_range!r}")


@dataclass(frozen=True)
class StubServiceReader:
    """A decode-only service binding; tests must not call its transport."""

    binding_identity: str = "https://service.example.test/v1"

    def validate(self, path: str, query: Sequence[QueryParameter]) -> None:
        """Accept valid fixture rows without reaching a transport."""

    def read(self, path: str, query: Sequence[QueryParameter], byte_range: Range, total_size: int) -> bytes:
        raise AssertionError(f"unexpected service read for {path!r} at {byte_range!r}")


def source_bindings(
    *,
    alias: str = "source",
    binding_identity: str = "s3://source-bucket/objects",
    binding_revision: str = "source-revision-1",
) -> dict[str, SourceBinding]:
    """Build one configured object-source binding."""
    return {alias: SourceBinding(StubSourceReader(binding_identity), binding_revision)}


def service_bindings(
    *,
    alias: str = "service",
    binding_identity: str = "https://service.example.test/v1",
    binding_revision: str = "service-revision-1",
) -> dict[str, ServiceBinding]:
    """Build one configured deterministic-service binding."""
    return {alias: ServiceBinding(StubServiceReader(binding_identity), binding_revision)}


def object_row(**changes: Any) -> dict[str, Any]:
    """Return a valid, single-chunk object-backed manifest row."""
    row: dict[str, Any] = {
        "key": "logical/file.bin",
        "size_bytes": 3,
        "last_modified": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        "content_type": "application/octet-stream",
        "storage_class": "STANDARD",
        "metadata": {"source": "unit-test"},
        "chunk_index": 0,
        "chunk_size_bytes": 3,
        "chunk_kind": "object",
        "source_profile": "source",
        "source_path": "objects/a.bin",
        "source_offset": 7,
        "service_id": None,
        "service_path": None,
        "service_query": None,
    }
    row.update(changes)
    return row


def service_row(**changes: Any) -> dict[str, Any]:
    """Return a valid, single-chunk service-backed manifest row."""
    row = object_row(
        chunk_kind="service",
        source_profile=None,
        source_path=None,
        source_offset=None,
        service_id="service",
        service_path="clips/rendered.bin",
        service_query=[],
    )
    row.update(changes)
    return row


def write_manifest(
    rows: Sequence[Mapping[str, Any]],
    *,
    schema: Optional[pa.Schema] = None,
    footer_metadata: Optional[Mapping[bytes, bytes]] = None,
    row_group_size: Optional[int] = None,
) -> BytesIO:
    """Encode manifest rows into an in-memory Parquet stream."""
    effective_schema = schema or virtual_manifest_v2_schema()
    if footer_metadata is not None:
        effective_schema = effective_schema.with_metadata(dict(footer_metadata))

    if rows:
        table = pa.Table.from_pylist(list(rows), schema=effective_schema)
    else:
        table = pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in effective_schema],
            schema=effective_schema,
        )

    output = BytesIO()
    pq.write_table(table, output, row_group_size=row_group_size)
    output.seek(0)
    return output
