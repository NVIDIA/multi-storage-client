# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Strict Parquet decoder for virtual manifest v2."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import IO, Any, Optional

from . import ManifestValidationError
from .bindings import ServiceBinding, SourceBinding, validate_manifest_bindings
from .models import ManifestChunk, ManifestFile, ObjectChunk, QueryParameter, ServiceChunk
from .schema import (
    MANIFEST_KIND,
    MANIFEST_KIND_METADATA_KEY,
    MANIFEST_VERSION,
    MANIFEST_VERSION_METADATA_KEY,
    _require_pyarrow,
    virtual_manifest_v2_schema,
)

_BATCH_SIZE = 65_536
_INT64_MAX = (1 << 63) - 1
_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
_RESERVED_MANIFEST_METADATA_KEYS = frozenset(
    {
        MANIFEST_VERSION_METADATA_KEY,
        MANIFEST_KIND_METADATA_KEY,
    }
)


@dataclass(frozen=True, slots=True)
class _DecodedRow:
    """One validated physical row before logical-file grouping."""

    key: str
    size_bytes: int
    last_modified: datetime
    content_type: Optional[str]
    storage_class: Optional[str]
    metadata: Optional[tuple[tuple[str, str], ...]]
    chunk: ManifestChunk


def _error(message: str) -> ManifestValidationError:
    return ManifestValidationError(f"Invalid virtual manifest v2: {message}")


def _required_string(row: Mapping[str, Any], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str):
        raise _error(f"{name} must be a non-null string")
    return value


def _optional_string(row: Mapping[str, Any], name: str) -> Optional[str]:
    value = row.get(name)
    if value is not None and not isinstance(value, str):
        raise _error(f"{name} must be a string or null")
    return value


def _required_int(row: Mapping[str, Any], name: str) -> int:
    value = row.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise _error(f"{name} must be a non-null integer")
    return value


def _validate_relative_path(path: str, *, service: bool) -> None:
    if not path:
        raise _error("path must not be empty")
    if path.startswith("/") or "\\" in path or "\x00" in path or _URI_SCHEME.match(path):
        raise _error(f"path {path!r} is not a normalized relative POSIX path")
    if service and any(character in path for character in ("%", "?", "#")):
        raise _error(f"service path {path!r} must be unescaped and query-free")
    if any(segment in ("", ".", "..") for segment in path.split("/")):
        raise _error(f"path {path!r} is not normalized")


def _normalize_metadata(value: Any) -> Optional[tuple[tuple[str, str], ...]]:
    if value is None:
        return None
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise _error("metadata must be a map of strings to strings")
    normalized: dict[str, str] = {}
    for item in value:
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes, bytearray)) or len(item) != 2:
            raise _error("metadata entries must be key/value pairs")
        name, metadata_value = item
        if not isinstance(name, str) or not isinstance(metadata_value, str):
            raise _error("metadata keys and values must be strings")
        if name in normalized:
            raise _error(f"metadata contains duplicate key {name!r}")
        normalized[name] = metadata_value
    return tuple(sorted(normalized.items()))


def _normalize_timestamp(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise _error("last_modified must be a timezone-aware timestamp")
    return value.astimezone(timezone.utc)


def _validate_manifest_metadata(metadata: Mapping[bytes, bytes], *, location: str) -> None:
    """Validate the reserved v2 manifest metadata namespace at one wire location."""
    if metadata.get(MANIFEST_VERSION_METADATA_KEY) != MANIFEST_VERSION:
        raise _error(f"{location} manifest version is missing or unsupported")
    if metadata.get(MANIFEST_KIND_METADATA_KEY) != MANIFEST_KIND:
        raise _error(f"{location} manifest kind is missing or unsupported")
    for key in metadata:
        if key.startswith(b"msc.manifest.") and key not in _RESERVED_MANIFEST_METADATA_KEYS:
            raise _error(f"unknown reserved {location} key {key!r}")


def _decode_embedded_arrow_schema(encoded_schema: bytes, pa: Any) -> Any:
    """Decode the footer's Arrow schema only from canonical strict Base64."""
    try:
        decoded_schema = base64.b64decode(encoded_schema, validate=True)
        if base64.b64encode(decoded_schema) != encoded_schema:
            raise ValueError("embedded Arrow schema Base64 is not canonical")
        reader = pa.BufferReader(decoded_schema)
        schema = pa.ipc.read_schema(reader)
        if reader.read(1):
            raise ValueError("embedded Arrow schema has trailing bytes")
        return schema
    except Exception as exc:
        raise _error("embedded Arrow schema is malformed") from exc


def _validate_schema(parquet_file: Any, pa: Any) -> None:
    metadata = dict(parquet_file.metadata.metadata or {})
    expected = virtual_manifest_v2_schema()

    def normalize_map_container_name(schema: Any) -> Any:
        field_index = schema.get_field_index("metadata")
        if field_index < 0:
            return schema
        field = schema.field(field_index)
        if not pa.types.is_map(field.type):
            return schema
        map_type = pa.map_(
            field.type.key_field,
            field.type.item_field,
            keys_sorted=field.type.keys_sorted,
        )
        return schema.set(
            field_index,
            pa.field(
                field.name,
                map_type,
                nullable=field.nullable,
                metadata=field.metadata,
            ),
        )

    def require_manifest_schema(actual: Any, *, location: str) -> None:
        _validate_manifest_metadata(dict(actual.metadata or {}), location=location)
        normalized_actual = normalize_map_container_name(actual.remove_metadata())
        normalized_expected = normalize_map_container_name(expected.remove_metadata())
        if not normalized_actual.equals(normalized_expected, check_metadata=True):
            raise _error("Parquet schema does not exactly match virtual manifest v2")

    require_manifest_schema(parquet_file.schema_arrow, location="physical Arrow schema")
    encoded_arrow_schema = metadata.get(b"ARROW:schema")
    if encoded_arrow_schema is not None:
        if not isinstance(encoded_arrow_schema, bytes):
            raise _error("embedded Arrow schema is malformed")
        embedded_schema = _decode_embedded_arrow_schema(encoded_arrow_schema, pa)
        require_manifest_schema(embedded_schema, location="embedded Arrow schema")

    _validate_manifest_metadata(metadata, location="footer")


def _all_null(row: Mapping[str, Any], names: Sequence[str]) -> bool:
    return all(row.get(name) is None for name in names)


def _parse_service_query(value: Any) -> tuple[QueryParameter, ...]:
    if not isinstance(value, list):
        raise _error("service_query must be a non-null list")
    query: list[QueryParameter] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise _error("service_query items must be name/value objects")
        name = item.get("name")
        query_value = item.get("value")
        if not isinstance(name, str) or not name or not isinstance(query_value, str):
            raise _error("service query names must be non-empty strings and values must be strings")
        query.append(QueryParameter(name, query_value))
    return tuple(query)


def _decode_row(
    row: Mapping[str, Any],
    source_bindings: Mapping[str, SourceBinding],
    service_bindings: Mapping[str, ServiceBinding],
) -> _DecodedRow:
    key = _required_string(row, "key")
    _validate_relative_path(key, service=False)
    size_bytes = _required_int(row, "size_bytes")
    if size_bytes <= 0:
        raise _error("size_bytes must be positive")
    last_modified = _normalize_timestamp(row.get("last_modified"))
    content_type = _optional_string(row, "content_type")
    storage_class = _optional_string(row, "storage_class")
    metadata = _normalize_metadata(row.get("metadata"))
    chunk_index = _required_int(row, "chunk_index")
    chunk_size = _required_int(row, "chunk_size_bytes")
    chunk_kind = _required_string(row, "chunk_kind")
    if chunk_index < 0 or chunk_size <= 0:
        raise _error("chunk index must be non-negative and chunk size must be positive")

    source_fields = ("source_profile", "source_path", "source_offset")
    service_fields = ("service_id", "service_path", "service_query")
    if chunk_kind == "object":
        if not _all_null(row, service_fields):
            raise _error("object chunks cannot contain service fields")
        source_profile = _required_string(row, "source_profile")
        source_path = _required_string(row, "source_path")
        source_offset = _required_int(row, "source_offset")
        if source_offset < 0:
            raise _error("object source offset must be non-negative")
        if source_offset > _INT64_MAX - chunk_size:
            raise _error("object source range exceeds int64")
        _validate_relative_path(source_path, service=False)
        if source_profile not in source_bindings:
            raise _error(f"unknown source binding {source_profile!r}")
        chunk: ManifestChunk = ObjectChunk(chunk_index, chunk_size, source_profile, source_path, source_offset)
    elif chunk_kind == "service":
        if not _all_null(row, source_fields):
            raise _error("service chunks cannot contain object fields")
        service_id = _required_string(row, "service_id")
        service_path = _required_string(row, "service_path")
        service_query = _parse_service_query(row.get("service_query"))
        _validate_relative_path(service_path, service=True)
        binding = service_bindings.get(service_id)
        if binding is None:
            raise _error(f"unknown service binding {service_id!r}")
        try:
            binding.reader.validate(service_path, service_query)
        except Exception as exc:
            raise _error(f"service binding {service_id!r} rejected manifest request: {exc}") from exc
        chunk = ServiceChunk(chunk_index, chunk_size, service_id, service_path, service_query)
    else:
        raise _error(f"unsupported chunk kind {chunk_kind!r}")

    return _DecodedRow(
        key=key,
        size_bytes=size_bytes,
        last_modified=last_modified,
        content_type=content_type,
        storage_class=storage_class,
        metadata=metadata,
        chunk=chunk,
    )


def _timestamp_for_etag(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _make_etag(
    key: str,
    size_bytes: int,
    last_modified: datetime,
    content_type: Optional[str],
    storage_class: Optional[str],
    metadata: Optional[tuple[tuple[str, str], ...]],
    chunks: Sequence[ManifestChunk],
    source_bindings: Mapping[str, SourceBinding],
    service_bindings: Mapping[str, ServiceBinding],
) -> str:
    serialized_chunks: list[dict[str, Any]] = []
    for chunk in chunks:
        if isinstance(chunk, ObjectChunk):
            binding = source_bindings[chunk.source_profile]
            serialized_chunks.append(
                {
                    "index": chunk.index,
                    "kind": "object",
                    "size_bytes": chunk.size_bytes,
                    "source_profile": chunk.source_profile,
                    "source_path": chunk.source_path,
                    "source_offset": chunk.source_offset,
                    "binding": {
                        "alias": chunk.source_profile,
                        "identity": binding.reader.binding_identity,
                        "revision": binding.binding_revision,
                    },
                }
            )
        else:
            binding = service_bindings[chunk.service_id]
            serialized_chunks.append(
                {
                    "index": chunk.index,
                    "kind": "service",
                    "size_bytes": chunk.size_bytes,
                    "service_id": chunk.service_id,
                    "service_path": chunk.service_path,
                    "service_query": [[item.name, item.value] for item in chunk.service_query],
                    "binding": {
                        "alias": chunk.service_id,
                        "identity": binding.reader.binding_identity,
                        "revision": binding.binding_revision,
                    },
                }
            )
    canonical = json.dumps(
        {
            "file": {
                "key": key,
                "size_bytes": size_bytes,
                "last_modified": _timestamp_for_etag(last_modified),
                "content_type": content_type,
                "storage_class": storage_class,
                "metadata": dict(metadata) if metadata is not None else None,
            },
            "chunks": serialized_chunks,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    digest = hashlib.sha256(b"msc-virtual-manifest-v2\0" + canonical).hexdigest()
    return f"msc-v2-sha256:{digest}"


def _file_metadata(
    row: _DecodedRow,
) -> tuple[int, datetime, Optional[str], Optional[str], Optional[tuple[tuple[str, str], ...]]]:
    return (
        row.size_bytes,
        row.last_modified,
        row.content_type,
        row.storage_class,
        row.metadata,
    )


def _finalize_file(
    first: _DecodedRow,
    chunks: Sequence[ManifestChunk],
    cumulative_ends: Sequence[int],
    source_bindings: Mapping[str, SourceBinding],
    service_bindings: Mapping[str, ServiceBinding],
) -> ManifestFile:
    total = cumulative_ends[-1]
    if total != first.size_bytes:
        raise _error(f"file {first.key!r} chunk sizes do not match file size")
    immutable_chunks = tuple(chunks)
    return ManifestFile(
        key=first.key,
        size_bytes=first.size_bytes,
        last_modified=first.last_modified,
        content_type=first.content_type,
        storage_class=first.storage_class,
        metadata=first.metadata,
        chunks=immutable_chunks,
        cumulative_ends=tuple(cumulative_ends),
        etag=_make_etag(
            first.key,
            first.size_bytes,
            first.last_modified,
            first.content_type,
            first.storage_class,
            first.metadata,
            immutable_chunks,
            source_bindings,
            service_bindings,
        ),
    )


def _insert_file(files: dict[str, ManifestFile], file: ManifestFile) -> None:
    ancestor = file.key.rsplit("/", 1)[0] if "/" in file.key else None
    while ancestor is not None:
        if ancestor in files:
            raise _error(f"file key {ancestor!r} is an ancestor of file key {file.key!r}")
        ancestor = ancestor.rsplit("/", 1)[0] if "/" in ancestor else None
    files[file.key] = file


def load_virtual_manifest(
    stream: IO[bytes],
    source_bindings: Mapping[str, SourceBinding],
    service_bindings: Mapping[str, ServiceBinding],
) -> dict[str, ManifestFile]:
    """Decode and validate one single-Parquet virtual manifest."""
    try:
        validate_manifest_bindings(source_bindings, service_bindings)
    except ValueError as exc:
        raise _error(str(exc)) from exc

    pa = _require_pyarrow()
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ImportError(
            "PyArrow is required for virtual manifest support. "
            "Install it with: pip install multi-storage-client[virtual-manifest]"
        ) from exc

    try:
        parquet_file = pq.ParquetFile(stream)
        _validate_schema(parquet_file, pa)
        files: dict[str, ManifestFile] = {}
        first: Optional[_DecodedRow] = None
        chunks: list[ManifestChunk] = []
        cumulative_ends: list[int] = []
        previous_position: Optional[tuple[str, int]] = None
        for batch in parquet_file.iter_batches(batch_size=_BATCH_SIZE):
            for row in batch.to_pylist():
                decoded = _decode_row(row, source_bindings, service_bindings)
                position = (decoded.key, decoded.chunk.index)
                if previous_position is not None and position <= previous_position:
                    raise _error("rows must be strictly sorted by key and chunk_index")

                if first is not None and decoded.key != first.key:
                    _insert_file(
                        files,
                        _finalize_file(first, chunks, cumulative_ends, source_bindings, service_bindings),
                    )
                    first = None
                    chunks = []
                    cumulative_ends = []

                if first is None:
                    first = decoded
                elif _file_metadata(decoded) != _file_metadata(first):
                    raise _error(f"file {decoded.key!r} has conflicting file metadata")

                expected_index = len(chunks)
                if decoded.chunk.index != expected_index:
                    raise _error(f"file {decoded.key!r} chunk indexes must be contiguous and unique")
                total = cumulative_ends[-1] if cumulative_ends else 0
                if total > _INT64_MAX - decoded.chunk.size_bytes:
                    raise _error(f"file {decoded.key!r} chunk total overflows int64")
                chunks.append(decoded.chunk)
                cumulative_ends.append(total + decoded.chunk.size_bytes)
                previous_position = position

        if first is not None:
            _insert_file(
                files,
                _finalize_file(first, chunks, cumulative_ends, source_bindings, service_bindings),
            )
        return files
    except ManifestValidationError:
        raise
    except Exception as exc:
        raise _error("unable to decode Parquet manifest") from exc
