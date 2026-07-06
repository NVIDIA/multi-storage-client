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
from .bindings import ServiceBinding, SourceBinding
from .models import EmptyChunk, ManifestChunk, ManifestFile, ObjectChunk, QueryParameter, ServiceChunk
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
    metadata_json: Optional[str]
    metadata_value: Optional[dict[str, Any]]
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


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON value {value!r}")


def _unique_json_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError(f"duplicate JSON object key {name!r}")
        result[name] = value
    return result


def _normalize_metadata(value: Optional[str]) -> tuple[Optional[str], Optional[dict[str, Any]]]:
    if value is None:
        return None, None
    try:
        parsed = json.loads(value, parse_constant=_reject_json_constant, object_pairs_hook=_unique_json_object)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise _error("metadata must be strict JSON") from exc
    if not isinstance(parsed, dict):
        raise _error("metadata must be a JSON object")
    try:
        canonical = json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise _error("metadata cannot be canonically encoded") from exc
    return canonical, parsed


def _normalize_timestamp(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise _error("last_modified must be a timezone-aware timestamp")
    return value.astimezone(timezone.utc)


def _validate_bindings(bindings: Mapping[str, SourceBinding] | Mapping[str, ServiceBinding], *, service: bool) -> None:
    for alias, binding in bindings.items():
        if not isinstance(alias, str) or not alias:
            raise _error("binding aliases must be non-empty strings")
        expected_type = ServiceBinding if service else SourceBinding
        if not isinstance(binding, expected_type):
            raise _error(f"binding {alias!r} has an invalid type")
        revision = binding.binding_revision
        identity = binding.reader.binding_identity
        if not isinstance(revision, str) or not revision:
            raise _error(f"binding {alias!r} has an invalid revision")
        if not isinstance(identity, str) or not identity:
            raise _error(f"binding {alias!r} has an invalid identity")
        if service and not callable(getattr(binding.reader, "validate", None)):
            raise _error(f"service binding {alias!r} has no validator")


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

    def require_manifest_schema(actual: Any, *, location: str) -> None:
        _validate_manifest_metadata(dict(actual.metadata or {}), location=location)
        if not actual.remove_metadata().equals(expected.remove_metadata(), check_metadata=True):
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
    if size_bytes < 0:
        raise _error("size_bytes must be non-negative")
    last_modified = _normalize_timestamp(row.get("last_modified"))
    content_type = _optional_string(row, "content_type")
    storage_class = _optional_string(row, "storage_class")
    metadata_json, metadata_value = _normalize_metadata(_optional_string(row, "metadata"))
    chunk_index = _required_int(row, "chunk_index")
    chunk_size = _required_int(row, "chunk_size_bytes")
    chunk_kind = _required_string(row, "chunk_kind")
    if chunk_index < 0 or chunk_size < 0:
        raise _error("chunk index and size must be non-negative")

    source_fields = ("source_profile", "source_path", "source_offset")
    service_fields = ("service_id", "service_path", "service_query")
    if chunk_kind == "empty":
        if size_bytes != 0 or chunk_index != 0 or chunk_size != 0:
            raise _error("empty chunks require zero file size, zero index, and zero chunk size")
        if not _all_null(row, source_fields + service_fields):
            raise _error("empty chunks cannot contain object or service fields")
        chunk: ManifestChunk = EmptyChunk()
    elif chunk_kind == "object":
        if chunk_size <= 0:
            raise _error("object chunks must have a positive size")
        if not _all_null(row, service_fields):
            raise _error("object chunks cannot contain service fields")
        source_profile = _required_string(row, "source_profile")
        source_path = _required_string(row, "source_path")
        source_offset = _required_int(row, "source_offset")
        if source_offset < 0:
            raise _error("object source offset must be non-negative")
        _validate_relative_path(source_path, service=False)
        if source_profile not in source_bindings:
            raise _error(f"unknown source binding {source_profile!r}")
        chunk = ObjectChunk(chunk_index, chunk_size, source_profile, source_path, source_offset)
    elif chunk_kind == "service":
        if chunk_size <= 0:
            raise _error("service chunks must have a positive size")
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
        metadata_json=metadata_json,
        metadata_value=metadata_value,
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
    metadata_value: Optional[dict[str, Any]],
    chunks: Sequence[ManifestChunk],
    source_bindings: Mapping[str, SourceBinding],
    service_bindings: Mapping[str, ServiceBinding],
) -> str:
    serialized_chunks: list[dict[str, Any]] = []
    for chunk in chunks:
        if isinstance(chunk, EmptyChunk):
            serialized_chunks.append({"index": chunk.index, "kind": "empty", "size_bytes": chunk.size_bytes})
        elif isinstance(chunk, ObjectChunk):
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
                "metadata": metadata_value,
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


def _build_files(
    rows_by_key: Mapping[str, Sequence[_DecodedRow]],
    source_bindings: Mapping[str, SourceBinding],
    service_bindings: Mapping[str, ServiceBinding],
) -> dict[str, ManifestFile]:
    keys = set(rows_by_key)
    for key in sorted(keys):
        ancestor = key.rsplit("/", 1)[0] if "/" in key else None
        while ancestor is not None:
            if ancestor in keys:
                raise _error(f"file key {ancestor!r} is an ancestor of file key {key!r}")
            ancestor = ancestor.rsplit("/", 1)[0] if "/" in ancestor else None

    files: dict[str, ManifestFile] = {}
    for key, rows in rows_by_key.items():
        first = rows[0]
        metadata = (
            first.size_bytes,
            first.last_modified,
            first.content_type,
            first.storage_class,
            first.metadata_json,
        )
        if any(
            (
                row.size_bytes,
                row.last_modified,
                row.content_type,
                row.storage_class,
                row.metadata_json,
            )
            != metadata
            for row in rows[1:]
        ):
            raise _error(f"file {key!r} has conflicting file metadata")

        chunks = tuple(sorted((row.chunk for row in rows), key=lambda chunk: chunk.index))
        if any(isinstance(chunk, EmptyChunk) for chunk in chunks):
            if len(chunks) != 1 or not isinstance(chunks[0], EmptyChunk):
                raise _error(f"file {key!r} has mixed or multiple empty chunks")
        elif tuple(chunk.index for chunk in chunks) != tuple(range(len(chunks))):
            raise _error(f"file {key!r} chunk indexes must be contiguous and unique")

        total = 0
        cumulative_ends: list[int] = []
        for chunk in chunks:
            if total > _INT64_MAX - chunk.size_bytes:
                raise _error(f"file {key!r} chunk total overflows int64")
            total += chunk.size_bytes
            cumulative_ends.append(total)
        if total != first.size_bytes:
            raise _error(f"file {key!r} chunk sizes do not match file size")

        files[key] = ManifestFile(
            key=key,
            size_bytes=first.size_bytes,
            last_modified=first.last_modified,
            content_type=first.content_type,
            storage_class=first.storage_class,
            metadata_json=first.metadata_json,
            chunks=chunks,
            cumulative_ends=tuple(cumulative_ends),
            etag=_make_etag(
                key,
                first.size_bytes,
                first.last_modified,
                first.content_type,
                first.storage_class,
                first.metadata_value,
                chunks,
                source_bindings,
                service_bindings,
            ),
        )
    return files


def load_virtual_manifest(
    stream: IO[bytes],
    source_bindings: Mapping[str, SourceBinding],
    service_bindings: Mapping[str, ServiceBinding],
) -> dict[str, ManifestFile]:
    """Decode and validate one single-Parquet virtual manifest."""
    pa = _require_pyarrow()
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ImportError(
            "PyArrow is required for virtual manifest support. "
            "Install it with: pip install multi-storage-client[virtual-manifest]"
        ) from exc

    try:
        _validate_bindings(source_bindings, service=False)
        _validate_bindings(service_bindings, service=True)
        parquet_file = pq.ParquetFile(stream)
        _validate_schema(parquet_file, pa)
        rows_by_key: dict[str, list[_DecodedRow]] = {}
        for batch in parquet_file.iter_batches(batch_size=_BATCH_SIZE):
            for row in batch.to_pylist():
                decoded = _decode_row(row, source_bindings, service_bindings)
                rows_by_key.setdefault(decoded.key, []).append(decoded)
        return _build_files(rows_by_key, source_bindings, service_bindings)
    except ManifestValidationError:
        raise
    except Exception as exc:
        raise _error("unable to decode Parquet manifest") from exc
