# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Arrow schema for single-Parquet virtual manifests."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow

MANIFEST_VERSION_METADATA_KEY = b"msc.manifest.version"
MANIFEST_KIND_METADATA_KEY = b"msc.manifest.kind"
MANIFEST_VERSION = b"2"
MANIFEST_KIND = b"virtual-file-chunks"
VIRTUAL_MANIFEST_V2_FOOTER_METADATA = {
    MANIFEST_VERSION_METADATA_KEY: MANIFEST_VERSION,
    MANIFEST_KIND_METADATA_KEY: MANIFEST_KIND,
}


def _require_pyarrow() -> Any:
    try:
        import pyarrow
    except ImportError as exc:
        raise ImportError(
            "PyArrow is required for virtual manifest support. "
            "Install it with: pip install multi-storage-client[virtual-manifest]"
        ) from exc
    return pyarrow


def virtual_manifest_v2_schema() -> "pyarrow.Schema":
    """Return the exact Arrow schema for a single-Parquet virtual manifest v2.

    :return: A :class:`pyarrow.Schema` carrying the required manifest metadata.
    :raises ImportError: If PyArrow is not installed.
    """
    pa = _require_pyarrow()
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
    return pa.schema(
        [
            pa.field("key", pa.string(), nullable=False),
            pa.field("size_bytes", pa.int64(), nullable=False),
            pa.field("last_modified", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("content_type", pa.string()),
            pa.field("storage_class", pa.string()),
            pa.field("metadata", pa.string()),
            pa.field("chunk_index", pa.int32(), nullable=False),
            pa.field("chunk_size_bytes", pa.int64(), nullable=False),
            pa.field("chunk_kind", pa.string(), nullable=False),
            pa.field("source_profile", pa.string()),
            pa.field("source_path", pa.string()),
            pa.field("source_offset", pa.int64()),
            pa.field("service_id", pa.string()),
            pa.field("service_path", pa.string()),
            pa.field("service_query", pa.list_(query_item)),
        ],
        metadata=VIRTUAL_MANIFEST_V2_FOOTER_METADATA,
    )
