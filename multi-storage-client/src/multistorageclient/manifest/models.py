# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Immutable decoded virtual-manifest models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union


@dataclass(frozen=True, slots=True)
class QueryParameter:
    """One ordered service query parameter."""

    name: str
    value: str


@dataclass(frozen=True, slots=True)
class EmptyChunk:
    """Sentinel representing a zero-byte logical file."""

    index: int = 0
    size_bytes: int = 0


@dataclass(frozen=True, slots=True)
class ObjectChunk:
    """Byte range from a configured storage profile."""

    index: int
    size_bytes: int
    source_profile: str
    source_path: str
    source_offset: int


@dataclass(frozen=True, slots=True)
class ServiceChunk:
    """Byte range from deterministic service output."""

    index: int
    size_bytes: int
    service_id: str
    service_path: str
    service_query: tuple[QueryParameter, ...]


ManifestChunk = Union[EmptyChunk, ObjectChunk, ServiceChunk]


@dataclass(frozen=True, slots=True)
class ManifestFile:
    """Validated reconstruction plan for one logical file."""

    key: str
    size_bytes: int
    last_modified: datetime
    content_type: Optional[str]
    storage_class: Optional[str]
    metadata_json: Optional[str]
    chunks: tuple[ManifestChunk, ...]
    cumulative_ends: tuple[int, ...]
    etag: str


@dataclass(frozen=True, slots=True)
class PlannedChunkRead:
    """One chunk-local read placed at a deterministic output offset."""

    chunk: Union[ObjectChunk, ServiceChunk]
    chunk_offset: int
    output_offset: int
    size_bytes: int


@dataclass(frozen=True, slots=True)
class DownloadPlan:
    """Pure execution plan for one logical range read."""

    key: str
    offset: int
    size_bytes: int
    reads: tuple[PlannedChunkRead, ...]
