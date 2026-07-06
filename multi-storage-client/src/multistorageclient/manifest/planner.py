# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pure logical-range planning for virtual files."""

from __future__ import annotations

from bisect import bisect_right
from typing import Optional

from ..types import Range
from .models import DownloadPlan, ManifestFile, ObjectChunk, PlannedChunkRead, ServiceChunk


def plan_download(file: ManifestFile, byte_range: Optional[Range] = None) -> DownloadPlan:
    """Plan the chunk reads needed to satisfy one logical byte range."""
    if byte_range is None:
        offset = 0
        requested_size = file.size_bytes
    else:
        offset = byte_range.offset
        requested_size = byte_range.size
        if (
            not isinstance(offset, int)
            or isinstance(offset, bool)
            or not isinstance(requested_size, int)
            or isinstance(requested_size, bool)
            or offset < 0
            or requested_size < 0
        ):
            raise ValueError("Virtual manifest ranges require non-negative offset and size.")

    if offset >= file.size_bytes or requested_size == 0:
        return DownloadPlan(file.key, offset, 0, ())

    size_bytes = min(requested_size, file.size_bytes - offset)
    end = offset + size_bytes
    reads: list[PlannedChunkRead] = []
    first_chunk_index = bisect_right(file.cumulative_ends, offset)
    chunk_start = 0 if first_chunk_index == 0 else file.cumulative_ends[first_chunk_index - 1]

    for chunk_index in range(first_chunk_index, len(file.chunks)):
        chunk = file.chunks[chunk_index]
        chunk_end = file.cumulative_ends[chunk_index]
        if chunk_start >= end:
            break
        overlap_start = max(offset, chunk_start)
        overlap_end = min(end, chunk_end)
        if overlap_start < overlap_end and isinstance(chunk, (ObjectChunk, ServiceChunk)):
            reads.append(
                PlannedChunkRead(
                    chunk=chunk,
                    chunk_offset=overlap_start - chunk_start,
                    output_offset=overlap_start - offset,
                    size_bytes=overlap_end - overlap_start,
                )
            )
        chunk_start = chunk_end

    return DownloadPlan(file.key, offset, size_bytes, tuple(reads))
