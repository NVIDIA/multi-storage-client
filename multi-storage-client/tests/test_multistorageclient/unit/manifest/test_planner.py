# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pure range-planning tests for virtual files."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from typing import Any, cast

import pytest

from multistorageclient.manifest.models import (
    DownloadPlan,
    ManifestFile,
    ObjectChunk,
    PlannedChunkRead,
    QueryParameter,
    ServiceChunk,
)
from multistorageclient.manifest.planner import plan_download
from multistorageclient.types import Range


def _three_chunk_file() -> ManifestFile:
    """Build a 10-byte file with object, service, and object chunks."""
    chunks = (
        ObjectChunk(0, 3, "source", "objects/head.bin", 10),
        ServiceChunk(1, 5, "service", "clips/middle.bin", (QueryParameter("variant", "raw"),)),
        ObjectChunk(2, 2, "source", "objects/tail.bin", 0),
    )
    return ManifestFile(
        key="logical/file.bin",
        size_bytes=10,
        last_modified=datetime(2026, 1, 2, tzinfo=timezone.utc),
        content_type=None,
        storage_class=None,
        metadata=None,
        chunks=chunks,
        cumulative_ends=(3, 8, 10),
        etag="msc-v2-sha256:test",
    )


def _small_file() -> ManifestFile:
    """Build a five-byte file whose chunk boundaries exercise one-byte overlaps."""
    chunks = (
        ObjectChunk(0, 2, "source", "objects/first.bin", 0),
        ServiceChunk(1, 1, "service", "clips/middle.bin", ()),
        ObjectChunk(2, 2, "source", "objects/last.bin", 0),
    )
    return ManifestFile(
        key="logical/small.bin",
        size_bytes=5,
        last_modified=datetime(2026, 1, 2, tzinfo=timezone.utc),
        content_type=None,
        storage_class=None,
        metadata=None,
        chunks=chunks,
        cumulative_ends=(2, 3, 5),
        etag="msc-v2-sha256:small",
    )


def test_plan_download_without_a_range_covers_every_nonempty_chunk_in_order() -> None:
    """A whole-file read preserves chunk order and places every chunk contiguously."""
    file = _three_chunk_file()
    chunks = cast(tuple[ObjectChunk | ServiceChunk, ...], file.chunks)

    assert plan_download(file) == DownloadPlan(
        key="logical/file.bin",
        offset=0,
        size_bytes=10,
        reads=(
            PlannedChunkRead(chunks[0], 0, 0, 3),
            PlannedChunkRead(chunks[1], 0, 3, 5),
            PlannedChunkRead(chunks[2], 0, 8, 2),
        ),
    )


@pytest.mark.parametrize(
    ("byte_range", "expected_offset", "expected_size", "expected_reads"),
    [
        (Range(0, 0), 0, 0, ()),
        (Range(0, 3), 0, 3, ((0, 0, 0, 3),)),
        (Range(3, 5), 3, 5, ((1, 0, 0, 5),)),
        (Range(1, 7), 1, 7, ((0, 1, 0, 2), (1, 0, 2, 5))),
        (Range(1, 8), 1, 8, ((0, 1, 0, 2), (1, 0, 2, 5), (2, 0, 7, 1))),
        (Range(8, 99), 8, 2, ((2, 0, 0, 2),)),
        (Range(9, 1), 9, 1, ((2, 1, 0, 1),)),
        (Range(10, 0), 10, 0, ()),
        (Range(10, 1), 10, 0, ()),
        (Range(15, 1), 15, 0, ()),
    ],
)
def test_plan_download_handles_exact_boundaries_eof_and_clipping(
    byte_range: Range,
    expected_offset: int,
    expected_size: int,
    expected_reads: tuple[tuple[int, int, int, int], ...],
) -> None:
    """Ranges are half-open, clip at EOF, and become empty at or past EOF."""
    file = _three_chunk_file()

    plan = plan_download(file, byte_range)

    assert plan.key == file.key
    assert plan.offset == expected_offset
    assert plan.size_bytes == expected_size
    assert (
        tuple((read.chunk.index, read.chunk_offset, read.output_offset, read.size_bytes) for read in plan.reads)
        == expected_reads
    )


@pytest.mark.parametrize("byte_range", [Range(-1, 1), Range(0, -1), Range(-1, -1)])
def test_plan_download_rejects_negative_logical_offsets_and_sizes(byte_range: Range) -> None:
    """Negative values have no virtual-file interpretation and fail before planning."""
    with pytest.raises(ValueError):
        plan_download(_three_chunk_file(), byte_range)


@pytest.mark.parametrize(
    "byte_range",
    [
        pytest.param(Range(cast(Any, True), 1), id="boolean-offset"),
        pytest.param(Range(0, cast(Any, False)), id="boolean-size"),
        pytest.param(Range(cast(Any, 1.5), 1), id="float-offset"),
        pytest.param(Range(0, cast(Any, "1")), id="string-size"),
    ],
)
def test_plan_download_rejects_non_integer_or_boolean_range_values(byte_range: Range) -> None:
    """Logical ranges accept only concrete, non-boolean integers."""
    with pytest.raises(ValueError):
        plan_download(_three_chunk_file(), byte_range)


def test_plan_download_exhaustively_matches_small_file_offset_and_size_boundaries() -> None:
    """Every small nonnegative offset/size pair obeys clipping and exact chunk-overlap arithmetic."""
    file = _small_file()
    chunk_starts = (0, 2, 3)

    for offset in range(file.size_bytes + 3):
        for size in range(file.size_bytes + 3):
            plan = plan_download(file, Range(offset, size))
            expected_size = min(size, max(0, file.size_bytes - offset))
            expected_end = offset + expected_size
            expected_reads = []
            for chunk, chunk_start, chunk_end in zip(file.chunks, chunk_starts, file.cumulative_ends):
                overlap_start = max(offset, chunk_start)
                overlap_end = min(expected_end, chunk_end)
                if overlap_start < overlap_end:
                    expected_reads.append(
                        (chunk.index, overlap_start - chunk_start, overlap_start - offset, overlap_end - overlap_start)
                    )

            assert plan.offset == offset
            assert plan.size_bytes == expected_size
            assert tuple(
                (read.chunk.index, read.chunk_offset, read.output_offset, read.size_bytes) for read in plan.reads
            ) == tuple(expected_reads)


def test_download_plan_and_planned_chunk_read_are_immutable_and_preserve_tuple_reads() -> None:
    """Planning results cannot be changed after construction or have their read order mutated in place."""
    chunk = ObjectChunk(0, 3, "source", "objects/file.bin", 0)
    planned_read = PlannedChunkRead(chunk, 0, 0, 3)
    plan = DownloadPlan("logical/file.bin", 0, 3, (planned_read,))

    assert type(plan.reads) is tuple
    with pytest.raises(FrozenInstanceError):
        setattr(plan, "size_bytes", 4)
    with pytest.raises(FrozenInstanceError):
        setattr(planned_read, "output_offset", 1)


class _SliceRejectingTuple(tuple):
    """Tuple fixture that catches a planner copying an untouched tail via slicing."""

    def __getitem__(self, index):
        if isinstance(index, slice):
            raise AssertionError("range planning must index touched chunks instead of slicing a tuple tail")
        return super().__getitem__(index)


def test_plan_download_does_not_slice_a_large_untouched_chunk_tail() -> None:
    chunk_count = 4096
    chunks = _SliceRejectingTuple(
        ObjectChunk(index, 1, "source", f"objects/{index}.bin", 0) for index in range(chunk_count)
    )
    file = ManifestFile(
        key="logical/large.bin",
        size_bytes=chunk_count,
        last_modified=datetime(2026, 1, 2, tzinfo=timezone.utc),
        content_type=None,
        storage_class=None,
        metadata=None,
        chunks=chunks,
        cumulative_ends=_SliceRejectingTuple(range(1, chunk_count + 1)),
        etag="msc-v2-sha256:large",
    )

    plan = plan_download(file, Range(offset=0, size=1))

    assert plan.reads == (PlannedChunkRead(chunks[0], 0, 0, 1),)
