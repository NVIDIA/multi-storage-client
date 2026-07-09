# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import math
from collections.abc import Iterator
from typing import Optional

from ..types import (
    FileRangeMapping,
    InlineBytesMapping,
    ObjectMetadata,
    Range,
    RangeMapping,
    RangeMappingMetadataProvider,
    ResolvedPath,
    ResolvedPathState,
)

CHUNK_SIZE = 1024


class InvertedFileMetadataProvider(RangeMappingMetadataProvider):
    """
    A :py:class:`RangeMappingMetadataProvider` that serves logical files in reverse
    1 KB chunk order.

    The last ``CHUNK_SIZE`` bytes of the physical file are returned first, then the
    second-to-last chunk, and so on.  Every 5th chunk (1-indexed) is replaced with
    an equal-sized run of zero bytes instead of the physical file contents.

    :param files: Mapping of logical path to ``(physical_path, file_size)`` tuples.
    """

    def __init__(self, files: dict[str, tuple[str, int]]) -> None:
        self._files = files

    # -- RangeMappingMetadataProvider --

    def realpath(self, logical_path: str) -> ResolvedPath:
        if logical_path in self._files:
            physical_path, _ = self._files[logical_path]
            return ResolvedPath(physical_path=physical_path, state=ResolvedPathState.MAPPED)
        return ResolvedPath(physical_path=logical_path, state=ResolvedPathState.UNTRACKED)

    def real_mappings(self, logical_path: str, byte_range: Optional[Range] = None) -> list[RangeMapping]:
        """
        Return chunks of the physical file in reverse order, with every 5th chunk
        replaced by inline zero bytes.

        If *byte_range* is given, only return the segments that overlap the requested
        logical range, clipped to its boundaries.
        """
        physical_path, file_size = self._files[logical_path]
        if file_size == 0:
            return []

        num_chunks = math.ceil(file_size / CHUNK_SIZE)

        # Logical byte range we need to cover.
        logical_start = byte_range.offset if byte_range else 0
        logical_end = (byte_range.offset + byte_range.size) if byte_range else file_size

        result: list[RangeMapping] = []
        logical_cursor = 0

        for i in range(num_chunks):
            # Size of this logical chunk (last chunk may be smaller).
            chunk_logical_size = min(CHUNK_SIZE, file_size - i * CHUNK_SIZE)
            chunk_logical_end = logical_cursor + chunk_logical_size

            # Skip chunks entirely outside the requested range.
            if chunk_logical_end <= logical_start or logical_cursor >= logical_end:
                logical_cursor = chunk_logical_end
                continue

            # Clip this chunk to the requested range.
            clip_start = max(logical_cursor, logical_start) - logical_cursor
            clip_end = min(chunk_logical_end, logical_end) - logical_cursor
            clipped_size = clip_end - clip_start

            if (i + 1) % 5 == 0:
                result.append(InlineBytesMapping(data=b"\x00" * clipped_size))
            else:
                # This chunk corresponds to the (i+1)-th block from the end of the file.
                physical_offset = file_size - (i + 1) * CHUNK_SIZE + clip_start
                # Clamp to 0 for the very last (smallest) chunk.
                physical_offset = max(0, physical_offset)
                result.append(FileRangeMapping(physical_path=physical_path, range=Range(offset=physical_offset, size=clipped_size)))

            logical_cursor = chunk_logical_end

        return result

    # -- Unsupported MetadataProvider methods --

    def list_objects(self, path, start_after=None, end_at=None, include_directories=False, attribute_filter_expression=None, show_attributes=False) -> Iterator[ObjectMetadata]:
        raise NotImplementedError

    def get_object_metadata(self, path: str, include_pending: bool = False) -> ObjectMetadata:
        raise NotImplementedError

    def glob(self, pattern: str, attribute_filter_expression=None) -> list[str]:
        raise NotImplementedError

    def generate_physical_path(self, logical_path: str, for_overwrite: bool = False) -> ResolvedPath:
        raise NotImplementedError

    def add_file(self, path: str, metadata: ObjectMetadata) -> None:
        raise NotImplementedError

    def remove_file(self, path: str) -> None:
        raise NotImplementedError

    def commit_updates(self) -> None:
        raise NotImplementedError

    def is_writable(self) -> bool:
        return False

    def allow_overwrites(self) -> bool:
        return False

    def should_use_soft_delete(self) -> bool:
        return False
