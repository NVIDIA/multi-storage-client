# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import logging
import os
import threading
from enum import Enum
from typing import TYPE_CHECKING, Iterator, Optional

from ..constants import DEFAULT_SYNC_BATCH_SIZE
from ..types import ObjectMetadata, SymlinkHandling
from ..utils import PatternMatcher
from .progress_bar import ProgressBar
from .types import EventLike, OperationBatch, OperationType, QueueLike

if TYPE_CHECKING:
    from ..client.types import AbstractStorageClient

logger = logging.getLogger(__name__)

MIN_BATCH_SIZE = 10
MAX_BATCH_SIZE = 1000
MSC_SYNC_DISABLE_PARALLEL_LISTING_ENV = "MSC_SYNC_DISABLE_PARALLEL_LISTING"

# Size bucket thresholds for batching optimization
SIZE_SMALL_THRESHOLD = 1 * 1024 * 1024  # 1 MB
SIZE_MEDIUM_THRESHOLD = 64 * 1024 * 1024  # 64 MB
SIZE_LARGE_THRESHOLD = 1024 * 1024 * 1024  # 1 GB


class SizeBucket(Enum):
    """File size categories for batching optimization."""

    SMALL = "small"  # 0 - 1MB
    MEDIUM = "medium"  # 1MB - 64MB
    LARGE = "large"  # 64MB - 1GB
    VERY_LARGE = "very_large"  # > 1GB


class ProducerThread(threading.Thread):
    """
    A producer thread that compares source and target file listings to determine sync operations.

    This thread is responsible for iterating through both source and target storage locations,
    comparing their file listings, and queuing appropriate sync operations (ADD, DELETE, or STOP)
    for worker threads to process. It performs efficient merge-style iteration through sorted
    file listings to determine what files need to be synchronized.

    The thread compares files by their relative paths and metadata (content length,
    last modified time) to determine if files need to be copied, deleted, or can be skipped.

    Operations are batched together by type and file size for optimal load balancing.
    The thread will put OperationBatch objects into the file_queue, with each batch containing
    up to batch_size operations of the same type and similar size. Files are grouped into
    size buckets (small: <1MB, medium: 1-64MB, large: 64MB-1GB, very large: >1GB) to ensure
    workers receive similarly-sized workloads. Batches are flushed when the batch size is reached,
    when the operation type changes, when the size bucket changes, or when iteration completes.
    """

    def __init__(
        self,
        source_client: "AbstractStorageClient",
        source_path: str,
        target_client: "AbstractStorageClient",
        target_path: str,
        progress: ProgressBar,
        file_queue: QueueLike,
        num_workers: int,
        shutdown_event: EventLike,
        delete_unmatched_files: bool = False,
        pattern_matcher: Optional[PatternMatcher] = None,
        preserve_source_attributes: bool = False,
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
        source_files: Optional[list[str]] = None,
        ignore_hidden: bool = True,
        batch_size: int = DEFAULT_SYNC_BATCH_SIZE,
    ):
        super().__init__(daemon=True)
        if batch_size < MIN_BATCH_SIZE or batch_size > MAX_BATCH_SIZE:
            raise ValueError(f"batch_size must be between {MIN_BATCH_SIZE} and {MAX_BATCH_SIZE}, got {batch_size}")
        self.source_client = source_client
        self.target_client = target_client
        self.source_path = source_path
        self.target_path = target_path
        self.progress = progress
        self.file_queue = file_queue
        self.num_workers = num_workers
        self.shutdown_event = shutdown_event
        self.delete_unmatched_files = delete_unmatched_files
        self.pattern_matcher = pattern_matcher
        self.preserve_source_attributes = preserve_source_attributes
        self.symlink_handling = symlink_handling
        self.source_files = source_files
        self.ignore_hidden = ignore_hidden
        self.batch_size = batch_size
        self.error = None
        self.total_work_units = 0
        self._current_batch: list[tuple[ObjectMetadata, Optional[ObjectMetadata]]] = []
        self._current_batch_type: Optional[OperationType] = None
        self._current_batch_size_bucket: Optional[SizeBucket] = None

    def _get_size_bucket(self, content_length: int) -> SizeBucket:
        """Determine the size bucket for a file based on its content length."""
        if content_length <= SIZE_SMALL_THRESHOLD:
            return SizeBucket.SMALL
        elif content_length <= SIZE_MEDIUM_THRESHOLD:
            return SizeBucket.MEDIUM
        elif content_length <= SIZE_LARGE_THRESHOLD:
            return SizeBucket.LARGE
        else:
            return SizeBucket.VERY_LARGE

    def _flush_batch(self) -> None:
        """Flush the current batch to the queue."""
        if self._current_batch and self._current_batch_type is not None:
            batch = OperationBatch(operation=self._current_batch_type, items=self._current_batch)
            self.file_queue.put(batch)
            self._current_batch = []
            self._current_batch_type = None
            self._current_batch_size_bucket = None

    def _enqueue_operation(
        self,
        operation: OperationType,
        source_metadata: ObjectMetadata,
        target_metadata: Optional[ObjectMetadata] = None,
    ) -> None:
        """
        Add an operation to the current batch, flushing if necessary.

        Batches are flushed when:
        - Operation type changes (ADD vs DELETE vs SYMLINK)
        - File size bucket changes (small vs medium vs large vs very large)
        - Batch size limit is reached

        When *source_metadata* carries a ``symlink_target`` (PRESERVE mode), the
        enqueued operation is promoted to :py:attr:`OperationType.SYMLINK` and
        forced into :py:attr:`SizeBucket.SMALL`. ``make_symlink`` is an ``O(1)``
        metadata write regardless of the target file's size, so bucketing by
        ``content_length`` would misleadingly queue a symlink-to-10GB-file
        behind real byte transfers.
        """
        if operation == OperationType.ADD and source_metadata.symlink_target is not None:
            operation = OperationType.SYMLINK
            size_bucket = SizeBucket.SMALL
        else:
            size_bucket = self._get_size_bucket(source_metadata.content_length)

        if self._current_batch_type != operation or self._current_batch_size_bucket != size_bucket:
            self._flush_batch()
            self._current_batch_type = operation
            self._current_batch_size_bucket = size_bucket

        self._current_batch.append((source_metadata, target_metadata))

        if len(self._current_batch) >= self.batch_size:
            self._flush_batch()

    def _match_file_metadata(self, source_info: ObjectMetadata, target_info: ObjectMetadata) -> bool:
        # Symlink entries need a dedicated comparison because
        # ``content_length`` alone cannot distinguish two symlinks pointing
        # at different targets that happen to share the same size. Compare
        # symlink targets directly instead.
        if source_info.symlink_target is not None or target_info.symlink_target is not None:
            return source_info.symlink_target == target_info.symlink_target

        # Check file size is the same and the target's last_modified is newer than the source.
        # Compare timestamps at seconds resolution to avoid spurious mismatches from sub-second differences.
        source_sec = source_info.last_modified.replace(microsecond=0)
        target_sec = target_info.last_modified.replace(microsecond=0)

        if source_info.content_length != target_info.content_length or source_sec > target_sec:
            return False

        if self.preserve_source_attributes and getattr(self.target_client, "_metadata_provider", None):
            return (source_info.metadata or {}) == (target_info.metadata or {})

        return True

    def _is_hidden(self, path: str) -> bool:
        """Check if a path contains any hidden components (starting with dot)."""
        if not self.ignore_hidden:
            return False
        parts = path.split("/")
        return any(part.startswith(".") for part in parts)

    def _create_listing_iterator(
        self,
        client: "AbstractStorageClient",
        path: str,
        *,
        show_attributes: bool = False,
    ) -> Iterator[ObjectMetadata]:
        """Create a listing iterator, preferring recursive listing when compatible.

        Falls back to ``list`` when attributes are required or parallel listing is disabled.
        """
        if show_attributes or os.getenv(MSC_SYNC_DISABLE_PARALLEL_LISTING_ENV) is not None:
            return client.list(
                path=path,
                show_attributes=show_attributes,
                symlink_handling=self.symlink_handling,
            )

        return client.list_recursive(
            path=path,
            symlink_handling=self.symlink_handling,
        )

    def _create_source_iterator(self):
        """Create source iterator and enforce preserve_source_attributes policy."""
        if self.source_files is not None:
            for rel_file_path in self.source_files:
                rel_file_path = rel_file_path.lstrip("/")
                source_file_path = os.path.join(self.source_path, rel_file_path).lstrip("/")
                try:
                    source_metadata = self.source_client.info(
                        source_file_path, strict=False
                    )  # don't check if the path is a directory
                    if not self.preserve_source_attributes:
                        source_metadata.metadata = None
                    yield source_metadata
                except FileNotFoundError:
                    logger.warning(f"File in source_files not found at source: {source_file_path}")
                    continue
            return

        for source_metadata in self._create_listing_iterator(
            self.source_client,
            self.source_path,
            show_attributes=self.preserve_source_attributes,
        ):
            if not self.preserve_source_attributes:
                source_metadata.metadata = None
            yield source_metadata

    def run(self):
        try:
            source_iter = iter(self._create_source_iterator())

            target_show_attributes = bool(
                self.preserve_source_attributes and getattr(self.target_client, "_metadata_provider", None)
            )
            target_iter = iter(
                self._create_listing_iterator(
                    self.target_client,
                    self.target_path,
                    show_attributes=target_show_attributes,
                )
            )

            source_file = next(source_iter, None)
            target_file = next(target_iter, None)

            while source_file or target_file:
                if self.shutdown_event.is_set():
                    logger.info("ProducerThread: Shutdown event detected, stopping file enumeration")
                    break

                # Update progress and count each pair (or single) considered for syncing
                self.progress.update_total(self.total_work_units)

                if source_file and target_file:
                    source_key = source_file.key[len(self.source_path) :].lstrip("/")
                    target_key = target_file.key[len(self.target_path) :].lstrip("/")

                    # Skip hidden files and directories
                    if self._is_hidden(source_key):
                        source_file = next(source_iter, None)
                        continue

                    if self._is_hidden(target_key):
                        target_file = next(target_iter, None)
                        continue

                    if source_key < target_key:
                        # Check if file should be included based on patterns
                        if not self.pattern_matcher or self.pattern_matcher.should_include_file(source_key):
                            self._enqueue_operation(OperationType.ADD, source_file)
                            self.total_work_units += 1
                        source_file = next(source_iter, None)
                    elif source_key > target_key:
                        if self.delete_unmatched_files:
                            self._enqueue_operation(OperationType.DELETE, target_file)
                            self.total_work_units += 1
                        target_file = next(target_iter, None)  # Skip unmatched target file
                    else:
                        # Both exist, compare metadata
                        if not self._match_file_metadata(source_file, target_file):
                            # Check if file should be included based on patterns
                            if not self.pattern_matcher or self.pattern_matcher.should_include_file(source_key):
                                self._enqueue_operation(OperationType.ADD, source_file, target_file)
                        else:
                            self.progress.update_progress()

                        source_file = next(source_iter, None)
                        target_file = next(target_iter, None)
                        self.total_work_units += 1
                elif source_file:
                    source_key = source_file.key[len(self.source_path) :].lstrip("/")

                    # Skip hidden files and directories
                    if self._is_hidden(source_key):
                        source_file = next(source_iter, None)
                        continue

                    # Check if file should be included based on patterns
                    if not self.pattern_matcher or self.pattern_matcher.should_include_file(source_key):
                        self._enqueue_operation(OperationType.ADD, source_file)
                        self.total_work_units += 1
                    source_file = next(source_iter, None)
                elif target_file:
                    target_key = target_file.key[len(self.target_path) :].lstrip("/")

                    # Skip hidden files and directories
                    if self._is_hidden(target_key):
                        target_file = next(target_iter, None)
                        continue

                    if self.delete_unmatched_files:
                        self._enqueue_operation(OperationType.DELETE, target_file)
                        self.total_work_units += 1
                    target_file = next(target_iter, None)

            self.progress.update_total(self.total_work_units)
        except Exception as e:
            self.error = e
        finally:
            self._flush_batch()
            for _ in range(self.num_workers):
                self.file_queue.put(OperationBatch(operation=OperationType.STOP, items=[]))
