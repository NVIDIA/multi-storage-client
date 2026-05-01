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

import contextlib
import json
import logging
import os
import shutil
import tempfile
import threading
import traceback
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

import xattr

from ..types import ObjectMetadata
from ..utils import safe_makedirs
from .metadata_proxy import QueueBackedMetadataProvider
from .types import ErrorInfo, EventLike, OperationBatch, OperationType, QueueLike

if TYPE_CHECKING:
    from ..client.types import AbstractStorageClient

logger = logging.getLogger(__name__)

METADATA_LOOKUP_MIN_CONTENT_LENGTH = 16 * 1024 * 1024

# ---------------------------------------------------------------------------
# Public utility functions
# ---------------------------------------------------------------------------


def build_target_file_path(source_path: str, target_path: str, file_metadata: ObjectMetadata) -> str:
    source_key = file_metadata.key[len(source_path) :].lstrip("/")

    # Special case for single file sync: target file path is the target path + the source key.
    target_file_path = (
        os.path.join(target_path, source_key)
        if source_key
        else os.path.join(target_path, os.path.basename(file_metadata.key))
    )

    return target_file_path


def check_skip_and_track_with_metadata_provider(
    target_client: "AbstractStorageClient",
    target_file_path: str,
    file_metadata: ObjectMetadata,
    preserve_source_attributes: bool,
) -> bool:
    """Check if target is already up-to-date when a metadata provider is present.

    Resolves the physical path via the metadata provider, fetches target metadata
    from the storage provider, and tracks the file if up-to-date.

    Returns True (skip) if up-to-date, False if transfer is needed.
    Raises FileExistsError if the file needs updating but overwrites are disallowed.
    """
    if not target_client._storage_provider:
        raise RuntimeError("Invalid state, no storage provider configured.")

    if not target_client._metadata_provider:
        raise RuntimeError("Invalid state, no metadata provider configured.")

    # Small files are not worth the I/O of a metadata lookup
    if (
        file_metadata.content_length <= METADATA_LOOKUP_MIN_CONTENT_LENGTH
        and target_client._metadata_provider.allow_overwrites()
    ):
        return False

    target_metadata = None
    try:
        resolved = target_client._metadata_provider.realpath(target_file_path)
        if resolved.exists:
            physical_path = resolved.physical_path
        else:
            physical_path = target_client._metadata_provider.generate_physical_path(
                target_file_path, for_overwrite=False
            ).physical_path

        target_metadata = target_client._storage_provider.get_object_metadata(physical_path, strict=False)
    except FileNotFoundError:
        pass

    if target_metadata is None:
        return False

    if (
        target_metadata.content_length == file_metadata.content_length
        and target_metadata.last_modified >= file_metadata.last_modified
    ):
        logger.debug(f"File {target_file_path} already exists and is up-to-date, skipping copy")

        source_attrs = (
            (dict(file_metadata.metadata) if file_metadata.metadata else None) if preserve_source_attributes else None
        )
        metadata_for_tracking = (
            target_metadata.replace(metadata=source_attrs) if preserve_source_attributes else target_metadata
        )
        logger.debug(f"Adding existing file {target_file_path} to metadata provider for tracking")
        with target_client._metadata_provider_lock or contextlib.nullcontext():
            target_client._metadata_provider.add_file(target_file_path, metadata_for_tracking)

        return True

    if not target_client._metadata_provider.allow_overwrites():
        raise FileExistsError(
            f"Cannot sync '{file_metadata.key}' to '{target_file_path}': "
            f"file exists and needs updating, but overwrites are not allowed. "
            f"Enable overwrites in metadata provider configuration or remove the existing file."
        )

    return False


def update_posix_metadata(
    target_client: "AbstractStorageClient",
    target_physical_path: str,
    target_file_path: str,
    file_metadata: ObjectMetadata,
) -> None:
    """Update metadata for POSIX target (metadata provider or xattr)."""
    if target_client._metadata_provider:
        metadata_copy = dict(file_metadata.metadata) if file_metadata.metadata else None
        physical_metadata = ObjectMetadata(
            key=target_file_path,
            content_length=os.path.getsize(target_physical_path),
            last_modified=datetime.fromtimestamp(os.path.getmtime(target_physical_path), tz=timezone.utc),
            metadata=metadata_copy,
        )
        with target_client._metadata_provider_lock or contextlib.nullcontext():
            target_client._metadata_provider.add_file(target_file_path, physical_metadata)
    else:
        if file_metadata.metadata:
            # Update metadata for POSIX target (xattr).
            try:
                xattr.setxattr(
                    target_physical_path,
                    "user.json",
                    json.dumps(file_metadata.metadata).encode("utf-8"),
                )
            except OSError as e:
                logger.debug(f"Failed to set extended attributes on {target_physical_path}: {e}")

        # Update (atime, mtime) for POSIX target.
        try:
            last_modified = file_metadata.last_modified.timestamp()
            os.utime(target_physical_path, (last_modified, last_modified))
        except OSError as e:
            logger.debug(f"Failed to update (atime, mtime) on {target_physical_path}: {e}")


# ---------------------------------------------------------------------------
# BatchSyncHandler class hierarchy
# ---------------------------------------------------------------------------


class BatchSyncHandler(ABC):
    """Abstract base for batch sync operations between source and target storage.

    Subclasses implement ``_execute_batch_transfer`` for the specific
    source/target combination (POSIX-to-POSIX, POSIX-to-remote, etc.).
    The base class provides the common filtering, delete handling, and
    error/result reporting logic.
    """

    def __init__(
        self,
        source_client: "AbstractStorageClient",
        source_path: str,
        target_client: "AbstractStorageClient",
        target_path: str,
        preserve_source_attributes: bool,
        result_queue: QueueLike,
        error_queue: QueueLike,
    ):
        self.source_client = source_client
        self.source_path = source_path
        self.target_client = target_client
        self.target_path = target_path
        self.preserve_source_attributes = preserve_source_attributes
        self.result_queue = result_queue
        self.error_queue = error_queue

    def process_add_batch(
        self,
        worker_id: str,
        batch: OperationBatch,
    ) -> None:
        """Process an ADD batch: filter items via skip/track, then transfer."""
        transfer_items: list[tuple[ObjectMetadata, str]] = []

        if self.target_client._metadata_provider:
            # TODO: Avoid the HEAD-before-PUT by using conditional PUT.
            transfer_items = self._filter_with_metadata_provider(worker_id, batch)
        else:
            for file_metadata, _ in batch.items:
                target_file_path = build_target_file_path(self.source_path, self.target_path, file_metadata)
                transfer_items.append((file_metadata, target_file_path))

        if not transfer_items:
            return

        try:
            self._execute_batch_transfer(worker_id, transfer_items)
            for file_metadata, target_file_path in transfer_items:
                self._report_result(file_metadata, target_file_path)
        except Exception as e:
            self._report_error(worker_id, e, transfer_items[0][0].key, batch.operation)

    def _filter_with_metadata_provider(
        self,
        worker_id: str,
        batch: OperationBatch,
    ) -> list[tuple[ObjectMetadata, str]]:
        """Check items against the metadata provider concurrently.

        Each item performs I/O (realpath, get_object_metadata) that benefits
        from parallelism. Thread safety is guaranteed by the existing
        ``_metadata_provider_lock`` around ``add_file`` calls.
        """
        transfer_items: list[tuple[ObjectMetadata, str]] = []

        def _check_item(
            file_metadata: ObjectMetadata,
        ) -> tuple[ObjectMetadata, str, bool]:
            target_file_path = build_target_file_path(self.source_path, self.target_path, file_metadata)
            skip = check_skip_and_track_with_metadata_provider(
                self.target_client, target_file_path, file_metadata, self.preserve_source_attributes
            )
            return file_metadata, target_file_path, skip

        max_workers = min(len(batch.items), 16)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_metadata = {
                executor.submit(_check_item, file_metadata): file_metadata
                for file_metadata, _target_metadata in batch.items
            }
            for future in as_completed(future_to_metadata):
                file_metadata = future_to_metadata[future]
                try:
                    fm, target_file_path, skip = future.result()
                    if not skip:
                        transfer_items.append((fm, target_file_path))
                except Exception as e:
                    self._report_error(worker_id, e, file_metadata.key, batch.operation)

        return transfer_items

    def process_delete_batch(self, batch: OperationBatch) -> None:
        """Process a DELETE batch."""
        self.target_client.delete_many([file_metadata.key for file_metadata, _ in batch.items])
        for file_metadata, _ in batch.items:
            target_file_path = build_target_file_path(self.source_path, self.target_path, file_metadata)
            self.result_queue.put((OperationType.DELETE, target_file_path, file_metadata))

    def process_symlink_batch(self, worker_id: str, batch: OperationBatch) -> None:
        """Process a SYMLINK batch: recreate each symlink on the target via ``make_symlink``.

        No bytes are transferred. The parent-relative ``symlink_target`` is
        preserved verbatim — it is resolved against the *target-side* symlink
        key only to obtain the logical key ``make_symlink`` expects; each
        backend re-encodes it back to the same parent-relative bytes. Results
        are reported as :py:attr:`OperationType.ADD` so the
        :class:`ResultMonitorThread` counts them alongside regular file adds.
        """
        for file_metadata, _target_metadata in batch.items:
            try:
                if file_metadata.symlink_target is None:
                    raise RuntimeError(f"SYMLINK batch item has no symlink_target: {file_metadata.key}")

                target_file_path = build_target_file_path(self.source_path, self.target_path, file_metadata)
                translated_target = ObjectMetadata.resolve_symlink_target(
                    target_file_path, file_metadata.symlink_target
                )

                self.target_client.make_symlink(target_file_path, translated_target)

                if not self.target_client._metadata_provider:
                    physical_metadata = ObjectMetadata(
                        key=target_file_path,
                        content_length=0,
                        last_modified=file_metadata.last_modified,
                        type=file_metadata.type,
                        metadata=file_metadata.metadata,
                        symlink_target=file_metadata.symlink_target,
                    )
                    self.result_queue.put((OperationType.ADD, target_file_path, physical_metadata))
            except Exception as e:
                self._report_error(worker_id, e, file_metadata.key, batch.operation)

    def _report_result(self, file_metadata: ObjectMetadata, target_file_path: str) -> None:
        """Report a successful ADD to result_queue when there is no metadata provider.

        When a metadata provider is present the ``QueueBackedMetadataProvider``
        (or ``update_posix_metadata``) already enqueues results.
        """
        if not self.target_client._metadata_provider:
            physical_metadata = ObjectMetadata(
                key=target_file_path,
                content_length=file_metadata.content_length,
                last_modified=file_metadata.last_modified,
                metadata=file_metadata.metadata,
            )
            self.result_queue.put((OperationType.ADD, target_file_path, physical_metadata))

    def _report_error(
        self,
        worker_id: str,
        exception: Exception,
        file_key: Optional[str],
        operation: OperationType,
    ) -> None:
        """Report an error to the error queue."""
        if self.error_queue:
            error_info = ErrorInfo(
                worker_id=worker_id,
                exception_type=type(exception).__name__,
                exception_message=str(exception),
                traceback_str=traceback.format_exc(),
                file_key=file_key,
                operation=operation.value if operation else "unknown",
            )
            self.error_queue.put(error_info)
        else:
            logger.error(
                f"Worker {worker_id}: Exception during {operation} on {file_key}: {exception}\n{traceback.format_exc()}"
            )
            raise

    @abstractmethod
    def _execute_batch_transfer(
        self,
        worker_id: str,
        transfer_items: list[tuple[ObjectMetadata, str]],
    ) -> None:
        """Execute the batch transfer for items that passed filtering."""
        pass


class PosixToPosixHandler(BatchSyncHandler):
    """Handles sync from POSIX source to POSIX target using shutil.copy2.

    Individual copies are sequential within a batch; the worker-level thread
    pool provides concurrency across batches.
    """

    @staticmethod
    def _copy_to_posix_target(source_physical_path: str, target_physical_path: str) -> None:
        safe_makedirs(os.path.dirname(target_physical_path))
        shutil.copy2(source_physical_path, target_physical_path)

    def _execute_batch_transfer(
        self,
        worker_id: str,
        transfer_items: list[tuple[ObjectMetadata, str]],
    ) -> None:
        for file_metadata, target_file_path in transfer_items:
            source_physical_path = self.source_client.get_posix_path(file_metadata.key)
            target_physical_path = self.target_client.get_posix_path(target_file_path)
            assert source_physical_path is not None
            assert target_physical_path is not None
            self._copy_to_posix_target(source_physical_path, target_physical_path)
            update_posix_metadata(self.target_client, target_physical_path, target_file_path, file_metadata)


class PosixToRemoteHandler(BatchSyncHandler):
    """Handles sync from POSIX source to remote target using the upload_files batch API."""

    def _execute_batch_transfer(
        self,
        worker_id: str,
        transfer_items: list[tuple[ObjectMetadata, str]],
    ) -> None:
        source_local_paths: list[str] = []
        target_remote_paths: list[str] = []
        attributes: list[Optional[dict[str, Any]]] = []

        for file_metadata, target_file_path in transfer_items:
            source_physical_path = self.source_client.get_posix_path(file_metadata.key)
            assert source_physical_path is not None
            source_local_paths.append(source_physical_path)
            target_remote_paths.append(target_file_path)
            attributes.append(file_metadata.metadata)

        self.target_client.upload_files(target_remote_paths, source_local_paths, attributes)


class RemoteToPosixHandler(BatchSyncHandler):
    """Handles sync from remote source to POSIX target using the download_files batch API."""

    def _execute_batch_transfer(
        self,
        worker_id: str,
        transfer_items: list[tuple[ObjectMetadata, str]],
    ) -> None:
        source_remote_paths: list[str] = []
        target_local_paths: list[str] = []
        source_metadata: list[ObjectMetadata] = []
        items_with_physical: list[tuple[ObjectMetadata, str, str]] = []

        for file_metadata, target_file_path in transfer_items:
            target_physical_path = self.target_client.get_posix_path(target_file_path)
            assert target_physical_path is not None
            safe_makedirs(os.path.dirname(target_physical_path))
            source_remote_paths.append(file_metadata.key)
            target_local_paths.append(target_physical_path)
            source_metadata.append(file_metadata)
            items_with_physical.append((file_metadata, target_file_path, target_physical_path))

        self.source_client.download_files(source_remote_paths, target_local_paths, source_metadata)

        for file_metadata, target_file_path, target_physical_path in items_with_physical:
            update_posix_metadata(self.target_client, target_physical_path, target_file_path, file_metadata)


class RemoteToRemoteHandler(BatchSyncHandler):
    """Handles sync between two remote storages using download_files + upload_files."""

    def _execute_batch_transfer(
        self,
        worker_id: str,
        transfer_items: list[tuple[ObjectMetadata, str]],
    ) -> None:
        temp_dir = tempfile.mkdtemp()
        try:
            source_remote_paths: list[str] = []
            target_remote_paths: list[str] = []
            temp_local_paths: list[str] = []
            source_metadata: list[ObjectMetadata] = []
            attributes: list[Optional[dict[str, Any]]] = []

            for i, (file_metadata, target_file_path) in enumerate(transfer_items):
                source_remote_paths.append(file_metadata.key)
                target_remote_paths.append(target_file_path)
                temp_local_paths.append(os.path.join(temp_dir, str(i)))
                source_metadata.append(file_metadata)
                attributes.append(file_metadata.metadata)

            self.source_client.download_files(source_remote_paths, temp_local_paths, source_metadata)
            self.target_client.upload_files(target_remote_paths, temp_local_paths, attributes)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_sync_handler(
    source_client: "AbstractStorageClient",
    source_path: str,
    target_client: "AbstractStorageClient",
    target_path: str,
    preserve_source_attributes: bool,
    result_queue: QueueLike,
    error_queue: QueueLike,
) -> BatchSyncHandler:
    """Create the appropriate sync handler based on source/target storage types."""
    source_is_posix = source_client._is_posix_file_storage_provider()
    target_is_posix = target_client._is_posix_file_storage_provider()

    handler_class: type[BatchSyncHandler]
    if source_is_posix and target_is_posix:
        handler_class = PosixToPosixHandler
    elif source_is_posix:
        handler_class = PosixToRemoteHandler
    elif target_is_posix:
        handler_class = RemoteToPosixHandler
    else:
        handler_class = RemoteToRemoteHandler

    return handler_class(
        source_client,
        source_path,
        target_client,
        target_path,
        preserve_source_attributes,
        result_queue,
        error_queue,
    )


# ---------------------------------------------------------------------------
# Worker process entry point
# ---------------------------------------------------------------------------


def sync_worker_process(
    source_client: "AbstractStorageClient",
    source_path: str,
    target_client: "AbstractStorageClient",
    target_path: str,
    num_worker_threads: int,
    preserve_source_attributes: bool,
    file_queue: QueueLike,
    result_queue: QueueLike,
    error_queue: QueueLike,
    shutdown_event: EventLike,
):
    """Worker process that handles file synchronization operations.

    Consumes ``OperationBatch`` items from *file_queue* and delegates to the
    appropriate :class:`BatchSyncHandler` based on source/target storage types.
    The handler selection (POSIX-to-POSIX, POSIX-to-remote, remote-to-POSIX,
    remote-to-remote) is done once at startup, and batch APIs
    (``download_files``, ``upload_files``) are used for efficient bulk
    transfers.

    Worker threads pull directly from the shared queue for minimal dispatch
    overhead. A STOP sentinel is re-enqueued so sibling threads can also exit.
    """
    worker_id = f"process-{os.getpid()}"

    original_metadata_provider = getattr(target_client, "_metadata_provider", None)
    if original_metadata_provider:
        target_client._metadata_provider = QueueBackedMetadataProvider(original_metadata_provider, result_queue)

    try:
        handler = create_sync_handler(
            source_client,
            source_path,
            target_client,
            target_path,
            preserve_source_attributes,
            result_queue,
            error_queue,
        )

        def _worker_loop(thread_id: str) -> None:
            while not shutdown_event.is_set():
                batch = file_queue.get()

                if batch.operation == OperationType.STOP:
                    file_queue.put(batch)
                    break

                if batch.operation == OperationType.DELETE:
                    handler.process_delete_batch(batch)
                elif batch.operation == OperationType.ADD:
                    handler.process_add_batch(thread_id, batch)
                elif batch.operation == OperationType.SYMLINK:
                    handler.process_symlink_batch(thread_id, batch)

        if num_worker_threads <= 1:
            _worker_loop(worker_id)
        else:
            threads: list[threading.Thread] = []
            for i in range(num_worker_threads):
                t = threading.Thread(target=_worker_loop, args=(f"{worker_id}-thread-{i}",), daemon=True)
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
    finally:
        if original_metadata_provider:
            target_client._metadata_provider = original_metadata_provider
