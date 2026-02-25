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

import concurrent.futures
import contextlib
import json
import logging
import os
import shutil
import tempfile
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Union, cast

import xattr
from filelock import FileLock

from ..constants import MEMORY_LOAD_LIMIT
from ..providers.base import BaseStorageProvider
from ..types import ObjectMetadata
from ..utils import safe_makedirs
from .metadata_proxy import QueueBackedMetadataProvider
from .types import ErrorInfo, EventLike, OperationType, QueueLike

if TYPE_CHECKING:
    from ..client.types import AbstractStorageClient

logger = logging.getLogger(__name__)

DEFAULT_LOCK_TIMEOUT = 600  # 10 minutes
FILE_LOCK_SIZE_THRESHOLD = 64 * 1024 * 1024  # 64 MB - only lock files larger than this


def _build_target_file_path(source_path: str, target_path: str, file_metadata: ObjectMetadata) -> str:
    source_key = file_metadata.key[len(source_path) :].lstrip("/")

    # Special case for single file sync: target file path is the target path + the source key.
    target_file_path = (
        os.path.join(target_path, source_key)
        if source_key
        else os.path.join(target_path, os.path.basename(file_metadata.key))
    )

    return target_file_path


def _process_single_sync_operation(
    worker_id: str,
    source_client: "AbstractStorageClient",
    source_path: str,
    target_client: "AbstractStorageClient",
    target_path: str,
    file_metadata: ObjectMetadata,
    op: OperationType,
    result_queue: QueueLike,
    error_queue: QueueLike,
) -> None:
    """Process a single sync operation (ADD or DELETE) for one file."""
    target_file_path = _build_target_file_path(source_path, target_path, file_metadata)

    try:
        if op == OperationType.ADD:
            logger.debug(f"sync {file_metadata.key} -> {target_file_path}")
            with _create_exclusive_filelock(target_client, target_file_path, file_metadata.content_length):
                target_metadata = None
                if not target_client._storage_provider:
                    raise RuntimeError("Invalid state, no storage provider configured.")

                try:
                    if target_client._metadata_provider:
                        resolved = target_client._metadata_provider.realpath(target_file_path)
                        if resolved.exists:
                            physical_path = resolved.physical_path
                        else:
                            physical_path = target_client._metadata_provider.generate_physical_path(
                                target_file_path, for_overwrite=False
                            ).physical_path
                    else:
                        physical_path = target_file_path

                    target_metadata = target_client._storage_provider.get_object_metadata(physical_path, strict=False)
                except FileNotFoundError:
                    pass

                if target_metadata is not None:
                    if (
                        target_metadata.content_length == file_metadata.content_length
                        and target_metadata.last_modified >= file_metadata.last_modified
                    ):
                        logger.debug(f"File {target_file_path} already exists and is up-to-date, skipping copy")

                        if target_client._metadata_provider:
                            logger.debug(f"Adding existing file {target_file_path} to metadata provider for tracking")
                            with target_client._metadata_provider_lock or contextlib.nullcontext():
                                target_client._metadata_provider.add_file(target_file_path, target_metadata)

                        return

                    if target_client._metadata_provider and not target_client._metadata_provider.allow_overwrites():
                        raise FileExistsError(
                            f"Cannot sync '{file_metadata.key}' to '{target_file_path}': "
                            f"file exists and needs updating, but overwrites are not allowed. "
                            f"Enable overwrites in metadata provider configuration or remove the existing file."
                        )

                source_physical_path, target_physical_path = _check_posix_paths(
                    source_client, target_client, file_metadata.key, target_file_path
                )

                source_is_posix = source_physical_path is not None
                target_is_posix = target_physical_path is not None

                if source_is_posix and target_is_posix:
                    _copy_posix_to_posix(source_physical_path, target_physical_path)
                    _update_posix_metadata(target_client, target_physical_path, target_file_path, file_metadata)
                elif source_is_posix and not target_is_posix:
                    _copy_posix_to_remote(target_client, source_physical_path, target_file_path, file_metadata)
                elif not source_is_posix and target_is_posix:
                    _copy_remote_to_posix(source_client, file_metadata.key, target_physical_path)
                    _update_posix_metadata(target_client, target_physical_path, target_file_path, file_metadata)
                else:
                    _copy_remote_to_remote(
                        source_client, target_client, file_metadata.key, target_file_path, file_metadata
                    )

            if (
                target_client._is_posix_file_storage_provider()
                and file_metadata.content_length >= FILE_LOCK_SIZE_THRESHOLD
            ):
                try:
                    target_lock_file_path = os.path.join(
                        os.path.dirname(target_file_path), f".{os.path.basename(target_file_path)}.lock"
                    )
                    lock_path = cast(BaseStorageProvider, target_client._storage_provider)._prepend_base_path(
                        target_lock_file_path
                    )
                    os.remove(lock_path)
                except OSError:
                    pass

            if not target_client._metadata_provider:
                physical_metadata = ObjectMetadata(
                    key=target_file_path,
                    content_length=file_metadata.content_length,
                    last_modified=file_metadata.last_modified,
                    metadata=file_metadata.metadata,
                )
                result_queue.put((op, target_file_path, physical_metadata))
        else:
            raise ValueError(f"Unknown operation: {op}")
    except Exception as e:
        if error_queue:
            error_info = ErrorInfo(
                worker_id=worker_id,
                exception_type=type(e).__name__,
                exception_message=str(e),
                traceback_str=traceback.format_exc(),
                file_key=file_metadata.key if file_metadata else None,
                operation=op.value if op else "unknown",
            )
            error_queue.put(error_info)
        else:
            logger.error(
                f"Worker {worker_id}: Exception during {op} on {file_metadata.key}: {e}\n{traceback.format_exc()}"
            )
            raise


def _sync_worker_process(
    source_client: "AbstractStorageClient",
    source_path: str,
    target_client: "AbstractStorageClient",
    target_path: str,
    num_worker_threads: int,
    file_queue: QueueLike,
    result_queue: QueueLike,
    error_queue: QueueLike,
    shutdown_event: EventLike,
):
    """
    Worker process that handles file synchronization operations using a thread pool.

    This function is designed to run in a separate process as part of a multiprocessing
    sync operation. It creates a fixed-size thread pool and consumes batches of sync
    operations from the file_queue. Each individual operation (ADD or DELETE) from a
    batch is submitted to the thread pool for concurrent execution.

    The thread pool size is bounded by num_worker_threads, ensuring controlled
    parallelism. Backpressure is applied by limiting pending futures to twice the
    number of worker threads, preventing unbounded queue growth while keeping all
    threads saturated with minimal idle time.

    Exceptions that occur during file operations are caught, packaged as ErrorInfo
    objects, and sent to the error_queue for centralized error handling. The shutdown_event
    is checked periodically to enable graceful shutdown when errors occur elsewhere.
    """
    worker_id = f"process-{os.getpid()}"
    max_pending_futures = num_worker_threads * 2

    original_metadata_provider = getattr(target_client, "_metadata_provider", None)
    if original_metadata_provider:
        target_client._metadata_provider = QueueBackedMetadataProvider(original_metadata_provider, result_queue)

    try:
        _sync_worker_loop(
            worker_id,
            max_pending_futures,
            source_client,
            source_path,
            target_client,
            target_path,
            num_worker_threads,
            file_queue,
            result_queue,
            error_queue,
            shutdown_event,
        )
    finally:
        if original_metadata_provider:
            target_client._metadata_provider = original_metadata_provider


def _sync_worker_loop(
    worker_id: str,
    max_pending_futures: int,
    source_client: "AbstractStorageClient",
    source_path: str,
    target_client: "AbstractStorageClient",
    target_path: str,
    num_worker_threads: int,
    file_queue: QueueLike,
    result_queue: QueueLike,
    error_queue: QueueLike,
    shutdown_event: EventLike,
):
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_worker_threads) as executor:
        futures: list[concurrent.futures.Future] = []

        while not shutdown_event.is_set():
            batch = file_queue.get()

            if batch.operation == OperationType.STOP:
                break

            if batch.operation == OperationType.DELETE:
                target_client.delete_many([file_metadata.key for file_metadata in batch.items])
                for file_metadata in batch.items:
                    target_file_path = _build_target_file_path(source_path, target_path, file_metadata)
                    result_queue.put((OperationType.DELETE, target_file_path, file_metadata))

            if batch.operation == OperationType.ADD:
                for file_metadata in batch.items:
                    if shutdown_event.is_set():
                        logger.debug(f"Worker {worker_id}: Shutdown event detected during batch processing, exiting")
                        break

                    # Backpressure: wait if at capacity before submitting new work
                    while len(futures) >= max_pending_futures:
                        _, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        futures = list(not_done)

                    future = executor.submit(
                        _process_single_sync_operation,
                        worker_id,
                        source_client,
                        source_path,
                        target_client,
                        target_path,
                        file_metadata,
                        batch.operation,
                        result_queue,
                        error_queue,
                    )
                    futures.append(future)
        else:
            logger.debug(f"Worker {worker_id}: Shutdown event detected, exiting")


def _create_exclusive_filelock(
    target_client: "AbstractStorageClient",
    target_file_path: str,
    file_size: int,
) -> Union[FileLock, contextlib.AbstractContextManager]:
    """Create an exclusive file lock for large files on POSIX file storage providers.

    Acquires exclusive lock to prevent race conditions when multiple worker processes attempt concurrent
    writes to the same target location on shared filesystems. This can occur when users run multiple sync
    operations targeting the same filesystem location simultaneously.

    Uses size-based selective locking to balance performance and safety:
    - Small files (< 64MB): No lock - minimizes overhead for high-volume operations
    - Large files (≥ 64MB): Exclusive lock - prevents wasteful concurrent writes

    This approach avoids distributed lock overhead on parallel filesystems like Lustre
    when syncing millions of small files, while still protecting expensive large file transfers.
    """
    if target_client._is_posix_file_storage_provider() and file_size >= FILE_LOCK_SIZE_THRESHOLD:
        target_lock_file_path = os.path.join(
            os.path.dirname(target_file_path), f".{os.path.basename(target_file_path)}.lock"
        )
        lock_path = cast(BaseStorageProvider, target_client._storage_provider)._prepend_base_path(target_lock_file_path)
        safe_makedirs(os.path.dirname(lock_path))
        return FileLock(lock_path, timeout=DEFAULT_LOCK_TIMEOUT)
    else:
        return contextlib.nullcontext()


def _check_posix_paths(
    source_client: "AbstractStorageClient",
    target_client: "AbstractStorageClient",
    source_key: str,
    target_key: str,
) -> tuple[Optional[str], Optional[str]]:
    """Check if source and target are POSIX paths and return physical paths if available."""
    source_physical_path = source_client.get_posix_path(source_key)
    target_physical_path = target_client.get_posix_path(target_key)
    return source_physical_path, target_physical_path


def _copy_posix_to_posix(
    source_physical_path: str,
    target_physical_path: str,
) -> None:
    """Copy file from POSIX source to POSIX target using shutil.copy2."""
    safe_makedirs(os.path.dirname(target_physical_path))
    shutil.copy2(source_physical_path, target_physical_path)


def _copy_posix_to_remote(
    target_client: "AbstractStorageClient",
    source_physical_path: str,
    target_file_path: str,
    file_metadata: ObjectMetadata,
) -> None:
    """Upload file from POSIX source to remote target."""
    target_client.upload_file(
        remote_path=target_file_path,
        local_path=source_physical_path,
        attributes=file_metadata.metadata,
    )


def _copy_remote_to_posix(
    source_client: "AbstractStorageClient",
    source_key: str,
    target_physical_path: str,
) -> None:
    """Download file from remote source to POSIX target."""
    source_client.download_file(remote_path=source_key, local_path=target_physical_path)


def _copy_remote_to_remote(
    source_client: "AbstractStorageClient",
    target_client: "AbstractStorageClient",
    source_key: str,
    target_file_path: str,
    file_metadata: ObjectMetadata,
) -> None:
    """Copy file between two remote storages, using memory or temp file based on size."""
    if file_metadata.content_length < MEMORY_LOAD_LIMIT:
        file_content = source_client.read(source_key)
        target_client.write(target_file_path, file_content, attributes=file_metadata.metadata)
    else:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_filename = temp_file.name
        try:
            source_client.download_file(remote_path=source_key, local_path=temp_filename)
            target_client.upload_file(
                remote_path=target_file_path,
                local_path=temp_filename,
                attributes=file_metadata.metadata,
            )
        finally:
            os.remove(temp_filename)


def _update_posix_metadata(
    target_client: "AbstractStorageClient",
    target_physical_path: str,
    target_file_path: str,
    file_metadata: ObjectMetadata,
) -> None:
    """Update metadata for POSIX target (metadata provider or xattr)."""
    if target_client._metadata_provider:
        physical_metadata = ObjectMetadata(
            key=target_file_path,
            content_length=os.path.getsize(target_physical_path),
            last_modified=datetime.fromtimestamp(os.path.getmtime(target_physical_path), tz=timezone.utc),
            metadata=file_metadata.metadata,
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
