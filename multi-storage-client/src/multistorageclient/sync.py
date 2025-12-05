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
import importlib.util
import json
import logging
import multiprocessing
import os
import queue
import shutil
import tempfile
import threading
import time
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional, cast

import xattr
from filelock import FileLock

from multistorageclient.providers.base import BaseStorageProvider

from .constants import MEMORY_LOAD_LIMIT
from .progress_bar import ProgressBar
from .types import ExecutionMode, ObjectMetadata
from .utils import PatternMatcher, calculate_worker_processes_and_threads

logger = logging.getLogger(__name__)


def is_ray_available():
    return importlib.util.find_spec("ray") is not None


PLACEMENT_GROUP_STRATEGY = "SPREAD"
PLACEMENT_GROUP_TIMEOUT_SECONDS = 60  # Timeout for placement group creation
DEFAULT_LOCK_TIMEOUT = 600  # 10 minutes

HAVE_RAY = is_ray_available()

if TYPE_CHECKING:
    from .client.types import AbstractStorageClient

_Queue = Any  # queue.Queue | multiprocessing.Queue | SharedQueue


class _SyncOp(Enum):
    """Enumeration of sync operations that can be performed on files.

    This enum defines the different types of operations that can be queued
    during a synchronization process between source and target storage locations.
    """

    ADD = "add"
    DELETE = "delete"
    STOP = "stop"  # Signal to stop the thread.


class ProducerThread(threading.Thread):
    """
    A producer thread that compares source and target file listings to determine sync operations.

    This thread is responsible for iterating through both source and target storage locations,
    comparing their file listings, and queuing appropriate sync operations (ADD, DELETE, or STOP)
    for worker threads to process. It performs efficient merge-style iteration through sorted
    file listings to determine what files need to be synchronized.

    The thread compares files by their relative paths and metadata (content length,
    last modified time) to determine if files need to be copied, deleted, or can be skipped.

    The thread will put tuples of (_SyncOp, ObjectMetadata) into the file_queue.
    """

    def __init__(
        self,
        source_client: "AbstractStorageClient",
        source_path: str,
        target_client: "AbstractStorageClient",
        target_path: str,
        progress: ProgressBar,
        file_queue: _Queue,
        num_workers: int,
        delete_unmatched_files: bool = False,
        pattern_matcher: Optional[PatternMatcher] = None,
        preserve_source_attributes: bool = False,
        follow_symlinks: bool = True,
        source_files: Optional[list[str]] = None,
        ignore_hidden: bool = True,
    ):
        super().__init__(daemon=True)
        self.source_client = source_client
        self.target_client = target_client
        self.source_path = source_path
        self.target_path = target_path
        self.progress = progress
        self.file_queue = file_queue
        self.num_workers = num_workers
        self.delete_unmatched_files = delete_unmatched_files
        self.pattern_matcher = pattern_matcher
        self.preserve_source_attributes = preserve_source_attributes
        self.follow_symlinks = follow_symlinks
        self.source_files = source_files
        self.ignore_hidden = ignore_hidden
        self.error = None

    def _match_file_metadata(self, source_info: ObjectMetadata, target_info: ObjectMetadata) -> bool:
        # Check file size is the same and the target's last_modified is newer than the source.
        return (
            source_info.content_length == target_info.content_length
            and source_info.last_modified <= target_info.last_modified
        )

    def _is_hidden(self, path: str) -> bool:
        """Check if a path contains any hidden components (starting with dot)."""
        if not self.ignore_hidden:
            return False
        parts = path.split("/")
        return any(part.startswith(".") for part in parts)

    def _create_source_files_iterator(self):
        """Create an iterator from source_files that yields ObjectMetadata."""
        if self.source_files is not None:
            for rel_file_path in self.source_files:
                rel_file_path = rel_file_path.lstrip("/")
                source_file_path = os.path.join(self.source_path, rel_file_path).lstrip("/")
                try:
                    source_metadata = self.source_client.info(
                        source_file_path, strict=False
                    )  # don't check if the path is a directory
                    yield source_metadata
                except FileNotFoundError:
                    logger.warning(f"File in source_files not found at source: {source_file_path}")
                    continue

    def run(self):
        try:
            if self.source_files is not None:
                source_iter = iter(self._create_source_files_iterator())
            else:
                source_iter = iter(
                    self.source_client.list(
                        prefix=self.source_path,
                        show_attributes=self.preserve_source_attributes,
                        follow_symlinks=self.follow_symlinks,
                    )
                )

            target_iter = iter(self.target_client.list(prefix=self.target_path))
            total_count = 0

            source_file = next(source_iter, None)
            target_file = next(target_iter, None)

            while source_file or target_file:
                # Update progress and count each pair (or single) considered for syncing
                self.progress.update_total(total_count)

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
                            self.file_queue.put((_SyncOp.ADD, source_file))
                            total_count += 1
                        source_file = next(source_iter, None)
                    elif source_key > target_key:
                        if self.delete_unmatched_files:
                            self.file_queue.put((_SyncOp.DELETE, target_file))
                            total_count += 1
                        target_file = next(target_iter, None)  # Skip unmatched target file
                    else:
                        # Both exist, compare metadata
                        if not self._match_file_metadata(source_file, target_file):
                            # Check if file should be included based on patterns
                            if not self.pattern_matcher or self.pattern_matcher.should_include_file(source_key):
                                self.file_queue.put((_SyncOp.ADD, source_file))
                        else:
                            self.progress.update_progress()

                        source_file = next(source_iter, None)
                        target_file = next(target_iter, None)
                        total_count += 1
                elif source_file:
                    source_key = source_file.key[len(self.source_path) :].lstrip("/")

                    # Skip hidden files and directories
                    if self._is_hidden(source_key):
                        source_file = next(source_iter, None)
                        continue

                    # Check if file should be included based on patterns
                    if not self.pattern_matcher or self.pattern_matcher.should_include_file(source_key):
                        self.file_queue.put((_SyncOp.ADD, source_file))
                        total_count += 1
                    source_file = next(source_iter, None)
                elif target_file:
                    target_key = target_file.key[len(self.target_path) :].lstrip("/")

                    # Skip hidden files and directories
                    if self._is_hidden(target_key):
                        target_file = next(target_iter, None)
                        continue

                    if self.delete_unmatched_files:
                        self.file_queue.put((_SyncOp.DELETE, target_file))
                        total_count += 1
                    target_file = next(target_iter, None)

            self.progress.update_total(total_count)
        except Exception as e:
            self.error = e
        finally:
            for _ in range(self.num_workers):
                self.file_queue.put((_SyncOp.STOP, None))  # Signal consumers to stop


class ResultConsumerThread(threading.Thread):
    """
    A consumer thread that processes sync operation results and updates metadata.

    This thread is responsible for consuming results from worker processes/threads
    that have completed sync operations (ADD or DELETE). It updates the target
    client's metadata provider with information about the synchronized files,
    ensuring that the metadata store remains consistent with the actual file
    operations performed.
    """

    def __init__(
        self, target_client: "AbstractStorageClient", target_path: str, progress: ProgressBar, result_queue: _Queue
    ):
        super().__init__(daemon=True)
        self.target_client = target_client
        self.target_path = target_path
        self.progress = progress
        self.result_queue = result_queue
        self.error = None

    def run(self):
        try:
            # Pull from result_queue to collect pending updates from each multiprocessing worker.
            while True:
                op, target_file_path, physical_metadata = self.result_queue.get()

                logger.debug(
                    f"ResultConsumerThread: {op}, target_file_path: {target_file_path}, physical_metadata: {physical_metadata}"
                )

                if op == _SyncOp.STOP:
                    break

                if op in (_SyncOp.ADD, _SyncOp.DELETE):
                    self.progress.update_progress()
        except Exception as e:
            self.error = e


class SyncManager:
    """
    Manages the synchronization of files between two storage locations.

    This class orchestrates the entire sync process, coordinating between producer
    threads that identify files to sync, worker processes/threads that perform
    the actual file operations, and consumer threads that update metadata.
    """

    def __init__(
        self,
        source_client: "AbstractStorageClient",
        source_path: str,
        target_client: "AbstractStorageClient",
        target_path: str,
    ):
        self.source_client = source_client
        self.target_client = target_client
        self.source_path = source_path.lstrip("/")
        self.target_path = target_path.lstrip("/")

        same_client = source_client == target_client
        # Profile check is necessary because source might be StorageClient facade while target is SingleStorageClient.
        # NullStorageClient (used for delete through sync) doesn't have profile attribute so we need to explicitly check here.
        if not same_client and hasattr(source_client, "profile") and hasattr(target_client, "profile"):
            same_client = source_client.profile == target_client.profile

        # Check for overlapping paths on same storage backend
        if same_client and (source_path.startswith(target_path) or target_path.startswith(source_path)):
            raise ValueError("Source and target paths cannot overlap on same StorageClient.")

    def sync_objects(
        self,
        execution_mode: ExecutionMode = ExecutionMode.LOCAL,
        description: str = "Syncing",
        num_worker_processes: Optional[int] = None,
        delete_unmatched_files: bool = False,
        pattern_matcher: Optional[PatternMatcher] = None,
        preserve_source_attributes: bool = False,
        follow_symlinks: bool = True,
        source_files: Optional[list[str]] = None,
        ignore_hidden: bool = True,
    ):
        """
        Synchronize objects from source to target storage location.

        This method performs the actual synchronization by coordinating producer
        threads, worker processes/threads, and result consumer threads. It compares
        files between source and target, copying new/modified files and optionally
        deleting unmatched files from the target.

        The sync process uses file metadata (etag, size, modification time) to
        determine if files need to be copied. Files are processed in parallel
        using configurable numbers of worker processes and threads.


        :param execution_mode: Execution mode for sync operations.
        :param description: Description text shown in the progress bar.
        :param num_worker_processes: Number of worker processes to use. If None, automatically determined based on available CPU cores.
        :param delete_unmatched_files: If True, files present in target but not in source will be deleted from target.
        :param pattern_matcher: PatternMatcher instance for include/exclude filtering. If None, all files are included.
        :param preserve_source_attributes: Whether to preserve source file metadata attributes during synchronization.
            When False (default), only file content is copied. When True, custom metadata attributes are also preserved.

            .. warning::
                **Performance Impact**: When enabled without a ``metadata_provider`` configured, this will make a HEAD
                request for each object to retrieve attributes, which can significantly impact performance on large-scale
                sync operations. For production use at scale, configure a ``metadata_provider`` in your storage profile.
        :param follow_symlinks: Whether to follow symbolic links. Only applicable when source is POSIX file storage. When False, symlinks are skipped during sync.
        :param source_files: Optional list of file paths (relative to source_path) to sync. When provided, only these
            specific files will be synced, skipping enumeration of the source path.
        :param ignore_hidden: Whether to ignore hidden files and directories (starting with dot). Default is True.
        """
        logger.debug(f"Starting sync operation {description}")

        # Use provided pattern matcher for include/exclude filtering
        if pattern_matcher and pattern_matcher.has_patterns():
            logger.debug(f"Using pattern filtering: {pattern_matcher}")

        # Attempt to balance the number of worker processes and threads.
        num_worker_processes, num_worker_threads = calculate_worker_processes_and_threads(
            num_worker_processes, execution_mode, self.source_client, self.target_client
        )
        num_workers = num_worker_processes * num_worker_threads

        # Create the file and result queues.
        if execution_mode == ExecutionMode.LOCAL:
            if num_worker_processes == 1:
                file_queue = queue.Queue()
                result_queue = queue.Queue()
            else:
                file_queue = multiprocessing.Queue()
                result_queue = multiprocessing.Queue()
        else:
            if not HAVE_RAY:
                raise RuntimeError(
                    "Ray execution mode requested but Ray is not installed. "
                    "To use distributed sync with Ray, install it with: 'pip install ray'. "
                    "Alternatively, use ExecutionMode.LOCAL for single-machine sync operations."
                )

            from .contrib.ray.utils import SharedQueue

            file_queue = SharedQueue(maxsize=100000)
            result_queue = SharedQueue()

        # Create a progress bar to track the progress of the sync operation.
        progress = ProgressBar(desc=description, show_progress=True, total_items=0)

        # Start the producer thread to compare source and target file listings and queue sync operations.
        producer_thread = ProducerThread(
            self.source_client,
            self.source_path,
            self.target_client,
            self.target_path,
            progress,
            file_queue,
            num_workers,
            delete_unmatched_files,
            pattern_matcher,
            preserve_source_attributes,
            follow_symlinks,
            source_files,
            ignore_hidden,
        )
        producer_thread.start()

        # Start the result consumer thread to process the results of individual sync operations
        result_consumer_thread = ResultConsumerThread(
            self.target_client,
            self.target_path,
            progress,
            result_queue,
        )
        result_consumer_thread.start()

        if execution_mode == ExecutionMode.LOCAL:
            if num_worker_processes == 1:
                # Single process does not require multiprocessing.
                _sync_worker_process(
                    self.source_client,
                    self.source_path,
                    self.target_client,
                    self.target_path,
                    num_worker_threads,
                    file_queue,
                    result_queue,
                )
            else:
                # Create individual processes so they can share the multiprocessing.Queue
                processes = []
                for _ in range(num_worker_processes):
                    process = multiprocessing.Process(
                        target=_sync_worker_process,
                        args=(
                            self.source_client,
                            self.source_path,
                            self.target_client,
                            self.target_path,
                            num_worker_threads,
                            file_queue,
                            result_queue,
                        ),
                    )
                    processes.append(process)
                    process.start()

                # Wait for all processes to complete
                for process in processes:
                    process.join()
        elif execution_mode == ExecutionMode.RAY:
            if not HAVE_RAY:
                raise RuntimeError(
                    "Ray execution mode requested but Ray is not installed. "
                    "To use distributed sync with Ray, install it with: 'pip install ray'. "
                    "Alternatively, use ExecutionMode.LOCAL for single-machine sync operations."
                )

            import ray

            # Create a placement group to spread the workers across the cluster.
            from ray.util.placement_group import placement_group

            cluster_resources = ray.cluster_resources()
            available_resources = ray.available_resources()
            logger.debug(f"Ray cluster resources: {cluster_resources} Available resources: {available_resources}")

            # Check if we have enough resources before creating placement group
            required_cpus = num_worker_threads * num_worker_processes
            available_cpus = available_resources.get("CPU", 0)

            # Create placement group based on available resources
            if available_cpus > 0:
                # We have CPU resources, create CPU-based placement group
                if available_cpus < required_cpus:
                    # Not enough resources for requested configuration, create fallback
                    logger.warning(
                        f"Insufficient Ray cluster resources for requested configuration. "
                        f"Required: {required_cpus} CPUs, Available: {available_cpus} CPUs. "
                        f"Creating fallback placement group to utilize all available resources."
                    )

                    # Calculate optimal worker distribution
                    if available_cpus >= num_worker_processes:
                        # We can create all processes but with fewer threads per process
                        cpus_per_worker = max(1, available_cpus // num_worker_processes)
                        actual_worker_processes = num_worker_processes
                        actual_worker_threads = min(num_worker_threads, cpus_per_worker)
                    else:
                        # Not enough CPUs for all processes, reduce number of processes
                        actual_worker_processes = max(1, available_cpus)
                        actual_worker_threads = 1
                        cpus_per_worker = 1

                    logger.warning(
                        f"Fallback configuration: {actual_worker_processes} processes, "
                        f"{actual_worker_threads} threads per process, {cpus_per_worker} CPUs per worker"
                    )

                    # Create fallback placement group
                    bundle_specs = [{"CPU": float(cpus_per_worker)}] * int(actual_worker_processes)
                    msc_sync_placement_group = placement_group(bundle_specs, strategy=PLACEMENT_GROUP_STRATEGY)

                    # Update worker configuration for fallback
                    num_worker_processes = int(actual_worker_processes)
                    num_worker_threads = int(actual_worker_threads)
                else:
                    # Sufficient resources, use requested configuration
                    bundle_specs = [{"CPU": float(num_worker_threads)}] * num_worker_processes
                    msc_sync_placement_group = placement_group(bundle_specs, strategy=PLACEMENT_GROUP_STRATEGY)
            else:
                # No CPU resources, create placement group with minimal resource constraints
                logger.info("Creating placement group with minimal resource constraints")
                bundle_specs = [{"CPU": 1.0}] * int(num_worker_processes)
                msc_sync_placement_group = placement_group(bundle_specs, strategy=PLACEMENT_GROUP_STRATEGY)

            # Wait for placement group to be ready with timeout
            start_time = time.time()
            while time.time() - start_time < PLACEMENT_GROUP_TIMEOUT_SECONDS:
                try:
                    ray.get(msc_sync_placement_group.ready(), timeout=1.0)
                    break
                except Exception:
                    if time.time() - start_time >= PLACEMENT_GROUP_TIMEOUT_SECONDS:
                        raise RuntimeError(
                            f"Placement group creation timed out after {PLACEMENT_GROUP_TIMEOUT_SECONDS} seconds. "
                            f"Required: {required_cpus} CPUs, Available: {available_cpus} CPUs. "
                            f"Bundle specs: {bundle_specs}"
                            f"Please check your Ray cluster resources."
                        )
                    time.sleep(0.1)  # Small delay before retrying

            _sync_worker_process_ray = ray.remote(_sync_worker_process)

            # Start the sync worker processes.
            try:
                ray.get(
                    [
                        _sync_worker_process_ray.options(  # type: ignore
                            placement_group=msc_sync_placement_group, placement_group_bundle_index=worker_index
                        ).remote(
                            self.source_client,
                            self.source_path,
                            self.target_client,
                            self.target_path,
                            num_worker_threads,
                            file_queue,
                            result_queue,
                        )
                        for worker_index in range(int(num_worker_processes))
                    ]
                )
            finally:
                # Clean up the placement group
                try:
                    ray.util.remove_placement_group(msc_sync_placement_group)
                    start_time = time.time()
                    while time.time() - start_time < PLACEMENT_GROUP_TIMEOUT_SECONDS:
                        pg_info = ray.util.placement_group_table(msc_sync_placement_group)
                        if pg_info is None or pg_info.get("state") == "REMOVED":
                            break
                        time.sleep(1.0)
                except Exception as e:
                    logger.warning(f"Failed to remove placement group: {e}")

        # Wait for the producer thread to finish.
        producer_thread.join()

        # Signal the result consumer thread to stop.
        result_queue.put((_SyncOp.STOP, None, None))
        result_consumer_thread.join()

        # Commit the metadata to the target storage client.
        self.target_client.commit_metadata()

        # Log the completion of the sync operation.
        progress.close()
        logger.debug(f"Completed sync operation {description}")

        # Raise an error if the producer or result consumer thread encountered an error.
        errors = []

        if producer_thread.error:
            errors.append(f"Producer thread error: {producer_thread.error}")

        if result_consumer_thread.error:
            errors.append(f"Result consumer thread error: {result_consumer_thread.error}")

        if errors:
            raise RuntimeError(f"Errors in sync operation, caused by: {errors}")


def _sync_worker_process(
    source_client: "AbstractStorageClient",
    source_path: str,
    target_client: "AbstractStorageClient",
    target_path: str,
    num_worker_threads: int,
    file_queue: _Queue,
    result_queue: Optional[_Queue],
):
    """
    Worker process that handles file synchronization operations using multiple threads.

    This function is designed to run in a separate process as part of a multiprocessing
    sync operation. It spawns multiple worker threads that consume sync operations from
    the file_queue and perform the actual file transfers (ADD) or deletions (DELETE).
    """

    def _sync_consumer() -> None:
        """Processes files from the queue and copies them."""
        while True:
            op, file_metadata = file_queue.get()
            if op == _SyncOp.STOP:
                break

            source_key = file_metadata.key[len(source_path) :].lstrip("/")
            target_file_path = os.path.join(target_path, source_key)

            if op == _SyncOp.ADD:
                logger.debug(f"sync {file_metadata.key} -> {target_file_path}")
                # Acquire exclusive lock to prevent race conditions when multiple worker processes attempt concurrent
                # writes to the same target location on shared filesystems. This can occur when users run multiple sync
                # operations targeting the same filesystem location simultaneously.
                if target_client._is_posix_file_storage_provider():
                    target_lock_file_path = os.path.join(
                        os.path.dirname(target_file_path), f".{os.path.basename(target_file_path)}.lock"
                    )
                    lock_path = cast(BaseStorageProvider, target_client._storage_provider)._prepend_base_path(
                        target_lock_file_path
                    )
                    exclusive_lock = FileLock(lock_path, timeout=DEFAULT_LOCK_TIMEOUT)
                else:
                    exclusive_lock = contextlib.nullcontext()

                with exclusive_lock:
                    # Since this is an ADD operation, the file doesn't exist in the metadata provider.
                    # Check if it exists physically to support resume functionality.
                    target_metadata = None
                    if not target_client._storage_provider:
                        raise RuntimeError("Invalid state, no storage provider configured.")

                    try:
                        target_metadata = target_client._storage_provider.get_object_metadata(target_file_path)
                    except FileNotFoundError:
                        pass

                    # If file exists physically, check if sync is needed
                    if target_metadata is not None:
                        # First check if file is already up-to-date (optimization for all cases)
                        if (
                            target_metadata.content_length == file_metadata.content_length
                            and target_metadata.last_modified >= file_metadata.last_modified
                        ):
                            logger.debug(f"File {target_file_path} already exists and is up-to-date, skipping copy")

                            # Since this is an ADD operation, the file exists physically but not in metadata provider.
                            # Add it to metadata provider for tracking.
                            if target_client._metadata_provider:
                                logger.debug(
                                    f"Adding existing file {target_file_path} to metadata provider for tracking"
                                )
                                with target_client._metadata_provider_lock or contextlib.nullcontext():
                                    target_client._metadata_provider.add_file(target_file_path, target_metadata)

                            continue

                        # If we reach here, file exists but needs updating - check if overwrites are allowed
                        if target_client._metadata_provider and not target_client._metadata_provider.allow_overwrites():
                            # TODO(NGCDP-5748): This error is raised in a daemon thread and won't properly
                            # propagate to the caller. Need to implement proper exception handling in worker threads.
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

                # Clean up the lock file for POSIX file storage providers
                if target_client._is_posix_file_storage_provider():
                    try:
                        os.remove(lock_path)
                    except OSError:
                        # Lock file might already be removed or not accessible
                        pass
            elif op == _SyncOp.DELETE:
                logger.debug(f"rm {file_metadata.key}")
                target_client.delete(file_metadata.key)
            else:
                raise ValueError(f"Unknown operation: {op}")

            if result_queue:
                if op == _SyncOp.ADD:
                    # add tuple of (virtual_path, physical_metadata) to result_queue
                    if target_client._metadata_provider:
                        physical_metadata = target_client._metadata_provider.get_object_metadata(
                            target_file_path, include_pending=True
                        )
                    else:
                        physical_metadata = None
                    result_queue.put((op, target_file_path, physical_metadata))
                elif op == _SyncOp.DELETE:
                    result_queue.put((op, target_file_path, None))
                else:
                    raise RuntimeError(f"Unknown operation: {op}")

    # Worker process that spawns threads to handle syncing.
    threads = []
    for _ in range(num_worker_threads):
        thread = threading.Thread(target=_sync_consumer, daemon=True)
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()


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
    os.makedirs(os.path.dirname(target_physical_path), exist_ok=True)
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
    elif file_metadata.metadata:
        try:
            xattr.setxattr(
                target_physical_path,
                "user.json",
                json.dumps(file_metadata.metadata).encode("utf-8"),
            )
        except OSError as e:
            logger.debug(f"Failed to set extended attributes on {target_physical_path}: {e}")
