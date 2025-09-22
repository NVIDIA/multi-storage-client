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
import logging
import multiprocessing
import os
import queue
import tempfile
import threading
import time
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

from filelock import FileLock

from .constants import MEMORY_LOAD_LIMIT
from .progress_bar import ProgressBar
from .types import ExecutionMode, ObjectMetadata
from .utils import calculate_worker_processes_and_threads

logger = logging.getLogger(__name__)


def is_ray_available():
    return importlib.util.find_spec("ray") is not None


PLACEMENT_GROUP_STRATEGY = "SPREAD"
PLACEMENT_GROUP_TIMEOUT_SECONDS = 60  # Timeout for placement group creation
DEFAULT_LOCK_TIMEOUT = 600  # 10 minutes

HAVE_RAY = is_ray_available()

if TYPE_CHECKING:
    from .client import StorageClient

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
        source_client: "StorageClient",
        source_path: str,
        target_client: "StorageClient",
        target_path: str,
        progress: ProgressBar,
        file_queue: _Queue,
        num_workers: int,
        delete_unmatched_files: bool = False,
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
        self.error = None

    def _match_file_metadata(self, source_info: ObjectMetadata, target_info: ObjectMetadata) -> bool:
        # Check file size is the same and the target's last_modified is newer than the source.
        return (
            source_info.content_length == target_info.content_length
            and source_info.last_modified <= target_info.last_modified
        )

    def run(self):
        try:
            source_iter = iter(self.source_client.list(prefix=self.source_path))
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

                    if source_key < target_key:
                        self.file_queue.put((_SyncOp.ADD, source_file))
                        source_file = next(source_iter, None)
                        total_count += 1
                    elif source_key > target_key:
                        if self.delete_unmatched_files:
                            self.file_queue.put((_SyncOp.DELETE, target_file))
                            total_count += 1
                        target_file = next(target_iter, None)  # Skip unmatched target file
                    else:
                        # Both exist, compare metadata
                        if not self._match_file_metadata(source_file, target_file):
                            self.file_queue.put((_SyncOp.ADD, source_file))
                        else:
                            self.progress.update_progress()

                        source_file = next(source_iter, None)
                        target_file = next(target_iter, None)
                        total_count += 1
                elif source_file:
                    self.file_queue.put((_SyncOp.ADD, source_file))
                    source_file = next(source_iter, None)
                    total_count += 1
                else:
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

    def __init__(self, target_client: "StorageClient", target_path: str, progress: ProgressBar, result_queue: _Queue):
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

                if op == _SyncOp.STOP:
                    break

                if self.target_client._metadata_provider:
                    with self.target_client._metadata_provider_lock or contextlib.nullcontext():
                        if op == _SyncOp.ADD:
                            # Use realpath() to get physical path so metadata provider can
                            # track the logical/physical mapping.
                            phys_path, _ = self.target_client._metadata_provider.realpath(target_file_path)
                            physical_metadata.key = phys_path
                            self.target_client._metadata_provider.add_file(target_file_path, physical_metadata)
                        elif op == _SyncOp.DELETE:
                            self.target_client._metadata_provider.remove_file(target_file_path)
                        else:
                            raise RuntimeError(f"Unknown operation: {op}")

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
        self, source_client: "StorageClient", source_path: str, target_client: "StorageClient", target_path: str
    ):
        self.source_client = source_client
        self.target_client = target_client
        self.source_path = source_path.lstrip("/")
        self.target_path = target_path.lstrip("/")

        if source_client == target_client and (
            source_path.startswith(target_path) or target_path.startswith(source_path)
        ):
            raise ValueError("Source and target paths cannot overlap on same StorageClient.")

    def sync_objects(
        self,
        execution_mode: ExecutionMode = ExecutionMode.LOCAL,
        description: str = "Syncing",
        num_worker_processes: Optional[int] = None,
        delete_unmatched_files: bool = False,
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
        """
        logger.debug(f"Starting sync operation {description}")

        # Attempt to balance the number of worker processes and threads.
        num_worker_processes, num_worker_threads = calculate_worker_processes_and_threads(
            num_worker_processes, execution_mode, self.source_client, self.target_client
        )
        num_workers = num_worker_processes * num_worker_threads

        # Create the file and result queues.
        if execution_mode == ExecutionMode.LOCAL:
            if num_worker_processes == 1:
                file_queue = queue.Queue(maxsize=100000)
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
    source_client: "StorageClient",
    source_path: str,
    target_client: "StorageClient",
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
                    lock_path = target_client._storage_provider._prepend_base_path(target_lock_file_path)
                    exclusive_lock = FileLock(lock_path, timeout=DEFAULT_LOCK_TIMEOUT)
                else:
                    exclusive_lock = contextlib.nullcontext()

                with exclusive_lock:
                    # Skip if the file already exists and has the same content length but is newer.
                    try:
                        target_metadata = target_client.info(target_file_path, strict=False)
                        if (
                            target_metadata.content_length == file_metadata.content_length
                            and target_metadata.last_modified >= file_metadata.last_modified
                        ):
                            logger.debug(f"File {target_file_path} already exists, skipping")
                            continue
                    except FileNotFoundError:
                        pass

                    if file_metadata.content_length < MEMORY_LOAD_LIMIT:
                        file_content = source_client.read(file_metadata.key)
                        target_client.write(target_file_path, file_content)
                    else:
                        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                            temp_filename = temp_file.name

                        try:
                            source_client.download_file(remote_path=file_metadata.key, local_path=temp_filename)
                            target_client.upload_file(remote_path=target_file_path, local_path=temp_filename)
                        finally:
                            os.remove(temp_filename)  # Ensure the temporary file is removed

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
