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

import importlib.util
import json
import logging
import multiprocessing
import os
import queue
import tempfile
import threading
import time
from typing import TYPE_CHECKING, Optional

from ..constants import DEFAULT_SYNC_BATCH_SIZE
from ..types import DryrunResult, ExecutionMode, SymlinkHandling, SyncError, SyncResult
from ..utils import PatternMatcher, calculate_worker_processes_and_threads
from .monitors import ErrorMonitorThread, ResultMonitorThread
from .producer import ProducerThread
from .progress_bar import ProgressBar
from .types import OperationType
from .worker import sync_worker_process

if TYPE_CHECKING:
    from ..client.types import AbstractStorageClient

logger = logging.getLogger(__name__)


def is_ray_available():
    return importlib.util.find_spec("ray") is not None


HAVE_RAY = is_ray_available()


class SyncManager:
    """
    Manages the synchronization of files between two storage locations.

    This class orchestrates the entire sync process, coordinating between producer
    threads that identify files to sync, worker processes/threads that perform
    the actual file operations, and monitor threads that update metadata.
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
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
        source_files: Optional[list[str]] = None,
        ignore_hidden: bool = True,
        commit_metadata: bool = True,
        batch_size: int = DEFAULT_SYNC_BATCH_SIZE,
        dryrun: bool = False,
        dryrun_output_path: Optional[str] = None,
    ) -> SyncResult:
        """
        Synchronize objects from source to target storage location.

        This method performs the actual synchronization by coordinating producer
        threads, worker processes/threads, and result monitor threads. It compares
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
        :param symlink_handling: How to handle symbolic links during sync. :py:attr:`SymlinkHandling.FOLLOW` (default)
            dereferences symlinks and copies target bytes. :py:attr:`SymlinkHandling.SKIP` excludes symlinks.
            :py:attr:`SymlinkHandling.PRESERVE` recreates symlinks on the target via
            :py:meth:`AbstractStorageClient.make_symlink`.
        :param source_files: Optional list of file paths (relative to source_path) to sync. When provided, only these
            specific files will be synced, skipping enumeration of the source path.
        :param ignore_hidden: Whether to ignore hidden files and directories (starting with dot). Default is True.
        :param commit_metadata: When True (default), calls :py:meth:`StorageClient.commit_metadata` after sync completes.
            Set to False to skip the commit, allowing batching of multiple sync operations before committing manually.
        :param batch_size: Maximum number of operations to batch together before enqueueing. Must be between ``MIN_BATCH_SIZE`` (10)
            and ``MAX_BATCH_SIZE`` (500). Default is 100. Actual batch sizes may be smaller when the operation type changes
            (ADD to DELETE or vice versa), when file size buckets change (to group similar-sized files), or when the producer
            completes iteration. Batching reduces queue overhead, improves load balancing, and enables future bulk transfer
            optimizations.
        :param dryrun: If True, only enumerate and compare objects without performing any copy/delete operations.
            The returned SyncResult will include a :py:class:`DryrunResult` with paths to JSONL files.
        :param dryrun_output_path: Directory to write dryrun JSONL files into. If ``None``, a temporary
            directory is created automatically. Ignored when ``dryrun`` is ``False``.
        :raises SyncError: If errors occur during sync operations. Exception message contains details of all errors encountered.
            The sync operation will stop on the first error (fail-fast) and report all errors collected up to that point.
            The SyncError includes a partial SyncResult showing what was accomplished before the error occurred.
        """
        if dryrun:
            return self._sync_objects_dryrun(
                description=description,
                delete_unmatched_files=delete_unmatched_files,
                pattern_matcher=pattern_matcher,
                preserve_source_attributes=preserve_source_attributes,
                symlink_handling=symlink_handling,
                source_files=source_files,
                ignore_hidden=ignore_hidden,
                batch_size=batch_size,
                output_path=dryrun_output_path,
            )

        sync_start_time = time.time()

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
                error_queue = queue.Queue()
                shutdown_event = threading.Event()
            else:
                # Use spawn context to ensure fork-safety with boto3/cloud SDK clients.
                # Fork can corrupt shared connection pools and transfer manager state.
                ctx = multiprocessing.get_context("spawn")
                file_queue = ctx.Queue()
                result_queue = ctx.Queue()
                error_queue = ctx.Queue()
                shutdown_event = ctx.Event()
        else:
            if not HAVE_RAY:
                raise RuntimeError(
                    "Ray execution mode requested but Ray is not installed. "
                    "To use distributed sync with Ray, install it with: 'pip install ray'. "
                    "Alternatively, use ExecutionMode.LOCAL for single-machine sync operations."
                )

            from ..contrib.ray.utils import SharedEvent, SharedQueue

            file_queue = SharedQueue(maxsize=1_000_000)
            result_queue = SharedQueue()
            error_queue = SharedQueue()
            shutdown_event = SharedEvent()

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
            shutdown_event,
            delete_unmatched_files,
            pattern_matcher,
            preserve_source_attributes,
            symlink_handling,
            source_files,
            ignore_hidden,
            batch_size,
        )
        producer_thread.start()

        # Save a direct reference to the target client's metadata provider BEFORE the worker
        # installs the QueueBackedMetadataProvider proxy.  The monitor uses this
        # direct reference to replay add_file/remove_file, bypassing the proxy
        # and avoiding feedback loops.
        target_metadata_provider = getattr(self.target_client, "_metadata_provider", None)

        result_monitor_thread = ResultMonitorThread(
            self.target_client,
            self.target_path,
            progress,
            result_queue,
            metadata_provider=target_metadata_provider,
        )
        result_monitor_thread.start()

        # Start the error monitor thread to monitor and handle errors from worker threads
        error_monitor_thread = ErrorMonitorThread(
            error_queue,
            shutdown_event,
        )
        error_monitor_thread.start()

        if execution_mode == ExecutionMode.LOCAL:
            if num_worker_processes == 1:
                # Single process does not require multiprocessing.
                sync_worker_process(
                    self.source_client,
                    self.source_path,
                    self.target_client,
                    self.target_path,
                    num_worker_threads,
                    preserve_source_attributes,
                    file_queue,
                    result_queue,
                    error_queue,
                    shutdown_event,
                )
            else:
                # Create individual processes using spawn context for fork-safety
                processes = []
                for _ in range(num_worker_processes):
                    process = ctx.Process(
                        target=sync_worker_process,
                        args=(
                            self.source_client,
                            self.source_path,
                            self.target_client,
                            self.target_path,
                            num_worker_threads,
                            preserve_source_attributes,
                            file_queue,
                            result_queue,
                            error_queue,
                            shutdown_event,
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

            sync_worker_process_ray = ray.remote(sync_worker_process)

            ray.get(
                [
                    sync_worker_process_ray.options(
                        num_cpus=num_worker_threads,
                        scheduling_strategy="SPREAD",
                    ).remote(
                        self.source_client,
                        self.source_path,
                        self.target_client,
                        self.target_path,
                        num_worker_threads,
                        preserve_source_attributes,
                        file_queue,
                        result_queue,
                        error_queue,
                        shutdown_event,
                    )
                    for _ in range(num_worker_processes)
                ]
            )

        # Wait for the producer thread to finish.
        producer_thread.join()

        # Signal the result monitor thread to stop.
        result_queue.put((OperationType.STOP, None, None))
        result_monitor_thread.join()

        # Signal the error monitor thread to stop.
        error_queue.put(None)
        error_monitor_thread.join()

        # Commit the metadata to the target storage client (if commit_metadata is True).
        if commit_metadata:
            self.target_client.commit_metadata()

        # Log the completion of the sync operation.
        progress.close()
        logger.debug(f"Completed sync operation {description}")

        # Collect all errors from various sources
        error_messages = []

        if producer_thread.error:
            error_messages.append(f"Producer thread error: {producer_thread.error}")

        if result_monitor_thread.error:
            error_messages.append(f"Result monitor thread error: {result_monitor_thread.error}")

        if error_monitor_thread.error:
            error_messages.append(f"Error monitor thread error: {error_monitor_thread.error}")

        # Add worker errors with detailed information
        if error_monitor_thread.errors:
            error_messages.append(f"\nWorker errors ({len(error_monitor_thread.errors)} total):")
            for i, error_info in enumerate(error_monitor_thread.errors, 1):
                error_messages.append(
                    f"\n  Error {i}:\n"
                    f"    Worker: {error_info.worker_id}\n"
                    f"    Operation: {error_info.operation}\n"
                    f"    File: {error_info.file_key}\n"
                    f"    Exception: {error_info.exception_type}: {error_info.exception_message}\n"
                    f"    Traceback:\n{error_info.traceback_str}"
                )

        sync_result = SyncResult(
            total_work_units=producer_thread.total_work_units,
            total_files_added=result_monitor_thread.total_files_added,
            total_files_deleted=result_monitor_thread.total_files_deleted,
            total_bytes_added=result_monitor_thread.total_bytes_added,
            total_bytes_deleted=result_monitor_thread.total_bytes_deleted,
            total_time_seconds=time.time() - sync_start_time,
        )

        if error_messages:
            raise SyncError(
                f"Errors in sync operation: {''.join(error_messages)}",
                sync_result=sync_result,
            )

        return sync_result

    def _sync_objects_dryrun(
        self,
        description: str = "Syncing",
        delete_unmatched_files: bool = False,
        pattern_matcher: Optional[PatternMatcher] = None,
        preserve_source_attributes: bool = False,
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
        source_files: Optional[list[str]] = None,
        ignore_hidden: bool = True,
        batch_size: int = DEFAULT_SYNC_BATCH_SIZE,
        output_path: Optional[str] = None,
    ) -> SyncResult:
        """Dryrun variant: enumerate and compare only, stream results to JSONL files on disk."""
        sync_start_time = time.time()

        logger.debug(f"Starting dryrun sync operation {description}")

        if pattern_matcher and pattern_matcher.has_patterns():
            logger.debug(f"Using pattern filtering: {pattern_matcher}")

        file_queue: queue.Queue = queue.Queue()
        shutdown_event = threading.Event()

        progress = ProgressBar(desc=f"{description} (dryrun)", show_progress=True, total_items=0)

        producer_thread = ProducerThread(
            self.source_client,
            self.source_path,
            self.target_client,
            self.target_path,
            progress,
            file_queue,
            1,  # num_workers=1 so producer sends exactly one STOP sentinel
            shutdown_event,
            delete_unmatched_files,
            pattern_matcher,
            preserve_source_attributes,
            symlink_handling,
            source_files,
            ignore_hidden,
            batch_size,
        )
        producer_thread.start()
        producer_thread.join()

        if output_path:
            dryrun_dir = output_path
            os.makedirs(dryrun_dir, exist_ok=True)
        else:
            dryrun_dir = tempfile.mkdtemp(prefix="msc_dryrun_")
        add_path = os.path.join(dryrun_dir, "files_to_add.jsonl")
        delete_path = os.path.join(dryrun_dir, "files_to_delete.jsonl")

        total_files_added = 0
        total_files_deleted = 0
        total_bytes_added = 0
        total_bytes_deleted = 0

        with open(add_path, "w") as add_file, open(delete_path, "w") as delete_file:
            while True:
                batch = file_queue.get()
                if batch.operation == OperationType.STOP:
                    break
                if batch.operation == OperationType.ADD or batch.operation == OperationType.SYMLINK:
                    for item, _ in batch.items:
                        add_file.write(json.dumps(item.to_dict()) + "\n")
                        total_files_added += 1
                        total_bytes_added += item.content_length
                elif batch.operation == OperationType.DELETE:
                    for item, _ in batch.items:
                        delete_file.write(json.dumps(item.to_dict()) + "\n")
                        total_files_deleted += 1
                        total_bytes_deleted += item.content_length

        progress.close()
        logger.debug(f"Completed dryrun sync operation {description}")

        if producer_thread.error:
            sync_result = SyncResult(
                total_work_units=producer_thread.total_work_units,
                total_time_seconds=time.time() - sync_start_time,
                dryrun=DryrunResult(files_to_add=add_path, files_to_delete=delete_path),
            )
            raise SyncError(
                f"Errors in dryrun sync operation: Producer thread error: {producer_thread.error}",
                sync_result=sync_result,
            )

        return SyncResult(
            total_work_units=producer_thread.total_work_units,
            total_files_added=total_files_added,
            total_files_deleted=total_files_deleted,
            total_bytes_added=total_bytes_added,
            total_bytes_deleted=total_bytes_deleted,
            total_time_seconds=time.time() - sync_start_time,
            dryrun=DryrunResult(files_to_add=add_path, files_to_delete=delete_path),
        )
