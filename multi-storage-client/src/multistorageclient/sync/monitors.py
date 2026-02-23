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
import logging
import threading
from typing import TYPE_CHECKING, Optional

from ..types import MetadataProvider
from .progress_bar import ProgressBar
from .types import EventLike, OperationType, QueueLike

if TYPE_CHECKING:
    from ..client.types import AbstractStorageClient

logger = logging.getLogger(__name__)


class ResultMonitorThread(threading.Thread):
    """
    A monitor thread that processes sync operation results and updates metadata.

    This thread is responsible for consuming results from worker processes/threads
    that have completed sync operations (ADD or DELETE).
    """

    def __init__(
        self,
        target_client: "AbstractStorageClient",
        target_path: str,
        progress: ProgressBar,
        result_queue: QueueLike,
        metadata_provider: Optional[MetadataProvider] = None,
    ):
        super().__init__(daemon=True)
        self.target_client = target_client
        self.target_path = target_path
        self.progress = progress
        self.result_queue = result_queue
        self._metadata_provider = metadata_provider
        self._metadata_provider_lock = getattr(target_client, "_metadata_provider_lock", None)
        self.error = None
        self.total_files_added = 0
        self.total_files_deleted = 0
        self.total_bytes_added = 0
        self.total_bytes_deleted = 0

    def run(self):
        try:
            while True:
                op, target_file_path, physical_metadata = self.result_queue.get()

                logger.debug(
                    f"ResultMonitorThread: {op}, target_file_path: {target_file_path}, physical_metadata: {physical_metadata}"
                )

                if op == OperationType.STOP:
                    break

                if op == OperationType.ADD:
                    # ADD messages arrive from QueueBackedMetadataProvider (when metadata
                    # provider is configured) or from the worker directly.
                    # Replay add_file on the REAL metadata provider via the saved direct reference,
                    # bypassing any proxy to avoid feedback loops.
                    if self._metadata_provider:
                        with self._metadata_provider_lock or contextlib.nullcontext():
                            self._metadata_provider.add_file(target_file_path, physical_metadata)
                    self.total_files_added += 1
                    self.total_bytes_added += physical_metadata.content_length
                    self.progress.update_progress()
                elif op == OperationType.DELETE:
                    if self._metadata_provider:
                        with self._metadata_provider_lock or contextlib.nullcontext():
                            self._metadata_provider.remove_file(target_file_path)
                    self.total_files_deleted += 1
                    self.total_bytes_deleted += physical_metadata.content_length
                    self.progress.update_progress()
        except Exception as e:
            self.error = e


class ErrorMonitorThread(threading.Thread):
    """
    A monitor thread that monitors and processes errors from worker threads.

    This thread is responsible for consuming error information from worker
    processes/threads that encounter exceptions during sync operations.
    On the first error, it signals graceful shutdown to stop further work.
    """

    def __init__(
        self,
        error_queue: QueueLike,
        shutdown_event: EventLike,
    ):
        super().__init__(daemon=True)
        self.error_queue = error_queue
        self.shutdown_event = shutdown_event
        self.errors: list = []
        self.error = None

    def run(self):
        try:
            while True:
                error_info = self.error_queue.get()

                if error_info is None:
                    break

                logger.error(
                    f"Error in worker {error_info.worker_id} during {error_info.operation} "
                    f"on file {error_info.file_key}: {error_info.exception_type}: {error_info.exception_message}"
                )

                self.errors.append(error_info)

                # Signal shutdown on first error (fail-fast)
                if len(self.errors) == 1:
                    logger.info("Error detected, signaling shutdown")
                    self.shutdown_event.set()

        except Exception as e:
            self.error = e
