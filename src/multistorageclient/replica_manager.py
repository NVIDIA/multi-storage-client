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

import atexit
import logging
import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO, StringIO
from typing import IO, Union

from .types import StorageProvider

logger = logging.getLogger(__name__)


# multiprocessing.cpu_count() can easily return 64/128 on modern servers, spawning that many I/O threads per process.
# The async uploads are network-bound, so such a large pool wastes memory and harms context-switch performance.
DEFAULT_REPLICA_UPLOAD_WORKERS = 8

_REPLICA_THREAD_POOL = ThreadPoolExecutor(
    max_workers=int(os.getenv("MSC_REPLICA_UPLOAD_THREADS", DEFAULT_REPLICA_UPLOAD_WORKERS))
)

atexit.register(lambda: _REPLICA_THREAD_POOL.shutdown(wait=False))


class ReplicaManager:
    """
    Manages replica operations for storage clients.
    """

    def __init__(self, storage_client):
        self._storage_client = storage_client
        # Thread-safe set to track files currently being uploaded
        self._uploading_files = set()
        self._upload_lock = threading.Lock()

    def download_from_replica_or_primary(
        self, remote_path: str, file: Union[str, IO], storage_provider: StorageProvider
    ) -> None:
        """Download the file from replicas, falling back to the primary provider.

        :param remote_path: path to the file to download
        :param file: file-like object or string path
        :param storage_provider: storage provider to use if the file is not found in the replicas
        """
        file_exists = False
        replicas_that_need_updates = []

        for replica_client in self._storage_client.replicas:
            try:
                if replica_client.is_file(remote_path):
                    replica_client.download_file(remote_path, file)
                    file_exists = True
                    break
                replicas_that_need_updates.append(replica_client)
            except FileNotFoundError:
                logger.error(f"File not found in replica: {remote_path}")
                continue
            except Exception as e:
                logger.error(f"Error downloading from replica: {e}")
                continue

        if not file_exists:
            storage_provider.download_file(remote_path, file)

        if hasattr(file, "seek"):
            file.seek(0)  # type: ignore

        if replicas_that_need_updates:
            # Atomic check-and-add operation to prevent duplicate uploads
            with self._upload_lock:
                if remote_path in self._uploading_files:
                    logger.debug(f"File {remote_path} is already being uploaded, skipping duplicate upload")
                    return
                self._uploading_files.add(remote_path)

            # Submit replica upload - (fire-and-forget, non-blocking)
            # Pass the file object directly to avoid pickle issues
            _REPLICA_THREAD_POOL.submit(
                self._upload_to_replicas,
                file,
                remote_path,
                replicas_that_need_updates,
            )

            logger.debug(
                f"Submitted background replica upload for {remote_path} to {len(replicas_that_need_updates)} replicas"
            )

    def _prepare_file_for_upload(self, file: Union[str, IO]) -> tuple[str, bool]:
        """Convert file object to path and determine if cleanup is needed.

        :param file: file-like object or string path
        :return: path to temporary file and boolean indicating if cleanup is needed
        """
        if isinstance(file, (BytesIO, StringIO)):
            mode = "w" if isinstance(file, StringIO) else "wb"
            with tempfile.NamedTemporaryFile(mode=mode, delete=False) as temp_file:
                temp_file.write(file.getvalue())
                return temp_file.name, True
        elif isinstance(file, str):
            return file, False
        else:
            if not getattr(file, "name", None):
                raise TypeError("File-like object must expose a valid `.name`")
            return file.name, False

    def _upload_to_replicas(
        self,
        file: Union[str, IO],
        remote_path: str,
        replica_clients: list,
    ) -> None:
        """Upload to replicas in background thread - no callbacks, just logging.

        :param file: file-like object or string path to upload
        :param remote_path: path to the file to upload
        :param replica_clients: list of replica clients to upload to
        """
        local_file_path = None
        created_temp = False

        try:
            logger.debug(f"Starting replica upload for {remote_path}")

            # Handle file object conversion in the background thread
            local_file_path, created_temp = self._prepare_file_for_upload(file)

            for replica_client in replica_clients:
                try:
                    replica_client.upload_file(remote_path, local_file_path)
                    logger.debug(f"Successfully uploaded to replica {replica_client.profile}: {remote_path}")
                except Exception as e:
                    logger.warning(f"Failed to upload to replica {replica_client.profile}: {e}")

            logger.debug(f"Completed replica upload for {remote_path}")

        except Exception as e:
            logger.error(f"Replica upload process failed for {remote_path}: {e}", exc_info=True)
        finally:
            # Remove file from uploading set
            with self._upload_lock:
                self._uploading_files.discard(remote_path)

            # Clean up temporary file if it was created
            if created_temp and local_file_path and os.path.exists(local_file_path):
                try:
                    os.unlink(local_file_path)
                    logger.debug(f"Cleaned up temporary file: {local_file_path}")
                except OSError as e:
                    logger.warning(f"Failed to delete temp file {local_file_path}: {e}")

    def delete_from_replicas(self, path: str) -> None:
        """Delete the file from replicas.

        :param path: path to the file to delete
        """
        for replica_client in self._storage_client.replicas:
            try:
                replica_client.delete(path)
            except Exception as e:
                logger.error(f"Failed to delete from replica {replica_client.profile}: {e}")
