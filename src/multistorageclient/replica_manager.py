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


import logging
from typing import IO, Union

from .types import StorageProvider

logger = logging.getLogger(__name__)


class ReplicaManager:
    """
    Manages replica operations for storage clients.

    This class encapsulates replica functionality including:
    - Downloading from replicas with fallback to primary
    """

    def __init__(self, storage_client):
        """
        Initialize the ReplicaManager.

        :param storage_client: The storage client instance that owns this replica manager
        """
        self._storage_client = storage_client

    def download_from_replica_or_primary(
        self, remote_path: str, file: Union[str, IO], storage_provider: StorageProvider
    ) -> None:
        """Download the file from replicas, falling back to the primary provider.

        The method iterates over configured replicas. The first replica that
        reports the object exists is used to download the data. If no replica
        contains the object, the primary *storage_provider* performs the
        download.

        No replica upload or synchronisation is performed by this method.

        :param remote_path: Remote object path to fetch.
        :param file: Local path or writable file-like object that will receive the content.
        :param storage_provider: Storage provider to use for downloading if no replica contains the object.
        """
        file_exists = False
        for replica_client in self._storage_client.replicas:
            try:
                if replica_client.is_file(remote_path):
                    replica_client.download_file(remote_path, file)
                    file_exists = True
                    break
            except FileNotFoundError:
                logger.error(f"File not found in replica: {remote_path}")
                continue
            except Exception as e:
                logger.error(f"Error downloading from replica: {e}")
                continue

        if not file_exists:
            # Use the storage provider's public download_file method to ensure proper path handling
            storage_provider.download_file(remote_path, file)

        if hasattr(file, "seek"):
            file.seek(0)  # type: ignore

    def copy_to_replicas(self, src_path: str, dest_path: str) -> None:
        """Copy the file to replicas.

        The method iterates over configured replicas and copies the file to each replica.

        :param src_path: Source object path to copy.
        :param dest_path: Destination object path to copy to.
        """
        for replica_client in self._storage_client.replicas:
            replica_client.copy(src_path, dest_path)
