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

from typing import Awaitable

class RustClient:
    """
    RustClient provides asynchronous methods for interacting with an object storage backend (e.g., S3).
    """
    def __init__(self, provider: str = "s3", configs: dict | None = ...) -> None:
        """
        Initialize a RustClient instance.
        :param provider: The storage provider type (default: 's3').
        :param configs: Configuration dictionary for the provider (e.g., bucket, endpoint_url, credentials).
        """
        ...

    async def put(self, path: str, data: bytes) -> Awaitable[None]:
        """
        Upload data to the object store at the specified path.
        :param path: The remote object path in the storage backend.
        :param data: The data to upload as bytes.
        :return: None. Raises an exception on failure.
        """
        ...

    async def get(self, path: str, start: int | None = ..., end: int | None = ...) -> Awaitable[bytes]:
        """
        Download data from the object store at the specified path.
        :param path: The remote object path in the storage backend.
        :param start: Optional start byte index for range download.
        :param end: Optional end byte index for range download.
        :return: The downloaded data as bytes.
        """
        ...

    async def upload(self, local_path: str, remote_path: str) -> Awaitable[None]:
        """
        Upload a local file to the object store.
        :param local_path: Path to the local file to upload.
        :param remote_path: The destination path in the storage backend.
        :return: None. Raises an exception on failure.
        """
        ...

    async def download(self, remote_path: str, local_path: str) -> Awaitable[None]:
        """
        Download an object from the store and save it to a local file.
        :param remote_path: The remote object path in the storage backend.
        :param local_path: Path to the local file to save the downloaded data.
        :return: None. Raises an exception on failure.
        """
        ...

    async def upload_multipart(self, local_path: str, remote_path: str) -> Awaitable[None]:
        """
        Upload a local file to the object store using multipart upload.

        This method uploads large files by splitting them into smaller chunks and uploading
        those chunks in parallel. This approach provides better performance for large files
        compared to upload() method.

        :param local_path: Path to the local file to upload.
        :param remote_path: The destination path in the storage backend.
        :return: None. Raises an exception on failure.
        """
        ...

    async def download_multipart(self, remote_path: str, local_path: str) -> Awaitable[None]:
        """
        Download an object from the store and save it to a local file using multipart download.

        This method downloads large files by splitting them into smaller chunks and downloading
        those chunks in parallel. This approach provides better performance for large files
        compared to download() method.

        :param remote_path: The destination path in the storage backend.
        :param local_path: Path to the local file to upload.
        :return: None. Raises an exception on failure.
        """
        ...
