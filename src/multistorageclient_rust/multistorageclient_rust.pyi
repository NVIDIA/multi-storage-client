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

from typing import Any, Awaitable

class RustClient:
    """
    RustClient provides asynchronous methods for interacting with an object storage backend (e.g., S3).
    """
    def __init__(
        self, provider: str = "s3", configs: dict | None = ..., credentials_provider: Any | None = ...
    ) -> None:
        """
        Initialize a RustClient instance.
        :param provider: The storage provider type (default: 's3').
        :param configs: Configuration dictionary for the provider (e.g., bucket, endpoint_url).
        :param credentials_provider: Credentials provider for the provider (e.g., StaticS3CredentialsProvider).
        """
        ...

    async def put(self, path: str, data: bytes) -> Awaitable[int]:
        """
        Upload data to the object store at the specified path.
        :param path: The remote object path in the storage backend.
        :param data: The data to upload as bytes.
        :return: The number of bytes uploaded.
        """
        ...

    async def get(self, path: str, start: int | None = ..., end: int | None = ...) -> Awaitable[bytes]:
        """
        Download data from the object store at the specified path.
        :param path: The remote object path in the storage backend.
        :param start: Optional start byte index for range download (inclusive).
        :param end: Optional end byte index for range download (inclusive).
        :return: The downloaded data as bytes.
        """
        ...

    async def upload(self, local_path: str, remote_path: str) -> Awaitable[int]:
        """
        Upload a local file to the object store.
        :param local_path: Path to the local file to upload.
        :param remote_path: The destination path in the storage backend.
        :return: The number of bytes uploaded.
        """
        ...

    async def download(self, remote_path: str, local_path: str) -> Awaitable[int]:
        """
        Download an object from the store and save it to a local file.
        :param remote_path: The remote object path in the storage backend.
        :param local_path: Path to the local file to save the downloaded data.
        :return: The number of bytes downloaded.
        """
        ...

    async def upload_multipart_from_file(
        self,
        local_path: str,
        remote_path: str,
        multipart_chunksize: int | None = ...,
        max_concurrency: int | None = ...,
    ) -> Awaitable[int]:
        """
        Upload a local file to the object store using multipart upload.

        This method uploads large files by splitting them into smaller chunks and uploading
        those chunks in parallel. This approach provides better performance for large files
        compared to upload() method.

        :param local_path: Path to the local file to upload.
        :param remote_path: The destination path in the storage backend.
        :param multipart_chunksize: The size of the multipart chunks.
        :param max_concurrency: The maximum number of concurrent operations.
        :return: The number of bytes uploaded.
        """
        ...

    async def upload_multipart_from_bytes(
        self, remote_path: str, data: bytes, multipart_chunksize: int | None = ..., max_concurrency: int | None = ...
    ) -> Awaitable[int]:
        """
        Upload data to the object store at the specified remote_path using multipart upload.

        This method uploads large data by splitting them into smaller chunks and uploading
        those chunks in parallel. This approach provides better performance for large data
        compared to put() method.

        :param remote_path: The remote object path in the storage backend.
        :param data: The data to upload as bytes.
        :param multipart_chunksize: The size of the multipart chunks.
        :param max_concurrency: The maximum number of concurrent operations.
        :return: The number of bytes uploaded.
        """
        ...

    async def download_multipart_to_file(
        self,
        remote_path: str,
        local_path: str,
        multipart_chunksize: int | None = ...,
        max_concurrency: int | None = ...,
    ) -> Awaitable[int]:
        """
        Download an object from the store and save it to a local file using multipart download.

        This method downloads large files by splitting them into smaller chunks and downloading
        those chunks in parallel. This approach provides better performance for large files
        compared to download() method.

        :param remote_path: The destination path in the storage backend.
        :param local_path: Path to the local file to upload.
        :param multipart_chunksize: The size of the multipart chunks.
        :param max_concurrency: The maximum number of concurrent operations.
        :return: The number of bytes downloaded.
        """
        ...

    async def download_multipart_to_bytes(
        self,
        remote_path: str,
        start: int | None = ...,
        end: int | None = ...,
        multipart_chunksize: int | None = ...,
        max_concurrency: int | None = ...,
    ) -> Awaitable[bytes]:
        """
        Download an object from the store and return it as bytes using multipart download.

        This method downloads large data by splitting them into smaller chunks and downloading
        those chunks in parallel. This approach provides better performance for large data
        compared to get() method.

        :param remote_path: The destination path in the storage backend.
        :param start: Optional start byte index for range download (inclusive).
        :param end: Optional end byte index for range download (inclusive).
        :param multipart_chunksize: The size of the multipart chunks.
        :param max_concurrency: The maximum number of concurrent operations.
        """
        ...

    def list_recursive(
        self,
        prefixes: list[str],
        limit: int | None = ...,
        suffix: str | None = ...,
        max_depth: int | None = ...,
        max_concurrency: int | None = ...,
    ) -> ListResult:
        """
        List objects and directories recursively from the object store for the given prefixes input list.

        This method lists objects and directories recursively from the object store for the given prefixes.
        It supports filtering by suffix, limiting the number of objects returned,
        and setting the maximum depth of the directory tree to traverse.
        The method uses concurrent operations to improve performance. The default max_concurrency is 32.

        :param prefixes: List of prefixes to list objects from.
        :param limit: Maximum number of objects to return.
        :param suffix: Filter objects by suffix.
        :param max_depth: Maximum depth of the directory tree to traverse.
        :param max_concurrency: Maximum number of concurrent operations.
        """
        ...

class ObjectMetadata:
    """
    ObjectMetadata contains metadata about an object or a directory in the object store.
    """

    key: str
    content_length: int
    last_modified: str  # in RFC 3339 format
    object_type: str  # "object" or "directory"
    etag: str | None

class ListResult:
    """
    ListResult contains the result of a list operation.
    """

    objects: list[ObjectMetadata]
    prefixes: list[ObjectMetadata]

class RustRetryableError(Exception):
    """
    RustRetryableError is raised when a retryable error occurs.
    """

    ...
