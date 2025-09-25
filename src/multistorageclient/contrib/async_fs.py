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

import asyncio
import atexit
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Union

from fsspec.asyn import AsyncFileSystem, _run_coros_in_chunks

from ..client import StorageClient
from ..file import ObjectFile, PosixFile
from ..shortcuts import resolve_storage_client
from ..types import MSC_PROTOCOL_NAME

_GLOBAL_THREAD_POOL = ThreadPoolExecutor(max_workers=int(os.getenv("MSC_MAX_WORKERS", "8")))

atexit.register(lambda: _GLOBAL_THREAD_POOL.shutdown(wait=False))


# pyright: reportIncompatibleMethodOverride=false
class MultiStorageAsyncFileSystem(AsyncFileSystem):
    """
    Custom :py:class:`fsspec.asyn.AsyncFileSystem` implementation for MSC protocol (``msc://``).
    Uses :py:class:`multistorageclient.StorageClient` for backend operations.
    """

    protocol = MSC_PROTOCOL_NAME

    def __init__(self, **kwargs: Any) -> None:
        """
        Initializes the :py:class:`MultiStorageAsyncFileSystem`.

        :param kwargs: Additional arguments for the :py:class:`fsspec.asyn.AsyncFileSystem`.
        """
        super().__init__(**kwargs)

    def resolve_path_and_storage_client(self, path: Union[str, os.PathLike]) -> tuple[StorageClient, str]:
        """
        Resolves the path and retrieves the associated :py:class:`multistorageclient.StorageClient`.

        :param path: The file path to resolve.

        :return: A tuple containing the :py:class:`multistorageclient.StorageClient` and the resolved path.
        """
        # Use unstrip_protocol to prepend our 'msc://' protocol only if it wasn't given in "path".
        return resolve_storage_client(self.unstrip_protocol(str(path).lstrip("/")))

    @staticmethod
    def asynchronize_sync(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Runs a synchronous function asynchronously using asyncio.

        :param func: The synchronous function to be executed asynchronously.
        :param args: Positional arguments to pass to the function.
        :param kwargs: Keyword arguments to pass to the function.

        :return: The result of the asynchronous execution of the function.
        """
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(_GLOBAL_THREAD_POOL, partial(func, *args, **kwargs))

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> Union[list[dict[str, Any]], list[str]]:
        """
        Lists the contents of a directory.

        :param path: The directory path to list.
        :param detail: Whether to return detailed information for each file.
        :param kwargs: Additional arguments for list functionality.

        :return: A list of file names or detailed information depending on the 'detail' argument.
        """
        storage_client, dir_path = self.resolve_path_and_storage_client(path)

        if dir_path and not dir_path.endswith("/"):
            dir_path += "/"

        objects = storage_client.list(path=dir_path, include_directories=True)

        if detail:
            return [
                {
                    "name": os.path.join(storage_client.profile, obj.key),
                    "ETag": obj.etag,
                    "LastModified": obj.last_modified,
                    "size": obj.content_length,
                    "ContentType": obj.content_type,
                    "type": obj.type,
                }
                for obj in objects
            ]
        else:
            return [os.path.join(storage_client.profile, obj.key) for obj in objects]

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> Union[list[dict[str, Any]], list[str]]:
        """
        Asynchronously lists the contents of a directory.

        :param path: The directory path to list.
        :param detail: Whether to return detailed information for each file.
        :param kwargs: Additional arguments for list functionality.

        :return: A list of file names or detailed information depending on the 'detail' argument.
        """
        return await self.asynchronize_sync(self.ls, path, detail, **kwargs)

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """
        Retrieves metadata information for a file.

        :param path: The file path to retrieve information for.
        :param kwargs: Additional arguments for info functionality.

        :return: A dictionary containing file metadata such as ETag, last modified, and size.
        """
        storage_client, file_path = self.resolve_path_and_storage_client(path)
        metadata = storage_client.info(file_path)
        return {
            "name": os.path.join(storage_client.profile, metadata.key),
            "ETag": metadata.etag,
            "LastModified": metadata.last_modified,
            "size": metadata.content_length,
            "ContentType": metadata.content_type,
            "type": metadata.type,
        }

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """
        Asynchronously retrieves metadata information for a file.

        :param path: The file path to retrieve information for.
        :param kwargs: Additional arguments for info functionality.

        :return: A dictionary containing file metadata such as ETag, last modified, and size.
        """
        return await self.asynchronize_sync(self.info, path, **kwargs)

    def rm_file(self, path: str, **kwargs: Any):
        """
        Removes a file.

        :param path: The file or directory path to remove.
        :param kwargs: Additional arguments for remove functionality.
        """
        storage_client, file_path = self.resolve_path_and_storage_client(path)
        recursive = kwargs.get("recursive", False)
        storage_client.delete(file_path, recursive=recursive)

    async def _rm_file(self, path: str, **kwargs: Any):
        """
        Asynchronously removes a file.

        :param path: The file or directory path to remove.
        :param kwargs: Additional arguments for remove functionality.
        """
        return await self.asynchronize_sync(self.rm_file, path, **kwargs)

    async def _rm(self, path, recursive=False, batch_size=None, **kwargs):
        """
        Asynchronously removes a file or directory.
        Instead of using the implementation in the parent class to expand the path and parallel delete the files,
        we explicitly pass down the recursive value and use the delete method in the StorageClient to handle the directory deletion.

        :param path: The file or directory path to remove.
        :param recursive: Whether to recursively remove directories.
        :param batch_size: The number of files to process in each batch.
        :param kwargs: Additional arguments for remove functionality.
        """

        if "recursive" not in kwargs:
            kwargs["recursive"] = recursive

        return await _run_coros_in_chunks(
            [self._rm_file(path, **kwargs)],
            batch_size=-1,  # no throttling
            nofiles=True,
        )

    def cp_file(self, path1: str, path2: str, **kwargs: Any):
        """
        Copies a file from the source path to the destination path.

        :param path1: The source file path.
        :param path2: The destination file path.
        :param kwargs: Additional arguments for copy functionality.

        :raises AttributeError: If the source and destination paths are associated with different profiles.
        """
        src_storage_client, src_path = self.resolve_path_and_storage_client(path1)
        dest_storage_client, dest_path = self.resolve_path_and_storage_client(path2)

        if src_storage_client != dest_storage_client:
            raise AttributeError(
                f"Cannot copy file from '{path1}' to '{path2}' because the source and destination paths are associated with different profiles. Cross-profile file operations are not supported."
            )

        src_storage_client.copy(src_path, dest_path)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any):
        """
        Asynchronously copies a file from the source path to the destination path.

        :param path1: The source file path.
        :param path2: The destination file path.
        :param kwargs: Additional arguments for copy functionality.

        :raises AttributeError: If the source and destination paths are associated with different profiles.
        """
        await self.asynchronize_sync(self.cp_file, path1, path2, **kwargs)

    def get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        """
        Downloads a file from the remote path to the local path.

        :param rpath: The remote path of the file to download.
        :param lpath: The local path to store the file.
        :param kwargs: Additional arguments for file retrieval functionality.
        """
        storage_client, rpath = self.resolve_path_and_storage_client(rpath)
        storage_client.download_file(rpath, lpath)

    async def _get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        """
        Asynchronously downloads a file from the remote path to the local path.

        :param rpath: The remote path of the file to download.
        :param lpath: The local path to store the file.
        :param kwargs: Additional arguments for file retrieval functionality.
        """
        await self.asynchronize_sync(self.get_file, rpath, lpath, **kwargs)

    def put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        """
        Uploads a local file to the remote path.

        :param lpath: The local path of the file to upload.
        :param rpath: The remote path to store the file.
        :param kwargs: Additional arguments for file upload functionality.
        """
        storage_client, rpath = self.resolve_path_and_storage_client(rpath)
        storage_client.upload_file(rpath, lpath)

    async def _put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        """
        Asynchronously uploads a local file to the remote path.

        :param lpath: The local path of the file to upload.
        :param rpath: The remote path to store the file.
        :param kwargs: Additional arguments for file upload functionality.
        """
        await self.asynchronize_sync(self.put_file, lpath, rpath, **kwargs)

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> Union[PosixFile, ObjectFile]:
        """
        Opens a file at the given path.

        :param path: The file path to open.
        :param mode: The mode in which to open the file.
        :param kwargs: Additional arguments passed to client.open (e.g., check_source_version, prefetch_file, etc.)

        :return: A ManagedFile object representing the opened file.
        """
        storage_client, path = self.resolve_path_and_storage_client(path)
        return storage_client.open(path, mode, **kwargs)

    async def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Union[PosixFile, ObjectFile]:
        """
        Asynchronously opens a file at the given path.

        :param path: The file path to open.
        :param mode: The mode in which to open the file.
        :param kwargs: Additional arguments for file opening.

        :return: A ManagedFile object representing the opened file.
        """
        return await self.asynchronize_sync(self.open, path, mode, **kwargs)

    def pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """
        Writes a value (bytes) directly to a file at the given path.

        :param path: The file path to write the value to.
        :param value: The bytes to write to the file.
        :param kwargs: Additional arguments for writing functionality.
        """
        storage_client, path = self.resolve_path_and_storage_client(path)
        storage_client.write(path, value)

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """
        Asynchronously writes a value (bytes) directly to a file at the given path.

        :param path: The file path to write the value to.
        :param value: The bytes to write to the file.
        :param kwargs: Additional arguments for writing functionality.
        """
        await self.asynchronize_sync(self.pipe_file, path, value, **kwargs)

    def cat_file(self, path: str, **kwargs: Any) -> bytes:
        """
        Reads the contents of a file at the given path.

        :param path: The file path to read from.
        :param kwargs: Additional arguments for file reading functionality.

        :return: The contents of the file as bytes.
        """
        storage_client, path = self.resolve_path_and_storage_client(path)
        return storage_client.read(path)

    async def _cat_file(self, path: str, **kwargs: Any) -> bytes:
        """
        Asynchronously reads the contents of a file at the given path.

        :param path: The file path to read from.
        :param kwargs: Additional arguments for file reading functionality.

        :return: The contents of the file as bytes.
        """
        return await self.asynchronize_sync(self.cat_file, path, **kwargs)
