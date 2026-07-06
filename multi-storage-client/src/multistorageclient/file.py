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

from __future__ import annotations  # Enables forward references in type hints

import io
import json
import logging
import os
import shutil
import tempfile
import threading
from collections.abc import Callable, Iterator
from io import BytesIO, IOBase, StringIO
from typing import IO, TYPE_CHECKING, Any, Optional, cast

import xattr

from .cache import CacheManager
from .constants import MEMORY_LOAD_LIMIT
from .instrumentation.utils import file_metrics
from .providers.base import BaseStorageProvider
from .types import MAX_SYMLINK_DEPTH, ObjectMetadata, Range, SourceVersionCheckMode
from .utils import safe_makedirs, validate_attributes

if TYPE_CHECKING:
    from .client.types import AbstractStorageClient

logger = logging.getLogger(__name__)


class _BoundedBufferedReader(io.BufferedReader):
    """Bound ``read1`` calls to the configured remote range size."""

    def __init__(self, raw: io.RawIOBase, buffer_size: int) -> None:
        self._max_read_size = buffer_size
        super().__init__(raw, buffer_size=buffer_size)

    def read1(self, size: int = -1) -> bytes:
        bounded_size = self._max_read_size if size < 0 else min(size, self._max_read_size)
        return super().read1(bounded_size)

    def readinto1(self, buffer: Any) -> int:
        return super().readinto1(memoryview(buffer)[: self._max_read_size])


def _normalize_range_bytes(data: bytes) -> bytes:
    """Convert Rust/PyO3 bytes-like range results into native Python bytes."""
    to_bytes = getattr(data, "to_bytes", None)
    return bytes(cast(Callable[[], bytes], to_bytes)()) if callable(to_bytes) else data


class RemoteFileReader(io.RawIOBase):
    """
    A file-like object for reading large files from a remote storage provider using range requests.

    This class provides a readable and seekable interface to a file stored remotely, allowing for efficient
    range-based reading of large files without needing to load the entire file into memory.
    """

    def __init__(
        self,
        remote_path: str,
        file_size: int,
        storage_client: Optional[AbstractStorageClient] = None,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
        *,
        read_range: Optional[Callable[[int, int], bytes]] = None,
    ):
        """Create a raw reader backed by exactly one random-access source.

        :param remote_path: Logical name exposed through :attr:`name`.
        :param file_size: Total readable byte length.
        :param storage_client: Backward-compatible MSC range-read source.
        :param check_source_version: Cache source-version behavior for MSC reads.
        :param read_range: Injected ``(offset, size) -> bytes`` range reader.
        """
        if (storage_client is None) == (read_range is None):
            raise ValueError("Exactly one of storage_client or read_range must be provided.")
        self._remote_path = remote_path
        self._file_size = file_size
        self._pos = 0
        self._storage_client = storage_client
        self._read_range = read_range
        self._check_source_version = check_source_version

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file.")

    @property
    def name(self) -> str:
        return self._remote_path

    @property
    def size(self) -> int:
        return self._file_size

    def readable(self) -> bool:
        self._check_open()
        return True

    def writable(self) -> bool:
        self._check_open()
        return False

    def seekable(self) -> bool:
        self._check_open()
        return True

    def seek(self, position: int, whence: int = os.SEEK_SET) -> int:
        self._check_open()
        if whence == os.SEEK_SET:
            new_position = position
        elif whence == os.SEEK_CUR:
            new_position = self._pos + position
        elif whence == os.SEEK_END:
            new_position = self._file_size + position
        else:
            raise ValueError(f"Invalid whence ({whence}, should be 0, 1 or 2)")

        if new_position < 0:
            raise ValueError(f"Negative seek position {new_position}")
        self._pos = new_position
        return self._pos

    def tell(self) -> int:
        self._check_open()
        return self._pos

    def read(self, size: int = -1) -> bytes:
        self._check_open()
        # Calculate the start position for the range read
        offset = self._pos
        if size == 0 or offset >= self._file_size:
            return b""
        elif size < 0:
            # Any negative size reads to the end of the file.
            length = self._file_size - offset
        else:
            # Ensure we don't go past the file size
            length = min(size, self._file_size - offset)

        if self._read_range is not None:
            data = self._read_range(offset, length)
        else:
            storage_client = cast("AbstractStorageClient", self._storage_client)
            bytes_range = Range(offset=offset, size=length)
            data = storage_client.read(
                self._remote_path,
                byte_range=bytes_range,
                check_source_version=self._check_source_version,
            )

        data = _normalize_range_bytes(data)

        # Update the position by the number of bytes read
        bytes_read = len(data)
        self._pos += bytes_read
        return data

    def readinto(self, b: Any) -> int:
        buffer_size = len(b)
        data = self.read(buffer_size)
        bytes_read = len(data)
        mem_view = memoryview(b)
        mem_view[:bytes_read] = data
        return bytes_read

    @property
    def mode(self) -> str:
        return "rb"

    def isatty(self) -> bool:
        self._check_open()
        return False

    def fileno(self) -> int:
        self._check_open()
        # Remote file readers don't have real file descriptors, but some libraries (like energon)
        # expect fileno() to work for operations like os.posix_fadvise().
        # Return a temporary file descriptor to avoid UnsupportedOperation errors.
        if not hasattr(self, "_temp_fd_holder"):
            self._temp_fd_holder = tempfile.TemporaryFile()
        return self._temp_fd_holder.fileno()

    def write(self, b: Any) -> int:
        self._check_open()
        raise io.UnsupportedOperation("write operation is not supported on this file")

    def writelines(self, lines: Any) -> None:
        self._check_open()
        raise io.UnsupportedOperation("writelines operation is not supported on this file")

    def truncate(self, size: Optional[int] = None) -> int:
        self._check_open()
        raise io.UnsupportedOperation("truncate operation is not supported on this file")

    def flush(self) -> None:
        super().flush()

    def close(self) -> None:
        # Clean up temporary file descriptor if it was created
        if hasattr(self, "_temp_fd_holder"):
            self._temp_fd_holder.close()
            delattr(self, "_temp_fd_holder")
        super().close()


# pylint: disable=abstract-method
class ObjectFile(IOBase, IO):
    """
    A file-like object that handles remote file access with asynchronous downloads.

    This class provides a non-blocking way to open a remote file via a specified `StorageProvider`, allowing
    operations such as reading or writing to be performed as if the file was local. For files opened in read
    mode ('rb'), the file is downloaded in the background. Operations that rely on the file (such as `read`,
    `seek`, or `tell`) will block until the download is complete.

    For files opened in write mode ('wb'), the class writes locally to a specified path and uploads the file
    to the remote storage when the file is closed.
    """

    _file: IO
    _mode: str
    _remote_path: str
    _streaming_path: str
    _storage_client: AbstractStorageClient
    _cache_manager: Optional[CacheManager] = None

    _local_path: Optional[str] = None
    _attributes: Optional[dict[str, Any]] = None

    def __init__(
        self,
        storage_client: AbstractStorageClient,
        remote_path: str,
        mode: str = "rb",
        encoding: Optional[str] = None,
        disable_read_cache: bool = False,
        memory_load_limit: int = MEMORY_LOAD_LIMIT,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
        attributes: Optional[dict[str, Any]] = None,
        prefetch_file: Optional[bool] = None,
        buffering: int = -1,
    ):
        """
        Initialize the ObjectFile instance.

        :param storage_client: The storage client responsible for handling the remote file.
        :param remote_path: The path to the remote file.
        :param mode: The file mode ('r', 'w', 'rb' or 'wb'). Defaults to 'rb'.
        :param encoding: The encoding to use for text mode. Defaults to None.
        :param disable_read_cache: When set to True, disables caching for the file content. This parameter is only applicable when the mode is "r" or "rb".
        :param memory_load_limit: Size limit in bytes for loading files into memory. Defaults to 512MB. This parameter is only applicable when the mode is "r" or "rb".
        :param check_source_version: Whether to check the source version of cached objects.
        :param attributes: The attributes to add to the file if a new file is created.
        :param prefetch_file: If True, downloads the entire file to cache in the background for faster subsequent reads. If False, uses RemoteFileReader for streaming reads without caching. If None, inherits from cache configuration.
        :param buffering: The buffer size for streamed reads. Use 0 for unbuffered binary reads.
        """
        if mode not in ("r", "w", "rb", "wb", "a", "ab"):
            raise ValueError(f'Invalid mode "{mode}", only "w", "r", "a", "wb", "rb" and "ab" are supported.')

        if not remote_path:
            raise ValueError('Missing parameter "remote_path"')
        if not isinstance(buffering, int):
            raise TypeError("buffering must be an integer")
        if buffering < -1:
            raise ValueError("buffering must be -1, 0, or a positive integer")
        if buffering == 0 and "b" not in mode:
            raise ValueError("can't have unbuffered text I/O")

        self._mode = mode
        self._storage_client = storage_client
        self._remote_path = remote_path
        self._streaming_path = remote_path
        self._encoding = encoding
        self._cache_manager = storage_client._cache_manager
        self._memory_load_limit = memory_load_limit
        self._open_files = []
        self._check_source_version = check_source_version
        self._buffering = buffering
        self._local_path = None
        self._disable_read_cache = disable_read_cache
        self._download_error: Optional[Exception] = None

        if disable_read_cache:
            self._cache_manager = None

        if prefetch_file is not None:
            self._prefetch_file = prefetch_file
        elif self._cache_manager:
            self._prefetch_file = self._cache_manager.prefetch_file()
        else:
            self._prefetch_file = True

        if self._cache_manager:
            # Use local file as the fileobj
            if self._mode in ("r", "rb"):
                self._download_complete = threading.Event()
                cache_key = self._remote_path
                if not self._prefetch_file:
                    self._object_metadata = self._resolve_streaming_metadata()
                    cache_key = self._streaming_path

                # Cache-first optimization: prefetch reads with version checking disabled
                # can open directly from cache without a metadata request.
                need_version_check = self._check_source_version == SourceVersionCheckMode.ENABLE or (
                    self._check_source_version == SourceVersionCheckMode.INHERIT
                    and self._cache_manager.check_source_version()
                )

                if not need_version_check and self._cache_manager.contains(
                    cache_key, check_source_version=SourceVersionCheckMode.DISABLE
                ):
                    # Cache hit with no version check - open directly from cache
                    cached_file = self._cache_manager.open(
                        cache_key, mode="rb", check_source_version=SourceVersionCheckMode.DISABLE
                    )
                    if cached_file is not None:
                        self._set_downloaded_file(cached_file)
                        self._download_complete.set()
                    else:
                        # Unexpected cache failure - fetch metadata and proceed with download
                        if not hasattr(self, "_object_metadata"):
                            self._object_metadata = self._resolve_streaming_metadata()
                        if self._prefetch_file:
                            self._download_thread = threading.Thread(target=self._download_file)
                            self._download_thread.start()
                        else:
                            self._open_large_file()
                else:
                    # Cache miss or version check needed - fetch metadata and proceed with download
                    if not hasattr(self, "_object_metadata"):
                        self._object_metadata = self._resolve_streaming_metadata()

                    if self._prefetch_file:
                        # Use threaded download for prefetch
                        self._download_thread = threading.Thread(target=self._download_file)
                        self._download_thread.start()
                    else:
                        # Use a range-backed stream directly for non-prefetch reads.
                        self._open_large_file()
            else:
                # Write or append
                self._create_fileobj()
        else:
            # Use BytesIO or StringIO as the fileobj
            if self._mode in ("r", "rb"):
                # Read
                self._object_metadata = self._resolve_streaming_metadata()
                self._download_complete = threading.Event()
                if self._prefetch_file:
                    self._download_thread = threading.Thread(target=self._download_fileobj)
                    self._download_thread.start()
                else:
                    self._open_large_file()
            else:
                # Write or append
                self._create_fileobj()

        if attributes:
            self._attributes = attributes

    def _create_fileobj(self) -> None:
        """
        Create a file-like object depends on the mode.
        """
        if self._mode in ("rb", "wb", "ab"):
            self._file = BytesIO()
        else:
            self._file = StringIO()

        self._open_files.append(self._file)

    def _set_downloaded_file(self, file_object: IO) -> None:
        if self._mode == "r" and not isinstance(file_object, io.TextIOBase):
            self._file = io.TextIOWrapper(file_object, encoding=self._encoding)
        else:
            self._file = file_object
        self._open_files.append(self._file)

    def _resolve_streaming_metadata(self) -> ObjectMetadata:
        """Resolve marker symlinks before constructing a size-bound range stream."""
        path = self._remote_path
        visited: set[str] = set()
        depth = 0

        while True:
            metadata = self._storage_client.info(path)
            if not metadata.symlink_target:
                self._streaming_path = path
                return metadata
            if path in visited:
                raise ValueError(f"Symlink cycle detected at: {path}")
            if depth >= MAX_SYMLINK_DEPTH:
                raise ValueError(f"Too many levels of symlinks (>{MAX_SYMLINK_DEPTH}): {path}")
            visited.add(path)
            path = ObjectMetadata.resolve_symlink_target(path, metadata.symlink_target)
            depth += 1

    def _uses_remote_reader(self) -> bool:
        file_object = self._file
        if isinstance(file_object, io.TextIOWrapper):
            file_object = file_object.buffer
        if isinstance(file_object, io.BufferedReader):
            file_object = file_object.raw
        return isinstance(file_object, RemoteFileReader)

    def _download_file(self) -> None:
        """
        Download the file to the cache directory.
        """
        try:
            if not self._cache_manager:
                raise ValueError(f"Cannot download file {self._remote_path}, cache is not configured.")

            # Check if the file can be put into the cache
            if self._object_metadata.content_length >= self._cache_manager.get_max_cache_size():
                logging.warning(
                    f'The object "{self._remote_path}" is not cached because the file size ({self._object_metadata.content_length}) '
                    f"exceeds the cache size ({self._cache_manager.get_max_cache_size()}). Please increase the cache size "
                    f"in the config file to cache the file."
                )
                self._open_large_file()
                return

            if self._check_source_version == SourceVersionCheckMode.INHERIT:
                if self._cache_manager.check_source_version():
                    source_version = self._object_metadata.etag
                else:
                    source_version = None
            elif self._check_source_version == SourceVersionCheckMode.ENABLE:
                source_version = self._object_metadata.etag
            else:
                source_version = None

            requires_source_version = self._check_source_version == SourceVersionCheckMode.ENABLE or (
                self._check_source_version == SourceVersionCheckMode.INHERIT
                and self._cache_manager.check_source_version()
            )
            if requires_source_version and (not isinstance(source_version, str) or not source_version):
                self._open_large_file(uncached=True)
                return

            file_object = self._cache_manager.open(self._remote_path, "rb", source_version, self._check_source_version)
            if file_object is None:
                # A validated open miss can race with eviction or replacement, so retry
                # under the writer lock before downloading the current source revision.
                file_lock = self._cache_manager.acquire_lock(self._remote_path)
                with file_lock:
                    file_object = self._cache_manager.open(
                        self._remote_path, "rb", source_version, self._check_source_version
                    )
                    if file_object is None:
                        # The process writes the file to a temporary file and move it to the cache directory.
                        temp_file_path = self._generate_temp_file_path()
                        self._storage_client.download_file(self._streaming_path, temp_file_path)
                        self._cache_manager.set(self._remote_path, temp_file_path, source_version)

                        file_object = self._cache_manager.open(
                            self._remote_path, "rb", source_version, self._check_source_version
                        )
            if file_object is None:
                # Eviction can remove a freshly published entry before the final validated
                # reopen. Do not retry the unstable cache path; continue from the remote source.
                self._open_large_file(uncached=True)
                return

            self._set_downloaded_file(file_object)
        except Exception as e:
            self._record_download_error(e)
        finally:
            self._download_complete.set()

    def _generate_temp_file_path(self) -> str:
        """
        Generate a temporary file path. If the cache is enabled, the file will be stored in the cache directory.
        """
        if self._cache_manager:
            return self._cache_manager.generate_temp_file_path()

        with tempfile.NamedTemporaryFile(mode=self._mode) as temp_file:
            return temp_file.name

    def _download_fileobj(self) -> None:
        """
        Download the file to a file-like object.
        """
        file_size = self._object_metadata.content_length

        try:
            if file_size > self._memory_load_limit:
                self._open_large_file()
                return
            binary_file = BytesIO()
            self._storage_client.download_file(self._streaming_path, binary_file)
            binary_file.seek(0)
            self._set_downloaded_file(binary_file)
        except Exception as e:
            self._record_download_error(e)
        finally:
            self._download_complete.set()

    def _record_download_error(self, error: Exception) -> None:
        if isinstance(error, OSError):
            self._download_error = error
            return
        download_error = IOError(f"Failed to download file {self._remote_path}")
        download_error.__cause__ = error
        self._download_error = download_error

    def _wait_for_download(self) -> None:
        self._download_complete.wait()
        if self._download_error is not None:
            raise self._download_error

    def _open_large_file(self, *, uncached: bool = False) -> None:
        """
        Use RemoteFileReader to open the file without keeping the data in memory.
        """
        file_size = self._object_metadata.content_length
        if self._disable_read_cache or uncached:
            raw_file = RemoteFileReader(
                self._streaming_path,
                file_size,
                read_range=lambda offset, size: self._storage_client._read_uncached(
                    self._streaming_path,
                    Range(offset=offset, size=size),
                ),
            )
        else:
            raw_file = RemoteFileReader(
                self._streaming_path,
                file_size,
                self._storage_client,
                check_source_version=self._check_source_version,
            )

        if self._mode == "rb" and self._buffering == 0:
            self._file = cast(IO, raw_file)
        else:
            buffer_size = (
                io.DEFAULT_BUFFER_SIZE
                if self._buffering == -1 or (self._mode == "r" and self._buffering == 1)
                else self._buffering
            )
            buffered_file = _BoundedBufferedReader(raw_file, buffer_size=buffer_size)
            if self._mode == "r":
                self._file = io.TextIOWrapper(
                    buffered_file,
                    encoding=self._encoding,
                    newline=None,
                    line_buffering=self._buffering == 1,
                )
            else:
                self._file = buffered_file

        self._open_files.append(self._file)
        self._download_complete.set()

    @property
    def name(self) -> str:
        return self._remote_path

    @property
    def size(self) -> int:
        if not self.readable():
            raise io.UnsupportedOperation("size is only available in read mode")
        self._wait_for_download()
        if hasattr(self, "_object_metadata"):
            return self._object_metadata.content_length

        original_position = self._file.tell()
        try:
            return self._file.seek(0, os.SEEK_END)
        finally:
            self._file.seek(original_position)

    @property
    def closed(self) -> bool:
        if self.readable():
            self._download_complete.wait()
        if self._download_error is not None:
            return not self._open_files or all(file_object.closed for file_object in self._open_files)
        return self._file.closed

    def read(self, size: int = -1) -> Any:
        if self.readable():
            self._wait_for_download()
        return self._file.read(size)

    def readable(self) -> bool:
        return self._mode in ("r", "rb")

    def writable(self) -> bool:
        return self._mode in ("w", "wb", "a", "ab")

    def seekable(self) -> bool:
        if self.readable():
            self._wait_for_download()
        return self._file.seekable()

    def seek(self, position: int, whence: int = 0) -> int:
        if self.readable():
            self._wait_for_download()
        return self._file.seek(position, whence)

    def tell(self) -> int:
        if self.readable():
            self._wait_for_download()
        return self._file.tell()

    def readline(self, size: int = -1) -> Any:
        if self.readable():
            self._wait_for_download()
        return self._file.readline(size)

    def readlines(self, hint: int = -1) -> list[Any]:
        if self.readable():
            self._wait_for_download()
        return self._file.readlines(hint)

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        if self.readable():
            self._wait_for_download()
        return next(self._file)

    def __enter__(self) -> "ObjectFile":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    @property
    def mode(self) -> str:
        return self._mode

    def isatty(self) -> bool:
        if self.readable():
            self._wait_for_download()
        return self._file.isatty()

    def fileno(self) -> int:
        if self.readable():
            self._wait_for_download()
        if self.closed:
            raise ValueError("I/O operation on closed file.")

        if isinstance(self._file, StringIO) or isinstance(self._file, BytesIO):
            # In-memory file objects (StringIO/BytesIO) don't have real file descriptors.
            # Create a temporary file and return its file descriptor when needed for operations that require one.
            if not hasattr(self, "_fileno_holder"):
                self._fileno_holder = tempfile.TemporaryFile()
                self._open_files.append(self._fileno_holder)
            return self._fileno_holder.fileno()

        return self._file.fileno()

    def write(self, b: Any) -> int:
        return self._file.write(b)

    def writelines(self, lines: Any) -> None:
        self._file.writelines(lines)

    def truncate(self, size: Optional[int] = None) -> int:
        return self._file.truncate(size)

    def flush(self) -> None:
        pass

    def readinto(self, b: Any) -> int:
        if self.readable():
            self._wait_for_download()
        if hasattr(self._file, "readinto"):
            return self._file.readinto(b)  # type: ignore
        raise io.UnsupportedOperation(f"readinto operation is not supported on file {self._remote_path}")

    def readall(self) -> Any:
        return self.read(-1)

    def close(self) -> None:
        # If the file is already closed, return immediately.
        if self.closed:
            return

        if self.readable():
            # Ensure the download thread finishes (if it exists)
            if hasattr(self, "_download_thread") and self._download_thread.is_alive():
                self._download_thread.join()
        else:
            self._upload_file()

        for fp in self._open_files:
            fp.close()

    def _upload_file(self) -> None:
        """
        Upload the file to object store.
        """
        if self._mode in ("w", "wb"):
            self._file.seek(0)
            self._storage_client.upload_file(self._remote_path, self._file, attributes=self._attributes)
        elif self._mode in ("a", "ab"):
            # The append mode downloads the file first (if applicable), then upload it again with the appended content.
            temp_file_path = self._generate_temp_file_path()
            try:
                self._storage_client.download_file(self._remote_path, temp_file_path)
                if os.path.getsize(temp_file_path) > self._memory_load_limit:
                    logger.warning(
                        "The append mode ('a' or 'ab') is not suitable for appending to large files. "
                        "The file at '%s' exceeds the recommended size threshold "
                        "(%d bytes). This operation will result in poor performance "
                        "due to the need to download and re-upload the entire file.",
                        self._remote_path,
                        self._memory_load_limit,
                    )
            except FileNotFoundError:
                pass

            # Append the content to the downloaded file
            with open(temp_file_path, self._mode, encoding=self._encoding) as fp:
                self._file.seek(0)
                fp.write(self._file.read())

            self._storage_client.upload_file(self._remote_path, temp_file_path, attributes=self._attributes)
            os.unlink(temp_file_path)

    def resolve_filesystem_path(self) -> str:
        """
        Get filesystem path for the file content. Only available in read modes.

        With cache manager: Returns path to cached file after download completes.
        Without cache manager: Creates and returns path to temporary file with copied content.

        :return: Path to local file in read mode, raises a ValueError in write mode
        """
        if not self.readable():
            raise ValueError("resolve_filesystem_path operation not supported in write mode")

        self._wait_for_download()
        if self._local_path is not None:
            return self._local_path

        candidate = getattr(self._file, "name", None)
        if not self._uses_remote_reader() and isinstance(candidate, (str, os.PathLike)):
            candidate_path = os.fspath(candidate)
            if os.path.isabs(candidate_path) and os.path.isfile(candidate_path):
                return candidate_path

        logger.warning(
            "Creating temporary file for %s. For better performance, please enable cache in the MSC config file.",
            self._remote_path,
        )
        with tempfile.NamedTemporaryFile(mode="wb", prefix=".msc_", delete=False) as temp_file:
            materialized_path = temp_file.name

        try:
            if self._uses_remote_reader():
                self._storage_client.download_file(self._streaming_path, materialized_path)
            else:
                self._materialize_open_bytes(materialized_path)
        except BaseException:
            try:
                os.unlink(materialized_path)
            except FileNotFoundError:
                pass
            raise

        self._local_path = materialized_path
        return self._local_path

    def _materialize_open_bytes(self, destination_path: str) -> None:
        """Copy the already-open binary representation without changing the caller's position."""
        source = self._file.buffer if isinstance(self._file, io.TextIOWrapper) else self._file
        if isinstance(source, io.TextIOBase):
            raise OSError(f"Cannot materialize exact source bytes for text-only file {self._remote_path}")

        original_position = self._file.tell()
        try:
            source.seek(0)
            with open(destination_path, "wb") as destination:
                shutil.copyfileobj(source, destination)
        finally:
            self._file.seek(original_position)

    def fsync(self) -> None:
        pass

    def discard(self) -> None:
        pass


class PosixFile(IOBase, IO):
    """
    A file-like object that wraps a POSIX file.

    This class provides a standardized interface to interact with local files, integrating features
    such as tracing file operations with OpenTelemetry spans.

    The class implements atomic write semantics by writing to a temporary file and then renaming it
    to the target file upon close. This ensures that other processes reading the file will either
    see the old content or the new content, but never partial content.
    """

    _storage_client: AbstractStorageClient
    _file: IO
    _attributes: Optional[dict[str, Any]] = None

    def __init__(
        self,
        storage_client: AbstractStorageClient,
        path: str,
        mode: str = "rb",
        buffering: int = -1,
        encoding: Optional[str] = None,
        atomic: bool = True,
        attributes: Optional[dict[str, Any]] = None,
    ):
        # Store storage_client for emitting metrics
        self._storage_client = storage_client

        # If metadata provider is enabled, resolve the logical path to physical path.
        if storage_client._metadata_provider:
            resolved = storage_client._metadata_provider.realpath(path)
            if not resolved.exists:
                raise FileNotFoundError(f"The file at path '{path}' was not found.")
            realpath = resolved.physical_path
        else:
            realpath = path

        # Required to get the absolute POSIX path.
        self._real_path = cast(BaseStorageProvider, storage_client._storage_provider)._prepend_base_path(realpath)

        self._path = path
        self._mode = mode
        self._atomic = atomic

        # Ensure the parent directory exists only for write/append modes
        if "w" in mode or "a" in mode:
            safe_makedirs(os.path.dirname(self._real_path))

        if "w" in mode and self._atomic:
            # Create a temporary file in the same directory as the target file
            self._temp_path = os.path.join(
                os.path.dirname(self._real_path), f".{os.path.basename(self._real_path)}.tmp"
            )
            self._file = open(self._temp_path, mode=mode, buffering=buffering, encoding=encoding)
        else:
            self._file = open(self._real_path, mode=mode, buffering=buffering, encoding=encoding)

        self._attributes = attributes

    @property
    def name(self) -> str:
        return self._path

    @property
    def closed(self) -> bool:
        return self._file.closed

    @file_metrics(operation=BaseStorageProvider._Operation.READ)
    def read(self, size: int = -1) -> Any:
        return self._file.read(size)

    def readable(self) -> bool:
        return self._file.readable()

    def writable(self) -> bool:
        return self._file.writable()

    def seekable(self) -> bool:
        return self._file.seekable()

    def seek(self, position: int, whence: int = 0) -> int:
        return self._file.seek(position, whence)

    def tell(self) -> int:
        return self._file.tell()

    @file_metrics(operation=BaseStorageProvider._Operation.READ)
    def readline(self, size: int = -1) -> Any:
        return self._file.readline(size)

    @file_metrics(operation=BaseStorageProvider._Operation.READ)
    def readlines(self, hint: int = -1) -> list[Any]:
        return self._file.readlines()

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        return next(self._file)

    def __enter__(self) -> "PosixFile":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    @property
    def mode(self) -> str:
        return self._file.mode

    def isatty(self) -> bool:
        return self._file.isatty()

    def fileno(self) -> int:
        return self._file.fileno()

    @file_metrics(operation=BaseStorageProvider._Operation.WRITE)
    def write(self, b: Any) -> int:
        return self._file.write(b)

    @file_metrics(operation=BaseStorageProvider._Operation.WRITE)
    def writelines(self, lines: Any) -> None:
        self._file.writelines(lines)

    @file_metrics(operation=BaseStorageProvider._Operation.WRITE)
    def truncate(self, size: Optional[int] = None) -> int:
        return self._file.truncate(size)

    def flush(self) -> None:
        self._file.flush()

    @file_metrics(operation=BaseStorageProvider._Operation.READ)
    def readinto(self, b: Any) -> int:
        if hasattr(self._file, "readinto"):
            return self._file.readinto(b)  # type: ignore
        raise io.UnsupportedOperation(f"readinto operation is not supported on file {self._file.name}")

    @file_metrics(operation=BaseStorageProvider._Operation.READ)
    def readall(self) -> Any:
        return self.read(-1)

    def close(self) -> None:
        """
        Close the file and rename the temporary file to the target file if atomic write is enabled.
        """
        # If the file is already closed, return immediately.
        if self.closed:
            return

        self._file.close()

        if self._atomic and "w" in self._mode:
            # Rename the temporary file to the target file
            os.rename(self._temp_path, self._real_path)

        if self._attributes and ("w" in self._mode or "a" in self._mode):
            validated_attributes = validate_attributes(self._attributes)
            if validated_attributes:
                try:
                    xattr.setxattr(self._real_path, "user.json", json.dumps(validated_attributes).encode("utf-8"))
                except OSError as e:
                    logger.warning("Failed to set extended attributes on %s: %s", self._real_path, e)

    def resolve_filesystem_path(self) -> str:
        return self._file.name

    def fsync(self) -> None:
        os.fsync(self.fileno())

    def discard(self) -> None:
        """
        Discard the temporary file if it exists.
        """
        # If the file is already closed, return immediately.
        if self.closed:
            return

        if self._atomic and "w" in self._mode:
            self._file.close()
            os.unlink(self._temp_path)
