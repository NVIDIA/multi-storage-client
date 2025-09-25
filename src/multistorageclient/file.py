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
import tempfile
import threading
from collections.abc import Iterator
from io import BytesIO, StringIO
from typing import IO, TYPE_CHECKING, Any, Optional

import xattr
from opentelemetry.trace import Span

from .cache import CacheManager
from .constants import MEMORY_LOAD_LIMIT
from .instrumentation.utils import (
    DEFAULT_ATTRIBUTES,
    TRACER,
    collect_default_attributes,
    file_tracer,
)
from .types import Range, SourceVersionCheckMode
from .utils import validate_attributes

if TYPE_CHECKING:
    from .client import StorageClient

logger = logging.getLogger(__name__)


class RemoteFileReader(IO[bytes]):
    """
    A file-like object for reading large files from a remote storage provider using range requests.

    This class provides a readable and seekable interface to a file stored remotely, allowing for efficient
    range-based reading of large files without needing to load the entire file into memory.
    """

    def __init__(
        self,
        remote_path: str,
        file_size: int,
        storage_client: StorageClient,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
    ):
        self._remote_path = remote_path
        self._file_size = file_size
        self._pos = 0
        self._storage_client = storage_client
        self._check_source_version = check_source_version

    @property
    def name(self) -> str:
        return self._remote_path

    @property
    def closed(self) -> bool:
        return False

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True

    def seek(self, position: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            self._pos = position
        elif whence == os.SEEK_CUR:
            self._pos += position
        elif whence == os.SEEK_END:
            self._pos = self._file_size + position
        return self._pos

    def tell(self) -> int:
        return self._pos

    def read(self, size: int = -1) -> bytes:
        # Calculate the start position for the range read
        offset = self._pos
        if size == 0 or offset >= self._file_size:
            return b""
        elif size == -1:
            # If size is -1, read to the end of the file
            length = self._file_size - offset
        else:
            # Ensure we don't go past the file size
            length = min(size, self._file_size - offset)

        # Perform range read from storage provider
        bytes_range = Range(offset=offset, size=length)
        data = self._storage_client.read(
            self._remote_path, byte_range=bytes_range, check_source_version=self._check_source_version
        )
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

    def readline(self, size: int = -1) -> bytes:
        raise io.UnsupportedOperation("readline operation is not supported on this file")

    def readlines(self, hint: int = -1) -> list[bytes]:
        raise io.UnsupportedOperation("readlines operation is not supported on this file")

    @property
    def mode(self) -> str:
        return "rb"

    def isatty(self) -> bool:
        return False

    def fileno(self) -> int:
        # Remote file readers don't have real file descriptors, but some libraries (like energon)
        # expect fileno() to work for operations like os.posix_fadvise().
        # Return a temporary file descriptor to avoid UnsupportedOperation errors.
        if not hasattr(self, "_temp_fd_holder"):
            self._temp_fd_holder = tempfile.TemporaryFile()
        return self._temp_fd_holder.fileno()

    def write(self, b: Any) -> int:
        raise io.UnsupportedOperation("write operation is not supported on this file")

    def writelines(self, lines: Any) -> None:
        raise io.UnsupportedOperation("writelines operation is not supported on this file")

    def truncate(self, size: Optional[int] = None) -> int:
        raise io.UnsupportedOperation("truncate operation is not supported on this file")

    def flush(self) -> None:
        pass

    def close(self) -> None:
        # Clean up temporary file descriptor if it was created
        if hasattr(self, "_temp_fd_holder"):
            self._temp_fd_holder.close()
            delattr(self, "_temp_fd_holder")

    def __enter__(self) -> RemoteFileReader:
        return self

    def __exit__(self, exc_type: Optional[Any], exc_val: Optional[Any], exc_tb: Optional[Any]) -> None:
        self.close()

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        # Read one byte at a time
        byte = self.read(1)
        if not byte:
            raise StopIteration
        return byte


# pylint: disable=abstract-method
class ObjectFile(IO):
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
    _storage_client: StorageClient
    _cache_manager: Optional[CacheManager] = None

    _local_path: Optional[str] = None
    _trace_span: Optional[Span] = None
    _attributes: Optional[dict[str, str]] = None

    def __init__(
        self,
        storage_client: StorageClient,
        remote_path: str,
        mode: str = "rb",
        encoding: Optional[str] = None,
        disable_read_cache: bool = False,
        memory_load_limit: int = MEMORY_LOAD_LIMIT,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
        attributes: Optional[dict[str, str]] = None,
        prefetch_file: bool = True,
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
        :param prefetch_file: If True, downloads the entire file to cache in the background for faster subsequent reads. If False, uses RemoteFileReader for streaming reads without caching. Defaults to True.
        """
        # Initialize parent trace span for this file to share the context with following R/W operations
        self._trace_span = TRACER.start_span("ObjectFile Lifecycle", attributes=DEFAULT_ATTRIBUTES)
        self._trace_span.set_attribute("profile", storage_client.profile)
        self._trace_span.set_attribute("storage_provider", str(storage_client._storage_provider))
        self._trace_span.set_attribute("mode", mode)
        for k, v in collect_default_attributes().items():
            self._trace_span.set_attribute(k, v)

        if mode not in ("r", "w", "rb", "wb", "a", "ab"):
            raise ValueError(f'Invalid mode "{mode}", only "w", "r", "a", "wb", "rb" and "ab" are supported.')

        if not remote_path:
            raise ValueError('Missing parameter "remote_path"')

        self._mode = mode
        self._storage_client = storage_client
        self._remote_path = remote_path
        self._encoding = encoding
        self._cache_manager = storage_client._cache_manager
        self._memory_load_limit = memory_load_limit
        self._open_files = []
        self._check_source_version = check_source_version
        self._prefetch_file = prefetch_file

        if disable_read_cache:
            self._cache_manager = None

        if self._cache_manager:
            # Use local file as the fileobj
            if self._mode in ("r", "rb"):
                # Read - common setup for both prefetch and non-prefetch
                self._object_metadata = self._storage_client.info(self._remote_path)
                self._download_complete = threading.Event()

                # RemoteFileReader only supports binary mode, so force prefetch for text mode
                if self._prefetch_file or self._mode == "r":
                    # Use threaded download for prefetch
                    self._download_thread = threading.Thread(target=self._download_file)
                    self._download_thread.start()
                else:
                    # Use RemoteFileReader directly for non-prefetch (binary mode only)
                    self._open_large_file()
            else:
                # Write or append
                self._create_fileobj()
        else:
            # Use BytesIO or StringIO as the fileobj
            if self._mode in ("r", "rb"):
                # Read
                self._object_metadata = self._storage_client.info(self._remote_path)
                self._download_complete = threading.Event()
                self._download_thread = threading.Thread(target=self._download_fileobj)
                self._download_thread.start()
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

    def _download_file(self) -> None:
        """
        Download the file to the cache directory.
        """
        if not self._cache_manager:
            raise ValueError(f"Cannot download file {self._remote_path}, cache is not configured.")

        # Check if the file can be put into the cache
        if self._object_metadata.content_length >= self._cache_manager.get_max_cache_size():
            logging.warning(
                f'The object "{self._remote_path}" is not cached because the file size ({self._object_metadata.content_length}) '
                f"exceeds the cache size ({self._cache_manager.get_max_cache_size()}). Please increase the cache size "
                f"in the config file to cache the file."
            )

            return self._open_large_file()

        try:
            if self._check_source_version == SourceVersionCheckMode.INHERIT:
                if self._cache_manager.check_source_version():
                    source_version = self._object_metadata.etag
                else:
                    source_version = None
            elif self._check_source_version == SourceVersionCheckMode.ENABLE:
                source_version = self._object_metadata.etag
            else:
                source_version = None

            if self._cache_manager.contains(
                key=self._remote_path, check_source_version=self._check_source_version, source_version=source_version
            ):
                # Read from cache
                file_object = self._cache_manager.open(
                    self._remote_path, self._mode, source_version, self._check_source_version
                )
            else:
                # Download file and put it into the cache
                file_lock = self._cache_manager.acquire_lock(self._remote_path)
                with file_lock:
                    if not self._cache_manager.contains(
                        key=self._remote_path,
                        check_source_version=self._check_source_version,
                        source_version=source_version,
                    ):
                        # The process writes the file to a temporary file and move it to the cache directory.
                        temp_file_path = self._get_temp_file_path()
                        self._storage_client.download_file(self._remote_path, temp_file_path)
                        self._cache_manager.set(self._remote_path, temp_file_path, source_version)

                file_object = self._cache_manager.open(
                    self._remote_path, self._mode, source_version, self._check_source_version
                )
            if file_object is None:
                raise FileNotFoundError(f"Unexpected error, file not found at {self._remote_path}")

            self._file = file_object
            self._open_files.append(self._file)
        except Exception as e:
            raise IOError(f"Failed to download file {self._remote_path}") from e
        finally:
            self._download_complete.set()

    def _get_temp_file_path(self) -> str:
        """
        Generate a temporary file path.
        """
        temp_file = tempfile.NamedTemporaryFile(mode=self._mode, delete=False)
        temp_file_path = temp_file.name
        temp_file.close()
        os.unlink(temp_file_path)
        return temp_file_path

    def _download_fileobj(self) -> None:
        """
        Download the file to a file-like object.
        """
        file_size = self._object_metadata.content_length

        if file_size > self._memory_load_limit:
            return self._open_large_file()

        try:
            self._create_fileobj()
            self._storage_client.download_file(self._remote_path, self._file)
            self._file.seek(0)
        except Exception as e:
            raise IOError(f"Failed to download file {self._remote_path}") from e
        finally:
            self._download_complete.set()

    def _open_large_file(self) -> None:
        """
        Use RemoteFileReader to open the file without keeping the data in memory.
        """
        file_size = self._object_metadata.content_length

        # Only support binary mode in reading large files
        if self._mode == "r":
            raise ValueError(
                f"Failed to open large file {self._remote_path} in text mode; "
                f'use mode "rb" to open files larger than {self._memory_load_limit}.'
            )
        self._file = RemoteFileReader(self._remote_path, file_size, self._storage_client)
        self._download_complete.set()

    @property
    def name(self) -> str:
        return self._remote_path

    @property
    def closed(self) -> bool:
        if self.readable():
            self._download_complete.wait()
        return self._file.closed

    @file_tracer
    def read(self, size: int = -1) -> Any:
        if self.readable():
            self._download_complete.wait()
        return self._file.read(size)

    def readable(self) -> bool:
        return self._mode in ("r", "rb")

    def writable(self) -> bool:
        return self._mode in ("w", "wb", "a", "ab")

    def seekable(self) -> bool:
        if self.readable():
            self._download_complete.wait()
        return self._file.seekable()

    def seek(self, position: int, whence: int = 0) -> int:
        if self.readable():
            self._download_complete.wait()
        return self._file.seek(position, whence)

    def tell(self) -> int:
        if self.readable():
            self._download_complete.wait()
        return self._file.tell()

    @file_tracer
    def readline(self, size: int = -1) -> Any:
        if self.readable():
            self._download_complete.wait()
        return self._file.readline(size)

    @file_tracer
    def readlines(self, hint: int = -1) -> list[Any]:
        if self.readable():
            self._download_complete.wait()
        return self._file.readlines()

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        if self.readable():
            self._download_complete.wait()
        return next(self._file)

    def __enter__(self) -> "ObjectFile":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def __del__(self) -> None:
        if self._trace_span:
            self._trace_span.end()
            self._trace_span = None

    @property
    def mode(self) -> str:
        return self._file.mode

    def isatty(self) -> bool:
        if self.readable():
            self._download_complete.wait()
        return self._file.isatty()

    def fileno(self) -> int:
        if self.readable():
            self._download_complete.wait()

        if isinstance(self._file, StringIO) or isinstance(self._file, BytesIO):
            # In-memory file objects (StringIO/BytesIO) don't have real file descriptors.
            # Create a temporary file and return its file descriptor when needed for operations that require one.
            fd_holder = tempfile.TemporaryFile()
            self._open_files.append(fd_holder)
            return fd_holder.fileno()

        return self._file.fileno()

    @file_tracer
    def write(self, b: Any) -> int:
        return self._file.write(b)

    @file_tracer
    def writelines(self, lines: Any) -> None:
        self._file.writelines(lines)

    @file_tracer
    def truncate(self, size: Optional[int] = None) -> int:
        return self._file.truncate(size)

    def flush(self) -> None:
        pass

    @file_tracer
    def readinto(self, b: Any) -> int:
        if self.readable():
            self._download_complete.wait()
        if hasattr(self._file, "readinto"):
            return self._file.readinto(b)  # type: ignore
        raise io.UnsupportedOperation(f"readinto operation is not supported on file {self._remote_path}")

    @file_tracer
    def readall(self) -> Any:
        return self.read(-1)

    @file_tracer
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
            temp_file_path = self._get_temp_file_path()
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

    def resolve_filesystem_path(self) -> Optional[str]:
        """
        Get filesystem path for the file content. Only available in read modes.

        With cache manager: Returns path to cached file after download completes.
        Without cache manager: Creates and returns path to temporary file with copied content.

        :return: Path to local file in read mode, None in write mode
        """
        if self.readable():
            self._download_complete.wait()
            if self._cache_manager:
                # Get the cached path of the file
                return self._file.name
            else:
                logger.warning(
                    f"Creating temporary file for {self._remote_path}. For better performance, please enable cache in the MSC config file."
                )
                # Create a temporary file and write the content to it
                mode = "w" if self._mode == "r" else "wb"
                temp_file = tempfile.NamedTemporaryFile(mode=mode, prefix=".msc_", delete=False)
                self._file.seek(0)
                temp_file.write(self._file.read())
                self._open_files.append(temp_file)
                return temp_file.name

        return None

    def fsync(self) -> None:
        pass


class PosixFile(IO):
    """
    A file-like object that wraps a POSIX file.

    This class provides a standardized interface to interact with local files, integrating features
    such as tracing file operations with OpenTelemetry spans.

    The class implements atomic write semantics by writing to a temporary file and then renaming it
    to the target file upon close. This ensures that other processes reading the file will either
    see the old content or the new content, but never partial content.
    """

    _storage_client: StorageClient
    _file: IO
    _trace_span: Optional[Span] = None
    _attributes: Optional[dict[str, str]] = None

    def __init__(
        self,
        storage_client: StorageClient,
        path: str,
        mode: str = "rb",
        buffering: int = -1,
        encoding: Optional[str] = None,
        atomic: bool = True,
        attributes: Optional[dict[str, str]] = None,
    ):
        # Initialize parent trace span for this file to share the context with following R/W operations
        self._trace_span = TRACER.start_span("PosixFile Lifecycle", attributes=DEFAULT_ATTRIBUTES)
        self._trace_span.set_attribute("mode", mode)
        for k, v in collect_default_attributes().items():
            self._trace_span.set_attribute(k, v)

        # If metadata provider is enabled, use the realpath to get the physical path for the storage provider.
        if storage_client._metadata_provider:
            realpath, exists = storage_client._metadata_provider.realpath(path)
            if not exists:
                raise FileNotFoundError(f"The file at path '{path}' was not found.")
        else:
            realpath = path

        # Required to get the absolute POSIX path.
        self._real_path = storage_client._storage_provider._prepend_base_path(realpath)

        self._path = path
        self._mode = mode
        self._atomic = atomic

        # Ensure the parent directory exists
        os.makedirs(os.path.dirname(self._real_path), exist_ok=True)

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

    @file_tracer
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

    @file_tracer
    def readline(self, size: int = -1) -> Any:
        return self._file.readline(size)

    @file_tracer
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

    def __del__(self) -> None:
        if self._trace_span:
            self._trace_span.end()
            self._trace_span = None

    @property
    def mode(self) -> str:
        return self._file.mode

    def isatty(self) -> bool:
        return self._file.isatty()

    def fileno(self) -> int:
        return self._file.fileno()

    @file_tracer
    def write(self, b: Any) -> int:
        return self._file.write(b)

    @file_tracer
    def writelines(self, lines: Any) -> None:
        self._file.writelines(lines)

    @file_tracer
    def truncate(self, size: Optional[int] = None) -> int:
        return self._file.truncate(size)

    @file_tracer
    def flush(self) -> None:
        self._file.flush()

    @file_tracer
    def readinto(self, b: Any) -> int:
        if hasattr(self._file, "readinto"):
            return self._file.readinto(b)  # type: ignore
        raise io.UnsupportedOperation(f"readinto operation is not supported on file {self._file.name}")

    @file_tracer
    def readall(self) -> Any:
        return self.read(-1)

    @file_tracer
    def close(self) -> None:
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

    def resolve_filesystem_path(self) -> Optional[str]:
        return self._file.name

    def fsync(self) -> None:
        os.fsync(self.fileno())
