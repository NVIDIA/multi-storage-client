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

import glob
import json
import logging
import os
import shutil
import tempfile
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from enum import Enum
from io import BytesIO, StringIO
from typing import IO, Any, Optional, TypeVar, Union

import xattr

from ..telemetry import Telemetry
from ..types import AWARE_DATETIME_MIN, ObjectMetadata, Range
from ..utils import create_attribute_filter_evaluator, matches_attribute_filter_expression, validate_attributes
from .base import BaseStorageProvider

_T = TypeVar("_T")

PROVIDER = "file"
READ_CHUNK_SIZE = 8192

logger = logging.getLogger(__name__)


class _EntryType(Enum):
    """
    An enum representing the type of an entry in a directory.
    """

    FILE = 1
    DIRECTORY = 2
    DIRECTORY_TO_EXPLORE = 3


def atomic_write(source: Union[str, IO], destination: str, attributes: Optional[dict[str, str]] = None):
    """
    Writes the contents of a file to the specified destination path.

    This function ensures that the file write operation is atomic, meaning the output file is either fully written or not modified at all.
    This is achieved by writing to a temporary file first and then renaming it to the destination path.

    :param source: The input file to read from. It can be a string representing the path to a file, or an open file-like object (IO).
    :param destination: The path to the destination file where the contents should be written.
    :param attributes: The attributes to set on the file.
    """

    with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(destination), prefix=".") as fp:
        temp_file_path = fp.name
        if isinstance(source, str):
            with open(source, mode="rb") as src:
                while chunk := src.read(READ_CHUNK_SIZE):
                    fp.write(chunk)
        else:
            while chunk := source.read(READ_CHUNK_SIZE):
                fp.write(chunk)

        # Set attributes on temp file if provided
        validated_attributes = validate_attributes(attributes)
        if validated_attributes:
            try:
                xattr.setxattr(temp_file_path, "user.json", json.dumps(validated_attributes).encode("utf-8"))
            except OSError as e:
                logger.debug(f"Failed to set extended attributes on temp file {temp_file_path}: {e}")

    os.rename(src=temp_file_path, dst=destination)


class PosixFileStorageProvider(BaseStorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with POSIX file systems.
    """

    def __init__(
        self,
        base_path: str,
        config_dict: Optional[dict[str, Any]] = None,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
        **kwargs: Any,
    ) -> None:
        """
        :param base_path: The root prefix path within the POSIX file system where all operations will be scoped.
        :param config_dict: Resolved MSC config.
        :param telemetry_provider: A function that provides a telemetry instance.
        """

        # Validate POSIX path
        if base_path == "":
            base_path = "/"

        if not base_path.startswith("/"):
            raise ValueError(f"The base_path {base_path} must be an absolute path.")

        super().__init__(
            base_path=base_path,
            provider_name=PROVIDER,
            config_dict=config_dict,
            telemetry_provider=telemetry_provider,
        )

    def _translate_errors(
        self,
        func: Callable[[], _T],
        operation: str,
        path: str,
    ) -> _T:
        """
        Translates errors like timeouts and client errors.

        :param func: The function that performs the actual file operation.
        :param operation: The type of operation being performed (e.g., "PUT", "GET", "DELETE").
        :param path: The path to the object.

        :return: The result of the file operation, typically the return value of the `func` callable.
        """
        try:
            return func()
        except FileNotFoundError:
            raise
        except Exception as error:
            raise RuntimeError(f"Failed to {operation} object(s) at {path}, error: {error}") from error

    def _put_object(
        self,
        path: str,
        body: bytes,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
    ) -> int:
        def _invoke_api() -> int:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            atomic_write(source=BytesIO(body), destination=path, attributes=attributes)
            return len(body)

        return self._translate_errors(_invoke_api, operation="PUT", path=path)

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        def _invoke_api() -> bytes:
            if byte_range:
                with open(path, "rb") as f:
                    f.seek(byte_range.offset)
                    return f.read(byte_range.size)
            else:
                with open(path, "rb") as f:
                    return f.read()

        return self._translate_errors(_invoke_api, operation="GET", path=path)

    def _copy_object(self, src_path: str, dest_path: str) -> int:
        src_object = self._get_object_metadata(src_path)

        def _invoke_api() -> int:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            atomic_write(source=src_path, destination=dest_path, attributes=src_object.metadata)

            return src_object.content_length

        return self._translate_errors(_invoke_api, operation="COPY", path=src_path)

    def _delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        def _invoke_api() -> None:
            if os.path.exists(path) and os.path.isfile(path):
                os.remove(path)

        return self._translate_errors(_invoke_api, operation="DELETE", path=path)

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        is_dir = os.path.isdir(path)
        if is_dir:
            path = self._append_delimiter(path)

        def _invoke_api() -> ObjectMetadata:
            # Get basic file attributes
            metadata_dict = {}
            try:
                json_bytes = xattr.getxattr(path, "user.json")
                metadata_dict = json.loads(json_bytes.decode("utf-8"))
            except (OSError, IOError, KeyError, json.JSONDecodeError, AttributeError) as e:
                # Silently ignore if xattr doesn't exist, can't be read, or is corrupted
                logger.debug(f"Failed to read extended attributes from {path}: {e}")
                pass

            return ObjectMetadata(
                key=path,
                type="directory" if is_dir else "file",
                content_length=0 if is_dir else os.path.getsize(path),
                last_modified=datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc),
                metadata=metadata_dict if metadata_dict else None,
            )

        return self._translate_errors(_invoke_api, operation="HEAD", path=path)

    def _list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        follow_symlinks: bool = True,
    ) -> Iterator[ObjectMetadata]:
        start_after = os.path.relpath(start_after, self._base_path) if start_after else None
        end_at = os.path.relpath(end_at, self._base_path) if end_at else None

        def _invoke_api() -> Iterator[ObjectMetadata]:
            if os.path.isfile(path):
                yield ObjectMetadata(
                    key=os.path.relpath(path, self._base_path),  # relative path to the base path
                    content_length=os.path.getsize(path),
                    last_modified=datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc),
                )
            dir_path = path.rstrip("/") + "/"
            if not os.path.isdir(dir_path):  # expect the input to be a directory
                return

            yield from self._explore_directory(dir_path, start_after, end_at, include_directories, follow_symlinks)

        return self._translate_errors(_invoke_api, operation="LIST", path=path)

    def _explore_directory(
        self,
        dir_path: str,
        start_after: Optional[str],
        end_at: Optional[str],
        include_directories: bool,
        follow_symlinks: bool = True,
    ) -> Iterator[ObjectMetadata]:
        """
        Recursively explore a directory and yield objects in lexicographical order.

        :param dir_path: The directory path to explore
        :param start_after: The key to start after
        :param end_at: The key to end at
        :param include_directories: Whether to include directories in the result
        :param follow_symlinks: Whether to follow symbolic links. When False, symlinks are skipped.
        """
        try:
            # List contents of current directory
            dir_entries = os.listdir(dir_path)
            dir_entries.sort()  # Sort entries for consistent ordering

            # Collect all entries in this directory
            entries = []

            for entry in dir_entries:
                full_path = os.path.join(dir_path, entry)

                # Skip symlinks if follow_symlinks is False
                if not follow_symlinks and os.path.islink(full_path):
                    continue

                relative_path = os.path.relpath(full_path, self._base_path)

                # Check if this entry is within our range
                if (start_after is None or start_after < relative_path) and (end_at is None or relative_path <= end_at):
                    if os.path.isfile(full_path):
                        entries.append((relative_path, full_path, _EntryType.FILE))
                    elif os.path.isdir(full_path):
                        if include_directories:
                            entries.append((relative_path, full_path, _EntryType.DIRECTORY))
                        else:
                            # Add directory for recursive exploration
                            entries.append((relative_path, full_path, _EntryType.DIRECTORY_TO_EXPLORE))

            # Sort entries by relative path
            entries.sort(key=lambda x: x[0])

            # Process entries in order
            for relative_path, full_path, entry_type in entries:
                if entry_type == _EntryType.FILE:
                    yield ObjectMetadata(
                        key=relative_path,
                        content_length=os.path.getsize(full_path),
                        last_modified=datetime.fromtimestamp(os.path.getmtime(full_path), tz=timezone.utc),
                    )
                elif entry_type == _EntryType.DIRECTORY:
                    yield ObjectMetadata(
                        key=relative_path,
                        content_length=0,
                        type="directory",
                        last_modified=AWARE_DATETIME_MIN,
                    )
                elif entry_type == _EntryType.DIRECTORY_TO_EXPLORE:
                    # Recursively explore this directory
                    yield from self._explore_directory(
                        full_path, start_after, end_at, include_directories, follow_symlinks
                    )

        except (OSError, PermissionError) as e:
            logger.warning(f"Failed to list contents of {dir_path}, caused by: {e}")
            return

    def _upload_file(self, remote_path: str, f: Union[str, IO], attributes: Optional[dict[str, str]] = None) -> int:
        os.makedirs(os.path.dirname(remote_path), exist_ok=True)

        filesize: int = 0
        if isinstance(f, str):
            filesize = os.path.getsize(f)
        elif isinstance(f, StringIO):
            filesize = len(f.getvalue().encode("utf-8"))
        else:
            filesize = len(f.getvalue())  # type: ignore

        def _invoke_api() -> int:
            atomic_write(source=f, destination=remote_path, attributes=attributes)

            return filesize

        return self._translate_errors(_invoke_api, operation="PUT", path=remote_path)

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> int:
        filesize = metadata.content_length if metadata else os.path.getsize(remote_path)

        if isinstance(f, str):

            def _invoke_api() -> int:
                if os.path.dirname(f):
                    os.makedirs(os.path.dirname(f), exist_ok=True)
                atomic_write(source=remote_path, destination=f)

                return filesize

            return self._translate_errors(_invoke_api, operation="GET", path=remote_path)
        elif isinstance(f, StringIO):

            def _invoke_api() -> int:
                with open(remote_path, "r", encoding="utf-8") as src:
                    while chunk := src.read(READ_CHUNK_SIZE):
                        f.write(chunk)

                return filesize

            return self._translate_errors(_invoke_api, operation="GET", path=remote_path)
        else:

            def _invoke_api() -> int:
                with open(remote_path, "rb") as src:
                    while chunk := src.read(READ_CHUNK_SIZE):
                        f.write(chunk)

                return filesize

            return self._translate_errors(_invoke_api, operation="GET", path=remote_path)

    def glob(self, pattern: str, attribute_filter_expression: Optional[str] = None) -> list[str]:
        pattern = self._prepend_base_path(pattern)
        keys = list(glob.glob(pattern, recursive=True))
        if attribute_filter_expression:
            filtered_keys = []
            evaluator = create_attribute_filter_evaluator(attribute_filter_expression)
            for key in keys:
                obj_metadata = self._get_object_metadata(key)
                if matches_attribute_filter_expression(obj_metadata, evaluator):
                    filtered_keys.append(key)
            keys = filtered_keys
        if self._base_path == "/":
            return keys
        else:
            # NOTE: PosixStorageProvider does not have the concept of bucket and prefix.
            # So we drop the base_path from it.
            return [key.replace(self._base_path, "", 1).lstrip("/") for key in keys]

    def is_file(self, path: str) -> bool:
        path = self._prepend_base_path(path)
        return os.path.isfile(path)

    def rmtree(self, path: str) -> None:
        path = self._prepend_base_path(path)
        shutil.rmtree(path)
