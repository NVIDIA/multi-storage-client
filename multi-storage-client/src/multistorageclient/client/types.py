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

"""Internal types for storage client implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import IO, TYPE_CHECKING, Any, List, Optional, Union

from ..constants import MEMORY_LOAD_LIMIT
from ..types import ExecutionMode, SourceVersionCheckMode

if TYPE_CHECKING:
    from ..cache import CacheManager
    from ..config import StorageClientConfig
    from ..file import ObjectFile, PosixFile
    from ..types import (
        CredentialsProvider,
        MetadataProvider,
        ObjectMetadata,
        PatternList,
        Range,
        RetryConfig,
        StorageProvider,
    )


class AbstractStorageClient(ABC):
    """
    Abstract base class for all storage client implementations.

    Defines the contract for storage operations, supporting both single-backend
    (SingleStorageClient) and multi-backend (CompositeStorageClient) configurations.
    """

    _config: "StorageClientConfig"
    _storage_provider: Optional["StorageProvider"]
    _metadata_provider: Optional["MetadataProvider"]
    _metadata_provider_lock: Optional[Any]  # threading.Lock
    _credentials_provider: Optional["CredentialsProvider"]
    _retry_config: Optional["RetryConfig"]
    _cache_manager: Optional["CacheManager"]
    _replica_manager: Optional[Any]  # ReplicaManager

    @property
    @abstractmethod
    def profile(self) -> str:
        """
        :return: The profile name of the storage client.
        """
        pass

    @property
    @abstractmethod
    def replicas(self) -> List["AbstractStorageClient"]:
        """
        :return: List of replica storage clients, sorted by read priority.
        """
        pass

    @abstractmethod
    def is_default_profile(self) -> bool:
        """
        :return: ``True`` if the storage client is using the default profile, ``False`` otherwise.
        """
        pass

    @abstractmethod
    def _is_rust_client_enabled(self) -> bool:
        """
        :return: ``True`` if the storage provider is using the Rust client, ``False`` otherwise.
        """
        pass

    @abstractmethod
    def _is_posix_file_storage_provider(self) -> bool:
        """
        :return: ``True`` if the storage client is using a POSIX file storage provider, ``False`` otherwise.
        """
        pass

    @abstractmethod
    def get_posix_path(self, path: str) -> Optional[str]:
        """
        Returns the physical POSIX filesystem path for POSIX storage providers.

        :param path: The path to resolve (may be a symlink or virtual path).
        :return: Physical POSIX filesystem path if POSIX storage, None otherwise.
        """
        pass

    @abstractmethod
    def read(
        self,
        path: str,
        byte_range: Optional[Range] = None,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
    ) -> bytes:
        """
        Read bytes from a file at the specified logical path.

        :param path: The logical path of the object to read.
        :param byte_range: Optional byte range to read (offset and length).
        :param check_source_version: Whether to check the source version of cached objects.
        :return: The content of the object as bytes.
        :raises FileNotFoundError: If the file at the specified path does not exist.
        """
        pass

    @abstractmethod
    def open(
        self,
        path: str,
        mode: str = "rb",
        buffering: int = -1,
        encoding: Optional[str] = None,
        disable_read_cache: bool = False,
        memory_load_limit: int = MEMORY_LOAD_LIMIT,
        atomic: bool = True,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
        attributes: Optional[dict[str, str]] = None,
        prefetch_file: bool = True,
    ) -> Union[PosixFile, ObjectFile]:
        """
        Open a file for reading or writing.

        :param path: The logical path of the object to open.
        :param mode: The file mode. Supported modes: "r", "rb", "w", "wb", "a", "ab".
        :param buffering: The buffering mode. Only applies to PosixFile.
        :param encoding: The encoding to use for text files.
        :param disable_read_cache: When set to ``True``, disables caching for file content.
            This parameter is only applicable to ObjectFile when the mode is "r" or "rb".
        :param memory_load_limit: Size limit in bytes for loading files into memory. Defaults to 512MB.
            This parameter is only applicable to ObjectFile when the mode is "r" or "rb". Defaults to 512MB.
        :param atomic: When set to ``True``, file will be written atomically (rename upon close).
            This parameter is only applicable to PosixFile in write mode.
        :param check_source_version: Whether to check the source version of cached objects.
        :param attributes: Attributes to add to the file.
            This parameter is only applicable when the mode is "w" or "wb" or "a" or "ab". Defaults to None.
        :param prefetch_file: Whether to prefetch the file content.
            This parameter is only applicable to ObjectFile when the mode is "r" or "rb". Defaults to True.
        :return: A file-like object (PosixFile or ObjectFile) for the specified path.
        :raises FileNotFoundError: If the file does not exist (read mode).
        :raises NotImplementedError: If the operation is not supported (e.g., write on CompositeStorageClient).
        """
        pass

    @abstractmethod
    def download_file(self, remote_path: str, local_path: Union[str, IO]) -> None:
        """
        Download a remote file to a local path or file-like object.

        :param remote_path: The logical path of the remote file to download.
        :param local_path: The local file path or file-like object to write to.
        :raises FileNotFoundError: If the remote file does not exist.
        """
        pass

    @abstractmethod
    def glob(
        self,
        pattern: str,
        include_url_prefix: bool = False,
        attribute_filter_expression: Optional[str] = None,
    ) -> List[str]:
        """
        Matches and retrieves a list of object keys in the storage provider that match the specified pattern.

        :param pattern: The pattern to match object keys against, supporting wildcards (e.g., ``*.txt``).
        :param include_url_prefix: Whether to include the URL prefix ``msc://profile`` in the result.
        :param attribute_filter_expression: The attribute filter expression to apply to the result.
        :return: A list of object paths that match the specified pattern.
        """
        pass

    @abstractmethod
    def list(
        self,
        prefix: str = "",
        path: str = "",
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        include_url_prefix: bool = False,
        attribute_filter_expression: Optional[str] = None,
        show_attributes: bool = False,
        follow_symlinks: bool = True,
    ) -> Iterator[ObjectMetadata]:
        """
        List objects in the storage provider under the specified path.

        **IMPORTANT**: Use the ``path`` parameter for new code. The ``prefix`` parameter is
        deprecated and will be removed in a future version.

        :param prefix: [DEPRECATED] Use ``path`` instead. The prefix to list objects under.
        :param path: The directory or file path to list objects under. This should be a
                    complete filesystem path (e.g., "my-bucket/documents/" or "data/2024/").
                    Cannot be used together with ``prefix``.
        :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
        :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.
        :param include_directories: Whether to include directories in the result. when ``True``, directories are returned alongside objects.
        :param include_url_prefix: Whether to include the URL prefix ``msc://profile`` in the result.
        :param attribute_filter_expression: The attribute filter expression to apply to the result.
        :param show_attributes: Whether to return attributes in the result. WARNING: Depend on implementation, there might be performance impact if this set to ``True``.
        :param follow_symlinks: Whether to follow symbolic links. Only applicable for POSIX file storage providers. When ``False``, symlinks are skipped during listing.
        :return: An iterator over ObjectMetadata for matching objects.
        :raises ValueError: If both ``path`` and ``prefix`` parameters are provided (both non-empty).
        """
        pass

    @abstractmethod
    def is_file(self, path: str) -> bool:
        """
        Checks whether the specified path points to a file (rather than a folder or directory).

        :param path: The logical path to check.
        :return: ``True`` if the key points to a file, ``False`` otherwise.
        """
        pass

    @abstractmethod
    def is_empty(self, path: str) -> bool:
        """
        Check whether the specified path is empty. A path is considered empty if there are no
        objects whose keys start with the given path as a prefix.

        :param path: The logical path to check (typically a directory or folder prefix).
        :return: ``True`` if no objects exist under the specified path prefix, ``False`` otherwise.
        """
        pass

    @abstractmethod
    def info(self, path: str, strict: bool = True) -> ObjectMetadata:
        """
        Get metadata for a file at the specified path.

        :param path: The logical path of the object.
        :param strict: When ``True``, only return committed metadata. When ``False``, include pending changes.
        :return: ObjectMetadata containing file information (size, last modified, etc.).
        :raises FileNotFoundError: If the file at the specified path does not exist.
        """
        pass

    @abstractmethod
    def write(
        self,
        path: str,
        body: bytes,
        attributes: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Write bytes to a file at the specified path.

        :param path: The logical path where the object will be written.
        :param body: The content to write as bytes.
        :param attributes: Optional attributes to add to the file.
        :raises NotImplementedError: If write operations are not supported (e.g., CompositeStorageClient).
        """
        pass

    @abstractmethod
    def delete(self, path: str, recursive: bool = False) -> None:
        """
        Delete a file or directory at the specified path.

        :param path: The logical path of the object to delete.
        :param recursive: When True, delete directory and all its contents recursively.
        :raises FileNotFoundError: If the file or directory does not exist.
        :raises NotImplementedError: If delete operations are not supported (e.g., CompositeStorageClient).
        """
        pass

    @abstractmethod
    def copy(self, src_path: str, dest_path: str) -> None:
        """
        Copy a file from source path to destination path.

        :param src_path: The logical path of the source object.
        :param dest_path: The logical path where the object will be copied to.
        :raises FileNotFoundError: If the source file does not exist.
        :raises NotImplementedError: If copy operations are not supported (e.g., CompositeStorageClient).
        """
        pass

    @abstractmethod
    def upload_file(
        self,
        remote_path: str,
        local_path: Union[str, IO],
        attributes: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Upload a local file to remote storage.

        :param remote_path: The logical path where the file will be uploaded.
        :param local_path: The local file path or file-like object to upload.
        :param attributes: Optional attributes to add to the file.
        :raises FileNotFoundError: If the local file does not exist.
        :raises NotImplementedError: If upload operations are not supported (e.g., CompositeStorageClient).
        """
        pass

    @abstractmethod
    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        """
        Commits any pending updates to the metadata provider. No-op if not using a metadata provider.

        :param prefix: If provided, scans the prefix to find files to commit.
        """
        pass

    @abstractmethod
    def sync_from(
        self,
        source_client: "AbstractStorageClient",
        source_path: str = "",
        target_path: str = "",
        delete_unmatched_files: bool = False,
        description: str = "Syncing",
        num_worker_processes: Optional[int] = None,
        execution_mode: ExecutionMode = ExecutionMode.LOCAL,
        patterns: Optional[PatternList] = None,
        preserve_source_attributes: bool = False,
        follow_symlinks: bool = True,
        source_files: Optional[List[str]] = None,
        ignore_hidden: bool = True,
    ) -> None:
        """
        Syncs files from the source storage client to "path/".

        :param source_client: The source storage client.
        :param source_path: The logical path to sync from.
        :param target_path: The logical path to sync to.
        :param delete_unmatched_files: Whether to delete files at the target that are not present at the source.
        :param description: Description of sync process for logging purposes.
        :param num_worker_processes: The number of worker processes to use.
        :param execution_mode: The execution mode to use. Currently supports "local" and "ray".
        :param patterns: PatternList for include/exclude filtering. If None, all files are included.
            Cannot be used together with source_files.
        :param preserve_source_attributes: Whether to preserve source file metadata attributes during synchronization.
            When ``False`` (default), only file content is copied. When ``True``, custom metadata attributes are also preserved.

            .. warning::
                **Performance Impact**: When enabled without a ``metadata_provider`` configured, this will make a HEAD
                request for each object to retrieve attributes, which can significantly impact performance on large-scale
                sync operations. For production use at scale, configure a ``metadata_provider`` in your storage profile.

        :param follow_symlinks: If the source StorageClient is PosixFile, whether to follow symbolic links. Default is ``True``.
        :param source_files: Optional list of file paths (relative to source_path) to sync. When provided, only these
            specific files will be synced, skipping enumeration of the source path. Cannot be used together with patterns.
        :param ignore_hidden: Whether to ignore hidden files and directories. Default is ``True``.
        :raises ValueError: If both source_files and patterns are provided.
        :raises NotImplementedError: If sync operations are not supported (e.g., CompositeStorageClient as target).
        """
        pass

    @abstractmethod
    def sync_replicas(
        self,
        source_path: str,
        replica_indices: Optional[List[int]] = None,
        delete_unmatched_files: bool = False,
        description: str = "Syncing replica",
        num_worker_processes: Optional[int] = None,
        execution_mode: ExecutionMode = ExecutionMode.LOCAL,
        patterns: Optional[PatternList] = None,
        ignore_hidden: bool = True,
    ) -> None:
        """
        Sync files from this client to its replica storage clients.

        :param source_path: The logical path to sync from.
        :param replica_indices: Specific replica indices to sync to (0-indexed). If None, syncs to all replicas.
        :param delete_unmatched_files: When set to ``True``, delete files in replicas that don't exist in source.
        :param description: Description of sync process for logging purposes.
        :param num_worker_processes: Number of worker processes for parallel sync.
        :param execution_mode: Execution mode (LOCAL or REMOTE).
        :param patterns: PatternList for include/exclude filtering. If None, all files are included.
        :param ignore_hidden: When set to ``True``, ignore hidden files (starting with '.'). Defaults to ``True``.
        """
        pass
