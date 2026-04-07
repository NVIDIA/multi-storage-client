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
import contextlib
import logging
import os
import threading
from collections.abc import Iterator
from datetime import datetime, timezone
from io import BytesIO
from pathlib import PurePosixPath
from typing import IO, Any, Optional, Union, cast

from ..config import StorageClientConfig
from ..constants import DEFAULT_SYNC_BATCH_SIZE, MEMORY_LOAD_LIMIT
from ..file import ObjectFile, PosixFile
from ..providers.posix_file import PosixFileStorageProvider
from ..replica_manager import ReplicaManager
from ..retry import retry
from ..sync import SyncManager
from ..types import (
    AWARE_DATETIME_MIN,
    MSC_PROTOCOL,
    ExecutionMode,
    ObjectMetadata,
    PatternList,
    Range,
    Replica,
    ResolvedPathState,
    SignerType,
    SourceVersionCheckMode,
    StorageProvider,
    SyncResult,
)
from ..utils import NullStorageClient, PatternMatcher, join_paths
from .types import AbstractStorageClient

logger = logging.getLogger(__name__)


class SingleStorageClient(AbstractStorageClient):
    """
    Storage client for single-backend configurations.

    Supports full read and write operations against a single storage provider.
    """

    _config: StorageClientConfig
    _storage_provider: StorageProvider
    _metadata_provider_lock: Optional[threading.Lock] = None
    _stop_event: Optional[threading.Event] = None
    _replica_manager: Optional[ReplicaManager] = None

    def __init__(self, config: StorageClientConfig):
        """
        Initialize the :py:class:`SingleStorageClient` with the given configuration.

        :param config: Storage client configuration with storage_provider set
        :raises ValueError: If config has storage_provider_profiles (multi-backend)
        """
        self._initialize_providers(config)
        self._initialize_replicas(config.replicas)

    def _initialize_providers(self, config: StorageClientConfig) -> None:
        if config.storage_provider_profiles:
            raise ValueError(
                "SingleStorageClient requires storage_provider, not storage_provider_profiles. "
                "Use CompositeStorageClient for multi-backend configurations."
            )

        if config.storage_provider is None:
            raise ValueError("SingleStorageClient requires storage_provider to be set.")

        self._config = config
        self._credentials_provider = self._config.credentials_provider
        self._storage_provider = cast(StorageProvider, self._config.storage_provider)
        self._metadata_provider = self._config.metadata_provider
        self._cache_config = self._config.cache_config
        self._retry_config = self._config.retry_config
        self._cache_manager = self._config.cache_manager
        self._autocommit_config = self._config.autocommit_config

        if self._autocommit_config:
            if self._metadata_provider:
                logger.debug("Creating auto-commiter thread")

                if self._autocommit_config.interval_minutes:
                    self._stop_event = threading.Event()
                    self._commit_thread = threading.Thread(
                        target=self._committer_thread,
                        daemon=True,
                        args=(self._autocommit_config.interval_minutes, self._stop_event),
                    )
                    self._commit_thread.start()

                if self._autocommit_config.at_exit:
                    atexit.register(self._commit_on_exit)

                self._metadata_provider_lock = threading.Lock()
            else:
                logger.debug("No metadata provider configured, auto-commit will not be enabled")

    def _initialize_replicas(self, replicas: list[Replica]) -> None:
        """Initialize replica StorageClient instances (facade)."""
        # Import here to avoid circular dependency
        from .client import StorageClient as StorageClientFacade

        # Sort replicas by read_priority, the first one is the primary replica.
        sorted_replicas = sorted(replicas, key=lambda r: r.read_priority)

        replica_clients = []
        for replica in sorted_replicas:
            if self._config._config_dict is None:
                raise ValueError(f"Cannot initialize replica '{replica.replica_profile}' without a config")
            replica_config = StorageClientConfig.from_dict(
                config_dict=self._config._config_dict, profile=replica.replica_profile
            )

            storage_client = StorageClientFacade(config=replica_config)
            replica_clients.append(storage_client)

        self._replicas = replica_clients
        self._replica_manager = ReplicaManager(self) if len(self._replicas) > 0 else None

    def _committer_thread(self, commit_interval_minutes: float, stop_event: threading.Event):
        if not stop_event:
            raise RuntimeError("Stop event not set")

        while not stop_event.is_set():
            # Wait with the ability to exit early
            if stop_event.wait(timeout=commit_interval_minutes * 60):
                break
            logger.debug("Auto-committing to metadata provider")
            self.commit_metadata()

    def _commit_on_exit(self):
        logger.debug("Shutting down, committing metadata one last time...")
        self.commit_metadata()

    def _get_source_version(self, path: str) -> Optional[str]:
        """
        Get etag from metadata provider or storage provider.
        """
        if self._metadata_provider:
            metadata = self._metadata_provider.get_object_metadata(path)
        else:
            metadata = self._storage_provider.get_object_metadata(path)
        return metadata.etag

    def _is_cache_enabled(self) -> bool:
        enabled = self._cache_manager is not None and not self._is_posix_file_storage_provider()
        return enabled

    def _is_posix_file_storage_provider(self) -> bool:
        """
        :return: ``True`` if the storage client is using a POSIX file storage provider, ``False`` otherwise.
        """
        return isinstance(self._storage_provider, PosixFileStorageProvider)

    def _is_rust_client_enabled(self) -> bool:
        """
        :return: ``True`` if the storage provider is using the Rust client, ``False`` otherwise.
        """
        return getattr(self._storage_provider, "_rust_client", None) is not None

    def _read_from_replica_or_primary(self, path: str) -> bytes:
        """
        Read from replica or primary storage provider. Use BytesIO to avoid creating temporary files.
        """
        if self._replica_manager is None:
            raise RuntimeError("Replica manager is not initialized")
        file_obj = BytesIO()
        self._replica_manager.download_from_replica_or_primary(path, file_obj, self._storage_provider)
        return file_obj.getvalue()

    def __del__(self):
        if self._stop_event:
            self._stop_event.set()
            if self._commit_thread.is_alive():
                self._commit_thread.join(timeout=5.0)

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        del state["_credentials_provider"]
        del state["_storage_provider"]
        del state["_metadata_provider"]
        del state["_cache_manager"]

        if "_metadata_provider_lock" in state:
            del state["_metadata_provider_lock"]

        if "_replicas" in state:
            del state["_replicas"]

        # Replica manager could be disabled if it's set to None in the state.
        if "_replica_manager" in state:
            if state["_replica_manager"] is not None:
                del state["_replica_manager"]

        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        config = state["_config"]
        self._initialize_providers(config)

        # Replica manager could be disabled if it's set to None in the state.
        if "_replica_manager" in state and state["_replica_manager"] is None:
            self._replica_manager = None
        else:
            self._initialize_replicas(config.replicas)

        if self._metadata_provider:
            self._metadata_provider_lock = threading.Lock()

    @property
    def profile(self) -> str:
        """
        :return: The profile name of the storage client.
        """
        return self._config.profile

    def is_default_profile(self) -> bool:
        """
        :return: ``True`` if the storage client is using the reserved POSIX profile, ``False`` otherwise.
        """
        return self._config.profile == "__filesystem__"

    @property
    def replicas(self) -> list[AbstractStorageClient]:
        """
        :return: List of replica storage clients, sorted by read priority.
        """
        return self._replicas

    # -- Metadata resolution helpers --

    def _resolve_read_path(self, logical_path: str) -> str:
        """
        Resolve a logical path to its physical storage path for read operations.

        :param logical_path: The user-facing logical path.
        :return: The physical storage path.
        :raises FileNotFoundError: If the file does not exist in the metadata provider.
        """
        assert self._metadata_provider is not None
        resolved = self._metadata_provider.realpath(logical_path)
        if not resolved.exists:
            raise FileNotFoundError(f"The file at path '{logical_path}' was not found by metadata provider.")
        return resolved.physical_path

    def _resolve_write_path(self, logical_path: str) -> str:
        """
        Resolve a logical path to its physical storage path for write operations.

        Checks overwrite policy and generates the physical path via the metadata provider.

        :param logical_path: The user-facing logical path.
        :return: The physical storage path to write to.
        :raises FileExistsError: If the file exists and overwrites are not allowed.
        """
        assert self._metadata_provider is not None
        resolved = self._metadata_provider.realpath(logical_path)
        if resolved.state in (ResolvedPathState.EXISTS, ResolvedPathState.DELETED):
            if not self._metadata_provider.allow_overwrites():
                raise FileExistsError(
                    f"The file at path '{logical_path}' already exists; "
                    f"overwriting is not allowed when using a metadata provider."
                )
            return self._metadata_provider.generate_physical_path(logical_path, for_overwrite=True).physical_path
        return self._metadata_provider.generate_physical_path(logical_path, for_overwrite=False).physical_path

    def _register_written_file(
        self, virtual_path: str, physical_path: str, attributes: Optional[dict[str, str]] = None
    ) -> None:
        """
        Register a written file with the metadata provider.

        Fetches metadata from the storage provider, optionally merges custom attributes,
        and registers the file with the metadata provider.

        Caller must hold ``_metadata_provider_lock`` if thread-safety is required.

        .. note::
            TODO(NGCDP-3016): Handle eventual consistency of Swiftstack, without wait.
        """
        assert self._metadata_provider is not None
        obj_metadata = self._storage_provider.get_object_metadata(physical_path)
        if attributes:
            obj_metadata.metadata = (obj_metadata.metadata or {}) | attributes
        self._metadata_provider.add_file(virtual_path, obj_metadata)

    @retry
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
        if self._metadata_provider:
            path = self._resolve_read_path(path)

        # Handle caching logic
        if self._is_cache_enabled() and self._cache_manager:
            if byte_range:
                # Range request with cache
                try:
                    # Fetch metadata for source version checking (if needed)
                    metadata = None
                    source_version = None
                    if check_source_version == SourceVersionCheckMode.ENABLE:
                        metadata = self._storage_provider.get_object_metadata(path)
                        source_version = metadata.etag
                    elif check_source_version == SourceVersionCheckMode.INHERIT:
                        if self._cache_manager.check_source_version():
                            metadata = self._storage_provider.get_object_metadata(path)
                            source_version = metadata.etag

                    # Optimization: For full-file reads (offset=0, size >= file_size), cache whole file instead of chunking
                    # This avoids creating many small chunks when the user requests the entire file.
                    # Only apply this optimization when metadata is already available (i.e., when version checking is enabled),
                    # to respect the user's choice to disable version checking and avoid extra HEAD requests.
                    if byte_range.offset == 0 and metadata and byte_range.size >= metadata.content_length:
                        full_file_data = self._storage_provider.get_object(path)
                        self._cache_manager.set(path, full_file_data, source_version)
                        return full_file_data[: metadata.content_length]

                    # Use chunk-based caching for partial reads or when optimization doesn't apply
                    data = self._cache_manager.read(
                        key=path,
                        source_version=source_version,
                        byte_range=byte_range,
                        storage_provider=self._storage_provider,
                        source_size=metadata.content_length if metadata else None,
                    )
                    if data is not None:
                        return data
                    # Fallback (should not normally happen)
                    return self._storage_provider.get_object(path, byte_range=byte_range)
                except (FileNotFoundError, Exception):
                    # Fall back to direct read if metadata fetching fails
                    return self._storage_provider.get_object(path, byte_range=byte_range)
            else:
                # Full file read with cache
                # Only fetch source version if check_source_version is enabled
                source_version = None
                if check_source_version == SourceVersionCheckMode.ENABLE:
                    source_version = self._get_source_version(path)
                elif check_source_version == SourceVersionCheckMode.INHERIT:
                    if self._cache_manager.check_source_version():
                        source_version = self._get_source_version(path)

                data = self._cache_manager.read(path, source_version)
                if data is None:
                    if self._replica_manager:
                        data = self._read_from_replica_or_primary(path)
                    else:
                        data = self._storage_provider.get_object(path)
                    self._cache_manager.set(path, data, source_version)
                return data
        elif self._replica_manager:
            # No cache, but replicas available
            return self._read_from_replica_or_primary(path)
        else:
            # No cache, no replicas - direct storage provider read
            return self._storage_provider.get_object(path, byte_range=byte_range)

    def info(self, path: str, strict: bool = True) -> ObjectMetadata:
        """
        Get metadata for a file at the specified path.

        :param path: The logical path of the object.
        :param strict: When ``True``, only return committed metadata. When ``False``, include pending changes.
        :return: ObjectMetadata containing file information (size, last modified, etc.).
        :raises FileNotFoundError: If the file at the specified path does not exist.
        """
        if not path or path == ".":  # empty path or '.' provided by the user
            if self._is_posix_file_storage_provider():
                last_modified = datetime.fromtimestamp(os.path.getmtime("."), tz=timezone.utc)
            else:
                last_modified = AWARE_DATETIME_MIN
            return ObjectMetadata(key="", type="directory", content_length=0, last_modified=last_modified)

        if not self._metadata_provider:
            return self._storage_provider.get_object_metadata(path, strict=strict)

        return self._metadata_provider.get_object_metadata(path, include_pending=not strict)

    @retry
    def download_file(self, remote_path: str, local_path: Union[str, IO]) -> None:
        """
        Download a remote file to a local path or file-like object.

        :param remote_path: The logical path of the remote file to download.
        :param local_path: The local file path or file-like object to write to.
        :raises FileNotFoundError: If the remote file does not exist.
        """
        if self._metadata_provider:
            physical_path = self._resolve_read_path(remote_path)
            metadata = self._metadata_provider.get_object_metadata(remote_path)
            self._storage_provider.download_file(physical_path, local_path, metadata)
        elif self._replica_manager:
            self._replica_manager.download_from_replica_or_primary(remote_path, local_path, self._storage_provider)
        else:
            self._storage_provider.download_file(remote_path, local_path)

    def download_files(self, remote_paths: list[str], local_paths: list[str], max_workers: int = 16) -> None:
        """
        Download multiple remote files to local paths.

        :param remote_paths: List of logical paths of remote files to download.
        :param local_paths: List of local file paths to save the downloaded files to.
        :param max_workers: Maximum number of concurrent download workers (default: 16).
        :raises ValueError: If remote_paths and local_paths have different lengths.
        :raises FileNotFoundError: If any remote file does not exist.
        """
        if len(remote_paths) != len(local_paths):
            raise ValueError("remote_paths and local_paths must have the same length")

        if self._metadata_provider:
            physical_paths = [self._resolve_read_path(rp) for rp in remote_paths]
            self._storage_provider.download_files(physical_paths, local_paths, max_workers)
        elif self._replica_manager:
            for remote_path, local_path in zip(remote_paths, local_paths):
                self.download_file(remote_path, local_path)
        else:
            self._storage_provider.download_files(remote_paths, local_paths, max_workers)

    @retry
    def upload_file(
        self, remote_path: str, local_path: Union[str, IO], attributes: Optional[dict[str, str]] = None
    ) -> None:
        """
        Uploads a file from the local file system to the storage provider.

        :param remote_path: The path where the object will be stored.
        :param local_path: The source file to upload. This can either be a string representing the local
            file path, or a file-like object (e.g., an open file handle).
        :param attributes: The attributes to add to the file if a new file is created.
        """
        virtual_path = remote_path
        if self._metadata_provider:
            physical_path = self._resolve_write_path(remote_path)
            self._storage_provider.upload_file(physical_path, local_path, attributes=None)
            with self._metadata_provider_lock or contextlib.nullcontext():
                self._register_written_file(virtual_path, physical_path, attributes)
        else:
            self._storage_provider.upload_file(remote_path, local_path, attributes)

    def upload_files(self, remote_paths: list[str], local_paths: list[str], max_workers: int = 16) -> None:
        """
        Upload multiple local files to remote storage.

        :param remote_paths: List of logical paths where the files will be uploaded.
        :param local_paths: List of local file paths to upload.
        :param max_workers: Maximum number of concurrent upload workers (default: 16).
        :raises ValueError: If remote_paths and local_paths have different lengths.
        """
        if len(remote_paths) != len(local_paths):
            raise ValueError("remote_paths and local_paths must have the same length")

        if self._metadata_provider:
            physical_paths = [self._resolve_write_path(rp) for rp in remote_paths]
            self._storage_provider.upload_files(local_paths, physical_paths, max_workers)
            with self._metadata_provider_lock or contextlib.nullcontext():
                for virtual_path, physical_path in zip(remote_paths, physical_paths):
                    self._register_written_file(virtual_path, physical_path)
        else:
            self._storage_provider.upload_files(local_paths, remote_paths, max_workers)

    @retry
    def write(self, path: str, body: bytes, attributes: Optional[dict[str, str]] = None) -> None:
        """
        Write bytes to a file at the specified path.

        :param path: The logical path where the object will be written.
        :param body: The content to write as bytes.
        :param attributes: Optional attributes to add to the file.
        """
        virtual_path = path
        if self._metadata_provider:
            physical_path = self._resolve_write_path(path)
            self._storage_provider.put_object(physical_path, body, attributes=None)
            with self._metadata_provider_lock or contextlib.nullcontext():
                self._register_written_file(virtual_path, physical_path, attributes)
        else:
            self._storage_provider.put_object(path, body, attributes=attributes)

    def copy(self, src_path: str, dest_path: str) -> None:
        """
        Copy a file from source path to destination path.

        :param src_path: The logical path of the source object.
        :param dest_path: The logical path where the object will be copied to.
        :raises FileNotFoundError: If the source file does not exist.
        """
        virtual_dest_path = dest_path
        if self._metadata_provider:
            src_path = self._resolve_read_path(src_path)
            dest_path = self._resolve_write_path(dest_path)
            self._storage_provider.copy_object(src_path, dest_path)
            with self._metadata_provider_lock or contextlib.nullcontext():
                self._register_written_file(virtual_dest_path, dest_path)
        else:
            self._storage_provider.copy_object(src_path, dest_path)

    def delete(self, path: str, recursive: bool = False) -> None:
        """
        Deletes an object at the specified path.

        :param path: The logical path of the object or directory to delete.
        :param recursive: Whether to delete objects in the path recursively.
        """
        obj_metadata = self.info(path)
        is_dir = obj_metadata and obj_metadata.type == "directory"
        is_file = obj_metadata and obj_metadata.type == "file"
        if recursive and is_dir:
            self.sync_from(
                cast(AbstractStorageClient, NullStorageClient()),
                path,
                path,
                delete_unmatched_files=True,
                num_worker_processes=1,
                description="Deleting",
            )
            # If this is a posix storage provider, we need to also delete remaining directory stubs.
            # TODO: Notify metadata provider of the changes.
            if self._is_posix_file_storage_provider():
                posix_storage_provider = cast(PosixFileStorageProvider, self._storage_provider)
                posix_storage_provider.rmtree(path)
            return
        else:
            # 1) If path is a file: delete the file
            # 2) If path is a directory: raise an error to prompt the user to use the recursive flag
            if is_file:
                virtual_path = path
                if self._metadata_provider:
                    resolved = self._metadata_provider.realpath(path)
                    if not resolved.exists:
                        raise FileNotFoundError(f"The file at path '{virtual_path}' was not found.")

                    # Check if soft-delete is enabled
                    if not self._metadata_provider.should_use_soft_delete():
                        # Hard delete: remove both physical file and metadata
                        self._storage_provider.delete_object(resolved.physical_path)

                    with self._metadata_provider_lock or contextlib.nullcontext():
                        self._metadata_provider.remove_file(virtual_path)
                else:
                    self._storage_provider.delete_object(path)

                # Delete the cached file if it exists
                if self._is_cache_enabled():
                    if self._cache_manager is None:
                        raise RuntimeError("Cache manager is not initialized")
                    self._cache_manager.delete(virtual_path)

                # Delete from replicas if replica manager exists
                if self._replica_manager:
                    self._replica_manager.delete_from_replicas(virtual_path)
            elif is_dir:
                raise ValueError(f"'{path}' is a directory. Set recursive=True to delete entire directory.")
            else:
                raise FileNotFoundError(f"The file at '{path}' was not found.")

    def delete_many(self, paths: list[str]) -> None:
        """
        Delete multiple files at the specified paths. Only files are supported; directories are not deleted.

        :param paths: List of logical paths of the files to delete.
        """
        physical_paths_to_delete: list[str] = []
        for path in paths:
            if self._metadata_provider:
                resolved = self._metadata_provider.realpath(path)
                if not resolved.exists:
                    raise FileNotFoundError(f"The file at path '{path}' was not found.")
                if not self._metadata_provider.should_use_soft_delete():
                    physical_paths_to_delete.append(resolved.physical_path)
            else:
                physical_paths_to_delete.append(path)

        if physical_paths_to_delete:
            self._storage_provider.delete_objects(physical_paths_to_delete)

        for path in paths:
            virtual_path = path
            if self._metadata_provider:
                with self._metadata_provider_lock or contextlib.nullcontext():
                    self._metadata_provider.remove_file(virtual_path)
            if self._is_cache_enabled():
                if self._cache_manager is None:
                    raise RuntimeError("Cache manager is not initialized")
                self._cache_manager.delete(virtual_path)
            if self._replica_manager:
                self._replica_manager.delete_from_replicas(virtual_path)

    def glob(
        self,
        pattern: str,
        include_url_prefix: bool = False,
        attribute_filter_expression: Optional[str] = None,
    ) -> list[str]:
        """
        Matches and retrieves a list of object keys in the storage provider that match the specified pattern.

        :param pattern: The pattern to match object keys against, supporting wildcards (e.g., ``*.txt``).
        :param include_url_prefix: Whether to include the URL prefix ``msc://profile`` in the result.
        :param attribute_filter_expression: The attribute filter expression to apply to the result.
        :return: A list of object paths that match the specified pattern.
        """
        if self._metadata_provider:
            results = self._metadata_provider.glob(pattern, attribute_filter_expression)
        else:
            results = self._storage_provider.glob(pattern, attribute_filter_expression)

        if include_url_prefix:
            results = [join_paths(f"{MSC_PROTOCOL}{self._config.profile}", path) for path in results]

        return results

    def _resolve_single_file(
        self,
        path: str,
        start_after: Optional[str],
        end_at: Optional[str],
        include_url_prefix: bool,
        pattern_matcher: Optional[PatternMatcher],
    ) -> tuple[Optional[ObjectMetadata], Optional[str]]:
        """
        Resolve whether ``path`` should be handled as a single-file listing result.

        :param path: Candidate file path or directory prefix to resolve.
        :param start_after: Exclusive lower bound for file key filtering.
        :param end_at: Inclusive upper bound for file key filtering.
        :param include_url_prefix: Whether to prefix returned keys with ``msc://profile``.
        :param pattern_matcher: Optional include/exclude matcher for file keys.
        :return: A tuple of ``(single_file, normalized_path)``. Returns file metadata and
                 the original path when ``path`` resolves to a file that passes filters;
                 returns ``(None, normalized_directory_path)`` when the caller should
                 continue with directory listing; returns ``(None, None)`` when filtering
                 excludes the single-file candidate and listing should stop.
        """
        if not path:
            return None, path

        if self.is_file(path):
            if pattern_matcher and not pattern_matcher.should_include_file(path):
                return None, None

            try:
                object_metadata = self.info(path)
                if start_after and object_metadata.key <= start_after:
                    return None, None
                if end_at and object_metadata.key > end_at:
                    return None, None
                if include_url_prefix:
                    self._prepend_url_prefix(object_metadata)
                return object_metadata, path
            except FileNotFoundError:
                return None, path.rstrip("/") + "/"
        else:
            return None, path.rstrip("/") + "/"

    def _prepend_url_prefix(self, obj: ObjectMetadata) -> None:
        if self.is_default_profile():
            obj.key = str(PurePosixPath("/") / obj.key)
        else:
            obj.key = join_paths(f"{MSC_PROTOCOL}{self._config.profile}", obj.key)

    def _filter_and_decorate(
        self,
        objects: Iterator[ObjectMetadata],
        include_url_prefix: bool,
        pattern_matcher: Optional[PatternMatcher],
    ) -> Iterator[ObjectMetadata]:
        for obj in objects:
            if pattern_matcher and not pattern_matcher.should_include_file(obj.key):
                continue
            if include_url_prefix:
                self._prepend_url_prefix(obj)
            yield obj

    def list_recursive(
        self,
        path: str = "",
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        max_workers: int = 32,
        look_ahead: int = 2,
        include_url_prefix: bool = False,
        follow_symlinks: bool = True,
        patterns: Optional[PatternList] = None,
    ) -> Iterator[ObjectMetadata]:
        """
        List files recursively in the storage provider under the specified path.

        :param path: The directory or file path to list objects under. This should be a
                    complete filesystem path (e.g., "my-bucket/documents/" or "data/2024/").
        :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
        :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.
        :param max_workers: Maximum concurrent workers for provider-level recursive listing.
        :param look_ahead: Prefixes to buffer per worker for provider-level recursive listing.
        :param include_url_prefix: Whether to include the URL prefix ``msc://profile`` in the result.
        :param follow_symlinks: Whether to follow symbolic links. Only applicable for POSIX file storage providers. When ``False``, symlinks are skipped during listing.
        :param patterns: PatternList for include/exclude filtering. If None, all files are included.
        :return: An iterator over ObjectMetadata for matching files.
        """
        pattern_matcher = PatternMatcher(patterns) if patterns else None

        single_file, effective_path = self._resolve_single_file(
            path, start_after, end_at, include_url_prefix, pattern_matcher
        )
        if single_file is not None:
            yield single_file
            return
        if effective_path is None:
            return

        if self._metadata_provider:
            objects = self._metadata_provider.list_objects(
                effective_path,
                start_after=start_after,
                end_at=end_at,
                include_directories=False,
            )
        else:
            objects = self._storage_provider.list_objects_recursive(
                effective_path,
                start_after=start_after,
                end_at=end_at,
                max_workers=max_workers,
                look_ahead=look_ahead,
                follow_symlinks=follow_symlinks,
            )

        yield from self._filter_and_decorate(objects, include_url_prefix, pattern_matcher)

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
        """
        if self._is_posix_file_storage_provider():
            return PosixFile(
                self, path=path, mode=mode, buffering=buffering, encoding=encoding, atomic=atomic, attributes=attributes
            )
        else:
            if atomic is False:
                logger.warning("Non-atomic writes are not supported for object storage providers.")

            return ObjectFile(
                self,
                remote_path=path,
                mode=mode,
                encoding=encoding,
                disable_read_cache=disable_read_cache,
                memory_load_limit=memory_load_limit,
                check_source_version=check_source_version,
                attributes=attributes,
                prefetch_file=prefetch_file,
            )

    def get_posix_path(self, path: str) -> Optional[str]:
        """
        Returns the physical POSIX filesystem path for POSIX storage providers.

        :param path: The path to resolve (may be a symlink or virtual path).
        :return: Physical POSIX filesystem path if POSIX storage, None otherwise.
        """
        if not self._is_posix_file_storage_provider():
            return None

        if self._metadata_provider:
            resolved = self._metadata_provider.realpath(path)
            realpath = resolved.physical_path
        else:
            realpath = path

        return cast(PosixFileStorageProvider, self._storage_provider)._prepend_base_path(realpath)

    def is_file(self, path: str) -> bool:
        """
        Checks whether the specified path points to a file (rather than a folder or directory).

        :param path: The logical path to check.
        :return: ``True`` if the key points to a file, ``False`` otherwise.
        """
        if self._metadata_provider:
            resolved = self._metadata_provider.realpath(path)
            return resolved.exists

        return self._storage_provider.is_file(path)

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        """
        Commits any pending updates to the metadata provider. No-op if not using a metadata provider.

        :param prefix: If provided, scans the prefix to find files to commit.
        """
        if self._metadata_provider:
            with self._metadata_provider_lock or contextlib.nullcontext():
                if prefix:
                    base_resolved = self._metadata_provider.generate_physical_path("")
                    physical_base = base_resolved.physical_path

                    prefix_resolved = self._metadata_provider.generate_physical_path(prefix)
                    physical_prefix = prefix_resolved.physical_path

                    for obj in self._storage_provider.list_objects(physical_prefix):
                        virtual_path = obj.key[len(physical_base) :].lstrip("/")
                        self._metadata_provider.add_file(virtual_path, obj)
                self._metadata_provider.commit_updates()

    def is_empty(self, path: str) -> bool:
        """
        Check whether the specified path is empty. A path is considered empty if there are no
        objects whose keys start with the given path as a prefix.

        :param path: The logical path to check (typically a directory or folder prefix).
        :return: ``True`` if no objects exist under the specified path prefix, ``False`` otherwise.
        """
        if self._metadata_provider:
            objects = self._metadata_provider.list_objects(path)
        else:
            objects = self._storage_provider.list_objects(path)

        try:
            return next(objects) is None
        except StopIteration:
            pass

        return True

    def sync_from(
        self,
        source_client: AbstractStorageClient,
        source_path: str = "",
        target_path: str = "",
        delete_unmatched_files: bool = False,
        description: str = "Syncing",
        num_worker_processes: Optional[int] = None,
        execution_mode: ExecutionMode = ExecutionMode.LOCAL,
        patterns: Optional[PatternList] = None,
        preserve_source_attributes: bool = False,
        follow_symlinks: bool = True,
        source_files: Optional[list[str]] = None,
        ignore_hidden: bool = True,
        commit_metadata: bool = True,
        dryrun: bool = False,
        dryrun_output_path: Optional[str] = None,
    ) -> SyncResult:
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
        :param commit_metadata: When ``True`` (default), calls :py:meth:`StorageClient.commit_metadata` after sync completes.
            Set to ``False`` to skip the commit, allowing batching of multiple sync operations before committing manually.
        :param dryrun: If ``True``, only enumerate and compare objects without performing any copy/delete operations.
            The returned :py:class:`SyncResult` will include a :py:class:`DryrunResult` with paths to JSONL files.
        :param dryrun_output_path: Directory to write dryrun JSONL files into. If ``None`` (default), a temporary
            directory is created automatically. Ignored when ``dryrun`` is ``False``.
        :raises ValueError: If both source_files and patterns are provided.
        :raises RuntimeError: If errors occur during sync operations. The sync will stop on first error (fail-fast).
        """
        if source_files and patterns:
            raise ValueError("Cannot specify both 'source_files' and 'patterns'. Please use only one filtering method.")

        pattern_matcher = PatternMatcher(patterns) if patterns else None

        # Disable the replica manager during sync
        if not isinstance(source_client, NullStorageClient) and source_client._replica_manager:
            # Import here to avoid circular dependency
            from .client import StorageClient as StorageClientFacade

            source_client = StorageClientFacade(source_client._config)
            source_client._replica_manager = None

        m = SyncManager(source_client, source_path, self, target_path)
        batch_size = int(os.environ.get("MSC_SYNC_BATCH_SIZE", DEFAULT_SYNC_BATCH_SIZE))

        return m.sync_objects(
            execution_mode=execution_mode,
            description=description,
            num_worker_processes=num_worker_processes,
            delete_unmatched_files=delete_unmatched_files,
            pattern_matcher=pattern_matcher,
            preserve_source_attributes=preserve_source_attributes,
            follow_symlinks=follow_symlinks,
            source_files=source_files,
            ignore_hidden=ignore_hidden,
            commit_metadata=commit_metadata,
            batch_size=batch_size,
            dryrun=dryrun,
            dryrun_output_path=dryrun_output_path,
        )

    def sync_replicas(
        self,
        source_path: str,
        replica_indices: Optional[list[int]] = None,
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
        if not self._replicas:
            logger.warning(
                "No replicas found in profile '%s'. Add a 'replicas' section to your profile configuration to enable "
                "secondary storage locations for redundancy and performance.",
                self._config.profile,
            )
            return None

        if replica_indices:
            try:
                replicas = [self._replicas[i] for i in replica_indices]
            except IndexError as e:
                raise ValueError(f"Replica index out of range: {replica_indices}") from e
        else:
            replicas = self._replicas

        # Disable the replica manager during sync
        if self._replica_manager:
            # Import here to avoid circular dependency
            from .client import StorageClient as StorageClientFacade

            source_client = StorageClientFacade(self._config)
            source_client._replica_manager = None
        else:
            source_client = self

        for replica in replicas:
            replica.sync_from(
                source_client,
                source_path,
                source_path,
                delete_unmatched_files=delete_unmatched_files,
                description=f"{description} ({replica.profile})",
                num_worker_processes=num_worker_processes,
                execution_mode=execution_mode,
                patterns=patterns,
                ignore_hidden=ignore_hidden,
            )

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
        patterns: Optional[PatternList] = None,
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
        :param include_directories: Whether to include directories in the result. When ``True``, directories are returned alongside objects.
        :param include_url_prefix: Whether to include the URL prefix ``msc://profile`` in the result.
        :param attribute_filter_expression: The attribute filter expression to apply to the result.
        :param show_attributes: Whether to return attributes in the result. WARNING: Depending on implementation, there may be a performance impact if this is set to ``True``.
        :param follow_symlinks: Whether to follow symbolic links. Only applicable for POSIX file storage providers. When ``False``, symlinks are skipped during listing.
        :param patterns: PatternList for include/exclude filtering. If None, all files are included.
        :return: An iterator over ObjectMetadata for matching objects.
        :raises ValueError: If both ``path`` and ``prefix`` parameters are provided (both non-empty).
        """
        # Parameter validation - either path or prefix, not both
        if path and prefix:
            raise ValueError(
                f"Cannot specify both 'path' ({path!r}) and 'prefix' ({prefix!r}). "
                f"Please use only the 'path' parameter for new code. "
                f"Migration guide: Replace list(prefix={prefix!r}) with list(path={prefix!r})"
            )
        elif prefix:
            logger.debug(
                f"The 'prefix' parameter is deprecated and will be removed in a future version. "
                f"Please use the 'path' parameter instead. "
                f"Migration guide: Replace list(prefix={prefix!r}) with list(path={prefix!r})"
            )

        pattern_matcher = PatternMatcher(patterns) if patterns else None
        effective_path = path if path else prefix

        single_file, effective_path = self._resolve_single_file(
            effective_path, start_after, end_at, include_url_prefix, pattern_matcher
        )
        if single_file is not None:
            yield single_file
            return
        if effective_path is None:
            return

        if self._metadata_provider:
            objects = self._metadata_provider.list_objects(
                effective_path,
                start_after=start_after,
                end_at=end_at,
                include_directories=include_directories,
                attribute_filter_expression=attribute_filter_expression,
                show_attributes=show_attributes,
            )
        else:
            objects = self._storage_provider.list_objects(
                effective_path,
                start_after=start_after,
                end_at=end_at,
                include_directories=include_directories,
                attribute_filter_expression=attribute_filter_expression,
                show_attributes=show_attributes,
                follow_symlinks=follow_symlinks,
            )

        yield from self._filter_and_decorate(objects, include_url_prefix, pattern_matcher)

    def generate_presigned_url(
        self,
        path: str,
        *,
        method: str = "GET",
        signer_type: Optional[SignerType] = None,
        signer_options: Optional[dict[str, Any]] = None,
    ) -> str:
        return self._storage_provider.generate_presigned_url(
            path, method=method, signer_type=signer_type, signer_options=signer_options
        )
