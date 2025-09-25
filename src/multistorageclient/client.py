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
from typing import IO, Any, List, Optional, Union, cast

from .config import StorageClientConfig
from .constants import MEMORY_LOAD_LIMIT
from .file import ObjectFile, PosixFile
from .instrumentation.utils import instrumented
from .providers.posix_file import PosixFileStorageProvider
from .replica_manager import ReplicaManager
from .retry import retry
from .sync import SyncManager
from .types import (
    AWARE_DATETIME_MIN,
    MSC_PROTOCOL,
    ExecutionMode,
    ObjectMetadata,
    Range,
    Replica,
    SourceVersionCheckMode,
)
from .utils import NullStorageClient, join_paths

logger = logging.getLogger(__name__)


@instrumented
class StorageClient:
    """
    A client for interacting with different storage providers.
    """

    _config: StorageClientConfig
    _metadata_provider_lock: Optional[threading.Lock] = None
    _stop_event: Optional[threading.Event] = None
    _replica_manager: Optional[ReplicaManager] = None

    def __init__(self, config: StorageClientConfig):
        """
        Initializes the :py:class:`StorageClient` with the given configuration.

        :param config: The configuration object for the storage client.
        """
        self._initialize_providers(config)
        self._initialize_replicas(config.replicas)

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

    def _initialize_providers(self, config: StorageClientConfig) -> None:
        self._config = config
        self._credentials_provider = self._config.credentials_provider
        self._storage_provider = self._config.storage_provider
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

    def __del__(self):
        if self._stop_event:
            self._stop_event.set()
            if self._commit_thread.is_alive():
                self._commit_thread.join(timeout=5.0)

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
        return isinstance(self._storage_provider, PosixFileStorageProvider)

    def is_default_profile(self) -> bool:
        """
        Return True if the storage client is using the default profile.
        """
        return self._config.profile == "default"

    def _is_rust_client_enabled(self) -> bool:
        """
        Return True if the storage provider is using the Rust client.
        """
        return hasattr(self._storage_provider, "_rust_client")

    @property
    def profile(self) -> str:
        return self._config.profile

    def _initialize_replicas(self, replicas: list[Replica]) -> None:
        """Initialize replica StorageClient instances."""
        # Sort replicas by read_priority, the first one is the primary replica
        sorted_replicas = sorted(replicas, key=lambda r: r.read_priority)

        replica_clients = []
        for replica in sorted_replicas:
            if self._config._config_dict is None:
                raise ValueError(f"Cannot initialize replica '{replica.replica_profile}' without a config")
            replica_config = StorageClientConfig.from_dict(
                config_dict=self._config._config_dict, profile=replica.replica_profile
            )

            storage_client = StorageClient(config=replica_config)
            replica_clients.append(storage_client)

        self._replicas = replica_clients
        self._replica_manager = ReplicaManager(self) if len(self._replicas) > 0 else None

    @property
    def replicas(self) -> list["StorageClient"]:
        """Return StorageClient instances for all replicas, sorted by read priority."""
        return self._replicas

    @retry
    def read(
        self,
        path: str,
        byte_range: Optional[Range] = None,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
    ) -> bytes:
        """
        Reads an object from the specified logical path.

        :param path: The logical path of the object to read.
        :param byte_range: Optional byte range to read.
        :param check_source_version: Whether to check the source version of cached objects.
        :param size: Optional file size for range reads.
        :return: The content of the object.
        """
        if self._metadata_provider:
            path, exists = self._metadata_provider.realpath(path)
            if not exists:
                raise FileNotFoundError(f"The file at path '{path}' was not found.")

        # Handle caching logic
        if self._is_cache_enabled() and self._cache_manager:
            if byte_range:
                # Range request with cache
                try:
                    if check_source_version == SourceVersionCheckMode.ENABLE:
                        metadata = self._storage_provider.get_object_metadata(path)
                        source_version = metadata.etag
                    elif check_source_version == SourceVersionCheckMode.INHERIT:
                        if self._cache_manager.check_source_version():
                            metadata = self._storage_provider.get_object_metadata(path)
                            source_version = metadata.etag
                        else:
                            metadata = None
                            source_version = None
                    else:
                        metadata = None
                        source_version = None

                    data = self._cache_manager.read(
                        key=path,
                        source_version=source_version,
                        byte_range=byte_range,
                        storage_provider=self._storage_provider,
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
                source_version = self._get_source_version(path)
                data = self._cache_manager.read(path, source_version)

        # Read from cache if the file exists
        if self._is_cache_enabled():
            if self._cache_manager is None:
                raise RuntimeError("Cache manager is not initialized")
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

    def _read_from_replica_or_primary(self, path: str) -> bytes:
        """
        Read from replica or primary storage provider. Use BytesIO to avoid creating temporary files.
        """
        if self._replica_manager is None:
            raise RuntimeError("Replica manager is not initialized")
        file_obj = BytesIO()
        self._replica_manager.download_from_replica_or_primary(path, file_obj, self._storage_provider)
        return file_obj.getvalue()

    def info(self, path: str, strict: bool = True) -> ObjectMetadata:
        """
        Retrieves metadata or information about an object stored at the specified path.

        :param path: The logical path to the object for which metadata or information is being retrieved.
        :param strict: If True, performs additional validation to determine whether the path refers to a directory.

        :return: A dictionary containing metadata about the object.
        """

        if not path or path == ".":  # for the empty path provided by the user
            if self._is_posix_file_storage_provider():
                last_modified = datetime.fromtimestamp(os.path.getmtime("."), tz=timezone.utc)
            else:
                last_modified = AWARE_DATETIME_MIN
            return ObjectMetadata(key="", type="directory", content_length=0, last_modified=last_modified)

        if not self._metadata_provider:
            return self._storage_provider.get_object_metadata(path, strict=strict)

        # For metadata_provider, first check if the path exists as a file, then fallback to detecting if path is a directory.
        try:
            return self._metadata_provider.get_object_metadata(path)
        except FileNotFoundError:
            # Try listing from the parent to determine if path is a valid directory
            parent = os.path.dirname(path.rstrip("/")) + "/"
            parent = "" if parent == "/" else parent
            target = path.rstrip("/") + "/"

            try:
                entries = self._metadata_provider.list_objects(parent, include_directories=True)
                for entry in entries:
                    if entry.key == target and entry.type == "directory":
                        return entry
            except Exception:
                pass
            raise  # Raise original FileNotFoundError

    @retry
    def download_file(self, remote_path: str, local_path: Union[str, IO]) -> None:
        """
        Downloads a file to the local file system.

        :param remote_path: The logical path of the file in the storage provider.
        :param local_path: The local path where the file should be downloaded.
        """

        if self._metadata_provider:
            real_path, exists = self._metadata_provider.realpath(remote_path)
            if not exists:
                raise FileNotFoundError(f"The file at path '{remote_path}' was not found by metadata provider.")
            metadata = self._metadata_provider.get_object_metadata(remote_path)
            self._storage_provider.download_file(real_path, local_path, metadata)
        elif self._replica_manager:
            self._replica_manager.download_from_replica_or_primary(remote_path, local_path, self._storage_provider)
        else:
            self._storage_provider.download_file(remote_path, local_path)

    @retry
    def upload_file(self, remote_path: str, local_path: str, attributes: Optional[dict[str, str]] = None) -> None:
        """
        Uploads a file from the local file system.

        :param remote_path: The logical path where the file should be stored.
        :param local_path: The local path of the file to upload.
        :param attributes: The attributes to add to the file.
        """
        virtual_path = remote_path
        if self._metadata_provider:
            remote_path, exists = self._metadata_provider.realpath(remote_path)
            if exists:
                raise FileExistsError(
                    f"The file at path '{virtual_path}' already exists; "
                    f"overwriting is not yet allowed when using a metadata provider."
                )
        if self._metadata_provider:
            # if metdata provider is present, we only write attributes to the metadata provider
            self._storage_provider.upload_file(remote_path, local_path, attributes=None)
            obj_metadata = self._storage_provider.get_object_metadata(remote_path)
            obj_metadata.metadata = (obj_metadata.metadata or {}) | (attributes or {})
            with self._metadata_provider_lock or contextlib.nullcontext():
                self._metadata_provider.add_file(virtual_path, obj_metadata)
        else:
            self._storage_provider.upload_file(remote_path, local_path, attributes)

    @retry
    def write(self, path: str, body: bytes, attributes: Optional[dict[str, str]] = None) -> None:
        """
        Writes an object at the specified path.

        :param path: The logical path where the object should be written.
        :param body: The content to write to the object.
        :param attributes: The attributes to add to the file.
        """
        virtual_path = path
        if self._metadata_provider:
            path, exists = self._metadata_provider.realpath(path)
            if exists:
                raise FileExistsError(
                    f"The file at path '{virtual_path}' already exists; "
                    f"overwriting is not yet allowed when using a metadata provider."
                )
        if self._metadata_provider:
            # if metadata provider is present, we only write attributes to the metadata provider
            self._storage_provider.put_object(path, body, attributes=None)
            # TODO(NGCDP-3016): Handle eventual consistency of Swiftstack, without wait.
            obj_metadata = self._storage_provider.get_object_metadata(path)
            obj_metadata.metadata = (obj_metadata.metadata or {}) | (attributes or {})
            with self._metadata_provider_lock or contextlib.nullcontext():
                self._metadata_provider.add_file(virtual_path, obj_metadata)
        else:
            self._storage_provider.put_object(path, body, attributes=attributes)

    def copy(self, src_path: str, dest_path: str) -> None:
        """
        Copies an object from source to destination path.

        :param src_path: The logical path of the source object to copy.
        :param dest_path: The logical path of the destination.
        """
        virtual_dest_path = dest_path
        if self._metadata_provider:
            src_path, exists = self._metadata_provider.realpath(src_path)
            if not exists:
                raise FileNotFoundError(f"The file at path '{src_path}' was not found.")

            dest_path, exists = self._metadata_provider.realpath(dest_path)
            if exists:
                raise FileExistsError(
                    f"The file at path '{virtual_dest_path}' already exists; "
                    f"overwriting is not yet allowed when using a metadata provider."
                )

        self._storage_provider.copy_object(src_path, dest_path)
        if self._metadata_provider:
            metadata = self._storage_provider.get_object_metadata(dest_path)
            with self._metadata_provider_lock or contextlib.nullcontext():
                self._metadata_provider.add_file(virtual_dest_path, metadata)

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
                NullStorageClient(),
                path,
                path,
                delete_unmatched_files=True,
                num_worker_processes=1,
                description="Deleting",
            )
            # If this is a posix storage provider, we need to also delete remaining directory stubs.
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
                    path, exists = self._metadata_provider.realpath(path)
                    if not exists:
                        raise FileNotFoundError(f"The file at path '{virtual_path}' was not found.")
                    with self._metadata_provider_lock or contextlib.nullcontext():
                        self._metadata_provider.remove_file(virtual_path)

                # Delete the cached file if it exists
                if self._is_cache_enabled():
                    if self._cache_manager is None:
                        raise RuntimeError("Cache manager is not initialized")
                    self._cache_manager.delete(virtual_path)
                self._storage_provider.delete_object(path)

                # Delete from replicas if replica manager exists
                if self._replica_manager:
                    self._replica_manager.delete_from_replicas(virtual_path)
            elif is_dir:
                raise ValueError(f"'{path}' is a directory. Set recursive=True to delete entire directory.")
            else:
                raise FileNotFoundError(f"The file at '{path}' was not found.")

    def glob(
        self,
        pattern: str,
        include_url_prefix: bool = False,
        attribute_filter_expression: Optional[str] = None,
    ) -> list[str]:
        """
        Matches and retrieves a list of objects in the storage provider that
        match the specified pattern.

        :param pattern: The pattern to match object paths against, supporting wildcards (e.g., ``*.txt``).
        :param include_url_prefix: Whether to include the URL prefix ``msc://profile`` in the result.
        :param attribute_filter_expression: The attribute filter expression to apply to the result.

        :return: A list of object paths that match the pattern.
        """
        if self._metadata_provider:
            results = self._metadata_provider.glob(pattern, attribute_filter_expression)
        else:
            results = self._storage_provider.glob(pattern, attribute_filter_expression)

        if include_url_prefix:
            results = [join_paths(f"{MSC_PROTOCOL}{self._config.profile}", path) for path in results]

        return results

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
    ) -> Iterator[ObjectMetadata]:
        """
        Lists objects in the storage provider under the specified path.

        **IMPORTANT**: Use the ``path`` parameter for new code. The ``prefix`` parameter is
        deprecated and will be removed in a future version.

        :param prefix: [DEPRECATED] Use ``path`` instead. The prefix to list objects under.
        :param path: The directory or file path to list objects under. This should be a
                    complete filesystem path (e.g., "my-bucket/documents/" or "data/2024/").
                    Cannot be used together with ``prefix``.
        :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
        :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.
        :param include_directories: Whether to include directories in the result. When True, directories are returned alongside objects.
        :param include_url_prefix: Whether to include the URL prefix ``msc://profile`` in the result.
        :param attribute_filter_expression: The attribute filter expression to apply to the result.
        :param show_attributes: Whether to return attributes in the result.  WARNING: Depend on implementation, there might be performance impact if this set to True.

        :return: An iterator over objects.

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

        # Use path if provided, otherwise fall back to prefix
        effective_path = path if path else prefix
        if effective_path and not self.is_file(effective_path):
            effective_path = (
                effective_path.rstrip("/") + "/"
            )  # assume it's a directory if the effective path is not empty

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
            )

        for object in objects:
            if include_url_prefix:
                if self.is_default_profile():
                    object.key = str(PurePosixPath("/") / object.key)
                else:
                    object.key = join_paths(f"{MSC_PROTOCOL}{self._config.profile}", object.key)
            yield object

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
        Returns a file-like object from the specified path.

        :param path: The logical path of the object to read.
        :param mode: The file mode, only "w", "r", "a", "wb", "rb" and "ab" are supported.
        :param buffering: The buffering mode. Only applies to PosixFile.
        :param encoding: The encoding to use for text files.
        :param disable_read_cache: When set to True, disables caching for the file content.
            This parameter is only applicable to ObjectFile when the mode is "r" or "rb".
        :param memory_load_limit: Size limit in bytes for loading files into memory. Defaults to 512MB.
            This parameter is only applicable to ObjectFile when the mode is "r" or "rb".
        :param atomic: When set to True, the file will be written atomically (rename upon close).
            This parameter is only applicable to PosixFile in write mode.
        :param check_source_version: Whether to check the source version of cached objects.
        :param attributes: The attributes to add to the file.  This parameter is only applicable when the mode is "w" or "wb" or "a" or "ab".
        :return: A file-like object (PosixFile or ObjectFile) for the specified path.
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

    def is_file(self, path: str) -> bool:
        """
        Checks whether the specified path points to a file (rather than a directory or folder).

        :param path: The logical path to check.

        :return: ``True`` if the path points to a file, ``False`` otherwise.
        """
        if self._metadata_provider:
            _, exists = self._metadata_provider.realpath(path)
            return exists
        return self._storage_provider.is_file(path)

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        """
        Commits any pending updates to the metadata provider. No-op if not using a metadata provider.

        :param prefix: If provided, scans the prefix to find files to commit.
        """
        if self._metadata_provider:
            with self._metadata_provider_lock or contextlib.nullcontext():
                if prefix:
                    # The logical path for each item will be the physical path with
                    # the base physical path removed from the beginning.
                    physical_base, _ = self._metadata_provider.realpath("")
                    physical_prefix, _ = self._metadata_provider.realpath(prefix)
                    for obj in self._storage_provider.list_objects(physical_prefix):
                        virtual_path = obj.key[len(physical_base) :].lstrip("/")
                        self._metadata_provider.add_file(virtual_path, obj)
                self._metadata_provider.commit_updates()

    def is_empty(self, path: str) -> bool:
        """
        Checks whether the specified path is empty. A path is considered empty if there are no
        objects whose keys start with the given path as a prefix.

        :param path: The logical path to check. This is typically a prefix representing a directory or folder.

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

    def sync_from(
        self,
        source_client: "StorageClient",
        source_path: str = "",
        target_path: str = "",
        delete_unmatched_files: bool = False,
        description: str = "Syncing",
        num_worker_processes: Optional[int] = None,
        execution_mode: ExecutionMode = ExecutionMode.LOCAL,
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
        """
        # Disable the replica manager during sync
        if not isinstance(source_client, NullStorageClient) and source_client._replica_manager:
            source_client = StorageClient(source_client._config)
            source_client._replica_manager = None

        m = SyncManager(source_client, source_path, self, target_path)

        m.sync_objects(
            execution_mode=execution_mode,
            description=description,
            num_worker_processes=num_worker_processes,
            delete_unmatched_files=delete_unmatched_files,
        )

    def sync_replicas(
        self,
        source_path: str,
        replica_indices: Optional[List[int]] = None,
        delete_unmatched_files: bool = False,
        description: str = "Syncing replica",
        num_worker_processes: Optional[int] = None,
        execution_mode: ExecutionMode = ExecutionMode.LOCAL,
    ) -> None:
        """
        Sync files from the source storage client to target replicas.

        :param source_path: The logical path to sync from.
        :param replica_indices: Specify the indices of the replicas to sync to. If not provided, all replicas will be synced. Index starts from 0.
        :param delete_unmatched_files: Whether to delete files at the target that are not present at the source.
        :param description: Description of sync process for logging purposes.
        :param num_worker_processes: The number of worker processes to use.
        :param execution_mode: The execution mode to use. Currently supports "local" and "ray".
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
            source_client = StorageClient(self._config)
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
            )
