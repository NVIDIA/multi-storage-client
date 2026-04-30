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
from collections.abc import Iterator, Sequence
from typing import IO, Any, Optional, Union

from ..config import StorageClientConfig
from ..constants import MEMORY_LOAD_LIMIT
from ..file import ObjectFile, PosixFile
from ..types import (
    MSC_PROTOCOL,
    ExecutionMode,
    MetadataProvider,
    ObjectMetadata,
    PatternList,
    Range,
    SignerType,
    SourceVersionCheckMode,
    SymlinkHandling,
    SyncResult,
)
from ..utils import PatternMatcher, join_paths
from .single import SingleStorageClient
from .types import AbstractStorageClient

logger = logging.getLogger(__name__)


class CompositeStorageClient(AbstractStorageClient):
    """
    READ-ONLY storage client for multi-backend configurations.

    Routes read operations to child SingleStorageClient instances based on
    metadata provider's routing information (ResolvedPath.profile).

    Write operations raise NotImplementedError with clear error messages.
    """

    _metadata_provider: MetadataProvider

    def __init__(self, config: StorageClientConfig):
        """
        Initialize a composite storage client.

        :param config: Storage client configuration with storage_provider_profiles set
        :raises ValueError: If config doesn't have storage_provider_profiles
        """
        if not config.storage_provider_profiles:
            raise ValueError(
                "CompositeStorageClient requires storage_provider_profiles. "
                "Use SingleStorageClient for single-backend configurations."
            )

        if config.metadata_provider is None:
            raise ValueError("CompositeStorageClient requires a metadata_provider for routing decisions.")

        self._config = config
        self._profile = config.profile
        self._metadata_provider = config.metadata_provider
        self._metadata_provider_lock = None
        self._storage_provider = config.storage_provider
        self._credentials_provider = config.credentials_provider
        self._retry_config = config.retry_config
        self._cache_manager = config.cache_manager
        self._replica_manager = None

        self._child_clients: dict[str, SingleStorageClient] = {}
        self._child_profile_names = config.storage_provider_profiles

        if config.child_configs:
            # ProviderBundleV2 path: child configs are pre-built
            for child_name, child_config in config.child_configs.items():
                self._child_clients[child_name] = SingleStorageClient(child_config)
        else:
            # Config-dict path: child profiles have their own credentials defined
            if not config._config_dict:
                raise ValueError("CompositeStorageClient requires _config_dict to build child clients")

            for child_profile in self._child_profile_names:
                child_config = StorageClientConfig.from_dict(
                    config_dict=config._config_dict,
                    profile=child_profile,
                )
                self._child_clients[child_profile] = SingleStorageClient(child_config)

    @property
    def profile(self) -> str:
        """
        :return: The profile name of the storage client.
        """
        return self._profile

    @property
    def replicas(self) -> list[AbstractStorageClient]:
        """
        :return: List of replica storage clients (empty list for CompositeStorageClient).
        """
        return []

    def is_default_profile(self) -> bool:
        return self._config.profile == "__filesystem__"

    def _is_rust_client_enabled(self) -> bool:
        """
        Check if Rust client is enabled for all child storage clients.

        When all child backends are Rust-enabled, MSC can use single-process
        multi-threaded mode instead of multi-process mode, as Rust handles
        parallelism internally via async I/O without Python GIL contention.

        :return: True if all child clients are Rust-enabled, False otherwise.
        """
        return all(client._is_rust_client_enabled() for client in self._child_clients.values())

    def _is_posix_file_storage_provider(self) -> bool:
        """
        Check if using POSIX file storage provider.

        :return: False - composite client doesn't have a single storage provider.
        """
        return False

    def get_posix_path(self, path: str) -> Optional[str]:
        """
        Get the POSIX filesystem path for a given logical path.

        :param path: The logical path to resolve.
        :return: None - composite client doesn't support POSIX path resolution.
        """
        return None

    def _get_child_client(self, profile: Optional[str]) -> SingleStorageClient:
        """
        Get the child client for the specified profile.

        :param profile: Profile name from ResolvedPath
        :return: Child SingleStorageClient instance
        :raises ValueError: If profile is None or not found
        """
        if profile is None:
            raise ValueError(
                "CompositeStorageClient requires profile from ResolvedPath for routing. "
                "Metadata provider must return ResolvedPath with profile set."
            )

        if profile not in self._child_clients:
            raise ValueError(
                f"Profile '{profile}' not found in composite client. "
                f"Available profiles: {list(self._child_clients.keys())}"
            )

        return self._child_clients[profile]

    def read(
        self,
        path: str,
        byte_range: Optional[Range] = None,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
    ) -> bytes:
        resolved = self._metadata_provider.realpath(path)
        if not resolved.exists:
            raise FileNotFoundError(f"Path '{path}' not found")

        child = self._get_child_client(resolved.profile)
        return child.read(resolved.physical_path, byte_range, check_source_version)

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
        prefetch_file: Optional[bool] = None,
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
            This parameter is only applicable to ObjectFile when the mode is "r" or "rb".
            If None, inherits from cache configuration.
        :return: A file-like object (PosixFile or ObjectFile) for the specified path.
        :raises FileNotFoundError: If the file does not exist (read mode).
        :raises NotImplementedError: If the operation is not supported (e.g., write on CompositeStorageClient).
        """
        if mode not in ["r", "rb"]:
            raise NotImplementedError(
                f"CompositeStorageClient only supports read mode (got '{mode}'). "
                "Write operations are not implemented for multi-location datasets."
            )

        resolved = self._metadata_provider.realpath(path)
        if not resolved.exists:
            raise FileNotFoundError(f"Path '{path}' not found")

        child = self._get_child_client(resolved.profile)
        return child.open(
            resolved.physical_path,
            mode,
            buffering,
            encoding,
            disable_read_cache,
            memory_load_limit,
            atomic,
            check_source_version,
            attributes,
            prefetch_file,
        )

    def download_file(self, remote_path: str, local_path: Union[str, IO]) -> None:
        resolved = self._metadata_provider.realpath(remote_path)
        if not resolved.exists:
            raise FileNotFoundError(f"Path '{remote_path}' not found")

        child = self._get_child_client(resolved.profile)
        child.download_file(resolved.physical_path, local_path)

    def download_files(
        self,
        remote_paths: list[str],
        local_paths: list[str],
        metadata: Optional[Sequence[Optional[ObjectMetadata]]] = None,
        max_workers: int = 16,
    ) -> None:
        if len(remote_paths) != len(local_paths):
            raise ValueError("remote_paths and local_paths must have the same length")

        groups: dict[str, tuple[list[str], list[str], list[Optional[ObjectMetadata]]]] = {}
        for i, (remote_path, local_path) in enumerate(zip(remote_paths, local_paths)):
            resolved = self._metadata_provider.realpath(remote_path)
            if not resolved.exists:
                raise FileNotFoundError(f"Path '{remote_path}' not found")

            profile = resolved.profile
            if profile is None:
                raise ValueError(
                    "CompositeStorageClient requires profile from ResolvedPath for routing. "
                    "Metadata provider must return ResolvedPath with profile set."
                )
            if profile not in groups:
                groups[profile] = ([], [], [])
            groups[profile][0].append(resolved.physical_path)
            groups[profile][1].append(local_path)
            groups[profile][2].append(metadata[i] if metadata is not None else None)

        for profile, (physical_paths, group_local_paths, group_metadata) in groups.items():
            child = self._get_child_client(profile)
            has_any_metadata = any(m is not None for m in group_metadata)
            child.download_files(
                physical_paths, group_local_paths, group_metadata if has_any_metadata else None, max_workers
            )

    def glob(
        self,
        pattern: str,
        include_url_prefix: bool = False,
        attribute_filter_expression: Optional[str] = None,
    ) -> list[str]:
        results = self._metadata_provider.glob(
            pattern,
            attribute_filter_expression=attribute_filter_expression,
        )

        if include_url_prefix:
            results = [join_paths(f"{MSC_PROTOCOL}{self._config.profile}", path) for path in results]

        return results

    def list_recursive(
        self,
        path: str = "",
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        max_workers: int = 32,
        look_ahead: int = 2,
        include_url_prefix: bool = False,
        follow_symlinks: Optional[bool] = None,
        patterns: Optional[PatternList] = None,
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
    ) -> Iterator[ObjectMetadata]:
        yield from self.list(
            path=path,
            start_after=start_after,
            end_at=end_at,
            include_directories=False,
            include_url_prefix=include_url_prefix,
            follow_symlinks=follow_symlinks,
            patterns=patterns,
            symlink_handling=symlink_handling,
        )

    def is_file(self, path: str) -> bool:
        resolved = self._metadata_provider.realpath(path)
        return resolved.exists

    def is_empty(self, path: str) -> bool:
        objects = self._metadata_provider.list_objects(path)

        try:
            return next(objects) is None
        except StopIteration:
            pass

        return True

    def info(self, path: str, strict: bool = True) -> ObjectMetadata:
        return self._metadata_provider.get_object_metadata(path, include_pending=not strict)

    def write(
        self,
        path: str,
        body: bytes,
        attributes: Optional[dict[str, str]] = None,
    ) -> None:
        """Write operations not supported in read-only mode."""
        raise NotImplementedError(
            "CompositeStorageClient is read-only. "
            "Write operations are not implemented for multi-location datasets. "
            "Use a single-location dataset for write operations."
        )

    def delete(self, path: str, recursive: bool = False) -> None:
        """Delete operations not supported in read-only mode."""
        raise NotImplementedError(
            "CompositeStorageClient is read-only. Delete operations are not implemented for multi-location datasets."
        )

    def delete_many(self, paths: list[str]) -> None:
        """Delete operations not supported in read-only mode."""
        raise NotImplementedError(
            "CompositeStorageClient is read-only. Delete operations are not implemented for multi-location datasets."
        )

    def copy(self, src_path: str, dest_path: str) -> None:
        """Copy operations not supported in read-only mode."""
        raise NotImplementedError(
            "CompositeStorageClient is read-only. Copy operations are not implemented for multi-location datasets."
        )

    def make_symlink(self, path: str, target: str) -> None:
        """Symlink operations not supported in read-only mode."""
        raise NotImplementedError(
            "CompositeStorageClient is read-only. Symlink operations are not implemented for multi-location datasets."
        )

    def upload_file(
        self,
        remote_path: str,
        local_path: Union[str, IO],
        attributes: Optional[dict[str, str]] = None,
    ) -> None:
        """Upload operations not supported in read-only mode."""
        raise NotImplementedError(
            "CompositeStorageClient is read-only. Upload operations are not implemented for multi-location datasets."
        )

    def upload_files(
        self,
        remote_paths: list[str],
        local_paths: list[str],
        attributes: Optional[Sequence[Optional[dict[str, str]]]] = None,
        max_workers: int = 16,
    ) -> None:
        """Upload operations not supported in read-only mode."""
        raise NotImplementedError(
            "CompositeStorageClient is read-only. Upload operations are not implemented for multi-location datasets."
        )

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        """No-op for read-only client."""
        pass

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
        follow_symlinks: Optional[bool] = None,
        source_files: Optional[list[str]] = None,
        ignore_hidden: bool = True,
        commit_metadata: bool = True,
        dryrun: bool = False,
        dryrun_output_path: Optional[str] = None,
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
    ) -> SyncResult:
        raise NotImplementedError(
            "CompositeStorageClient cannot be used as sync target (write operation). "
            "Use CompositeStorageClient as source only, or use a single-location dataset as target."
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
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
    ) -> None:
        """No-op for read-only client."""
        pass

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
        follow_symlinks: Optional[bool] = None,
        patterns: Optional[PatternList] = None,
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
    ) -> Iterator[ObjectMetadata]:
        # Parameter validation - either path or prefix, not both
        if path and prefix:
            raise ValueError(
                f"Cannot specify both 'path' ({path!r}) and 'prefix' ({prefix!r}). "
                f"Please use only the 'path' parameter for new code. "
                f"Migration guide: Replace list(prefix={prefix!r}) with list(path={prefix!r})"
            )

        # Use path if provided, otherwise fall back to prefix
        effective_path = path if path else prefix

        # Apply patterns to the objects
        pattern_matcher = PatternMatcher(patterns) if patterns else None

        # Delegate to metadata provider (always present for CompositeStorageClient)
        for obj in self._metadata_provider.list_objects(
            effective_path,
            start_after=start_after,
            end_at=end_at,
            include_directories=include_directories,
            attribute_filter_expression=attribute_filter_expression,
            show_attributes=show_attributes,
        ):
            # Skip objects that do not match the patterns
            if pattern_matcher and not pattern_matcher.should_include_file(obj.key):
                continue

            if include_url_prefix:
                obj.key = join_paths(f"{MSC_PROTOCOL}{self._config.profile}", obj.key)

            yield obj

    def generate_presigned_url(
        self,
        path: str,
        *,
        method: str = "GET",
        signer_type: Optional[SignerType] = None,
        signer_options: Optional[dict[str, Any]] = None,
    ) -> str:
        resolved = self._metadata_provider.realpath(path)
        if not resolved.exists:
            raise FileNotFoundError(f"Path '{path}' not found")

        child = self._get_child_client(resolved.profile)
        return child.generate_presigned_url(
            resolved.physical_path, method=method, signer_type=signer_type, signer_options=signer_options
        )

    def __getstate__(self) -> dict[str, Any]:
        """Support for pickling."""
        return {"_config": self._config}

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Support for unpickling - reinitialize from config."""
        config = state["_config"]
        self.__init__(config)
