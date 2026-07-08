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

import hashlib
import json
import logging
import os
import stat
import tempfile
import threading
import time
import unicodedata
from collections import OrderedDict
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Any, NoReturn, Optional, Union

import xattr
from filelock import BaseFileLock, FileLock, Timeout

from .caching.cache_config import CacheConfig
from .caching.cache_item import CacheItem
from .caching.eviction_policy import FIFO, LRU, MRU, NO_EVICTION, RANDOM, EvictionPolicyFactory
from .types import Range, RetryableError, SourceVersionCheckMode
from .utils import safe_makedirs

DEFAULT_CACHE_SIZE = "10G"


class _SourceReadError(Exception):
    """Carry a source exception through cache-local fallback boundaries."""

    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


DEFAULT_CACHE_SIZE_MB = "10000"
DEFAULT_CACHE_REFRESH_INTERVAL = 300  # 5 minutes
DEFAULT_LOCK_TIMEOUT = 600  # 10 minutes
DEFAULT_CACHE_LINE_SIZE = "64M"
_CHUNK_METADATA_SUFFIX = ".metadata"
_RANGE_CACHE_LOCK_DIRECTORY = ".locks"
_RANGE_CACHE_LOCK_STRIPE_COUNT = 256
_PROMOTED_CHUNK_METADATA_NAME = f"promoted{_CHUNK_METADATA_SUFFIX}"
_FULL_CACHE_PUBLICATION_MARKER_NAME = ".publishing"
_FULL_CACHE_DESCRIPTOR_PUBLICATION_DIRECTORY = ".full-publications"
_FULL_CACHE_DESCRIPTOR_PUBLICATION_SUFFIX = ".publishing"
_CACHE_INTERNAL_DIRECTORY = ".msc-cache-internal"
_CACHE_INTERNAL_RANGE_DIRECTORY = "range"
_CACHE_INTERNAL_TEMP_DIRECTORY = "tmp"
_CACHE_ROOT_EVICTION_LOCK_NAME = ".cache_refresh.lock"
_CACHE_IDENTITY_XATTR = "user.msc-cache-identity"
_CACHE_IDENTITY_METADATA_NAME = "cache_identity"
_CACHE_IDENTITY_DOMAIN = b"multistorageclient/full-cache-identity/v1\x00"
_INVALID_CACHE_IDENTITY = object()


def _validate_cache_profile_name(profile: str) -> str:
    """Validate a cache profile as a non-reserved path component on normalizing filesystems."""
    if not isinstance(profile, str) or not profile:
        raise ValueError("Cache profile must be a non-empty single path component.")

    normalized_profile = unicodedata.normalize("NFKC", profile)
    reserved_profile = normalized_profile.casefold()
    if reserved_profile.startswith(".tmp-"):
        raise ValueError("Cache profiles beginning with '.tmp-' are reserved for legacy temporary downloads.")
    if reserved_profile in {".", "..", _CACHE_INTERNAL_DIRECTORY.casefold()}:
        raise ValueError("Cache profile must be a non-reserved single path component.")
    if (
        "/" in normalized_profile
        or "\\" in normalized_profile
        or any(ord(character) < 32 or ord(character) == 127 for character in normalized_profile)
    ):
        raise ValueError("Cache profile must be a non-reserved single path component.")
    return profile


class _ValidatingCacheFile:
    """Proxy one cached descriptor and reject reads after its recorded size changes."""

    def __init__(self, file: Any, expected_size: int, invalidate: Callable[[], None], *, binary: bool) -> None:
        self._file = file
        self._expected_size = expected_size
        self._invalidate = invalidate
        self._binary = binary
        self._failed = False

    def _expected_read_length(self, position: int, size: int) -> int:
        remaining = max(self._expected_size - position, 0)
        return remaining if size < 0 else min(size, remaining)

    def _fail(self, message: str) -> NoReturn:
        if not self._failed:
            self._failed = True
            self._invalidate()
        raise IOError(message)

    def _verify_size(self) -> None:
        if self._failed:
            raise IOError("Cached file was invalidated after a previous failed read.")
        try:
            actual_size = os.fstat(self._file.fileno()).st_size
        except OSError:
            self._fail("Cached file descriptor became unavailable while reading.")
        if actual_size != self._expected_size:
            self._fail(f"Cached file changed size while reading: expected={self._expected_size}, actual={actual_size}.")

    def _verify_read(self, position: int, size: int, actual_length: int) -> None:
        expected_length = self._expected_read_length(position, size)
        if self._binary and actual_length != expected_length:
            self._fail(f"Cached file returned a short read: expected={expected_length}, actual={actual_length}.")
        self._verify_size()

    def read(self, size: int = -1) -> Any:
        position = self._file.tell()
        data = self._file.read(size)
        self._verify_read(position, size, len(data))
        return data

    def readinto(self, buffer: Any) -> int:
        position = self._file.tell()
        read = self._file.readinto(buffer)
        if not isinstance(read, int):
            self._fail("Cached file returned no progress while reading into a buffer.")
        self._verify_read(position, len(buffer), read)
        return read

    def read1(self, size: int = -1) -> Any:
        data = self._file.read1(size)
        self._verify_size()
        return data

    def readinto1(self, buffer: Any) -> int:
        read = self._file.readinto1(buffer)
        if not isinstance(read, int):
            self._fail("Cached file returned no progress while reading into a buffer.")
        self._verify_size()
        return read

    def readline(self, size: int = -1) -> Any:
        data = self._file.readline(size)
        self._verify_size()
        return data

    def readlines(self, hint: int = -1) -> list[Any]:
        lines = self._file.readlines(hint)
        self._verify_size()
        return lines

    def __iter__(self):
        return self

    def __next__(self) -> Any:
        line = self.readline()
        if line:
            return line
        raise StopIteration

    def __enter__(self):
        self._file.__enter__()
        return self

    def __exit__(self, *args: Any) -> Any:
        return self._file.__exit__(*args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._file, name)


class CacheManager:
    """
    A concrete implementation of the :py:class:`CacheBackend` that stores cache data in the local filesystem.
    """

    DEFAULT_FILE_LOCK_TIMEOUT = 600

    def __init__(self, profile: str, cache_config: CacheConfig):
        """
        Initializes the :py:class:`CacheManager` with the given profile and configuration.

        :param profile: The profile name for the cache.
        :param cache_config: The cache configuration settings.
        """
        self._profile = self._validate_profile_name(profile)
        self._cache_config = cache_config
        self._max_cache_size = cache_config.size_bytes()
        self._last_refresh_time = datetime.now()
        self._cache_refresh_interval = cache_config.eviction_policy.refresh_interval
        self._cache_refresh_thread: Optional[threading.Thread] = None
        self._cache_refresh_thread_lock = threading.Lock()

        # Range cache configuration
        self._cache_line_size = cache_config.cache_line_size_bytes()

        default_location = os.path.join(tempfile.gettempdir(), "msc-cache")
        # Create cache directory if it doesn't exist, this is used to download files
        self._cache_dir = os.path.realpath(os.path.abspath(cache_config.location or default_location))
        self._cache_path = os.path.join(self._cache_dir, self._profile)
        self._cache_internal_dir = os.path.join(self._cache_dir, _CACHE_INTERNAL_DIRECTORY)
        self._cache_temp_dir = os.path.join(
            self._cache_internal_dir,
            _CACHE_INTERNAL_TEMP_DIRECTORY,
            hashlib.sha256(os.fsencode(self._profile)).hexdigest(),
        )
        self._range_cache_dir = os.path.join(self._cache_internal_dir, _CACHE_INTERNAL_RANGE_DIRECTORY)
        safe_makedirs(self._cache_dir)
        self._resolve_cache_root_path(self._cache_path, "profile")
        self._resolve_cache_root_path(self._cache_internal_dir, "internal")
        self._resolve_cache_root_path(self._cache_temp_dir, "temporary")
        self._resolve_cache_root_path(self._range_cache_dir, "range")
        safe_makedirs(self._cache_path)
        safe_makedirs(self._cache_internal_dir)
        safe_makedirs(self._cache_temp_dir)
        safe_makedirs(self._range_cache_dir)

        # Check if eviction policy is valid for this backend
        if not self._check_if_eviction_policy_is_valid(cache_config.eviction_policy.policy):
            raise ValueError(f"Invalid eviction policy: {cache_config.eviction_policy.policy}")

        self._eviction_policy = EvictionPolicyFactory.create(cache_config.eviction_policy.policy)

        # Coordinate eviction across every profile that shares this cache root.
        self._cache_refresh_lock_file = FileLock(
            os.path.join(self._cache_internal_dir, _CACHE_ROOT_EVICTION_LOCK_NAME),
            timeout=self.DEFAULT_FILE_LOCK_TIMEOUT,
        )

        # Populate cache with existing files in the cache directory
        self.refresh_cache()

    @staticmethod
    def _validate_profile_name(profile: str) -> str:
        """Validate one cache profile as a non-reserved single path component."""
        return _validate_cache_profile_name(profile)

    @staticmethod
    def _has_source_version(source_version: object) -> bool:
        """Return whether a source revision is a non-empty string suitable for required validation."""
        return isinstance(source_version, str) and bool(source_version)

    def _resolve_cache_root_path(self, path: str, description: str) -> str:
        """Resolve one cache-owned path and reject a pre-existing symlink escape."""
        cache_root = os.path.realpath(self._cache_dir)
        resolved_path = os.path.realpath(path)
        try:
            if os.path.commonpath([cache_root, resolved_path]) != cache_root or resolved_path == cache_root:
                raise ValueError(f"Cache {description} path escapes the cache root.")
        except ValueError as exc:
            if str(exc).startswith("Cache "):
                raise
            raise ValueError(f"Cache {description} path escapes the cache root.") from exc
        return resolved_path

    def generate_temp_file_path(self) -> str:
        """
        Create a temporary file in the cache temporary directory.
        """
        temporary_directory = self._resolve_cache_root_path(self._cache_temp_dir, "temporary")
        with tempfile.NamedTemporaryFile(mode="wb", dir=temporary_directory, prefix=".") as temp_file:
            return temp_file.name

    def _check_if_eviction_policy_is_valid(self, eviction_policy: str) -> bool:
        """Check if the eviction policy is valid for this backend.

        :param eviction_policy: The eviction policy to check.
        :return: True if the policy is valid, False otherwise.
        """
        return eviction_policy.lower() in {LRU, MRU, FIFO, RANDOM, NO_EVICTION}

    def get_file_size(self, file_path: str) -> Optional[int]:
        """
        Get the size of the file in bytes.

        :param file_path: Path to the file
        :return: Size of the file in bytes, or None if file doesn't exist
        """
        try:
            return os.path.getsize(file_path)
        except OSError:
            return None

    def delete_file(self, file_path: str) -> None:
        """
        Delete a file from the profile cache directory.

        :param file_path: Path to the file relative to the profile cache directory
        """
        abs_path = self._resolve_profile_cache_delete_path(file_path)
        with self._cache_refresh_lock_file:
            self._delete_cache_file_at_path(abs_path, self._cache_identity(file_path), legacy_key=file_path)

    def _resolve_profile_cache_delete_path(self, file_path: str) -> str:
        """Resolve a profile-relative delete path and ensure it stays within the profile cache root."""
        if os.path.isabs(file_path):
            raise ValueError(f"Cache delete path must be relative to the profile cache root: {file_path}")

        normalized_path = os.path.normpath(file_path)
        if normalized_path in ("", "."):
            raise ValueError("Cache delete path must reference a file under the profile cache root.")

        cache_root = os.path.realpath(self._get_cache_dir())
        abs_path = os.path.realpath(os.path.join(cache_root, normalized_path))
        try:
            if os.path.commonpath([cache_root, abs_path]) != cache_root or abs_path == cache_root:
                raise ValueError(f"Cache delete path escapes the profile cache root: {file_path}")
        except ValueError as exc:
            raise ValueError(f"Cache delete path escapes the profile cache root: {file_path}") from exc

        return abs_path

    def _delete_cache_file_at_path(
        self,
        abs_path: str,
        expected_identity: Optional[str] = None,
        *,
        legacy_key: Optional[str] = None,
    ) -> None:
        """Delete a cached file and its sibling lock file using an absolute cache path."""
        is_range_cache_file = self._is_range_cache_path(abs_path)
        published_identity: Optional[str] = None
        descriptor_marker_path: Optional[str] = None
        legacy_owned = False
        if not is_range_cache_file:
            try:
                with open(abs_path, "rb") as cached_file:
                    descriptor_marker_path = self._get_full_cache_descriptor_publication_marker_path(
                        os.fstat(cached_file.fileno())
                    )
                    descriptor_identity = self._descriptor_cache_identity(
                        abs_path,
                        cached_file.fileno(),
                        allow_descriptor_size_mismatch=expected_identity is None,
                    )
                    legacy_owned = (
                        descriptor_identity is None
                        and legacy_key is not None
                        and self._legacy_cache_path_has_exact_native_ownership(legacy_key, cached_file.fileno())
                    )
            except FileNotFoundError:
                descriptor_identity = None
            except OSError:
                descriptor_identity = _INVALID_CACHE_IDENTITY

            if expected_identity is not None and descriptor_identity != expected_identity and not legacy_owned:
                self._invalidate_chunks(abs_path, expected_identity, infer_cache_identity=False)
                return
            if isinstance(descriptor_identity, str):
                published_identity = descriptor_identity
            elif expected_identity is not None:
                published_identity = expected_identity
        try:
            os.unlink(abs_path)
        except OSError:
            pass
        self._remove_chunk_metadata_sidecar(abs_path)

        if not is_range_cache_file:
            self._remove_full_cache_publication_marker(self._get_full_cache_publication_marker_path(abs_path))
            if descriptor_marker_path is not None:
                self._remove_full_cache_publication_marker(descriptor_marker_path)
            self._invalidate_chunks(abs_path, published_identity, infer_cache_identity=False)

        lock_name = f".{os.path.basename(abs_path)}.lock"
        lock_path = os.path.join(os.path.dirname(abs_path), lock_name)
        try:
            os.unlink(lock_path)
        except OSError:
            pass

    def _cache_root_namespace(self, path: str) -> Optional[str]:
        """Classify one path under the shared cache root's internal namespaces."""
        cache_root = os.path.realpath(self._cache_dir)
        try:
            resolved_path = os.path.realpath(path)
            if os.path.commonpath([cache_root, resolved_path]) != cache_root:
                return None
        except ValueError:
            return None

        relative_path = os.path.relpath(resolved_path, cache_root)
        first_component = relative_path.split(os.sep, 1)[0]
        if first_component.startswith(".tmp-"):
            return "temporary"
        if first_component != _CACHE_INTERNAL_DIRECTORY:
            return None

        internal_relative_path = os.path.relpath(resolved_path, os.path.realpath(self._cache_internal_dir))
        internal_first_component = internal_relative_path.split(os.sep, 1)[0]
        if internal_first_component == _CACHE_INTERNAL_RANGE_DIRECTORY:
            return "range"
        if internal_first_component == _CACHE_INTERNAL_TEMP_DIRECTORY:
            return "temporary"
        return "internal"

    def _is_range_cache_path(self, path: str) -> bool:
        """Return whether a path belongs to any profile's private range-cache namespace."""
        return self._cache_root_namespace(path) == "range"

    def _is_range_cache_sidecar(self, path: str) -> bool:
        """Return whether a file is an internal portable range-cache metadata sidecar."""
        return self._is_range_cache_path(path) and path.endswith(_CHUNK_METADATA_SUFFIX)

    def _is_cache_temporary_path(self, path: str) -> bool:
        """Return whether a path belongs to any profile's non-cache temporary-file directory."""
        return self._cache_root_namespace(path) == "temporary"

    def evict_files(self) -> None:
        """
        Evict cache entries based on the configured eviction policy.
        """
        with self._cache_refresh_lock_file:
            self._evict_files()

    def _evict_files(self) -> None:
        """Evict entries while the shared cache-root coordinator is held."""
        logging.debug("\nStarting evict_files...")
        cache_items: list[CacheItem] = []

        # Traverse the directory and subdirectories
        for dirpath, _, filenames in os.walk(self._cache_dir):
            for file_name in filenames:
                file_path = os.path.join(dirpath, file_name)
                # Internal sidecars are bookkeeping, while user objects named *.metadata are cache data.
                namespace = self._cache_root_namespace(file_path)
                if (
                    file_name.endswith(".lock")
                    or self._is_range_cache_sidecar(file_path)
                    or namespace in {"temporary", "internal"}
                    or (namespace == "range" and file_name.startswith("."))
                ):
                    continue
                try:
                    if os.path.isfile(file_path):
                        # Get the relative path from the cache directory
                        rel_path = os.path.relpath(file_path, self._cache_path)
                        cache_item = CacheItem.from_path(file_path, rel_path)
                        if cache_item and cache_item.file_size:
                            logging.debug(f"Found file: {rel_path}, size: {cache_item.file_size}")
                            cache_items.append(cache_item)
                except OSError:
                    # Ignore if file has already been evicted
                    pass

        logging.debug(f"\nFound {len(cache_items)} files before sorting")

        # Sort items according to eviction policy
        cache_items = self._eviction_policy.sort_items(cache_items)
        logging.debug("\nFiles after sorting by policy:")
        for item in cache_items:
            logging.debug(f"File: {item.file_path}")

        # Rebuild the cache
        cache = OrderedDict()
        cache_size = 0
        for item in cache_items:
            cache[item.file_path] = item.file_size
            cache_size += item.file_size
        logging.debug(f"Total cache size: {cache_size}, Max allowed: {self._max_cache_size}")

        # Calculate target size based on purge_factor
        # purge_factor = percentage of cache to DELETE (0-100)
        # target_size = what cache size should be after purging
        purge_factor = self._cache_config.eviction_policy.purge_factor
        target_size = self._max_cache_size * (1.0 - purge_factor / 100.0)
        logging.debug(f"Purge factor: {purge_factor}%, Target size after purge: {target_size}")

        # Evict files to reduce cache size to target_size
        # Once eviction is triggered, evict down to target_size (not just to max_cache_size)
        if cache_size > self._max_cache_size:
            logging.debug(
                f"Cache size {cache_size} exceeds max {self._max_cache_size}, starting eviction to target {target_size}"
            )
            while cache_size > target_size and cache:
                # Pop the first item in the OrderedDict (according to policy's sorting)
                file_to_evict, file_size = cache.popitem(last=False)
                cache_size -= file_size
                logging.debug(f"Evicting file: {file_to_evict}, size: {file_size}, remaining: {cache_size}")
                self._delete_cache_file_at_path(file_to_evict)

        logging.debug("\nFinal cache contents:")
        for file_path in cache.keys():
            logging.debug(f"Remaining file: {file_path}")

    def check_source_version(self) -> bool:
        """Check if etag is used in the cache config."""
        return self._cache_config.check_source_version

    def prefetch_file(self) -> bool:
        """Return whether open() should prefetch full files by default."""
        return self._cache_config.prefetch_file

    def get_max_cache_size(self) -> int:
        """Return the cache size in bytes from the cache config."""
        return self._max_cache_size

    def _get_cache_dir(self) -> str:
        """Return the path to the local cache directory."""
        return os.path.join(self._cache_dir, self._profile)

    @staticmethod
    def _logical_cache_key(key: str) -> str:
        """Return the lexical cache key used for a path without folding case or Unicode bytes."""
        if not isinstance(key, str) or not key:
            raise ValueError("Cache key must be a non-empty string.")
        relative_key = os.path.relpath(key, "/") if os.path.isabs(key) else key
        return os.path.normpath(relative_key)

    def _cache_identity(self, key: str) -> str:
        """Return the deterministic identity for one profile and logical key."""
        hasher = hashlib.sha256()
        hasher.update(_CACHE_IDENTITY_DOMAIN)
        for component in (self._profile, self._logical_cache_key(key)):
            encoded_component = component.encode("utf-8")
            hasher.update(len(encoded_component).to_bytes(8, byteorder="big"))
            hasher.update(encoded_component)
        return hasher.hexdigest()

    def _get_cache_file_path(self, key: str) -> str:
        """Return the path to the local cache file for the given key."""
        relative_key = self._logical_cache_key(key)

        cache_root = self._resolve_cache_root_path(self._get_cache_dir(), "profile")
        cache_path = os.path.realpath(os.path.join(cache_root, relative_key))
        try:
            if os.path.commonpath([cache_root, cache_path]) != cache_root or cache_path == cache_root:
                raise ValueError(f"Cache key escapes the profile cache root: {key}")
        except ValueError as exc:
            if str(exc).startswith("Cache key escapes"):
                raise
            raise ValueError(f"Cache key escapes the profile cache root: {key}") from exc
        return cache_path

    def _legacy_cache_path_has_exact_native_ownership(self, key: str, file_descriptor: int) -> bool:
        """Return whether a legacy descriptor has this request's exact native path spelling."""
        components = (self._profile, *self._logical_cache_key(key).split(os.sep))
        parent = self._cache_dir
        try:
            descriptor_stat = os.fstat(file_descriptor)
            for index, component in enumerate(components):
                with os.scandir(parent) as entries:
                    entry = next((candidate for candidate in entries if candidate.name == component), None)
                if entry is None or entry.is_symlink():
                    return False
                entry_stat = entry.stat(follow_symlinks=False)
                if index == len(components) - 1:
                    return os.path.samestat(entry_stat, descriptor_stat)
                if not stat.S_ISDIR(entry_stat.st_mode):
                    return False
                parent = os.path.join(parent, component)
        except OSError:
            return False
        return False

    def read(
        self,
        key: str,
        source_version: Optional[str] = None,
        byte_range: Optional[Range] = None,
        storage_provider: Optional[Any] = None,
        source_size: Optional[int] = None,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
        source_size_resolver: Optional[Callable[[], int]] = None,
    ) -> Optional[bytes]:
        """Read the contents of a file from the cache if it exists.

        This method handles both full-file reads and partial file caching. For range reads
        (when byte_range is provided), it delegates to _read_range to implement partial
        file caching using chunks. For full-file reads, it uses the existing cache logic.

        :param key: The cache key for the file
        :param source_version: Optional source version for cache validation
        :param byte_range: Optional byte range for partial file reads
        :param storage_provider: Storage provider required for range reads
        :param source_size: Optional size of the source object in bytes
        :param check_source_version: Source-version policy for full-file cache validation
        :param source_size_resolver: Optional lazy source-size lookup used only when a range-cache miss needs fetching
        :return: The file contents as bytes, or None if not found in cache
        """
        # If this is a range read, check for full cached file first
        if byte_range:
            require_source_version = self._requires_source_version_check(check_source_version)
            if require_source_version and not self._has_source_version(source_version):
                return None
            validate_source_version = require_source_version or (
                check_source_version != SourceVersionCheckMode.DISABLE and self._has_source_version(source_version)
            )
            effective_source_version = source_version if validate_source_version else None
            cache_path = self._get_cache_file_path(key)
            cache_identity = self._cache_identity(key)

            # Try to read range from full cached file first
            range_data = self._read_range_from_full_cached_file(
                cache_path,
                byte_range,
                effective_source_version,
                cache_identity=cache_identity,
                require_source_version=validate_source_version,
                legacy_key=key,
            )
            if range_data is not None:
                return range_data

            # No valid full cached file, delegate to chunk-based range reading
            return self._read_range(  # type: ignore[arg-type]
                cache_path,
                byte_range,
                storage_provider,
                effective_source_version,
                source_size,
                key,
                cache_identity=cache_identity,
                require_source_version=validate_source_version,
                source_size_resolver=source_size_resolver,
            )

        # Full-file cached read (existing behavior)
        try:
            file_path = self._get_cache_file_path(key)
            cached_file = self._open_validated_cache_file(
                key,
                mode="rb",
                source_version=source_version,
                check_source_version=check_source_version,
            )
            if cached_file is not None:
                with cached_file as fp:
                    data = fp.read()
                # Update access time based on eviction policy
                try:
                    self._update_access_time(file_path)
                except OSError:
                    pass
                return data
        except OSError:
            pass

        # cache miss
        return None

    def open(
        self,
        key: str,
        mode: str = "rb",
        source_version: Optional[str] = None,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
    ) -> Optional[Any]:
        """Open a file from the cache and return the file object."""
        try:
            cached_file = self._open_validated_cache_file(
                key,
                mode=mode,
                source_version=source_version,
                check_source_version=check_source_version,
            )
            if cached_file is not None:
                file_path = self._get_cache_file_path(key)
                # Update access time based on eviction policy
                try:
                    self._update_access_time(file_path)
                except OSError:
                    pass
                return cached_file
        except OSError:
            pass

        # cache miss
        return None

    def set(self, key: str, source: Union[str, bytes], source_version: Optional[str] = None) -> None:
        """Store a file in the cache."""
        file_path = self._get_cache_file_path(key)
        cache_identity = self._cache_identity(key)
        # Ensure the directory exists
        safe_makedirs(os.path.dirname(file_path))

        temporary_path: Optional[str] = None
        owns_temporary_path = False
        if isinstance(source, str):
            temporary_path = source
        else:
            # Create a temporary file and atomically publish it to the cache directory.
            temporary_directory = self._resolve_cache_root_path(self._cache_temp_dir, "temporary")
            with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=temporary_directory, prefix=".") as temp_file:
                temporary_path = temp_file.name
                owns_temporary_path = True
                temp_file.write(source)

        try:
            if temporary_path is None:  # pragma: no cover - source accepts only str or bytes
                raise ValueError("Cache source must be a path or bytes.")
            object_size = os.path.getsize(temporary_path)
            metadata_in_xattrs = self._set_chunk_metadata(
                temporary_path,
                source_version,
                self._cache_line_size,
                object_size,
                cache_identity=cache_identity,
            )
            with self._cache_refresh_lock_file:
                if self._cache_path_is_bound_to_other_identity(file_path, cache_identity, key):
                    return
                marker_paths = (
                    () if metadata_in_xattrs else self._mark_full_cache_publication(file_path, temporary_path)
                )
                published = False
                try:
                    os.replace(temporary_path, file_path)
                    temporary_path = None
                    if metadata_in_xattrs:
                        self._remove_chunk_metadata_sidecar(file_path)
                    else:
                        self._write_chunk_metadata_sidecar(
                            file_path,
                            source_version,
                            self._cache_line_size,
                            object_size,
                            cache_identity=cache_identity,
                        )

                    # Keep the complete data/metadata/permission/access transition invisible to eviction.
                    self._make_readonly(file_path)
                    self._update_access_time(file_path)
                    published = True
                finally:
                    if published:
                        for marker_path in marker_paths:
                            self._remove_full_cache_publication_marker(marker_path)
        finally:
            if owns_temporary_path and temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except OSError:
                    pass

        self._schedule_refresh_if_needed()

    def contains(
        self,
        key: str,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
        source_version: Optional[str] = None,
    ) -> bool:
        """Check if the cache contains a file corresponding to the given key."""
        try:
            # Get cache path
            file_path = self._get_cache_file_path(key)

            # If file doesn't exist or its portable metadata is still publishing, return False.
            if not os.path.exists(file_path) or self._full_cache_publication_in_progress(file_path):
                return False

            try:
                with open(file_path, "rb") as cached_file:
                    if self._full_cache_publication_in_progress(file_path, cached_file.fileno()):
                        return False
                    requires_source_version_check = self._requires_source_version_check(check_source_version)
                    if not self._descriptor_matches_cache_identity(
                        file_path,
                        cached_file.fileno(),
                        self._cache_identity(key),
                        legacy_key=key,
                    ):
                        return False
                    # With source checks disabled, an identity-bound entry is sufficient.
                    if not requires_source_version_check:
                        return True
                    # Verify source revision against xattrs or an identity-bound sidecar.
                    if not self._has_source_version(source_version):
                        return False
                    return self._descriptor_matches_source_version(
                        file_path,
                        cached_file.fileno(),
                        source_version,
                    )
            except OSError:
                return False

        except Exception as e:
            logging.error(f"Error checking cache: {e}")
            return False

    def _requires_source_version_check(self, check_source_version: SourceVersionCheckMode) -> bool:
        """Return whether one operation must verify the cached source revision."""
        return check_source_version == SourceVersionCheckMode.ENABLE or (
            check_source_version == SourceVersionCheckMode.INHERIT and self.check_source_version()
        )

    def _descriptor_matches_source_version(
        self,
        file_path: str,
        file_descriptor: int,
        source_version: Optional[str],
    ) -> bool:
        """Validate one already-open full-cache file against its source revision."""
        if not self._has_source_version(source_version):
            return False
        metadata = self._get_chunk_metadata(
            file_path,
            require_source_version=True,
            file_descriptor=file_descriptor,
        )
        if metadata is not None:
            return self._has_source_version(metadata["source_version"]) and metadata["source_version"] == source_version

        try:
            stored_version = xattr.getxattr(file_descriptor, "user.etag").decode("utf-8")
        except (OSError, AttributeError, UnicodeDecodeError):
            return False
        return self._has_source_version(stored_version) and stored_version == source_version

    def _descriptor_matches_cache_identity(
        self,
        file_path: str,
        file_descriptor: int,
        expected_identity: str,
        *,
        legacy_key: Optional[str] = None,
    ) -> bool:
        """Return whether a published identity matches or a legacy path has exact native ownership."""
        stored_identity = self._descriptor_cache_identity(file_path, file_descriptor)
        if stored_identity is None:
            return legacy_key is not None and self._legacy_cache_path_has_exact_native_ownership(
                legacy_key, file_descriptor
            )
        return isinstance(stored_identity, str) and stored_identity == expected_identity

    def _descriptor_cache_identity(
        self,
        file_path: str,
        file_descriptor: int,
        *,
        allow_descriptor_size_mismatch: bool = False,
    ) -> object:
        """Return the published cache identity, ``None`` for a legacy entry, or an invalid sentinel."""
        try:
            stored_identity = xattr.getxattr(file_descriptor, _CACHE_IDENTITY_XATTR).decode("ascii")
        except UnicodeDecodeError:
            return _INVALID_CACHE_IDENTITY
        except (OSError, AttributeError):
            stored_identity = None

        if stored_identity is not None:
            return stored_identity

        metadata, saw_sidecar = self._read_full_cache_metadata_sidecar(
            file_path,
            file_descriptor=file_descriptor,
            allow_descriptor_size_mismatch=allow_descriptor_size_mismatch,
        )
        if metadata is None:
            return _INVALID_CACHE_IDENTITY if saw_sidecar else None
        if _CACHE_IDENTITY_METADATA_NAME not in metadata:
            # Full entries created before identity binding remain compatible.
            return None
        stored_identity = metadata[_CACHE_IDENTITY_METADATA_NAME]
        return stored_identity if isinstance(stored_identity, str) else _INVALID_CACHE_IDENTITY

    def _cache_path_is_bound_to_other_identity(
        self,
        file_path: str,
        expected_identity: str,
        key: str,
    ) -> bool:
        """Conservatively detect an existing full-cache entry belonging to another logical identity."""
        try:
            with open(file_path, "rb") as cached_file:
                return not self._descriptor_matches_cache_identity(
                    file_path,
                    cached_file.fileno(),
                    expected_identity,
                    legacy_key=key,
                )
        except FileNotFoundError:
            return False
        except OSError:
            return True

    def _validated_descriptor_size(self, file_path: str, file_descriptor: int) -> Optional[int]:
        """Return the recorded descriptor size after validating the published size when present."""
        stored_object_size = self._get_chunk_object_size(
            file_path,
            file_descriptor=file_descriptor,
            allow_descriptor_size_mismatch=True,
        )
        try:
            descriptor_size = os.fstat(file_descriptor).st_size
        except OSError:
            return None
        if stored_object_size is None:
            return descriptor_size
        if descriptor_size == stored_object_size:
            return stored_object_size
        self._remove_invalid_chunk_for_descriptor(file_path, file_descriptor)
        return None

    def _descriptor_matches_expected_size(self, file_path: str, file_descriptor: int, expected_size: int) -> bool:
        """Check that a descriptor still has its operation-bound expected size."""
        try:
            descriptor_size = os.fstat(file_descriptor).st_size
        except OSError:
            return False
        if descriptor_size == expected_size:
            return True
        self._remove_invalid_chunk_for_descriptor(file_path, file_descriptor)
        return False

    def _open_validated_cache_file(
        self,
        key: str,
        *,
        mode: str,
        source_version: Optional[str],
        check_source_version: SourceVersionCheckMode,
    ) -> Optional[Any]:
        """Open and validate one full-cache entry without reopening its pathname."""
        file_path = self._get_cache_file_path(key)
        if self._full_cache_publication_in_progress(file_path):
            return None
        try:
            cached_file = open(file_path, mode)
        except OSError:
            return None

        valid = False
        try:
            if self._full_cache_publication_in_progress(file_path, cached_file.fileno()):
                return None
            requires_source_version_check = self._requires_source_version_check(check_source_version)
            if not self._descriptor_matches_cache_identity(
                file_path,
                cached_file.fileno(),
                self._cache_identity(key),
                legacy_key=key,
            ):
                return None
            expected_size = self._validated_descriptor_size(file_path, cached_file.fileno())
            if expected_size is None:
                return None
            if requires_source_version_check and not self._descriptor_matches_source_version(
                file_path, cached_file.fileno(), source_version
            ):
                return None
            valid = True
            return _ValidatingCacheFile(
                cached_file,
                expected_size,
                lambda: self._remove_invalid_chunk_for_descriptor(file_path, cached_file.fileno()),
                binary="b" in mode,
            )
        except (OSError, AttributeError, UnicodeDecodeError):
            return None
        finally:
            if not valid:
                try:
                    cached_file.close()
                except OSError:
                    pass

    def delete(self, key: str) -> None:
        """Delete a file from the cache."""
        self.delete_file(key)

    def cache_size(self) -> int:
        """Return the current size of the cache in bytes."""
        file_size = 0

        # Traverse the directory and subdirectories
        for dirpath, _, filenames in os.walk(self._cache_dir):
            for file_name in filenames:
                file_path = os.path.join(dirpath, file_name)
                namespace = self._cache_root_namespace(file_path)
                if (
                    os.path.isfile(file_path)
                    and not file_name.endswith(".lock")
                    and not self._is_range_cache_sidecar(file_path)
                    and namespace not in {"temporary", "internal"}
                    and not (namespace == "range" and file_name.startswith("."))
                ):
                    size = self.get_file_size(file_path)
                    if size:
                        file_size += size

        return file_size

    def refresh_cache(self) -> bool:
        """Scan the cache directory and evict cache entries."""
        try:
            # Skip eviction if policy is NO_EVICTION
            if self._cache_config.eviction_policy.policy.lower() == NO_EVICTION:
                self._last_refresh_time = datetime.now()
                return True

            # If the process acquires the lock, then proceed with the cache eviction
            with self._cache_refresh_lock_file.acquire(blocking=False):
                self._evict_files()
                self._last_refresh_time = datetime.now()
                return True
        except Timeout:
            # If the process cannot acquire the lock, ignore and wait for the next turn
            pass

        return False

    def acquire_lock(self, key: str) -> BaseFileLock:
        """Create a FileLock object for a given key."""
        if os.path.isabs(key):
            internal_root = os.path.realpath(self._cache_internal_dir)
            lock_target = os.path.realpath(key)
            try:
                if os.path.commonpath([internal_root, lock_target]) != internal_root:
                    raise ValueError(f"Cache lock path escapes the cache internal root: {key}")
            except ValueError as exc:
                if str(exc).startswith("Cache lock path escapes"):
                    raise
                raise ValueError(f"Cache lock path escapes the cache internal root: {key}") from exc
            file_dir = os.path.dirname(lock_target)
            lock_name = f".{os.path.basename(lock_target)}.lock"
            return FileLock(os.path.join(file_dir, lock_name), timeout=self.DEFAULT_FILE_LOCK_TIMEOUT)

        cache_path = self._get_cache_file_path(key)
        file_dir = os.path.dirname(cache_path)

        # Create lock file in the same directory as the file
        lock_name = f".{os.path.basename(key)}.lock"
        lock_file = os.path.join(file_dir, lock_name)
        return FileLock(lock_file, timeout=self.DEFAULT_FILE_LOCK_TIMEOUT)

    def _make_writable(self, file_path: str) -> None:
        """Make file writable by owner while keeping it readable by all.

        Changes permissions to 644 (rw-r--r--).

        :param file_path: Path to the file to make writable.
        """
        os.chmod(file_path, mode=stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)

    def _make_readonly(self, file_path: str) -> None:
        """Make file read-only for all users.

        Changes permissions to 444 (r--r--r--).

        :param file_path: Path to the file to make read-only.
        """
        os.chmod(file_path, mode=stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    def _update_access_time(self, file_path: str) -> None:
        """Update access time to current time for LRU policy.

        Only updates atime, preserving mtime for FIFO ordering.
        This is used to track when files are accessed for LRU eviction.

        :param file_path: Path to the file to update access time.
        """
        current_time_ns = time.time_ns()
        try:
            # Make file writable to update timestamps
            self._make_writable(file_path)
            # Only update atime, preserve mtime for FIFO ordering
            file_stat = os.stat(file_path)
            os.utime(file_path, ns=(current_time_ns, file_stat.st_mtime_ns))
        except (OSError, FileNotFoundError):
            # File might be deleted by another process or have permission issues
            # Just continue without updating the access time
            pass
        finally:
            # Restore read-only permissions
            try:
                self._make_readonly(file_path)
            except OSError:
                pass

    def _should_refresh_cache(self) -> bool:
        """Check if enough time has passed since the last refresh."""
        now = datetime.now()
        return (now - self._last_refresh_time).total_seconds() > self._cache_refresh_interval

    def _run_refresh_cache(self) -> None:
        """Run refresh_cache and clear the scheduling guard when finished."""
        try:
            self.refresh_cache()
        finally:
            with self._cache_refresh_thread_lock:
                if self._cache_refresh_thread is threading.current_thread():
                    self._cache_refresh_thread = None

    def _schedule_refresh_if_needed(self) -> None:
        """Schedule an asynchronous cache refresh if the interval has elapsed."""
        if not self._should_refresh_cache():
            return

        with self._cache_refresh_thread_lock:
            if self._cache_refresh_thread and self._cache_refresh_thread.is_alive():
                return

            thread = threading.Thread(target=self._run_refresh_cache, daemon=True)
            self._cache_refresh_thread = thread
            thread.start()

    # Range cache methods
    @staticmethod
    def _range_cache_key(cache_path: str, cache_identity: Optional[str] = None) -> str:
        """Return the collision-resistant private namespace key for one logical cache path."""
        if cache_identity is not None:
            return f"identity-{cache_identity}"
        return hashlib.sha256(os.fsencode(os.path.abspath(cache_path))).hexdigest()

    def _get_range_cache_entry_dir(self, cache_path: str, cache_identity: Optional[str] = None) -> str:
        """Return the shared private range-cache directory for one logical cache path."""
        return self._resolve_cache_root_path(
            os.path.join(self._range_cache_dir, self._range_cache_key(cache_path, cache_identity)),
            "range entry",
        )

    def _cache_identity_for_cache_path(self, cache_path: str) -> Optional[str]:
        """Infer an identity only for this manager's ordinary profile-local cache paths."""
        profile_root = os.path.abspath(self._get_cache_dir())
        candidate = os.path.abspath(cache_path)
        try:
            if os.path.commonpath([profile_root, candidate]) != profile_root or candidate == profile_root:
                return None
        except ValueError:
            return None
        return self._cache_identity(os.path.relpath(candidate, profile_root))

    def _get_chunk_path(self, cache_path: str, chunk_idx: int, cache_identity: Optional[str] = None) -> str:
        """Return the private path for a range-cache chunk.

        :param cache_path: The base cache path for the original file
        :param chunk_idx: The index of the chunk (0-based)
        :return: The full path to the chunk file
        """
        identity = cache_identity or self._cache_identity_for_cache_path(cache_path)
        return os.path.join(self._get_range_cache_entry_dir(cache_path, identity), f"chunk-{chunk_idx}")

    def _get_chunk_lock_key(self, cache_path: str, chunk_idx: int, cache_identity: Optional[str] = None) -> str:
        """Return the stable bounded lock stripe for one private range-cache chunk."""
        lock_dir = self._resolve_cache_root_path(
            os.path.join(self._range_cache_dir, _RANGE_CACHE_LOCK_DIRECTORY),
            "range lock",
        )
        safe_makedirs(lock_dir)
        identity = cache_identity or self._cache_identity_for_cache_path(cache_path)
        chunk_identity = f"{self._range_cache_key(cache_path, identity)}:{chunk_idx}".encode("ascii")
        stripe = int.from_bytes(hashlib.sha256(chunk_identity).digest()[:8], byteorder="big")
        return self._resolve_cache_root_path(
            os.path.join(lock_dir, f"stripe-{stripe % _RANGE_CACHE_LOCK_STRIPE_COUNT:03d}"),
            "range lock",
        )

    def _get_chunk_metadata_path(self, chunk_path: str) -> str:
        """Return the private portable metadata sidecar for a chunk or promoted cache entry."""
        if self._is_range_cache_path(chunk_path):
            return f"{chunk_path}{_CHUNK_METADATA_SUFFIX}"
        return os.path.join(self._get_range_cache_entry_dir(chunk_path), _PROMOTED_CHUNK_METADATA_NAME)

    def _get_full_cache_publication_marker_path(self, cache_path: str) -> str:
        """Return the fail-closed marker used while a no-xattr full entry is being published."""
        return os.path.join(self._get_range_cache_entry_dir(cache_path), _FULL_CACHE_PUBLICATION_MARKER_NAME)

    @staticmethod
    def _full_cache_descriptor_publication_key(file_stat: os.stat_result) -> str:
        """Return a stable key for one immutable full-cache file descriptor."""
        descriptor = f"{file_stat.st_dev}:{file_stat.st_ino}:{file_stat.st_mtime_ns}:{file_stat.st_size}"
        return hashlib.sha256(descriptor.encode("ascii")).hexdigest()

    def _get_full_cache_descriptor_publication_marker_path(self, file_stat: os.stat_result) -> str:
        """Return the descriptor-bound marker path for an in-progress portable full entry."""
        marker_dir = self._resolve_cache_root_path(
            os.path.join(self._range_cache_dir, _FULL_CACHE_DESCRIPTOR_PUBLICATION_DIRECTORY),
            "full cache publication",
        )
        return os.path.join(
            marker_dir,
            f"{self._full_cache_descriptor_publication_key(file_stat)}{_FULL_CACHE_DESCRIPTOR_PUBLICATION_SUFFIX}",
        )

    def _full_cache_publication_in_progress(self, cache_path: str, file_descriptor: Optional[int] = None) -> bool:
        """Return whether a full-cache path must be treated as a miss until metadata publication finishes."""
        if os.path.exists(self._get_full_cache_publication_marker_path(cache_path)):
            return True
        try:
            file_stat = os.fstat(file_descriptor) if file_descriptor is not None else os.stat(cache_path)
        except OSError:
            return False
        return os.path.exists(self._get_full_cache_descriptor_publication_marker_path(file_stat))

    @staticmethod
    def _write_full_cache_publication_marker(marker_path: str) -> None:
        """Atomically create one publication marker."""
        safe_makedirs(os.path.dirname(marker_path))
        temporary_path: Optional[str] = None
        try:
            with tempfile.NamedTemporaryFile(
                delete=False, dir=os.path.dirname(marker_path), prefix=".publishing_"
            ) as temp:
                temporary_path = temp.name
            os.replace(temporary_path, marker_path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except OSError:
                    pass

    def _mark_full_cache_publication(self, cache_path: str, pending_data_path: str) -> tuple[str, ...]:
        """Atomically mark a no-xattr full-cache entry as unavailable during data and sidecar publication."""
        marker_paths = [self._get_full_cache_publication_marker_path(cache_path)]
        try:
            descriptor_marker_path = self._get_full_cache_descriptor_publication_marker_path(os.stat(pending_data_path))
        except OSError:
            descriptor_marker_path = None
        if descriptor_marker_path is not None and descriptor_marker_path not in marker_paths:
            marker_paths.append(descriptor_marker_path)
        for marker_path in marker_paths:
            self._write_full_cache_publication_marker(marker_path)
        return tuple(marker_paths)

    @staticmethod
    def _remove_full_cache_publication_marker(marker_path: str) -> None:
        """Remove one full-cache publication marker after metadata publication completes."""
        try:
            os.unlink(marker_path)
        except OSError:
            pass

    def _remove_empty_range_cache_entry(self, cache_path: str, cache_identity: Optional[str] = None) -> None:
        """Remove an empty private range-cache directory after its data and sidecars are deleted."""
        entry_dir = (
            os.path.dirname(cache_path)
            if self._is_range_cache_path(cache_path)
            else self._get_range_cache_entry_dir(cache_path, cache_identity)
        )
        try:
            os.rmdir(entry_dir)
        except OSError:
            pass

    def _cached_range_object_size(
        self,
        cache_path: str,
        cache_line_size: int,
        source_version: Optional[str],
        *,
        cache_identity: Optional[str] = None,
        require_source_version: bool,
    ) -> Optional[int]:
        """Return the object size recorded by a valid cached range chunk, if available."""
        entry_dir = self._get_range_cache_entry_dir(cache_path, cache_identity)
        try:
            with os.scandir(entry_dir) as entries:
                for entry in entries:
                    chunk_suffix = entry.name.removeprefix("chunk-")
                    if chunk_suffix == entry.name or not chunk_suffix.isdigit():
                        continue
                    try:
                        if not entry.is_file(follow_symlinks=False):
                            continue
                        with open(entry.path, "rb") as chunk_file:
                            if not self._is_chunk_valid(
                                entry.path,
                                source_version,
                                cache_line_size,
                                remove_invalid=False,
                                require_source_version=require_source_version,
                                file_descriptor=chunk_file.fileno(),
                            ):
                                continue
                            object_size = self._get_chunk_object_size(
                                entry.path,
                                file_descriptor=chunk_file.fileno(),
                            )
                    except OSError:
                        continue
                    if object_size is not None and object_size >= 0:
                        return object_size
        except OSError:
            pass
        return None

    def _download_missing_chunks(
        self,
        cache_path: str,
        original_key: str,
        start_chunk: int,
        end_chunk: int,
        configured_cache_line_size: int,
        storage_provider,
        source_version: Optional[str],
        source_size: Optional[int],
        *,
        cache_identity: Optional[str] = None,
        require_source_version: bool,
        source_size_resolver: Optional[Callable[[], int]] = None,
        request_offset: Optional[int] = None,
    ) -> Optional[int]:
        """Download all missing chunks for a given range with optimized performance."""

        if source_size is None:
            source_size = self._cached_range_object_size(
                cache_path,
                configured_cache_line_size,
                source_version,
                cache_identity=cache_identity,
                require_source_version=require_source_version,
            )

        if source_size is None and source_size_resolver is not None:
            first_missing_chunk = next(
                self._identify_missing_chunks(
                    cache_path,
                    start_chunk,
                    end_chunk,
                    configured_cache_line_size,
                    source_version,
                    cache_identity=cache_identity,
                    require_source_version=require_source_version,
                ),
                None,
            )
            if first_missing_chunk is None:
                return None
            source_size = source_size_resolver()

        if source_size is not None:
            set_source_size = getattr(storage_provider, "set_source_size", None)
            if callable(set_source_size):
                set_source_size(source_size)
            if source_size <= 0:
                return source_size
            if request_offset is not None and request_offset >= source_size:
                return source_size
            end_chunk = min(end_chunk, (source_size - 1) // configured_cache_line_size)
            if start_chunk > end_chunk:
                return source_size

        # Download missing chunks with minimal locking.
        for chunk_idx in self._identify_missing_chunks(
            cache_path,
            start_chunk,
            end_chunk,
            configured_cache_line_size,
            source_version,
            cache_identity=cache_identity,
            require_source_version=require_source_version,
        ):
            self._download_single_chunk(
                cache_path,
                original_key,
                chunk_idx,
                configured_cache_line_size,
                storage_provider,
                source_version,
                source_size,
                cache_identity=cache_identity,
                require_source_version=require_source_version,
            )
        return source_size

    def _identify_missing_chunks(
        self,
        cache_path: str,
        start_chunk: int,
        end_chunk: int,
        cache_line_size: int,
        source_version: Optional[str],
        *,
        cache_identity: Optional[str] = None,
        require_source_version: bool,
    ) -> Iterator[int]:
        """Yield chunks that are missing or invalid without materializing the request span."""
        for chunk_idx in range(start_chunk, end_chunk + 1):
            chunk_path = self._get_chunk_path(cache_path, chunk_idx, cache_identity)

            if not self._is_chunk_valid(
                chunk_path,
                source_version,
                cache_line_size,
                remove_invalid=False,
                require_source_version=require_source_version,
            ):
                yield chunk_idx

    def _is_chunk_valid(
        self,
        chunk_path: str,
        source_version: Optional[str],
        cache_line_size: int,
        *,
        remove_invalid: bool = True,
        require_source_version: bool = False,
        file_descriptor: Optional[int] = None,
    ) -> bool:
        """Check if a chunk exists and has valid metadata."""
        if file_descriptor is None:
            if not os.path.exists(chunk_path):
                return False
        else:
            try:
                os.fstat(file_descriptor)
            except OSError:
                return False

        if require_source_version and not self._has_source_version(source_version):
            return False

        metadata = self._get_chunk_metadata(
            chunk_path,
            require_source_version=require_source_version,
            file_descriptor=file_descriptor,
        )
        if metadata is None or metadata["cache_line_size"] != cache_line_size:
            if remove_invalid:
                self._remove_invalid_chunk_for_descriptor(chunk_path, file_descriptor)
            return False
        if require_source_version and metadata["source_version"] != source_version:
            if remove_invalid:
                self._remove_invalid_chunk_for_descriptor(chunk_path, file_descriptor)
            return False
        return True

    def _remove_invalid_chunk(self, chunk_path: str) -> None:
        """Safely remove an invalid chunk file."""
        try:
            os.unlink(chunk_path)
        except OSError:
            pass
        self._remove_chunk_metadata_sidecar(chunk_path)
        self._remove_empty_range_cache_entry(chunk_path)

    def _remove_invalid_chunk_for_descriptor(self, chunk_path: str, file_descriptor: Optional[int]) -> None:
        """Remove an invalid chunk only when its path still names the inspected file descriptor."""
        if file_descriptor is None:
            self._remove_invalid_chunk(chunk_path)
            return
        with self._cache_refresh_lock_file:
            try:
                if not os.path.samestat(os.stat(chunk_path), os.fstat(file_descriptor)):
                    return
            except OSError:
                return
            self._remove_invalid_chunk(chunk_path)

    def _download_single_chunk(
        self,
        cache_path: str,
        original_key: str,
        chunk_idx: int,
        cache_line_size: int,
        storage_provider,
        source_version: Optional[str],
        source_size: Optional[int],
        *,
        cache_identity: Optional[str] = None,
        require_source_version: bool,
    ) -> None:
        """Download and cache a single chunk with proper locking.

        :param cache_path: The base cache path for the original file
        :param original_key: The original key for the object
        :param chunk_idx: The index of the chunk (0-based)
        :param cache_line_size: The size of each chunk in bytes
        :param storage_provider: The storage provider to fetch the chunk from
        :param source_version: The source version of the object

        This method downloads a single chunk from the storage provider and caches it locally.
        It also sets the appropriate metadata on the chunk file.

        If the chunk already exists, it is not downloaded again.

        If the chunk is created by another thread, it is skipped.
        """
        chunk_path = self._get_chunk_path(cache_path, chunk_idx, cache_identity)
        chunk_lock_key = self._get_chunk_lock_key(cache_path, chunk_idx, cache_identity)

        with self.acquire_lock(chunk_lock_key):
            # Revalidate after waiting: another reader may have published a chunk
            # from an older source revision or a differently sized cache line.
            if self._is_chunk_valid(
                chunk_path,
                source_version,
                cache_line_size,
                require_source_version=require_source_version,
            ):
                return

            # Fetch and cache the chunk
            self._fetch_and_cache_chunk(
                cache_path,
                original_key,
                chunk_idx,
                cache_line_size,
                storage_provider,
                source_version,
                source_size,
                cache_identity=cache_identity,
            )

    def _fetch_and_cache_chunk(
        self,
        cache_path: str,
        original_key: str,
        chunk_idx: int,
        cache_line_size: int,
        storage_provider,
        source_version: Optional[str],
        source_size: Optional[int],
        *,
        cache_identity: Optional[str] = None,
    ) -> None:
        """Fetch chunk data from storage and cache it locally.

        :param cache_path: The base cache path for the original file
        :param original_key: The original key for the object
        :param chunk_idx: The index of the chunk (0-based)
        :param cache_line_size: The size of each chunk in bytes
        :param storage_provider: The storage provider to fetch the chunk from
        :param source_version: The source version of the object
        """
        chunk_path = self._get_chunk_path(cache_path, chunk_idx, cache_identity)

        # Calculate chunk range
        chunk_start = chunk_idx * cache_line_size
        chunk_end = chunk_start + cache_line_size - 1

        # Fetch chunk data
        chunk_data = storage_provider.get_object(
            original_key, Range(offset=chunk_start, size=chunk_end - chunk_start + 1)
        )

        # Cache the chunk
        self._write_chunk_to_cache(
            chunk_path,
            chunk_data,
            source_version,
            chunk_idx,
            cache_line_size,
            source_size,
            cache_identity=cache_identity,
        )

        # Handle special case for chunk 0 with small files
        self._handle_chunk0_renaming(cache_path, chunk_path, chunk_idx, cache_line_size, cache_identity=cache_identity)

        # Update access time
        self._update_chunk_access_time(cache_path, chunk_path, chunk_idx)
        self._schedule_refresh_if_needed()

    def _write_chunk_to_cache(
        self,
        chunk_path: str,
        chunk_data: bytes,
        source_version: Optional[str],
        chunk_idx: int,
        cache_line_size: int,
        source_size: Optional[int],
        *,
        cache_identity: Optional[str] = None,
    ) -> None:
        """Atomically write chunk data to cache with metadata.

        Writes to a temporary file, attaches xattrs when available, then
        atomically replaces the target path. Filesystems without user xattrs
        receive an atomically published metadata sidecar tied to the chunk's
        filesystem identity.

        :param chunk_path: The path to the chunk file
        :param chunk_data: The data to write to the chunk file
        :param source_version: The source version of the object
        :param chunk_idx: The index of the chunk being cached
        :param cache_line_size: The size of each chunk in bytes
        :param source_size: Optional size of the source object in bytes
        """
        safe_makedirs(os.path.dirname(chunk_path))

        object_size = source_size
        # If metadata was not fetched, a short chunk implies we reached the final chunk.
        # In this case the object size is chunk_idx * cache_line_size + len(chunk_data)
        if object_size is None and len(chunk_data) < cache_line_size:
            object_size = chunk_idx * cache_line_size + len(chunk_data)

        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", delete=False, dir=os.path.dirname(chunk_path), prefix=".chunk_tmp_"
            ) as temp_file:
                temp_path = temp_file.name
                temp_file.write(chunk_data)
            metadata_in_xattrs = self._set_chunk_metadata(
                temp_path,
                source_version,
                cache_line_size,
                object_size,
                cache_identity=cache_identity,
            )
            with self._cache_refresh_lock_file:
                os.replace(temp_path, chunk_path)
                temp_path = None
                if metadata_in_xattrs:
                    self._remove_chunk_metadata_sidecar(chunk_path)
                else:
                    self._write_chunk_metadata_sidecar(
                        chunk_path,
                        source_version,
                        cache_line_size,
                        object_size,
                        cache_identity=cache_identity,
                    )
                self._make_readonly(chunk_path)
                self._update_access_time(chunk_path)
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

    def _set_chunk_metadata(
        self,
        chunk_path: str,
        source_version: Optional[str],
        cache_line_size: int,
        object_size: Optional[int],
        *,
        cache_identity: Optional[str] = None,
    ) -> bool:
        """Set xattr metadata for a chunk file.
        :param chunk_path: The path to the chunk file
        :param source_version: The source version of the object
        :param cache_line_size: The size of each chunk in bytes
        :param object_size: Optional total size of the source object in bytes
        """
        try:
            if source_version:
                xattr.setxattr(chunk_path, "user.etag", source_version.encode("utf-8"))

            xattr.setxattr(chunk_path, "user.cache_line_size", str(cache_line_size).encode("utf-8"))
            if object_size is not None:
                xattr.setxattr(chunk_path, "user.size", str(object_size).encode("utf-8"))
            if cache_identity is not None:
                xattr.setxattr(chunk_path, _CACHE_IDENTITY_XATTR, cache_identity.encode("ascii"))
        except OSError:
            return False
        return True

    def _write_chunk_metadata_sidecar(
        self,
        chunk_path: str,
        source_version: Optional[str],
        cache_line_size: int,
        object_size: Optional[int],
        *,
        cache_identity: Optional[str] = None,
    ) -> None:
        """Atomically persist chunk metadata when the cache filesystem has no user xattrs."""
        chunk_stat = os.stat(chunk_path)
        metadata = {
            "cache_line_size": cache_line_size,
            "source_version": source_version,
            "object_size": object_size,
            "device": chunk_stat.st_dev,
            "inode": chunk_stat.st_ino,
            "mtime_ns": chunk_stat.st_mtime_ns,
            "chunk_size": chunk_stat.st_size,
        }
        if cache_identity is not None:
            metadata[_CACHE_IDENTITY_METADATA_NAME] = cache_identity
        metadata_path = self._get_chunk_metadata_path(chunk_path)
        safe_makedirs(os.path.dirname(metadata_path))
        temporary_path: Optional[str] = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                delete=False,
                dir=os.path.dirname(metadata_path),
                prefix=".chunk_metadata_tmp_",
            ) as temporary:
                temporary_path = temporary.name
                json.dump(metadata, temporary, sort_keys=True, separators=(",", ":"))
                temporary.flush()
            os.replace(temporary_path, metadata_path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except OSError:
                    pass

    def _remove_chunk_metadata_sidecar(self, chunk_path: str) -> None:
        """Remove portable range-cache metadata without treating an absent sidecar as an error."""
        try:
            os.unlink(self._get_chunk_metadata_path(chunk_path))
        except OSError:
            pass
        self._remove_empty_range_cache_entry(chunk_path)

    def _get_chunk_metadata(
        self,
        chunk_path: str,
        *,
        require_source_version: bool = False,
        require_object_size: bool = False,
        file_descriptor: Optional[int] = None,
        allow_descriptor_size_mismatch: bool = False,
    ) -> Optional[dict[str, Any]]:
        """Load xattr metadata when complete, otherwise use a verified portable sidecar."""
        metadata_target: Union[str, int] = file_descriptor if file_descriptor is not None else chunk_path
        try:
            metadata: dict[str, Any] = {
                "cache_line_size": int(xattr.getxattr(metadata_target, "user.cache_line_size").decode("utf-8")),
                "source_version": None,
                "object_size": None,
            }
            if require_source_version:
                source_version = xattr.getxattr(metadata_target, "user.etag").decode("utf-8")
                if not self._has_source_version(source_version):
                    return None
                metadata["source_version"] = source_version
            if require_object_size:
                metadata["object_size"] = int(xattr.getxattr(metadata_target, "user.size").decode("utf-8"))
            return metadata
        except (OSError, UnicodeDecodeError, ValueError):
            sidecar_metadata = self._read_chunk_metadata_sidecar(
                chunk_path,
                require_source_version=require_source_version,
                file_descriptor=file_descriptor,
                allow_descriptor_size_mismatch=allow_descriptor_size_mismatch,
            )
            if sidecar_metadata is not None or self._is_range_cache_path(chunk_path) or file_descriptor is None:
                return sidecar_metadata
            matched_metadata, _ = self._read_full_cache_metadata_sidecar(
                chunk_path,
                require_source_version=require_source_version,
                file_descriptor=file_descriptor,
                allow_descriptor_size_mismatch=allow_descriptor_size_mismatch,
                include_direct_path=False,
            )
            return matched_metadata

    def _full_cache_metadata_sidecar_paths(self, cache_path: str, *, include_direct_path: bool = True):
        """Yield portable full-cache sidecars that may describe an opened descriptor."""
        direct_path = self._get_chunk_metadata_path(cache_path)
        if include_direct_path and os.path.exists(direct_path):
            yield direct_path
        try:
            entries = list(os.scandir(self._range_cache_dir))
        except OSError:
            return
        for entry in entries:
            if not entry.is_dir(follow_symlinks=False):
                continue
            metadata_path = os.path.join(entry.path, _PROMOTED_CHUNK_METADATA_NAME)
            if include_direct_path and metadata_path == direct_path:
                continue
            if os.path.exists(metadata_path):
                yield metadata_path

    def _read_full_cache_metadata_sidecar(
        self,
        cache_path: str,
        *,
        require_source_version: bool = False,
        file_descriptor: int,
        allow_descriptor_size_mismatch: bool = False,
        include_direct_path: bool = True,
    ) -> tuple[Optional[dict[str, Any]], bool]:
        """Find metadata for a full cache descriptor even when a native filesystem aliases its path."""
        direct_path = self._get_chunk_metadata_path(cache_path)
        saw_direct_sidecar = include_direct_path and os.path.exists(direct_path)
        for metadata_path in self._full_cache_metadata_sidecar_paths(
            cache_path,
            include_direct_path=include_direct_path,
        ):
            metadata = self._read_chunk_metadata_sidecar(
                cache_path,
                require_source_version=require_source_version,
                file_descriptor=file_descriptor,
                allow_descriptor_size_mismatch=allow_descriptor_size_mismatch,
                metadata_path=metadata_path,
            )
            if metadata is not None:
                return metadata, True
        return None, saw_direct_sidecar

    def _read_chunk_metadata_sidecar(
        self,
        chunk_path: str,
        *,
        require_source_version: bool = False,
        file_descriptor: Optional[int] = None,
        allow_descriptor_size_mismatch: bool = False,
        metadata_path: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """Return sidecar metadata only when it belongs to the current atomically published chunk."""
        try:
            sidecar_path = metadata_path or self._get_chunk_metadata_path(chunk_path)
            with open(sidecar_path, encoding="utf-8") as metadata_file:
                metadata = json.load(metadata_file)
            if not isinstance(metadata, dict):
                return None
            for name in ("cache_line_size", "device", "inode", "mtime_ns", "chunk_size"):
                value = metadata.get(name)
                if not isinstance(value, int) or isinstance(value, bool):
                    return None
            source_version = metadata.get("source_version")
            if source_version is not None and not isinstance(source_version, str):
                return None
            if require_source_version and not self._has_source_version(source_version):
                return None
            object_size = metadata.get("object_size")
            if object_size is not None and (not isinstance(object_size, int) or isinstance(object_size, bool)):
                return None
            chunk_stat = os.fstat(file_descriptor) if file_descriptor is not None else os.stat(chunk_path)
            if (metadata["device"], metadata["inode"]) != (chunk_stat.st_dev, chunk_stat.st_ino):
                return None
            if not allow_descriptor_size_mismatch and (
                metadata["mtime_ns"],
                metadata["chunk_size"],
            ) != (
                chunk_stat.st_mtime_ns,
                chunk_stat.st_size,
            ):
                return None
            return metadata
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return None

    def _get_chunk_object_size(
        self,
        chunk_path: str,
        *,
        file_descriptor: Optional[int] = None,
        allow_descriptor_size_mismatch: bool = False,
    ) -> Optional[int]:
        """Return the stored object size for a chunk, if available."""
        metadata = self._get_chunk_metadata(
            chunk_path,
            require_object_size=True,
            file_descriptor=file_descriptor,
            allow_descriptor_size_mismatch=allow_descriptor_size_mismatch,
        )
        if metadata is None:
            return None
        object_size = metadata["object_size"]
        return object_size if isinstance(object_size, int) and not isinstance(object_size, bool) else None

    @staticmethod
    def _copy_metadata_sidecar_for_promotion(source_path: str, destination_path: str) -> None:
        """Atomically copy one chunk sidecar before exposing its hard-linked full-cache entry."""
        safe_makedirs(os.path.dirname(destination_path))
        temporary_path: Optional[str] = None
        try:
            with tempfile.NamedTemporaryFile(
                delete=False, dir=os.path.dirname(destination_path), prefix=".promoted_"
            ) as temp:
                temporary_path = temp.name
            os.unlink(temporary_path)
            os.link(source_path, temporary_path)
            os.replace(temporary_path, destination_path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except OSError:
                    pass

    def _handle_chunk0_renaming(
        self,
        cache_path: str,
        chunk_path: str,
        chunk_idx: int,
        cache_line_size: int,
        *,
        cache_identity: Optional[str] = None,
    ) -> None:
        """Handle special case where chunk 0 becomes the full file for small files.
        :param cache_path: The base cache path for the original file
        :param chunk_path: The path to the chunk file
        :param chunk_idx: The index of the chunk (0-based)
        :param cache_line_size: The size of each chunk in bytes
        """
        if chunk_idx != 0:
            return

        with self._cache_refresh_lock_file:
            if cache_identity is not None:
                try:
                    with open(chunk_path, "rb") as chunk_file:
                        if not self._descriptor_matches_cache_identity(
                            chunk_path,
                            chunk_file.fileno(),
                            cache_identity,
                        ):
                            return
                except OSError:
                    return
            if os.path.exists(cache_path):
                return
            try:
                chunk_size = os.path.getsize(chunk_path)
                if chunk_size >= cache_line_size:
                    return
                safe_makedirs(os.path.dirname(cache_path))
                chunk_metadata_path = self._get_chunk_metadata_path(chunk_path)
                if os.path.exists(chunk_metadata_path):
                    self._copy_metadata_sidecar_for_promotion(
                        chunk_metadata_path,
                        self._get_chunk_metadata_path(cache_path),
                    )
                os.link(chunk_path, cache_path)
                os.unlink(chunk_path)
                if os.path.exists(chunk_metadata_path):
                    os.unlink(chunk_metadata_path)
                else:
                    self._remove_chunk_metadata_sidecar(cache_path)
                self._make_readonly(cache_path)
                self._update_access_time(cache_path)
                self._remove_full_cache_publication_marker(self._get_full_cache_publication_marker_path(cache_path))
                self._remove_empty_range_cache_entry(chunk_path)
            except FileExistsError:
                pass
            except OSError:
                pass

    def _update_chunk_access_time(self, cache_path: str, chunk_path: str, chunk_idx: int) -> None:
        """Update access time for proper LRU eviction.
        :param cache_path: The base cache path for the original file
        :param chunk_path: The path to the chunk file
        :param chunk_idx: The index of the chunk (0-based)
        """
        with self._cache_refresh_lock_file:
            if chunk_idx == 0 and os.path.exists(cache_path) and not os.path.exists(chunk_path):
                self._update_access_time(cache_path)
            else:
                self._update_access_time(chunk_path)

    def _assemble_result_from_chunks(
        self,
        cache_path: str,
        start_chunk: int,
        end_chunk: int,
        configured_cache_line_size: int,
        byte_range: Range,
        source_version: Optional[str] = None,
        *,
        cache_identity: Optional[str] = None,
        require_source_version: bool = False,
        legacy_key: Optional[str] = None,
    ) -> bytes:
        """Assemble the requested byte range from locally cached chunks.

        Reads only the overlapping portion of each chunk and copies it into a
        pre-allocated result buffer. When a chunk on disk is shorter than the
        slice needed from it, the stored object-size metadata is consulted to
        determine whether the chunk is a valid short final chunk or corrupt.
        An ``IOError`` is raised (and the chunk invalidated) when the chunk
        is too short and the xattr is missing or contradicts the file size,
        or when reading the expected bytes still returns a short read.
        Full-size chunks are read without consulting xattr at all.

        :param cache_path: The base cache path for the original file
        :param start_chunk: The starting chunk index
        :param end_chunk: The ending chunk index
        :param configured_cache_line_size: The size of each chunk in bytes
        :param byte_range: The byte range to read (offset and size)
        :param source_version: Source revision that every assembled chunk must match
        :return: The assembled byte range data
        :raises IOError: If a short chunk is missing size metadata, is
            smaller than the stored object size indicates, or returns a
            short read
        """
        # Pre-allocate the largest possible buffer and trim to actual bytes read.
        result = bytearray(byte_range.size)
        result_offset = 0

        for chunk_idx in range(start_chunk, end_chunk + 1):
            chunk_path = self._get_chunk_path(cache_path, chunk_idx, cache_identity)
            chunk_lock_key = self._get_chunk_lock_key(cache_path, chunk_idx, cache_identity)

            with self.acquire_lock(chunk_lock_key):
                # Chunk zero may have been promoted to a full-file cache entry.
                if chunk_idx == 0 and os.path.exists(cache_path) and not os.path.exists(chunk_path):
                    actual_path = cache_path
                else:
                    actual_path = chunk_path

                with open(actual_path, "rb") as chunk_file:
                    file_descriptor = chunk_file.fileno()
                    if (
                        actual_path == cache_path
                        and cache_identity is not None
                        and not self._descriptor_matches_cache_identity(
                            cache_path,
                            file_descriptor,
                            cache_identity,
                            legacy_key=legacy_key,
                        )
                    ):
                        raise IOError(f"Chunk file {actual_path} belongs to another logical cache identity")
                    if not self._is_chunk_valid(
                        actual_path,
                        source_version,
                        configured_cache_line_size,
                        remove_invalid=False,
                        require_source_version=require_source_version,
                        file_descriptor=file_descriptor,
                    ):
                        raise IOError(f"Chunk file {actual_path} does not match the requested source revision")

                    # Calculate the portion of this chunk that overlaps with the requested range
                    chunk_start = chunk_idx * configured_cache_line_size
                    chunk_end = chunk_start + configured_cache_line_size - 1

                    overlap_start = max(byte_range.offset, chunk_start)
                    overlap_end = min(byte_range.offset + byte_range.size - 1, chunk_end)

                    if overlap_start <= overlap_end:
                        # Calculate positions within the chunk file and result buffer
                        chunk_offset = overlap_start - chunk_start
                        length = overlap_end - overlap_start + 1

                        # The chunk on disk may be shorter than cache_line_size (final chunk).
                        file_size = os.fstat(file_descriptor).st_size
                        expected_length = length
                        if file_size < chunk_offset + length:
                            # Chunk is too small for the naive slice -- consult object size metadata.
                            object_size = self._get_chunk_object_size(actual_path, file_descriptor=file_descriptor)
                            if object_size is None:
                                # No metadata to validate; treat as corrupt.
                                self._remove_invalid_chunk_for_descriptor(actual_path, file_descriptor)
                                raise IOError(
                                    f"Chunk file {actual_path} is too small for requested slice: "
                                    f"size={file_size}, needed={chunk_offset + length}"
                                )

                            # Verify the chunk isn't truncated relative to what object size implies.
                            expected_chunk_size = min(configured_cache_line_size, max(object_size - chunk_start, 0))
                            if file_size < expected_chunk_size:
                                self._remove_invalid_chunk_for_descriptor(actual_path, file_descriptor)
                                raise IOError(
                                    f"Chunk file {actual_path} is smaller than expected object metadata: "
                                    f"size={file_size}, expected={expected_chunk_size}"
                                )

                            # Clamp to the bytes that actually exist in the object.
                            expected_length = max(min(length, object_size - overlap_start), 0)
                            if expected_length == 0:
                                # Requested range is entirely past the object's EOF.
                                continue
                        chunk_file.seek(chunk_offset)
                        chunk_data = chunk_file.read(expected_length)
                        if len(chunk_data) != expected_length:
                            self._remove_invalid_chunk_for_descriptor(actual_path, file_descriptor)
                            raise IOError(
                                f"Chunk file {actual_path} returned a short read: "
                                f"expected={expected_length}, actual={len(chunk_data)}"
                            )

                        # Copy directly to result buffer
                        result[result_offset : result_offset + expected_length] = chunk_data
                        result_offset += expected_length

        return bytes(result[:result_offset])

    def _invalidate_chunks(
        self,
        cache_path: str,
        cache_identity: Optional[str] = None,
        *,
        infer_cache_identity: bool = True,
    ):
        """Delete all chunks and metadata for a path.

        Removes every data file and sidecar in the private range-cache entry
        associated with the given logical cache path.

        :param cache_path: The base cache path for which to invalidate all chunks
        """
        identity = cache_identity
        if identity is None and infer_cache_identity:
            identity = self._cache_identity_for_cache_path(cache_path)
        entry_dir = self._get_range_cache_entry_dir(cache_path, identity)
        try:
            paths = [entry.path for entry in os.scandir(entry_dir) if entry.is_file()]
        except OSError:
            paths = []

        for path in paths:
            try:
                os.unlink(path)
            except OSError:
                pass
        if identity is None:
            self._remove_chunk_metadata_sidecar(cache_path)
        self._remove_empty_range_cache_entry(cache_path, identity)

    def _read_range_from_full_cached_file(
        self,
        cache_path: str,
        byte_range: Range,
        source_version: Optional[str],
        *,
        cache_identity: str,
        require_source_version: bool = False,
        legacy_key: Optional[str] = None,
    ) -> Optional[bytes]:
        """Read a byte range from a full cached file if it exists and is valid.

        This method checks if a full cached file exists at the given cache path,
        validates its etag against the source version, and if valid, reads the
        requested byte range directly from the cached file.

        :param cache_path: Path to the cached file
        :param byte_range: The byte range to read (offset and size)
        :param source_version: Source version identifier for cache validation
        :return: The requested byte range data if successful, None otherwise
        """
        if self._full_cache_publication_in_progress(cache_path):
            return None
        try:
            with open(cache_path, "rb") as cached_file:
                file_descriptor = cached_file.fileno()
                if self._full_cache_publication_in_progress(cache_path, file_descriptor):
                    return None
                if not self._descriptor_matches_cache_identity(
                    cache_path,
                    file_descriptor,
                    cache_identity,
                    legacy_key=legacy_key,
                ):
                    return None
                expected_size = self._validated_descriptor_size(cache_path, file_descriptor)
                if expected_size is None:
                    return None
                if require_source_version and not self._descriptor_matches_source_version(
                    cache_path,
                    file_descriptor,
                    source_version,
                ):
                    return None

                cached_file.seek(byte_range.offset)
                expected_length = max(min(byte_range.size, expected_size - byte_range.offset), 0)
                data = cached_file.read(expected_length)
                if len(data) != expected_length:
                    self._remove_invalid_chunk_for_descriptor(cache_path, file_descriptor)
                    return None
                if not self._descriptor_matches_expected_size(cache_path, file_descriptor, expected_size):
                    return None
        except OSError:
            return None

        self._update_access_time(cache_path)
        return data

    def _read_range(
        self,
        cache_path: str,
        byte_range: Range,
        storage_provider,
        source_version: Optional[str],
        source_size: Optional[int],
        original_key: str,
        *,
        cache_identity: Optional[str] = None,
        require_source_version: bool,
        source_size_resolver: Optional[Callable[[], int]] = None,
    ) -> bytes:
        """Read a byte range using optimized chunk-based caching.

        This method implements partial file caching with performance optimizations:
        1. Direct read for small ranges (bypass chunking overhead)
        2. Optimized chunk downloading with minimal locking
        3. Streaming assembly with reduced memory usage

        Note: This method assumes that full-file cache optimization has already been
        checked by the calling read() method.

        :param cache_path: Path where chunks should be cached locally
        :param byte_range: The byte range to read (offset and size)
        :param storage_provider: Storage provider to fetch chunks from if not cached
        :param source_version: Source version identifier for cache validation
        :param original_key: Original object key for fetching from storage provider
        :return: The requested byte range data
        """
        configured_cache_line_size = (
            self._cache_line_size
        )  # Type checker knows this is not None due to range_cache_enabled check
        if configured_cache_line_size is None:
            raise RuntimeError("Cache line size is not configured")
        if byte_range.size == 0:
            return b""

        # Calculate needed chunks
        start_chunk = byte_range.offset // configured_cache_line_size
        end_chunk = (byte_range.offset + byte_range.size - 1) // configured_cache_line_size

        try:
            # Step 1: Download all missing chunks (optimized)
            source_size = self._download_missing_chunks(
                cache_path,
                original_key,
                start_chunk,
                end_chunk,
                configured_cache_line_size,
                storage_provider,
                source_version,
                source_size,
                cache_identity=cache_identity,
                require_source_version=require_source_version,
                source_size_resolver=source_size_resolver,
                request_offset=byte_range.offset,
            )

            if source_size is not None:
                remaining = source_size - byte_range.offset
                if remaining <= 0:
                    return b""
                byte_range = Range(offset=byte_range.offset, size=min(byte_range.size, remaining))
                end_chunk = (byte_range.offset + byte_range.size - 1) // configured_cache_line_size

            # Step 2: Assemble result from cached chunks (streaming)
            return self._assemble_result_from_chunks(
                cache_path,
                start_chunk,
                end_chunk,
                configured_cache_line_size,
                byte_range,
                source_version,
                cache_identity=cache_identity,
                require_source_version=require_source_version,
                legacy_key=original_key,
            )
        except _SourceReadError:
            raise
        except RetryableError:
            raise
        except Exception as e:
            # If any step fails, fall back to getting the object directly
            logging.warning(f"Failed to process chunks for {original_key}: {e}")
            if source_size is not None:
                remaining = source_size - byte_range.offset
                if remaining <= 0:
                    return b""
                byte_range = Range(offset=byte_range.offset, size=min(byte_range.size, remaining))
            return storage_provider.get_object(original_key, byte_range=byte_range)
