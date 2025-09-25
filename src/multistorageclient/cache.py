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
import logging
import os
import stat
import tempfile
import threading
import time
from collections import OrderedDict
from datetime import datetime
from typing import Any, Optional, Union

import xattr
from filelock import BaseFileLock, FileLock, Timeout

from .caching.cache_config import CacheConfig
from .caching.cache_item import CacheItem
from .caching.eviction_policy import FIFO, LRU, NO_EVICTION, RANDOM, EvictionPolicyFactory
from .instrumentation.utils import CacheManagerMetricsHelper
from .types import Range, SourceVersionCheckMode

DEFAULT_CACHE_SIZE = "10G"
DEFAULT_CACHE_SIZE_MB = "10000"
DEFAULT_CACHE_REFRESH_INTERVAL = 300  # 5 minutes
DEFAULT_LOCK_TIMEOUT = 600  # 10 minutes
DEFAULT_CACHE_LINE_SIZE = "64M"


class CacheManager:
    """
    A concrete implementation of the :py:class:`CacheBackend` that stores cache data in the local filesystem.
    """

    DEFAULT_FILE_LOCK_TIMEOUT = 600

    def __init__(self, profile: str, cache_config: CacheConfig):
        """
        Initializes the :py:class:`FileSystemBackend` with the given profile and configuration.

        :param profile: The profile name for the cache.
        :param cache_config: The cache configuration settings.
        """
        self._profile = profile
        self._cache_config = cache_config
        self._max_cache_size = cache_config.size_bytes()
        self._last_refresh_time = datetime.now()
        self._metrics_helper = CacheManagerMetricsHelper()
        self._cache_refresh_interval = cache_config.eviction_policy.refresh_interval

        # Range cache configuration
        self._cache_line_size = cache_config.cache_line_size_bytes()

        default_location = os.path.join(tempfile.gettempdir(), "msc-cache")
        # Create cache directory if it doesn't exist, this is used to download files
        self._cache_dir = os.path.abspath(cache_config.location or default_location)
        self._cache_path = os.path.join(self._cache_dir, self._profile)
        os.makedirs(self._cache_path, exist_ok=True)

        # Check if eviction policy is valid for this backend
        if not self._check_if_eviction_policy_is_valid(cache_config.eviction_policy.policy):
            raise ValueError(f"Invalid eviction policy: {cache_config.eviction_policy.policy}")

        self._eviction_policy = EvictionPolicyFactory.create(cache_config.eviction_policy.policy)

        # Create a lock file for cache refresh operations
        self._cache_refresh_lock_file = FileLock(
            os.path.join(self._cache_path, ".cache_refresh.lock"), timeout=self.DEFAULT_FILE_LOCK_TIMEOUT
        )

        # Populate cache with existing files in the cache directory
        self.refresh_cache()

    def _check_if_eviction_policy_is_valid(self, eviction_policy: str) -> bool:
        """Check if the eviction policy is valid for this backend.

        :param eviction_policy: The eviction policy to check.
        :return: True if the policy is valid, False otherwise.
        """
        return eviction_policy.lower() in {LRU, FIFO, RANDOM, NO_EVICTION}

    def get_file_size(self, file_path: str) -> Optional[int]:
        """Get the size of the file in bytes.

        Args:
            file_path: Path to the file

        Returns:
            Optional[int]: Size of the file in bytes, or None if file doesn't exist
        """
        try:
            return os.path.getsize(file_path)
        except OSError:
            return None

    def delete_file(self, file_path: str) -> None:
        """Delete a file from the cache directory.

        Args:
            file_path: Path to the file relative to cache directory
        """
        try:
            # Construct absolute path using cache directory as base
            abs_path = os.path.join(self._get_cache_dir(), file_path)
            os.unlink(abs_path)

            # Handle lock file - keep it in same directory as the file
            lock_name = f".{os.path.basename(file_path)}.lock"
            lock_path = os.path.join(os.path.dirname(abs_path), lock_name)
            os.unlink(lock_path)
        except OSError:
            pass

    def evict_files(self) -> None:
        """
        Evict cache entries based on the configured eviction policy.
        """
        logging.debug("\nStarting evict_files...")
        cache_items: list[CacheItem] = []

        # Traverse the directory and subdirectories
        for dirpath, _, filenames in os.walk(self._cache_dir):
            for file_name in filenames:
                file_path = os.path.join(dirpath, file_name)
                # Skip lock files, but allow chunk files (hidden files that contain '#chunk')
                if file_name.endswith(".lock") or (file_name.startswith(".") and "#chunk" not in file_name):
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
            # Use the relative path from cache directory
            rel_path = os.path.relpath(item.file_path, self._cache_path)
            cache[rel_path] = item.file_size
            cache_size += item.file_size
        logging.debug(f"Total cache size: {cache_size}, Max allowed: {self._max_cache_size}")

        # Evict old files if necessary in case the existing files exceed cache size
        while cache_size > self._max_cache_size:
            # Pop the first item in the OrderedDict (according to policy's sorting)
            oldest_file, file_size = cache.popitem(last=False)
            cache_size -= file_size
            logging.debug(f"Evicting file: {oldest_file}, size: {file_size}")
            self.delete_file(oldest_file)

        logging.debug("\nFinal cache contents:")
        for file_path in cache.keys():
            logging.debug(f"Remaining file: {file_path}")

    def check_source_version(self) -> bool:
        """Check if etag is used in the cache config."""
        return self._cache_config.check_source_version

    def get_max_cache_size(self) -> int:
        """Return the cache size in bytes from the cache config."""
        return self._max_cache_size

    def _get_cache_dir(self) -> str:
        """Return the path to the local cache directory."""
        return os.path.join(self._cache_dir, self._profile)

    def _get_cache_file_path(self, key: str) -> str:
        """Return the path to the local cache file for the given key."""
        # Handle absolute paths by making them relative to the cache directory
        if os.path.isabs(key):
            # For absolute paths, use os.path.relpath() to preserve the full path structure
            relative_key = os.path.relpath(key, "/")
        else:
            relative_key = key

        cache_path = os.path.join(self._cache_dir, self._profile, relative_key)
        return cache_path

    def read(
        self,
        key: str,
        source_version: Optional[str] = None,
        byte_range: Optional[Range] = None,
        storage_provider: Optional[Any] = None,
    ) -> Optional[bytes]:
        """Read the contents of a file from the cache if it exists.

        This method handles both full-file reads and partial file caching. For range reads
        (when byte_range is provided), it delegates to _read_range to implement partial
        file caching using chunks. For full-file reads, it uses the existing cache logic.

        :param key: The cache key for the file
        :param source_version: Optional source version for cache validation
        :param byte_range: Optional byte range for partial file reads
        :param storage_provider: Storage provider required for range reads
        :return: The file contents as bytes, or None if not found in cache
        """
        # If this is a range read, check for full cached file first
        if byte_range:
            cache_path = self._get_cache_file_path(key)

            # Try to read range from full cached file first
            range_data = self._read_range_from_full_cached_file(cache_path, byte_range, source_version)
            if range_data is not None:
                return range_data

            # No valid full cached file, delegate to chunk-based range reading
            return self._read_range(cache_path, byte_range, storage_provider, source_version, key)  # type: ignore[arg-type]

        # Full-file cached read (existing behavior)
        success = True
        try:
            try:
                if self.contains(key=key, source_version=source_version):
                    file_path = self._get_cache_file_path(key)
                    with open(file_path, "rb") as fp:
                        data = fp.read()
                    # Update access time based on eviction policy
                    self._update_access_time(file_path)
                    return data
            except OSError:
                pass

            # cache miss
            success = False
            return None
        finally:
            self._metrics_helper.increase(operation="READ", success=success)

    def open(
        self,
        key: str,
        mode: str = "rb",
        source_version: Optional[str] = None,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
    ) -> Optional[Any]:
        """Open a file from the cache and return the file object."""
        success = True
        try:
            try:
                if self.contains(key=key, check_source_version=check_source_version, source_version=source_version):
                    file_path = self._get_cache_file_path(key)
                    # Update access time based on eviction policy
                    self._update_access_time(file_path)
                    return open(file_path, mode)
            except OSError:
                pass

            # cache miss
            success = False
            return None
        finally:
            self._metrics_helper.increase(operation="OPEN", success=success)

    def set(self, key: str, source: Union[str, bytes], source_version: Optional[str] = None) -> None:
        """Store a file in the cache."""
        success = True
        try:
            file_path = self._get_cache_file_path(key)
            # Ensure the directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            if isinstance(source, str):
                # Move the file to the cache directory
                os.rename(src=source, dst=file_path)
            else:
                # Create a temporary file and move the file to the cache directory
                with tempfile.NamedTemporaryFile(
                    mode="wb", delete=False, dir=os.path.dirname(file_path), prefix="."
                ) as temp_file:
                    temp_file_path = temp_file.name
                    temp_file.write(source)
                os.rename(src=temp_file_path, dst=file_path)

            # Set extended attribute (e.g., ETag)
            if source_version:
                try:
                    xattr.setxattr(file_path, "user.etag", source_version.encode("utf-8"))
                except OSError as e:
                    logging.warning(f"Failed to set xattr on {file_path}: {e}")

            # Make the file read-only for all users
            self._make_readonly(file_path)

            # Update access time if applicable
            self._update_access_time(file_path)

            # Refresh cache after a few minutes
            if self._should_refresh_cache():
                thread = threading.Thread(target=self.refresh_cache)
                thread.daemon = True
                thread.start()
        except Exception:
            success = False
            raise
        finally:
            self._metrics_helper.increase(operation="SET", success=success)

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

            # If file doesn't exist, return False
            if not os.path.exists(file_path):
                return False

            # If etag checking is disabled, return True if file exists
            if check_source_version == SourceVersionCheckMode.INHERIT:
                if not self.check_source_version():
                    return True
            elif check_source_version == SourceVersionCheckMode.DISABLE:
                return True

            # Verify etag matches if checking is enabled
            try:
                xattr_value = xattr.getxattr(file_path, "user.etag")
                stored_version = xattr_value.decode("utf-8")
                return stored_version is not None and stored_version == source_version
            except OSError:
                # If xattr fails, assume version doesn't match
                return False

        except Exception as e:
            logging.error(f"Error checking cache: {e}")
            return False

    def delete(self, key: str) -> None:
        """Delete a file from the cache."""
        try:
            self.delete_file(key)
        finally:
            self._metrics_helper.increase(operation="DELETE", success=True)

    def cache_size(self) -> int:
        """Return the current size of the cache in bytes."""
        file_size = 0

        # Traverse the directory and subdirectories
        for dirpath, _, filenames in os.walk(self._cache_dir):
            for file_name in filenames:
                file_path = os.path.join(dirpath, file_name)
                if os.path.isfile(file_path) and not file_name.endswith(".lock"):
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
                self.evict_files()
                self._last_refresh_time = datetime.now()
                return True
        except Timeout:
            # If the process cannot acquire the lock, ignore and wait for the next turn
            pass

        return False

    def acquire_lock(self, key: str) -> BaseFileLock:
        """Create a FileLock object for a given key."""
        file_dir = os.path.dirname(os.path.join(self._get_cache_dir(), key))

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
        current_time = time.time()
        try:
            # Make file writable to update timestamps
            self._make_writable(file_path)
            # Only update atime, preserve mtime for FIFO ordering
            stat = os.stat(file_path)
            os.utime(file_path, (current_time, stat.st_mtime))
        except (OSError, FileNotFoundError):
            # File might be deleted by another process or have permission issues
            # Just continue without updating the access time
            pass
        finally:
            # Restore read-only permissions
            self._make_readonly(file_path)

    def _should_refresh_cache(self) -> bool:
        """Check if enough time has passed since the last refresh."""
        now = datetime.now()
        return (now - self._last_refresh_time).seconds > self._cache_refresh_interval

    # Range cache methods
    def _get_chunk_path(self, cache_path: str, chunk_idx: int) -> str:
        """Get the path for a chunk file.

        Constructs the path for a specific chunk file using the pattern '.{base}#chunk{idx}'
        where base is the original file name and idx is the chunk index.

        :param cache_path: The base cache path for the original file
        :param chunk_idx: The index of the chunk (0-based)
        :return: The full path to the chunk file
        """
        cache_dir = os.path.dirname(cache_path)
        base_name = os.path.basename(cache_path)
        return os.path.join(cache_dir, f".{base_name}#chunk{chunk_idx}")

    def _download_missing_chunks(
        self,
        cache_path: str,
        original_key: str,
        start_chunk: int,
        end_chunk: int,
        configured_cache_line_size: int,
        storage_provider,
        source_version: Optional[str],
    ) -> None:
        """Download all missing chunks for a given range with optimized performance."""

        # Identify which chunks need to be downloaded
        chunks_to_download = self._identify_missing_chunks(
            cache_path, start_chunk, end_chunk, configured_cache_line_size, source_version
        )

        # Download missing chunks with minimal locking
        for chunk_idx in chunks_to_download:
            self._download_single_chunk(
                cache_path, original_key, chunk_idx, configured_cache_line_size, storage_provider, source_version
            )

    def _identify_missing_chunks(
        self,
        cache_path: str,
        start_chunk: int,
        end_chunk: int,
        cache_line_size: int,
        source_version: Optional[str],
    ) -> list[int]:
        """Identify which chunks are missing or invalid."""
        chunks_to_download = []

        for chunk_idx in range(start_chunk, end_chunk + 1):
            chunk_path = self._get_chunk_path(cache_path, chunk_idx)

            if not self._is_chunk_valid(chunk_path, source_version, cache_line_size):
                chunks_to_download.append(chunk_idx)

        return chunks_to_download

    def _is_chunk_valid(self, chunk_path: str, source_version: Optional[str], cache_line_size: int) -> bool:
        """Check if a chunk exists and has valid metadata."""
        if not os.path.exists(chunk_path):
            return False

        # If no metadata to validate, chunk is valid if it exists
        if not source_version:
            return True

        try:
            # Validate chunk metadata
            chunk_etag = xattr.getxattr(chunk_path, "user.etag").decode("utf-8")
            stored_cache_line_size = int(xattr.getxattr(chunk_path, "user.cache_line_size").decode("utf-8"))

            # Check if chunk is invalid
            if (source_version and chunk_etag != source_version) or stored_cache_line_size != cache_line_size:
                self._remove_invalid_chunk(chunk_path)
                return False

            return True
        except Exception:
            # xattr read failed or chunk corrupted
            self._remove_invalid_chunk(chunk_path)
            return False

    def _remove_invalid_chunk(self, chunk_path: str) -> None:
        """Safely remove an invalid chunk file."""
        try:
            os.unlink(chunk_path)
        except OSError:
            pass

    def _download_single_chunk(
        self,
        cache_path: str,
        original_key: str,
        chunk_idx: int,
        cache_line_size: int,
        storage_provider,
        source_version: Optional[str],
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
        chunk_path = self._get_chunk_path(cache_path, chunk_idx)
        chunk_lock_key = f"{cache_path}#chunk{chunk_idx}"

        with self.acquire_lock(chunk_lock_key):
            # Double-check if chunk was created by another thread
            if os.path.exists(chunk_path):
                return

            # Fetch and cache the chunk
            self._fetch_and_cache_chunk(
                cache_path, original_key, chunk_idx, cache_line_size, storage_provider, source_version
            )

            # Clean up lock file
            self._cleanup_lock_file(chunk_lock_key)

    def _fetch_and_cache_chunk(
        self,
        cache_path: str,
        original_key: str,
        chunk_idx: int,
        cache_line_size: int,
        storage_provider,
        source_version: Optional[str],
    ) -> None:
        """Fetch chunk data from storage and cache it locally.

        :param cache_path: The base cache path for the original file
        :param original_key: The original key for the object
        :param chunk_idx: The index of the chunk (0-based)
        :param cache_line_size: The size of each chunk in bytes
        :param storage_provider: The storage provider to fetch the chunk from
        :param source_version: The source version of the object
        """
        chunk_path = self._get_chunk_path(cache_path, chunk_idx)

        # Calculate chunk range
        chunk_start = chunk_idx * cache_line_size
        chunk_end = chunk_start + cache_line_size - 1

        # Fetch chunk data
        chunk_data = storage_provider.get_object(
            original_key, Range(offset=chunk_start, size=chunk_end - chunk_start + 1)
        )

        # Cache the chunk
        self._write_chunk_to_cache(chunk_path, chunk_data, source_version, cache_line_size)

        # Handle special case for chunk 0 with small files
        self._handle_chunk0_renaming(cache_path, chunk_path, chunk_idx, cache_line_size)

        # Update access time
        self._update_chunk_access_time(cache_path, chunk_path, chunk_idx)

    def _write_chunk_to_cache(
        self, chunk_path: str, chunk_data: bytes, source_version: Optional[str], cache_line_size: int
    ) -> None:
        """Write chunk data to cache with proper metadata.
        :param chunk_path: The path to the chunk file
        :param chunk_data: The data to write to the chunk file
        :param source_version: The source version of the object
        :param cache_line_size: The size of each chunk in bytes
        """
        os.makedirs(os.path.dirname(chunk_path), exist_ok=True)

        with open(chunk_path, "wb") as f:
            f.write(chunk_data)

        # Set chunk metadata
        self._set_chunk_metadata(chunk_path, source_version, cache_line_size)

    def _set_chunk_metadata(self, chunk_path: str, source_version: Optional[str], cache_line_size: int) -> None:
        """Set xattr metadata for a chunk file.
        :param chunk_path: The path to the chunk file
        :param source_version: The source version of the object
        :param cache_line_size: The size of each chunk in bytes
        """
        try:
            if source_version:
                xattr.setxattr(chunk_path, "user.etag", source_version.encode("utf-8"))

            xattr.setxattr(chunk_path, "user.cache_line_size", str(cache_line_size).encode("utf-8"))
        except OSError:
            # xattrs may not be supported; continue without failing
            pass

    def _handle_chunk0_renaming(self, cache_path: str, chunk_path: str, chunk_idx: int, cache_line_size: int) -> None:
        """Handle special case where chunk 0 becomes the full file for small files.
        :param cache_path: The base cache path for the original file
        :param chunk_path: The path to the chunk file
        :param chunk_idx: The index of the chunk (0-based)
        :param cache_line_size: The size of each chunk in bytes
        """
        if chunk_idx == 0 and not os.path.exists(cache_path):
            # Check if the chunk is smaller than the cache line size
            # This indicates the file is small enough to fit in a single chunk
            try:
                chunk_size = os.path.getsize(chunk_path)
                if chunk_size < cache_line_size:
                    os.rename(chunk_path, cache_path)
            except OSError:
                # If we can't get the size, don't rename
                pass

    def _update_chunk_access_time(self, cache_path: str, chunk_path: str, chunk_idx: int) -> None:
        """Update access time for proper LRU eviction.
        :param cache_path: The base cache path for the original file
        :param chunk_path: The path to the chunk file
        :param chunk_idx: The index of the chunk (0-based)
        """
        if chunk_idx == 0 and os.path.exists(cache_path) and not os.path.exists(chunk_path):
            # Chunk 0 was renamed to original file name
            self._update_access_time(cache_path)
        else:
            # Regular chunk file
            self._update_access_time(chunk_path)

    def _cleanup_lock_file(self, lock_key: str) -> None:
        """Clean up lock file after chunk operations.
        :param lock_key: The key for the lock file
        """
        try:
            lock_file_path = os.path.join(os.path.dirname(lock_key), f".{os.path.basename(lock_key)}.lock")
            if os.path.exists(lock_file_path):
                os.unlink(lock_file_path)
        except OSError:
            pass

    def _assemble_result_from_chunks(
        self, cache_path: str, start_chunk: int, end_chunk: int, configured_cache_line_size: int, byte_range: Range
    ) -> bytes:
        """Assemble the final result from cached chunks using optimized streaming reads.

        This method efficiently reads only the needed portions of chunks from cache
        and assembles them into the final byte range result. It uses streaming reads
        to minimize memory usage and I/O operations.

        :param cache_path: The base cache path for the original file
        :param start_chunk: The starting chunk index
        :param end_chunk: The ending chunk index
        :param configured_cache_line_size: The size of each chunk in bytes
        :param byte_range: The byte range to read (offset and size)
        :return: The assembled byte range data
        """
        # Pre-allocate result buffer
        result = bytearray(byte_range.size)
        result_offset = 0

        for chunk_idx in range(start_chunk, end_chunk + 1):
            chunk_path = self._get_chunk_path(cache_path, chunk_idx)

            # Check if chunk 0 was renamed to original file name (for small files)
            if chunk_idx == 0 and os.path.exists(cache_path) and not os.path.exists(chunk_path):
                actual_path = cache_path
            else:
                actual_path = chunk_path

            # Calculate the portion of this chunk that overlaps with the requested range
            chunk_start = chunk_idx * configured_cache_line_size
            chunk_end = chunk_start + configured_cache_line_size - 1

            overlap_start = max(byte_range.offset, chunk_start)
            overlap_end = min(byte_range.offset + byte_range.size - 1, chunk_end)

            if overlap_start <= overlap_end:
                # Calculate positions within the chunk file and result buffer
                chunk_offset = overlap_start - chunk_start
                length = overlap_end - overlap_start + 1

                # Stream read only the needed portion from the chunk file
                with open(actual_path, "rb") as f:
                    f.seek(chunk_offset)
                    chunk_data = f.read(length)

                # Copy directly to result buffer
                result[result_offset : result_offset + length] = chunk_data
                result_offset += length

        return bytes(result)

    def _invalidate_chunks(self, cache_path: str):
        """Delete all chunks and metadata for a path.

        Removes all chunk files associated with the given cache path by finding
        all files matching the pattern '.{base}#chunk*' and deleting them.

        :param cache_path: The base cache path for which to invalidate all chunks
        """
        cache_dir = os.path.dirname(cache_path)
        base_name = os.path.basename(cache_path)
        pattern = os.path.join(cache_dir, f".{base_name}#chunk*")

        for path in glob.glob(pattern):
            try:
                os.unlink(path)
            except OSError:
                pass

    def _read_range_from_full_cached_file(
        self, cache_path: str, byte_range: Range, source_version: Optional[str]
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
        # Check if we have a full cached file that's valid
        if os.path.exists(cache_path):
            try:
                # Validate the full cached file's etag
                cached_etag = xattr.getxattr(cache_path, "user.etag").decode("utf-8")
                if cached_etag == source_version or source_version is None:
                    # Full file is cached and valid, read range directly from it
                    with open(cache_path, "rb") as f:
                        f.seek(byte_range.offset)
                        data = f.read(byte_range.size)
                    # Update access time for LRU
                    self._update_access_time(cache_path)
                    return data
            except (OSError, AttributeError):
                # xattrs not supported or file corrupted, fall through to chunking
                pass

        # No valid full cached file available
        return None

    def _read_range(
        self,
        cache_path: str,
        byte_range: Range,
        storage_provider,
        source_version: Optional[str],
        original_key: str,
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
        assert configured_cache_line_size is not None  # For type checker

        # Calculate needed chunks
        start_chunk = byte_range.offset // configured_cache_line_size
        end_chunk = (byte_range.offset + byte_range.size - 1) // configured_cache_line_size

        try:
            # Step 1: Download all missing chunks (optimized)
            self._download_missing_chunks(
                cache_path,
                original_key,
                start_chunk,
                end_chunk,
                configured_cache_line_size,
                storage_provider,
                source_version,
            )

            # Step 2: Assemble result from cached chunks (streaming)
            return self._assemble_result_from_chunks(
                cache_path, start_chunk, end_chunk, configured_cache_line_size, byte_range
            )
        except Exception as e:
            # If any step fails, fall back to getting the object directly
            logging.warning(f"Failed to process chunks for {original_key}: {e}")
            return storage_provider.get_object(original_key, byte_range=byte_range)
