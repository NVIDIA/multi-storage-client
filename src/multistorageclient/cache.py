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
        return os.path.join(self._cache_dir, self._profile, key)

    def read(
        self,
        key: str,
        source_version: Optional[str] = None,
        byte_range: Optional[Range] = None,
        storage_provider: Optional[Any] = None,
        size: Optional[int] = None,
    ) -> Optional[bytes]:
        """Read the contents of a file from the cache if it exists.

        This method handles both full-file reads and partial file caching. For range reads
        (when byte_range is provided), it delegates to _read_range to implement partial
        file caching using chunks. For full-file reads, it uses the existing cache logic.

        :param key: The cache key for the file
        :param source_version: Optional source version for cache validation
        :param byte_range: Optional byte range for partial file reads
        :param storage_provider: Storage provider required for range reads
        :param size: Optional file size for range reads
        :return: The file contents as bytes, or None if not found in cache
        """
        # If this is a range read, check for full cached file first
        if byte_range:
            assert storage_provider is not None, "storage_provider is required for range reads"
            assert source_version is not None, "source_version is required for range reads"
            assert size is not None, "size is required for range reads"
            cache_path = self._get_cache_file_path(key)

            # Try to read range from full cached file first
            range_data = self._read_range_from_full_cached_file(cache_path, byte_range, source_version)
            if range_data is not None:
                return range_data

            # No valid full cached file, delegate to chunk-based range reading
            return self._read_range(cache_path, byte_range, storage_provider, source_version, size, key)  # type: ignore[arg-type]

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

            # No full cached file found, check if we can reconstruct from chunks
            if source_version is not None:
                cache_path = self._get_cache_file_path(key)
                # Check if we have a chunk that contains the full file
                chunk_data = self._read_full_file_from_chunks(cache_path, source_version)
                if chunk_data is not None:
                    return chunk_data

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
        source_version: str,
        size: Optional[int],
    ) -> None:
        """Download all missing chunks for a given range.

        This method handles the downloading and caching of chunks that are not already
        present in the cache or have invalid metadata. It implements thread-safe chunk
        downloading with race condition prevention.

        Logic flow:
        1. For each chunk in the range [start_chunk, end_chunk]:
           a. Check if chunk file exists and validate its metadata (etag, cache_line_size)
           b. If invalid or missing, acquire per-chunk lock to prevent race conditions
           c. Double-check chunk existence after acquiring lock (another thread may have created it)
           d. If still missing, fetch chunk from storage provider
           e. Cache chunk file and set extended attributes (etag, cache_line_size, size)
           f. Update access time for LRU eviction

        :param cache_path: The base cache path for the original file
        :param original_key: The original key for fetching from storage provider
        :param start_chunk: The starting chunk index
        :param end_chunk: The ending chunk index
        :param configured_cache_line_size: The size of each chunk in bytes
        :param storage_provider: The storage provider to fetch chunks from
        :param source_version: The source version for validation
        :param size: Optional file size for metadata
        """
        for chunk_idx in range(start_chunk, end_chunk + 1):
            chunk_path = self._get_chunk_path(cache_path, chunk_idx)
            chunk_data: Optional[bytes] = None

            if os.path.exists(chunk_path):
                # Validate xattrs
                try:
                    xattr_chunk_etag = xattr.getxattr(chunk_path, "user.etag").decode("utf-8")
                except Exception:
                    xattr_chunk_etag = None
                try:
                    xattr_cache_line_size_raw = xattr.getxattr(chunk_path, "user.cache_line_size")
                    xattr_cache_line_size = int(xattr_cache_line_size_raw.decode("utf-8"))
                except Exception:
                    xattr_cache_line_size = None

                if xattr_chunk_etag != source_version or xattr_cache_line_size != configured_cache_line_size:
                    try:
                        os.unlink(chunk_path)
                    except OSError:
                        pass
                else:
                    continue  # Chunk is valid, skip to next

            # Use per-chunk locking to prevent race conditions
            chunk_lock_key = f"{cache_path}#chunk{chunk_idx}"

            with self.acquire_lock(chunk_lock_key):
                # Double-check if chunk was created by another thread
                if os.path.exists(chunk_path):
                    try:
                        xattr_chunk_etag = xattr.getxattr(chunk_path, "user.etag").decode("utf-8")
                        xattr_chunk_size_raw = xattr.getxattr(chunk_path, "user.cache_line_size")
                        xattr_chunk_size = int(xattr_chunk_size_raw.decode("utf-8"))
                        if xattr_chunk_etag == source_version and xattr_chunk_size == configured_cache_line_size:
                            continue  # Chunk was created by another thread and is valid
                    except Exception:
                        pass  # Proceed to download if validation fails

                # Fetch from storage provider using original key
                chunk_start = chunk_idx * configured_cache_line_size
                chunk_end = chunk_start + configured_cache_line_size - 1

                chunk_data = storage_provider.get_object(
                    original_key, Range(offset=chunk_start, size=chunk_end - chunk_start + 1)
                )

                # Cache the chunk and set xattrs
                os.makedirs(os.path.dirname(chunk_path), exist_ok=True)
                with open(chunk_path, "wb") as f:
                    assert isinstance(chunk_data, (bytes, bytearray))
                    f.write(chunk_data)
                try:
                    xattr.setxattr(chunk_path, "user.etag", source_version.encode("utf-8"))
                    xattr.setxattr(chunk_path, "user.cache_line_size", str(configured_cache_line_size).encode("utf-8"))
                    if size:
                        xattr.setxattr(chunk_path, "user.size", str(size).encode("utf-8"))
                except OSError:
                    # xattrs may not be supported; continue without failing
                    pass

                # Update access time for LRU
                self._update_access_time(chunk_path)

    def _assemble_result_from_chunks(
        self, cache_path: str, start_chunk: int, end_chunk: int, configured_cache_line_size: int, byte_range: Range
    ) -> bytes:
        """Assemble the final result from cached chunks.

        This method reads all the required chunks from cache and assembles them
        into the final byte range result.

        :param cache_path: The base cache path for the original file
        :param start_chunk: The starting chunk index
        :param end_chunk: The ending chunk index
        :param configured_cache_line_size: The size of each chunk in bytes
        :param byte_range: The byte range to read (offset and size)
        :return: The assembled byte range data
        """
        result = bytearray(byte_range.size)

        for chunk_idx in range(start_chunk, end_chunk + 1):
            chunk_path = self._get_chunk_path(cache_path, chunk_idx)

            # Read chunk data from cache (guaranteed to exist after download step)
            with open(chunk_path, "rb") as f:
                chunk_data = f.read()

            # Copy relevant portion to result
            chunk_start = chunk_idx * configured_cache_line_size
            chunk_end = chunk_start + configured_cache_line_size - 1

            overlap_start = max(byte_range.offset, chunk_start)
            overlap_end = min(byte_range.offset + byte_range.size - 1, chunk_end)

            if overlap_start <= overlap_end:
                chunk_offset = overlap_start - chunk_start
                result_offset_in_chunk = overlap_start - byte_range.offset
                length = overlap_end - overlap_start + 1
                assert isinstance(chunk_data, (bytes, bytearray))
                result[result_offset_in_chunk : result_offset_in_chunk + length] = chunk_data[
                    chunk_offset : chunk_offset + length
                ]

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

    def _read_full_file_from_chunks(self, cache_path: str, source_version: str) -> Optional[bytes]:
        """Read a full file by reconstructing it from cached chunks.

        This method checks if we have cached chunks that can be used to reconstruct
        the full file. This is useful when a range read has cached chunks that contain
        the entire file, and then a full file read is requested.

        :param cache_path: Path to the cached file
        :param source_version: Source version identifier for cache validation
        :return: The full file data if successful, None otherwise
        """

        # Check if we have chunk 0 (which should contain the full file if file size < chunk size)
        chunk0_path = self._get_chunk_path(cache_path, 0)

        if not os.path.exists(chunk0_path):
            return None

        try:
            # Validate chunk 0's etag
            chunk_etag = xattr.getxattr(chunk0_path, "user.etag").decode("utf-8")
            if chunk_etag != source_version:
                return None

            # Check if chunk 0 contains the full file by looking at the size xattr
            try:
                size_xattr = xattr.getxattr(chunk0_path, "user.size")
                file_size = int(size_xattr.decode("utf-8"))
            except (OSError, AttributeError, ValueError):
                # No size xattr or invalid, can't determine if chunk contains full file
                return None

            # If file size is less than or equal to chunk size, chunk 0 contains the full file
            if file_size <= self._cache_line_size:
                with open(chunk0_path, "rb") as f:
                    # Read only the actual file size, not the entire chunk
                    data = f.read(file_size)
                # Update access time for LRU
                self._update_access_time(chunk0_path)
                return data

        except (OSError, AttributeError):
            # xattrs not supported or file corrupted
            pass

        return None

    def _read_range_from_full_cached_file(
        self, cache_path: str, byte_range: Range, source_version: str
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
                if cached_etag == source_version:
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
        self, cache_path: str, byte_range: Range, storage_provider, source_version: str, size: int, original_key: str
    ) -> bytes:
        """Read a byte range using chunk-based caching.

        This method implements partial file caching by calculating which chunks are needed
        for the requested byte range, checking if chunks exist in cache and validating
        their metadata (etag, cache_line_size), fetching missing or invalid chunks from the
        storage provider, caching the fetched chunks with metadata stored as extended
        attributes, and extracting the requested byte range from the cached chunks.

        Note: This method assumes that full-file cache optimization has already been
        checked by the calling read() method.

        :param cache_path: Path where chunks should be cached locally
        :param byte_range: The byte range to read (offset and size)
        :param storage_provider: Storage provider to fetch chunks from if not cached
        :param source_version: Source version identifier for cache validation
        :param size: Total size of the original object
        :param original_key: Original object key for fetching from storage provider
        :return: The requested byte range data
        """
        # Per-chunk validation via xattrs. Chunks with mismatched etag/cache_line_size are deleted.

        # Calculate needed chunks
        configured_cache_line_size = (
            self._cache_line_size
        )  # Type checker knows this is not None due to range_cache_enabled check
        assert configured_cache_line_size is not None  # For type checker
        start_chunk = byte_range.offset // configured_cache_line_size
        end_chunk = (byte_range.offset + byte_range.size - 1) // configured_cache_line_size

        try:
            # Step 1: Download all missing chunks
            self._download_missing_chunks(
                cache_path,
                original_key,
                start_chunk,
                end_chunk,
                configured_cache_line_size,
                storage_provider,
                source_version,
                size,
            )

            # Step 2: Assemble result from cached chunks
            return self._assemble_result_from_chunks(
                cache_path, start_chunk, end_chunk, configured_cache_line_size, byte_range
            )
        except Exception as e:
            # If any step fails, fall back to getting the object directly
            logging.warning(f"Failed to process chunks for {original_key}: {e}")
            return storage_provider.get_object(original_key, byte_range=byte_range)
