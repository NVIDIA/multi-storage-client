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
from .types import SourceVersionCheckMode

DEFAULT_CACHE_SIZE = "10G"
DEFAULT_CACHE_SIZE_MB = "10000"
DEFAULT_CACHE_REFRESH_INTERVAL = 300  # 5 minutes
DEFAULT_LOCK_TIMEOUT = 600  # 10 minutes


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
                # Skip lock files and hidden files
                if file_name.endswith(".lock") or file_name.startswith("."):
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

    def read(self, key: str, source_version: Optional[str] = None) -> Optional[bytes]:
        """Read the contents of a file from the cache if it exists."""
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
