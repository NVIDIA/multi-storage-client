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

import os
import queue
import shutil
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest
import xattr

import multistorageclient.cache as cache_module
import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient import StorageClient
from multistorageclient.cache import DEFAULT_CACHE_REFRESH_INTERVAL, CacheManager
from multistorageclient.caching.cache_config import (
    CacheConfig,
    EvictionPolicyConfig,
)
from multistorageclient.client.single import SingleStorageClient
from multistorageclient.config import StorageClientConfig
from multistorageclient.types import Range, SourceVersionCheckMode
from test_multistorageclient.unit.utils.tempdatastore import create_test_data


class RangeAwareStorageProvider:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self.call_count = 0
        self._lock = threading.Lock()

    def get_object(self, _key: str, byte_range: Range | None = None) -> bytes:
        with self._lock:
            self.call_count += 1

        if byte_range is None:
            return self._data

        return self._data[byte_range.offset : byte_range.offset + byte_range.size]


@pytest.fixture
def profile_name():
    return "test-cache"


@pytest.fixture
def cache_config(tmpdir):
    """Fixture for CacheConfig object."""
    return CacheConfig(size="10M", cache_line_size="64M", check_source_version=False, location=str(tmpdir))


@pytest.fixture
def cache_config_with_etag(tmpdir):
    """Fixture for CacheConfig object with etag support enabled."""
    return CacheConfig(size="10M", cache_line_size="64M", check_source_version=True, location=str(tmpdir))


@pytest.fixture
def cache_manager(profile_name, cache_config):
    """Fixture for CacheManager object."""
    return CacheManager(profile=profile_name, cache_config=cache_config)


@pytest.fixture
def cache_manager_with_etag(profile_name, cache_config_with_etag):
    """Fixture for CacheManager object with etag support enabled."""
    return CacheManager(profile=profile_name, cache_config=cache_config_with_etag)


def test_cache_config_size_bytes(cache_config):
    """Test that CacheConfig size_bytes converts MB to bytes correctly."""
    assert cache_config.size_bytes() == 10 * 1024 * 1024  # 10 MB


def test_cache_manager_read_file(profile_name, tmpdir, cache_manager):
    """Test that CacheManager can read a file from the cache."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    cache_manager.set("bucket/test_file.txt", str(file))
    assert cache_manager.read("bucket/test_file.txt") == b"cached data"

    cache_manager.set("bucket/test_file.bin", b"binary data")
    assert cache_manager.read("bucket/test_file.bin") == b"binary data"


def test_cache_manager_preserves_directory_structure(profile_name, tmpdir, cache_manager):
    """Test that CacheManager preserves directory structure in the cache."""
    # Create test files in different directories with more diverse paths

    test_uuid = str(uuid.uuid4())

    # Generate unique file namestest
    files = {
        "folder1/file1.txt": "data1",
        "folder1/subfolder/file2.txt": "data2",
        "folder2/file3.txt": "data3",
        "folder3/folder4/file4.txt": "data4",
        "folder3/folder4/subfolder/file5.txt": "data5",
        "folder3/folder4/subfolder/deep/file6.txt": "data6",
        "root_file.txt": "data7",
        "folder4/empty_folder/file7.txt": "data8",
    }

    # Store files directly in cache
    for path, content in files.items():
        cache_manager.set(f"bucket/{test_uuid}/{path}", content.encode())

    # Verify each file exists in cache with correct directory structure and content
    for path, content in files.items():
        # Read from cache
        cached_data = cache_manager.read(f"bucket/{test_uuid}/{path}")
        assert cached_data == content.encode(), f"Content mismatch for {path}"

        # Verify file exists in cache
        cache_path = os.path.join(tmpdir, profile_name, f"bucket/{test_uuid}/{path}")
        assert os.path.exists(cache_path), f"File not found in cache: {path}"

    # Get all directories in the cache
    cache_root = os.path.join(tmpdir, profile_name)
    all_dirs = set()
    for root, dirs, _ in os.walk(cache_root):
        for dir_name in dirs:
            # Skip lock files
            if not dir_name.startswith("."):
                rel_path = os.path.relpath(os.path.join(root, dir_name), cache_root)
                all_dirs.add(rel_path)

    # Expected directory structure
    expected_dirs = {
        "bucket",
        os.path.join("bucket", test_uuid),
        os.path.join(f"bucket/{test_uuid}", "folder1"),
        os.path.join(f"bucket/{test_uuid}", "folder1", "subfolder"),
        os.path.join(f"bucket/{test_uuid}", "folder2"),
        os.path.join(f"bucket/{test_uuid}", "folder3"),
        os.path.join(f"bucket/{test_uuid}", "folder3", "folder4"),
        os.path.join(f"bucket/{test_uuid}", "folder3", "folder4", "subfolder"),
        os.path.join(f"bucket/{test_uuid}", "folder3", "folder4", "subfolder", "deep"),
        os.path.join(f"bucket/{test_uuid}", "folder4"),
        os.path.join(f"bucket/{test_uuid}", "folder4", "empty_folder"),
    }

    assert all_dirs == expected_dirs, (
        f"Unexpected directories found in cache. Got: {all_dirs}, Expected: {expected_dirs}"
    )

    # Verify that all files are accessible through the cache manager
    for path in files.keys():
        assert cache_manager.contains(f"bucket/{test_uuid}/{path}"), f"Cache manager should contain {path}"


def test_cache_manager_read_file_with_etag(profile_name, tmpdir, cache_manager_with_etag):
    """Test that CacheManager can read a file from the cache with etag in the key."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    test_uuid = str(uuid.uuid4())
    # Test with etag in the key
    key = f"bucket/{test_uuid}/test_file.txt"
    source_version = "etag123"
    cache_manager_with_etag.set(key, str(file), source_version=source_version)
    assert cache_manager_with_etag.read(key, source_version=source_version) == b"cached data"

    # Test with binary data and etag
    key_bin = f"bucket/{test_uuid}/test_file.bin"
    source_version = "etag456"
    cache_manager_with_etag.set(key_bin, b"binary data", source_version=source_version)
    assert cache_manager_with_etag.read(key_bin, source_version=source_version) == b"binary data"

    # Verify that the file is stored with the etag in the path
    expected_path = os.path.join(tmpdir, profile_name, key)
    assert os.path.exists(expected_path), f"File should exist at {expected_path}"

    # Test that reading without etag returns None
    key_without_etag = f"bucket/{test_uuid}/test_file.txt"
    assert cache_manager_with_etag.read(key_without_etag) is None


def test_cache_manager_read_disable_reuses_version_tagged_entry(cache_manager_with_etag):
    key = "bucket/virtual-file.bin"
    content = b"cached virtual content"
    cache_manager_with_etag.set(key, content, source_version="manifest-synthetic-etag")

    assert cache_manager_with_etag.read(key, check_source_version=SourceVersionCheckMode.DISABLE) == content


def test_cache_manager_read_enable_validates_synthetic_etag(cache_manager_with_etag):
    key = "bucket/virtual-file.bin"
    content = b"cached virtual content"
    source_version = "manifest-synthetic-etag"
    cache_manager_with_etag.set(key, content, source_version=source_version)

    assert (
        cache_manager_with_etag.read(
            key,
            source_version=source_version,
            check_source_version=SourceVersionCheckMode.ENABLE,
        )
        == content
    )
    assert (
        cache_manager_with_etag.read(
            key,
            source_version="different-etag",
            check_source_version=SourceVersionCheckMode.ENABLE,
        )
        is None
    )


@pytest.mark.parametrize(
    ("check_source_version", "expected_source_version"),
    [
        pytest.param(SourceVersionCheckMode.DISABLE, None, id="disable"),
        pytest.param(SourceVersionCheckMode.ENABLE, "manifest-synthetic-etag", id="enable"),
        pytest.param(SourceVersionCheckMode.INHERIT, "manifest-synthetic-etag", id="inherit"),
    ],
)
def test_single_client_full_cache_read_propagates_source_version_mode(
    check_source_version: SourceVersionCheckMode,
    expected_source_version: str | None,
) -> None:
    client = SingleStorageClient.__new__(SingleStorageClient)
    client._metadata_provider = None
    client._replica_manager = None
    client._cache_manager = MagicMock()
    client._cache_manager.check_source_version.return_value = True
    client._cache_manager.read.return_value = b"cached virtual content"
    client._storage_provider = MagicMock()
    client._storage_provider.get_object_metadata.return_value.etag = "manifest-synthetic-etag"

    assert client.read("virtual-file.bin", check_source_version=check_source_version) == b"cached virtual content"
    client._cache_manager.read.assert_called_once_with(
        "virtual-file.bin",
        expected_source_version,
        check_source_version=check_source_version,
    )


def test_cache_manager_read_delete_file_with_etag(profile_name, tmpdir, cache_manager_with_etag):
    """Test that CacheManager can read and delete a file from the cache with etag in the key."""

    test_uuid = str(uuid.uuid4())
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    key = f"bucket/{test_uuid}/test_file.txt"
    source_version = "etag123"

    with cache_manager_with_etag.acquire_lock(key):
        cache_manager_with_etag.set(key, str(file), source_version=source_version)

    # Verify the lock file is in the same directory as the file
    lock_path = os.path.join(tmpdir, profile_name, os.path.dirname(key), f".{os.path.basename(key)}.lock")
    assert os.path.exists(lock_path)

    # Verify we can read the file
    assert cache_manager_with_etag.read(key, source_version=source_version) == b"cached data"

    # Delete the file
    cache_manager_with_etag.delete(key)

    # Verify the file and its lock are deleted
    assert not os.path.exists(os.path.join(tmpdir, profile_name, key))
    assert not os.path.exists(lock_path)

    # Test that reading after delete returns None
    assert cache_manager_with_etag.read(key, source_version=source_version) is None


def test_cache_manager_read_delete_file(profile_name, tmpdir, cache_manager):
    """Test that CacheManager can read a file from the cache."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    test_uuid = str(uuid.uuid4())
    key = f"bucket/{test_uuid}/test_file.txt"

    with cache_manager.acquire_lock(key):
        cache_manager.set(key, str(file))

    # Verify the lock file is in the same directory
    lock_path = os.path.join(tmpdir, profile_name, os.path.dirname(key), f".{os.path.basename(key)}.lock")
    assert os.path.exists(lock_path)

    assert cache_manager.read(key) == b"cached data"

    cache_manager.delete(key)

    # Verify the file and its lock are deleted
    assert not os.path.exists(os.path.join(tmpdir, profile_name, key))
    assert not os.path.exists(lock_path)


def test_cache_manager_delete_rejects_absolute_path(profile_name, tmpdir, cache_manager):
    """Test that CacheManager rejects absolute delete paths."""
    outside_file = tmpdir.join("outside.txt")
    outside_file.write("outside data")
    absolute_key = str(outside_file)

    cache_manager.set(absolute_key, b"cached data")
    cached_path = cache_manager._get_cache_file_path(absolute_key)
    assert os.path.exists(cached_path)

    with pytest.raises(ValueError, match="relative to the profile cache root"):
        cache_manager.delete(absolute_key)

    assert outside_file.read() == "outside data"
    assert os.path.exists(cached_path)


def test_cache_manager_delete_rejects_path_outside_profile_root(profile_name, tmpdir, cache_manager):
    """Test that CacheManager rejects delete paths that escape the profile cache root."""
    sibling_dir = tmpdir.mkdir("other-profile")
    sibling_file = sibling_dir.join("victim.txt")
    sibling_file.write("victim data")

    with pytest.raises(ValueError, match="escapes the profile cache root"):
        cache_manager.delete_file(os.path.join("..", "other-profile", "victim.txt"))

    assert sibling_file.read() == "victim data"


def test_cache_manager_delete_rejects_symlink_escape(profile_name, tmpdir, cache_manager):
    """Test that CacheManager rejects delete paths that escape through a symlinked directory."""
    sibling_dir = tmpdir.mkdir("symlink-target")
    sibling_file = sibling_dir.join("victim.txt")
    sibling_file.write("victim data")

    escape_link = os.path.join(str(tmpdir), profile_name, "escape-link")
    os.symlink(str(sibling_dir), escape_link)

    with pytest.raises(ValueError, match="escapes the profile cache root"):
        cache_manager.delete_file(os.path.join("escape-link", "victim.txt"))

    assert sibling_file.read() == "victim data"


def test_cache_manager_delete_normalizes_safe_relative_path(profile_name, tmpdir, cache_manager):
    """Test that CacheManager allows normalized relative delete paths that stay within the profile cache root."""
    key = "bucket/test_file.txt"
    cache_manager.set(key, b"cached data")

    cache_manager.delete_file(os.path.join("bucket", "nested", "..", "test_file.txt"))

    assert not os.path.exists(cache_manager._get_cache_file_path(key))


def test_cache_manager_open_file(profile_name, tmpdir, cache_manager):
    """Test that CacheManager can open a file from the cache."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    test_uuid = str(uuid.uuid4())
    key = f"bucket/{test_uuid}/test_file.txt"

    cache_manager.set(key, str(file))

    with cache_manager.open(key, "r") as result:
        assert result.read() == "cached data"
        assert result.name == os.path.join(tmpdir, profile_name, key)

    with cache_manager.open(key, "rb") as result:
        assert result.read() == b"cached data"
        assert result.name == os.path.join(tmpdir, profile_name, key)


def test_partial_chunk_publish_is_atomic_without_source_version(tmpdir, monkeypatch):
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=False, location=str(tmpdir)),
    )
    storage_provider = RangeAwareStorageProvider(create_test_data(2))
    key = "bucket/concurrent.bin"
    byte_range = Range(offset=0, size=16 * 1024)

    cache_path = cache_manager._get_cache_file_path(key)
    chunk_path = cache_manager._get_chunk_path(cache_path, 0)
    replace_started = threading.Event()
    allow_replace = threading.Event()
    results: list[bytes] = []
    errors: list[Exception] = []
    original_replace = cache_module.os.replace

    def blocking_replace(src: str, dst: str) -> None:
        if dst == chunk_path:
            assert os.path.exists(src), "Temporary chunk should exist before publication"
            assert not os.path.exists(dst), "Chunk should remain invisible until replace completes"
            replace_started.set()
            assert allow_replace.wait(timeout=5), "Timed out waiting to complete atomic replace"
        original_replace(src, dst)

    monkeypatch.setattr(cache_module.os, "replace", blocking_replace)

    def read_range() -> None:
        try:
            results.append(
                cache_manager.read(key, byte_range=byte_range, storage_provider=storage_provider)  # type: ignore[arg-type]
                or b""
            )
        except Exception as exc:  # pragma: no cover - failure path asserted below
            errors.append(exc)

    thread1 = threading.Thread(target=read_range)
    thread2 = threading.Thread(target=read_range)

    thread1.start()
    assert replace_started.wait(timeout=5), "First reader never reached the publication point"
    assert not os.path.exists(chunk_path), "Chunk should not be visible while publication is paused"

    thread2.start()

    allow_replace.set()
    thread1.join(timeout=5)
    thread2.join(timeout=5)

    assert not errors, f"Unexpected read errors: {errors}"
    assert len(results) == 2
    expected = storage_provider._data[: byte_range.size]
    assert results == [expected, expected]
    assert os.path.exists(chunk_path), "Chunk should exist after atomic publication completes"
    assert storage_provider.call_count == 1
    assert not [name for name in os.listdir(os.path.dirname(chunk_path)) if name.startswith(".chunk_tmp_")]


def test_partial_chunk_lock_revalidates_a_newer_source_revision_before_reusing_a_published_chunk(tmpdir):
    probe_path = os.path.join(str(tmpdir), "xattr-probe")
    with open(probe_path, "wb") as probe_file:
        probe_file.write(b"probe")
    try:
        xattr.setxattr(probe_path, "user.etag", b"probe")
        xattr.getxattr(probe_path, "user.etag")
    except OSError:
        pytest.skip("xattr is not supported on this filesystem")
    finally:
        os.unlink(probe_path)

    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    key = "bucket/revisioned.bin"
    byte_range = Range(offset=0, size=3)
    old_data = b"old" + b"x" * (1024 * 1024 - 3)
    new_data = b"new" + b"y" * (1024 * 1024 - 3)
    old_fetch_started = threading.Event()
    allow_old_fetch = threading.Event()
    newer_attempt_started = threading.Event()

    class BlockingOldProvider(RangeAwareStorageProvider):
        def get_object(self, object_key: str, requested_range: Range | None = None) -> bytes:
            old_fetch_started.set()
            if not allow_old_fetch.wait(timeout=5):
                raise TimeoutError("timed out waiting to publish the old revision")
            return super().get_object(object_key, requested_range)

    old_provider = BlockingOldProvider(old_data)
    new_provider = RangeAwareStorageProvider(new_data)
    original_acquire_lock = cache_manager.acquire_lock

    def acquire_lock(lock_key: str):
        if threading.current_thread().name == "newer-reader":
            newer_attempt_started.set()
        return original_acquire_lock(lock_key)

    cache_manager.acquire_lock = acquire_lock  # type: ignore[method-assign]
    results: dict[str, bytes | None] = {}
    errors: list[BaseException] = []

    def read_old_revision() -> None:
        try:
            results["old"] = cache_manager.read(
                key,
                source_version="revision-1",
                byte_range=byte_range,
                storage_provider=old_provider,
                source_size=len(old_data),
            )
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)

    def read_new_revision() -> None:
        try:
            results["new"] = cache_manager.read(
                key,
                source_version="revision-2",
                byte_range=byte_range,
                storage_provider=new_provider,
                source_size=len(new_data),
            )
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)

    older = threading.Thread(target=read_old_revision, name="older-reader")
    newer = threading.Thread(target=read_new_revision, name="newer-reader")
    older.start()
    assert old_fetch_started.wait(timeout=5), "Older reader never acquired the chunk lock"
    newer.start()
    assert newer_attempt_started.wait(timeout=5), "Newer reader did not perform its pre-lock cache check"
    allow_old_fetch.set()
    older.join(timeout=5)
    newer.join(timeout=5)

    assert not errors
    assert results == {"old": b"old", "new": b"new"}
    assert old_provider.call_count == 1
    assert new_provider.call_count == 1
    chunk_path = cache_manager._get_chunk_path(cache_manager._get_cache_file_path(key), 0)
    assert open(chunk_path, "rb").read() == new_data
    assert xattr.getxattr(chunk_path, "user.etag") == b"revision-2"


def test_partial_chunk_assembly_never_returns_a_concurrently_replaced_revision(tmpdir, monkeypatch):
    probe_path = os.path.join(str(tmpdir), "xattr-probe")
    with open(probe_path, "wb") as probe_file:
        probe_file.write(b"probe")
    try:
        xattr.setxattr(probe_path, "user.etag", b"probe")
        xattr.getxattr(probe_path, "user.etag")
    except OSError:
        pytest.skip("xattr is not supported on this filesystem")
    finally:
        os.unlink(probe_path)

    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    key = "bucket/concurrent-revisions.bin"
    byte_range = Range(offset=0, size=3)
    old_data = b"old" + b"x" * (1024 * 1024 - 3)
    new_data = b"new" + b"y" * (1024 * 1024 - 3)
    old_provider = RangeAwareStorageProvider(old_data)
    new_provider = RangeAwareStorageProvider(new_data)
    old_ready_to_assemble = threading.Event()
    allow_old_assembly = threading.Event()
    original_assemble = cache_manager._assemble_result_from_chunks

    def controlled_assemble(*args, **kwargs):
        if threading.current_thread().name == "older-reader":
            old_ready_to_assemble.set()
            if not allow_old_assembly.wait(timeout=5):
                raise TimeoutError("timed out waiting to assemble the old revision")
        return original_assemble(*args, **kwargs)

    monkeypatch.setattr(cache_manager, "_assemble_result_from_chunks", controlled_assemble)
    results: dict[str, bytes | None] = {}
    errors: list[BaseException] = []

    def read_revision(name: str, revision: str, provider: RangeAwareStorageProvider) -> None:
        try:
            results[name] = cache_manager.read(
                key,
                source_version=revision,
                byte_range=byte_range,
                storage_provider=provider,
                source_size=len(provider._data),
            )
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)

    older = threading.Thread(
        target=read_revision,
        args=("old", "revision-1", old_provider),
        name="older-reader",
    )
    newer = threading.Thread(
        target=read_revision,
        args=("new", "revision-2", new_provider),
        name="newer-reader",
    )
    older.start()
    assert old_ready_to_assemble.wait(timeout=5)
    newer.start()
    newer.join(timeout=5)
    assert not newer.is_alive()
    allow_old_assembly.set()
    older.join(timeout=5)

    assert not errors
    assert results == {"new": b"new", "old": b"old"}
    assert old_provider.call_count == 2
    assert new_provider.call_count == 1
    chunk_path = cache_manager._get_chunk_path(cache_manager._get_cache_file_path(key), 0)
    assert open(chunk_path, "rb").read() == new_data
    assert xattr.getxattr(chunk_path, "user.etag") == b"revision-2"


def test_partial_chunk_metadata_is_set_before_publish(tmpdir, monkeypatch):
    probe_path = os.path.join(tmpdir, "xattr-probe")
    with open(probe_path, "wb") as probe_file:
        probe_file.write(b"probe")
    try:
        xattr.setxattr(probe_path, "user.etag", b"probe")
        xattr.getxattr(probe_path, "user.etag")
    except OSError:
        pytest.skip("xattr is not supported on this filesystem")
    finally:
        os.unlink(probe_path)

    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    storage_provider = RangeAwareStorageProvider(create_test_data(2))
    key = "bucket/versioned.bin"
    source_version = "etag-123"
    byte_range = Range(offset=0, size=16 * 1024)

    cache_path = cache_manager._get_cache_file_path(key)
    chunk_path = cache_manager._get_chunk_path(cache_path, 0)
    observed: dict[str, str | bool] = {}
    original_replace = cache_module.os.replace

    def checking_replace(src: str, dst: str) -> None:
        if dst == chunk_path:
            observed["chunk_visible_before_replace"] = os.path.exists(dst)
            observed["etag"] = xattr.getxattr(src, "user.etag").decode("utf-8")
            observed["cache_line_size"] = xattr.getxattr(src, "user.cache_line_size").decode("utf-8")
            observed["object_size"] = xattr.getxattr(src, "user.size").decode("utf-8")
        original_replace(src, dst)

    monkeypatch.setattr(cache_module.os, "replace", checking_replace)

    result = cache_manager.read(
        key,
        source_version=source_version,
        byte_range=byte_range,
        storage_provider=storage_provider,  # type: ignore[arg-type]
        source_size=len(storage_provider._data),
    )

    assert result == storage_provider._data[: byte_range.size]
    assert observed["chunk_visible_before_replace"] is False
    assert observed["etag"] == source_version
    assert observed["cache_line_size"] == str(1024 * 1024)
    assert observed["object_size"] == str(len(storage_provider._data))
    assert xattr.getxattr(chunk_path, "user.etag").decode("utf-8") == source_version


def test_assemble_result_handles_short_final_chunk(tmpdir):
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=False, location=str(tmpdir)),
    )
    key = "bucket/edge_case.bin"
    cache_path = cache_manager._get_cache_file_path(key)
    chunk_path = cache_manager._get_chunk_path(cache_path, 3)
    os.makedirs(os.path.dirname(chunk_path), exist_ok=True)

    # Simulate a valid final chunk for a non-4MiB object. The chunk is shorter than cache_line_size
    # because the object ends before the 4th MiB boundary.
    short_final_chunk = b"x" * 917504
    with open(chunk_path, "wb") as chunk_file:
        chunk_file.write(short_final_chunk)
    cache_manager._set_chunk_metadata(chunk_path, None, 1024 * 1024, 3 * 1024 * 1024 + len(short_final_chunk))

    # This range matches the existing edge-case test: it starts past the real EOF implied by the
    # short final chunk, so assembly should return empty bytes rather than invalidating the chunk.
    result = cache_manager._assemble_result_from_chunks(
        cache_path=cache_path,
        start_chunk=3,
        end_chunk=3,
        configured_cache_line_size=1024 * 1024,
        byte_range=Range(offset=4 * 1024 * 1024 - 1024, size=1024),
    )

    assert result == b""
    assert os.path.exists(chunk_path), "Valid short final chunk should not be treated as corruption"


def test_assemble_result_invalidates_corrupt_short_chunk(tmpdir):
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=False, location=str(tmpdir)),
    )
    key = "bucket/corrupt_chunk.bin"
    cache_path = cache_manager._get_cache_file_path(key)
    chunk_path = cache_manager._get_chunk_path(cache_path, 1)
    os.makedirs(os.path.dirname(chunk_path), exist_ok=True)

    with open(chunk_path, "wb") as chunk_file:
        chunk_file.write(b"x" * (512 * 1024))
    cache_manager._set_chunk_metadata(chunk_path, None, 1024 * 1024, 3 * 1024 * 1024)

    with pytest.raises(IOError, match="smaller than expected object metadata"):
        cache_manager._assemble_result_from_chunks(
            cache_path=cache_path,
            start_chunk=1,
            end_chunk=1,
            configured_cache_line_size=1024 * 1024,
            byte_range=Range(offset=1024 * 1024 + 700 * 1024, size=1024),
        )

    assert not os.path.exists(chunk_path), "Corrupt undersized chunk should be invalidated"


def test_short_final_chunk_without_xattr_uses_portable_metadata_and_reuses_the_cached_chunk(tmpdir, monkeypatch):
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=False, location=str(tmpdir)),
    )
    test_content = b"a" * (3 * 1024 * 1024) + b"tail"
    storage_provider = RangeAwareStorageProvider(test_content)
    key = "bucket/no_xattr_edge_case.bin"
    cache_path = cache_manager._get_cache_file_path(key)
    chunk_path = cache_manager._get_chunk_path(cache_path, 3)
    metadata_path = cache_manager._get_chunk_metadata_path(chunk_path)
    byte_range = Range(offset=len(test_content) - 2, size=4)

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)

    first_result = cache_manager.read(
        key,
        source_version="etag-123",
        byte_range=byte_range,
        storage_provider=storage_provider,  # type: ignore[arg-type]
        source_size=len(test_content),
    )

    assert first_result == b"il"
    assert os.path.exists(chunk_path)
    assert os.path.exists(metadata_path)
    assert storage_provider.call_count == 1

    assert (
        cache_manager.read(
            key,
            source_version="etag-123",
            byte_range=byte_range,
            storage_provider=storage_provider,  # type: ignore[arg-type]
            source_size=len(test_content),
        )
        == first_result
    )
    assert storage_provider.call_count == 1, "A portable metadata hit must not refetch the cache line"

    changed_content = b"b" * (3 * 1024 * 1024) + b"next"
    changed_provider = RangeAwareStorageProvider(changed_content)
    changed_range = Range(offset=len(changed_content) - 2, size=4)
    assert (
        cache_manager.read(
            key,
            source_version="etag-456",
            byte_range=changed_range,
            storage_provider=changed_provider,  # type: ignore[arg-type]
            source_size=len(changed_content),
        )
        == b"xt"
    )
    assert changed_provider.call_count == 1

    cache_manager._invalidate_chunks(cache_path)
    assert not os.path.exists(chunk_path)
    assert not os.path.exists(metadata_path)
    assert not os.path.exists(os.path.dirname(chunk_path))


@pytest.mark.parametrize("operation", ["read", "open"])
@pytest.mark.parametrize(
    ("cache_checks_source_version", "check_source_version"),
    [
        pytest.param(True, SourceVersionCheckMode.INHERIT, id="inherit-enabled"),
        pytest.param(False, SourceVersionCheckMode.ENABLE, id="explicit-enable"),
    ],
)
def test_full_cache_operations_remain_bound_to_the_descriptor_validated_for_the_source_version(
    tmpdir,
    monkeypatch,
    operation,
    cache_checks_source_version,
    check_source_version,
):
    """A replacement after version validation cannot change a full-cache result."""
    probe_path = os.path.join(str(tmpdir), "xattr-probe")
    with open(probe_path, "wb") as probe_file:
        probe_file.write(b"probe")
    try:
        xattr.setxattr(probe_path, "user.etag", b"probe")
        xattr.getxattr(probe_path, "user.etag")
    except OSError:
        pytest.skip("xattr is not supported on this filesystem")
    finally:
        os.unlink(probe_path)

    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(
            size="10M",
            cache_line_size="1M",
            check_source_version=cache_checks_source_version,
            location=str(tmpdir),
        ),
    )
    key = "bucket/full-cache-race.bin"
    source_version = "revision-1"
    old_data = b"old-full-cache"
    new_data = b"new-full-cache"
    cache_manager.set(key, old_data, source_version=source_version)
    cache_path = cache_manager._get_cache_file_path(key)
    replacement_path = os.path.join(os.path.dirname(cache_path), "replacement.bin")
    with open(replacement_path, "wb") as replacement:
        replacement.write(new_data)
    xattr.setxattr(replacement_path, "user.etag", b"revision-2")

    metadata_checked = threading.Event()
    allow_read = threading.Event()
    original_getxattr = cache_module.xattr.getxattr

    def synchronize_etag(target, name, *args, **kwargs):
        value = original_getxattr(target, name, *args, **kwargs)
        if name == "user.etag" and not metadata_checked.is_set():
            metadata_checked.set()
            assert allow_read.wait(timeout=5), "timed out waiting to replace the cache entry"
        return value

    monkeypatch.setattr(cache_module.xattr, "getxattr", synchronize_etag)
    result: list[bytes | None] = []
    errors: list[BaseException] = []

    def read_from_cache() -> None:
        try:
            if operation == "read":
                result.append(
                    cache_manager.read(
                        key,
                        source_version=source_version,
                        check_source_version=check_source_version,
                    )
                )
                return

            cached_file = cache_manager.open(
                key,
                source_version=source_version,
                check_source_version=check_source_version,
            )
            assert cached_file is not None
            with cached_file:
                result.append(cached_file.read())
        except BaseException as exc:
            errors.append(exc)

    reader = threading.Thread(target=read_from_cache)
    reader.start()
    assert metadata_checked.wait(timeout=5), "full-cache operation did not validate the cached revision"
    os.replace(replacement_path, cache_path)
    allow_read.set()
    reader.join(timeout=5)

    assert not reader.is_alive()
    assert not errors
    assert result == [old_data]


def test_full_cache_read_and_open_accept_a_promoted_portable_metadata_sidecar(tmpdir, monkeypatch):
    """Full-cache APIs validate a no-xattr promoted entry against its identity-bound sidecar."""
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=False, location=str(tmpdir)),
    )
    key = "bucket/promoted-portable-full-cache.bin"
    source_version = "revision-1"
    data = b"portable"

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)
    assert (
        cache_manager.read(
            key,
            source_version=source_version,
            byte_range=Range(offset=0, size=len(data)),
            storage_provider=RangeAwareStorageProvider(data),
            source_size=len(data),
        )
        == data
    )

    cache_path = cache_manager._get_cache_file_path(key)
    assert os.path.exists(cache_manager._get_chunk_metadata_path(cache_path))
    assert (
        cache_manager.read(
            key,
            source_version=source_version,
            check_source_version=SourceVersionCheckMode.ENABLE,
        )
        == data
    )
    cached_file = cache_manager.open(
        key,
        source_version=source_version,
        check_source_version=SourceVersionCheckMode.ENABLE,
    )
    assert cached_file is not None
    with cached_file:
        assert cached_file.read() == data


def test_range_read_from_an_ordinary_full_cache_uses_the_file_validated_by_xattr(tmpdir, monkeypatch):
    """Replacing a full cache path after metadata validation cannot change the returned revision."""
    probe_path = os.path.join(str(tmpdir), "xattr-probe")
    with open(probe_path, "wb") as probe_file:
        probe_file.write(b"probe")
    try:
        xattr.setxattr(probe_path, "user.etag", b"probe")
        xattr.getxattr(probe_path, "user.etag")
    except OSError:
        pytest.skip("xattr is not supported on this filesystem")
    finally:
        os.unlink(probe_path)

    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    key = "bucket/full-cache.bin"
    old_data = b"old-full-cache"
    new_data = b"new-full-cache"
    cache_manager.set(key, old_data, source_version="revision-1")
    cache_path = cache_manager._get_cache_file_path(key)
    replacement_path = os.path.join(os.path.dirname(cache_path), "replacement.bin")
    with open(replacement_path, "wb") as replacement:
        replacement.write(new_data)
    xattr.setxattr(replacement_path, "user.etag", b"revision-2")

    metadata_checked = threading.Event()
    allow_read = threading.Event()
    original_getxattr = cache_module.xattr.getxattr

    def synchronize_etag(target, name, *args, **kwargs):
        value = original_getxattr(target, name, *args, **kwargs)
        if name == "user.etag":
            metadata_checked.set()
            assert allow_read.wait(timeout=5), "timed out waiting to replace the cache entry"
        return value

    monkeypatch.setattr(cache_module.xattr, "getxattr", synchronize_etag)
    result: list[bytes | None] = []

    reader = threading.Thread(
        target=lambda: result.append(
            cache_manager.read(
                key,
                source_version="revision-1",
                byte_range=Range(offset=0, size=len(old_data)),
                storage_provider=RangeAwareStorageProvider(b"unexpected"),
                source_size=len(old_data),
            )
        )
    )
    reader.start()
    assert metadata_checked.wait(timeout=5), "range reader did not validate the cached revision"
    os.replace(replacement_path, cache_path)
    allow_read.set()
    reader.join(timeout=5)

    assert not reader.is_alive()
    assert result == [old_data]


def test_range_read_reuses_a_legacy_etag_only_full_cache_entry(tmpdir):
    """A pre-range-cache full entry remains valid for range reads when its revision matches."""
    probe_path = os.path.join(str(tmpdir), "xattr-probe")
    with open(probe_path, "wb") as probe_file:
        probe_file.write(b"probe")
    try:
        xattr.setxattr(probe_path, "user.etag", b"probe")
        xattr.getxattr(probe_path, "user.etag")
    except OSError:
        pytest.skip("xattr is not supported on this filesystem")
    finally:
        os.unlink(probe_path)

    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    key = "bucket/legacy-full-cache.bin"
    source_version = "legacy-revision"
    cached_data = b"legacy full-cache content"
    cache_path = cache_manager._get_cache_file_path(key)
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "wb") as cached_file:
        cached_file.write(cached_data)
    xattr.setxattr(cache_path, "user.etag", source_version.encode("utf-8"))

    assert (
        cache_manager.read(
            key,
            source_version=source_version,
            check_source_version=SourceVersionCheckMode.ENABLE,
        )
        == cached_data
    )

    provider = RangeAwareStorageProvider(b"unexpected remote data")
    assert (
        cache_manager.read(
            key,
            source_version=source_version,
            byte_range=Range(offset=7, size=9),
            storage_provider=provider,  # type: ignore[arg-type]
            source_size=len(cached_data),
            check_source_version=SourceVersionCheckMode.ENABLE,
        )
        == cached_data[7:16]
    )
    assert provider.call_count == 0


def test_range_read_from_a_promoted_chunk_uses_the_file_validated_by_xattr(tmpdir, monkeypatch):
    """A promoted chunk-zero entry remains bound to the descriptor validated for its revision."""
    probe_path = os.path.join(str(tmpdir), "xattr-probe")
    with open(probe_path, "wb") as probe_file:
        probe_file.write(b"probe")
    try:
        xattr.setxattr(probe_path, "user.etag", b"probe")
        xattr.getxattr(probe_path, "user.etag")
    except OSError:
        pytest.skip("xattr is not supported on this filesystem")
    finally:
        os.unlink(probe_path)

    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    key = "bucket/promoted-cache.bin"
    old_data = b"old"
    new_data = b"new"
    cache_manager.read(
        key,
        source_version="revision-1",
        byte_range=Range(offset=0, size=len(old_data)),
        storage_provider=RangeAwareStorageProvider(old_data),
        source_size=len(old_data),
    )
    cache_path = cache_manager._get_cache_file_path(key)
    assert os.path.exists(cache_path)

    replacement_path = os.path.join(os.path.dirname(cache_path), "replacement.bin")
    with open(replacement_path, "wb") as replacement:
        replacement.write(new_data)
    xattr.setxattr(replacement_path, "user.etag", b"revision-2")
    xattr.setxattr(replacement_path, "user.cache_line_size", str(1024 * 1024).encode("utf-8"))
    xattr.setxattr(replacement_path, "user.size", str(len(new_data)).encode("utf-8"))

    metadata_checked = threading.Event()
    allow_read = threading.Event()
    original_getxattr = cache_module.xattr.getxattr

    def synchronize_etag(target, name, *args, **kwargs):
        value = original_getxattr(target, name, *args, **kwargs)
        if name == "user.etag":
            metadata_checked.set()
            assert allow_read.wait(timeout=5), "timed out waiting to replace the cache entry"
        return value

    monkeypatch.setattr(cache_module.xattr, "getxattr", synchronize_etag)
    result: list[bytes | None] = []

    reader = threading.Thread(
        target=lambda: result.append(
            cache_manager.read(
                key,
                source_version="revision-1",
                byte_range=Range(offset=0, size=len(old_data)),
                storage_provider=RangeAwareStorageProvider(b"unexpected"),
                source_size=len(old_data),
            )
        )
    )
    reader.start()
    assert metadata_checked.wait(timeout=5), "range reader did not validate the promoted revision"
    os.replace(replacement_path, cache_path)
    allow_read.set()
    reader.join(timeout=5)

    assert not reader.is_alive()
    assert result == [old_data]


def test_range_read_from_a_promoted_chunk_uses_the_file_validated_by_portable_sidecar(tmpdir, monkeypatch):
    """A no-xattr promoted entry validates the descriptor rather than reopening its pathname."""
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    key = "bucket/promoted-sidecar-cache.bin"
    old_data = b"old"
    new_data = b"new"

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)
    cache_manager.read(
        key,
        source_version="revision-1",
        byte_range=Range(offset=0, size=len(old_data)),
        storage_provider=RangeAwareStorageProvider(old_data),
        source_size=len(old_data),
    )
    cache_path = cache_manager._get_cache_file_path(key)
    assert os.path.exists(cache_path)
    assert not os.path.exists(f"{cache_path}.metadata")
    assert os.path.exists(cache_manager._get_chunk_metadata_path(cache_path))

    metadata_checked = threading.Event()
    allow_read = threading.Event()
    original_read_sidecar = cache_manager._read_chunk_metadata_sidecar

    def synchronize_sidecar(*args, **kwargs):
        metadata = original_read_sidecar(*args, **kwargs)
        metadata_checked.set()
        assert allow_read.wait(timeout=5), "timed out waiting to replace the cache entry"
        return metadata

    monkeypatch.setattr(cache_manager, "_read_chunk_metadata_sidecar", synchronize_sidecar)
    result: list[bytes | None] = []

    reader = threading.Thread(
        target=lambda: result.append(
            cache_manager.read(
                key,
                source_version="revision-1",
                byte_range=Range(offset=0, size=len(old_data)),
                storage_provider=RangeAwareStorageProvider(b"unexpected"),
                source_size=len(old_data),
            )
        )
    )
    reader.start()
    assert metadata_checked.wait(timeout=5), "range reader did not validate the portable metadata"
    replacement_path = os.path.join(os.path.dirname(cache_path), "replacement.bin")
    with open(replacement_path, "wb") as replacement:
        replacement.write(new_data)
    os.replace(replacement_path, cache_path)
    allow_read.set()
    reader.join(timeout=5)

    assert not reader.is_alive()
    assert result == [old_data]


def test_range_cache_sidecars_do_not_collide_with_logical_metadata_keys(tmpdir, monkeypatch):
    """A logical ``*.metadata`` cache key remains independent from portable range metadata."""
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    key = "foo"
    logical_metadata_key = ".foo#chunk0.metadata"
    content = b"x" * (2 * 1024 * 1024)
    storage_provider = RangeAwareStorageProvider(content)

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)
    assert (
        cache_manager.read(
            key,
            source_version="revision-1",
            byte_range=Range(offset=0, size=3),
            storage_provider=storage_provider,
            source_size=len(content),
        )
        == b"xxx"
    )

    cache_path = cache_manager._get_cache_file_path(key)
    chunk_path = cache_manager._get_chunk_path(cache_path, 0)
    metadata_path = cache_manager._get_chunk_metadata_path(chunk_path)
    logical_metadata_path = cache_manager._get_cache_file_path(logical_metadata_key)
    assert metadata_path != logical_metadata_path
    assert os.path.exists(metadata_path)

    cache_manager.set(logical_metadata_key, b"logical metadata object")

    assert (
        cache_manager.read(logical_metadata_key, check_source_version=SourceVersionCheckMode.DISABLE)
        == b"logical metadata object"
    )
    assert (
        cache_manager.read(
            key,
            source_version="revision-1",
            byte_range=Range(offset=0, size=3),
            storage_provider=storage_provider,
            source_size=len(content),
        )
        == b"xxx"
    )
    assert storage_provider.call_count == 1


def test_range_chunk_lock_stripes_remain_bounded_after_access_eviction_and_invalidation(tmpdir):
    """Private range locks use a stable bounded pool rather than one inode per logical chunk."""
    cache_config = CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir))
    cache_manager = CacheManager(profile="test", cache_config=cache_config)
    stripe_count = cache_module._RANGE_CACHE_LOCK_STRIPE_COUNT
    accessed_chunks: list[tuple[str, int]] = []

    for index in range(stripe_count * 2):
        cache_path = cache_manager._get_cache_file_path(f"bucket/object-{index // 16}.bin")
        chunk_idx = index % 16
        lock_key = cache_manager._get_chunk_lock_key(cache_path, chunk_idx)
        assert lock_key == cache_manager._get_chunk_lock_key(cache_path, chunk_idx)
        with cache_manager.acquire_lock(lock_key):
            pass
        accessed_chunks.append((cache_path, chunk_idx))

    peer_cache_manager = CacheManager(profile="test", cache_config=cache_config)
    for cache_path, chunk_idx in accessed_chunks[:16]:
        assert peer_cache_manager._get_chunk_lock_key(cache_path, chunk_idx) == cache_manager._get_chunk_lock_key(
            cache_path, chunk_idx
        )
        chunk_path = cache_manager._get_chunk_path(cache_path, chunk_idx)
        with cache_manager.acquire_lock(cache_manager._get_chunk_lock_key(cache_path, chunk_idx)):
            cache_manager._write_chunk_to_cache(
                chunk_path,
                b"x",
                source_version="revision-1",
                chunk_idx=chunk_idx,
                cache_line_size=cache_manager._cache_line_size or 1,
                source_size=None,
            )

    cache_manager._max_cache_size = 0
    cache_manager.evict_files()
    for cache_path, _ in accessed_chunks[:16]:
        cache_manager._invalidate_chunks(cache_path)

    lock_root = os.path.join(cache_manager._range_cache_dir, cache_module._RANGE_CACHE_LOCK_DIRECTORY)
    observed_directories: list[str] = []
    lock_files: list[str] = []
    for root, directories, filenames in os.walk(lock_root):
        observed_directories.extend(os.path.join(root, directory) for directory in directories)
        lock_files.extend(os.path.join(root, filename) for filename in filenames)

    assert not observed_directories
    assert len(lock_files) <= stripe_count
    assert all(os.path.dirname(lock_file) == lock_root for lock_file in lock_files)
    assert all(
        os.path.basename(lock_file).startswith(".stripe-") and lock_file.endswith(".lock") for lock_file in lock_files
    )
    assert cache_manager.cache_size() == 0


def test_logical_metadata_files_are_quota_accounted_and_evicted(tmpdir):
    """Only internal range sidecars are exempt from cache size and eviction accounting."""
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(
            size="1M",
            cache_line_size="1M",
            check_source_version=False,
            location=str(tmpdir),
            eviction_policy=EvictionPolicyConfig(policy="fifo", purge_factor=0),
        ),
    )
    metadata_key = ".foo#chunk0.metadata"
    data_key = "second.bin"
    cache_manager.set(metadata_key, b"m" * 1024 * 1024)
    metadata_path = cache_manager._get_cache_file_path(metadata_key)
    cache_manager._make_writable(metadata_path)
    os.utime(metadata_path, ns=(1, 1))
    cache_manager._make_readonly(metadata_path)
    cache_manager.set(data_key, b"d" * 1024 * 1024)

    assert cache_manager.cache_size() == 2 * 1024 * 1024

    cache_manager.evict_files()

    assert not os.path.exists(metadata_path)
    assert cache_manager.cache_size() <= 1024 * 1024


def test_cross_profile_eviction_removes_foreign_range_data_and_portable_sidecar(tmpdir, monkeypatch):
    """A shared cache root evicts a foreign range chunk together with its metadata sidecar."""
    cache_config = CacheConfig(
        size="2M",
        cache_line_size="1M",
        check_source_version=True,
        location=str(tmpdir),
        eviction_policy=EvictionPolicyConfig(policy="fifo", purge_factor=0),
    )
    evictor = CacheManager(profile="evictor", cache_config=cache_config)
    owner = CacheManager(profile="owner", cache_config=cache_config)
    cache_line_size = 1024 * 1024

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)

    owner_cache_path = owner._get_cache_file_path("range-only.bin")
    owner_chunk_path = owner._get_chunk_path(owner_cache_path, 0)
    owner._write_chunk_to_cache(
        owner_chunk_path,
        b"o" * cache_line_size,
        source_version="owner-r1",
        chunk_idx=0,
        cache_line_size=cache_line_size,
        source_size=cache_line_size,
    )
    owner_sidecar_path = owner._get_chunk_metadata_path(owner_chunk_path)
    assert os.path.exists(owner_sidecar_path)
    os.utime(owner_chunk_path, ns=(1, 1))
    os.utime(owner_sidecar_path, ns=(1, 1))

    evictor.set("newer.bin", b"e" * cache_line_size)
    evictor._max_cache_size = 3 * cache_line_size // 2

    assert evictor.cache_size() == 2 * cache_line_size
    evictor.evict_files()

    assert not os.path.exists(owner_chunk_path)
    assert not os.path.exists(owner_sidecar_path)


def test_cross_profile_full_entry_eviction_cleans_the_owning_range_namespace(tmpdir, monkeypatch):
    """Evicting another profile's full entry removes its private promoted chunks and sidecars."""
    cache_config = CacheConfig(
        size="4M",
        cache_line_size="1M",
        check_source_version=True,
        location=str(tmpdir),
        eviction_policy=EvictionPolicyConfig(policy="fifo", purge_factor=0),
    )
    evictor = CacheManager(profile="evictor", cache_config=cache_config)
    owner = CacheManager(profile="owner", cache_config=cache_config)
    cache_line_size = 1024 * 1024

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)

    owner_key = "object.bin"
    owner.set(owner_key, b"o" * cache_line_size, source_version="owner-r1")
    owner_cache_path = owner._get_cache_file_path(owner_key)
    owner_chunk_path = owner._get_chunk_path(owner_cache_path, 0)
    owner._write_chunk_to_cache(
        owner_chunk_path,
        b"r" * cache_line_size,
        source_version="owner-r1",
        chunk_idx=0,
        cache_line_size=cache_line_size,
        source_size=cache_line_size,
    )
    owner_sidecar_path = owner._get_chunk_metadata_path(owner_chunk_path)
    assert os.path.exists(owner_sidecar_path)
    owner._make_writable(owner_cache_path)
    os.utime(owner_cache_path, ns=(1, 1))
    owner._make_readonly(owner_cache_path)
    os.utime(owner_chunk_path, ns=(2, 2))
    os.utime(owner_sidecar_path, ns=(2, 2))

    evictor.set("newer.bin", b"e" * cache_line_size)
    evictor._max_cache_size = 5 * cache_line_size // 2

    assert evictor._cache_refresh_lock_file.lock_file == owner._cache_refresh_lock_file.lock_file
    assert evictor.cache_size() == 3 * cache_line_size
    evictor.evict_files()

    assert not os.path.exists(owner_cache_path)
    assert not os.path.exists(owner_chunk_path)
    assert not os.path.exists(owner_sidecar_path)


def test_cross_profile_eviction_never_deletes_an_in_progress_foreign_temp_file(tmpdir):
    """A profile's eviction pass excludes every cache-root temporary namespace."""
    cache_config = CacheConfig(
        size="1M",
        cache_line_size="1M",
        check_source_version=False,
        location=str(tmpdir),
        eviction_policy=EvictionPolicyConfig(policy="fifo", purge_factor=0),
    )
    evictor = CacheManager(profile="evictor", cache_config=cache_config)
    owner = CacheManager(profile="owner", cache_config=cache_config)
    temporary_path = os.path.join(owner._cache_temp_dir, "in-progress-download")
    with open(temporary_path, "wb") as temporary:
        temporary.write(b"in progress")

    evictor._max_cache_size = 0
    evictor.evict_files()

    assert os.path.exists(temporary_path)
    assert evictor.cache_size() == 0


def test_cross_profile_eviction_preserves_a_legacy_in_progress_temp_download(tmpdir):
    """New cache roots continue to exclude baseline ``.tmp-<profile>`` downloads from quota management."""
    cache_config = CacheConfig(
        size="1M",
        cache_line_size="1M",
        check_source_version=False,
        location=str(tmpdir),
        eviction_policy=EvictionPolicyConfig(policy="fifo", purge_factor=0),
    )
    legacy_temporary_dir = os.path.join(str(tmpdir), ".tmp-legacy-profile")
    os.makedirs(legacy_temporary_dir)
    legacy_temporary_path = os.path.join(legacy_temporary_dir, "in-progress-download")
    with open(legacy_temporary_path, "wb") as temporary:
        temporary.write(b"legacy in progress" * (128 * 1024))

    evictor = CacheManager(profile="evictor", cache_config=cache_config)

    evictor._max_cache_size = 0
    assert evictor.cache_size() == 0
    evictor.evict_files()

    assert os.path.exists(legacy_temporary_path)


@pytest.mark.parametrize("operation", ["read", "open"])
def test_cache_manager_set_publishes_identity_bound_portable_metadata_without_xattrs(tmpdir, monkeypatch, operation):
    """Full-cache set() entries remain reusable for validated reads without xattrs."""
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    key = "bucket/no-xattr-full-cache.bin"
    source_version = "revision-2"
    data = b"portable full cache"

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)

    cache_manager.set(key, data, source_version=source_version)
    cache_path = cache_manager._get_cache_file_path(key)
    metadata_path = cache_manager._get_chunk_metadata_path(cache_path)

    assert os.path.exists(metadata_path)
    metadata = cache_manager._read_chunk_metadata_sidecar(cache_path)
    assert metadata is not None
    assert metadata["source_version"] == source_version
    assert metadata["object_size"] == len(data)

    if operation == "read":
        assert (
            cache_manager.read(
                key,
                source_version=source_version,
                check_source_version=SourceVersionCheckMode.ENABLE,
            )
            == data
        )
    else:
        cached_file = cache_manager.open(
            key,
            source_version=source_version,
            check_source_version=SourceVersionCheckMode.ENABLE,
        )
        assert cached_file is not None
        with cached_file:
            assert cached_file.read() == data


def test_cache_manager_serializes_no_xattr_publication_for_concurrent_revisions(tmpdir, monkeypatch):
    """The data replacement and portable metadata publication form one cache-root transition."""
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    key = "bucket/concurrent-no-xattr.bin"
    first_metadata_started = threading.Event()
    second_started = threading.Event()
    second_metadata_published = threading.Event()
    errors: list[BaseException] = []

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)
    original_write_sidecar = cache_manager._write_chunk_metadata_sidecar

    def serialize_first_sidecar(*args, **kwargs):
        source_version = kwargs.get("source_version", args[1])
        if source_version == "revision-a":
            first_metadata_started.set()
            assert second_started.wait(timeout=5), "second writer did not start"
            if not cache_manager._cache_refresh_lock_file.is_locked:
                assert second_metadata_published.wait(timeout=5), "second writer did not publish metadata"
        result = original_write_sidecar(*args, **kwargs)
        if source_version == "revision-b":
            second_metadata_published.set()
        return result

    monkeypatch.setattr(cache_manager, "_write_chunk_metadata_sidecar", serialize_first_sidecar)

    def publish(data: bytes, revision: str, started: threading.Event | None = None) -> None:
        try:
            if started is not None:
                started.set()
            cache_manager.set(key, data, source_version=revision)
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)

    first = threading.Thread(target=publish, args=(b"first", "revision-a"))
    first.start()
    assert first_metadata_started.wait(timeout=5), "first writer did not reach portable metadata publication"
    second = threading.Thread(target=publish, args=(b"second", "revision-b", second_started))
    second.start()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive()
    assert not second.is_alive()
    assert not errors
    assert (
        cache_manager.read(key, source_version="revision-b", check_source_version=SourceVersionCheckMode.ENABLE)
        == b"second"
    )
    assert (
        cache_manager.read(key, source_version="revision-a", check_source_version=SourceVersionCheckMode.ENABLE) is None
    )


def test_range_chunk_serializes_no_xattr_publication_for_concurrent_revisions(tmpdir, monkeypatch):
    """Private range chunks use the same publication coordinator as full cache entries."""
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    cache_path = cache_manager._get_cache_file_path("bucket/concurrent-chunk.bin")
    chunk_path = cache_manager._get_chunk_path(cache_path, 0)
    first_metadata_started = threading.Event()
    second_started = threading.Event()
    second_metadata_published = threading.Event()
    errors: list[BaseException] = []

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)
    original_write_sidecar = cache_manager._write_chunk_metadata_sidecar

    def serialize_first_sidecar(*args, **kwargs):
        source_version = kwargs.get("source_version", args[1])
        if source_version == "revision-a":
            first_metadata_started.set()
            assert second_started.wait(timeout=5), "second chunk writer did not start"
            if not cache_manager._cache_refresh_lock_file.is_locked:
                assert second_metadata_published.wait(timeout=5), "second chunk writer did not publish metadata"
        result = original_write_sidecar(*args, **kwargs)
        if source_version == "revision-b":
            second_metadata_published.set()
        return result

    monkeypatch.setattr(cache_manager, "_write_chunk_metadata_sidecar", serialize_first_sidecar)

    def publish(data: bytes, revision: str, started: threading.Event | None = None) -> None:
        try:
            if started is not None:
                started.set()
            cache_manager._write_chunk_to_cache(
                chunk_path,
                data,
                source_version=revision,
                chunk_idx=0,
                cache_line_size=1024 * 1024,
                source_size=len(data),
            )
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)

    first = threading.Thread(target=publish, args=(b"first", "revision-a"))
    first.start()
    assert first_metadata_started.wait(timeout=5), "first chunk writer did not reach portable metadata publication"
    second = threading.Thread(target=publish, args=(b"second", "revision-b", second_started))
    second.start()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive()
    assert not second.is_alive()
    assert not errors
    assert cache_manager._is_chunk_valid(
        chunk_path,
        "revision-b",
        1024 * 1024,
        require_source_version=True,
    )
    assert open(chunk_path, "rb").read() == b"second"


def test_cache_manager_publication_survives_eviction_attempt_after_data_replace(tmpdir, monkeypatch):
    """Eviction cannot see a data file before its portable metadata and cleanup are complete."""
    cache_config = CacheConfig(
        size="1M",
        cache_line_size="1M",
        check_source_version=True,
        location=str(tmpdir),
        eviction_policy=EvictionPolicyConfig(policy="fifo", purge_factor=0),
    )
    cache_manager = CacheManager(profile="test", cache_config=cache_config)
    evictor = CacheManager(profile="test", cache_config=cache_config)
    cache_manager._max_cache_size = 0
    evictor._max_cache_size = 0
    key = "bucket/evicted-publication.bin"
    cache_path = cache_manager._get_cache_file_path(key)
    eviction_started = threading.Event()
    eviction_finished = threading.Event()
    eviction_thread: list[threading.Thread] = []

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)
    original_replace = cache_module.os.replace

    def evict_after_data_replace(source: str, destination: str) -> None:
        original_replace(source, destination)
        if destination != cache_path:
            return

        def evict() -> None:
            eviction_started.set()
            evictor.evict_files()
            eviction_finished.set()

        thread = threading.Thread(target=evict)
        eviction_thread.append(thread)
        thread.start()
        assert eviction_started.wait(timeout=5), "eviction did not begin"
        if not cache_manager._cache_refresh_lock_file.is_locked:
            assert eviction_finished.wait(timeout=5), "eviction did not run after data replacement"

    monkeypatch.setattr(cache_module.os, "replace", evict_after_data_replace)

    cache_manager.set(key, b"cached", source_version="revision-1")

    assert eviction_thread
    eviction_thread[0].join(timeout=5)
    assert not eviction_thread[0].is_alive()
    assert eviction_finished.is_set()
    assert not os.path.exists(cache_path)
    assert not os.path.exists(cache_manager._get_chunk_metadata_path(cache_path))


def test_cache_manager_serializes_explicit_delete_with_no_xattr_publication(tmpdir, monkeypatch):
    """An explicit delete cannot run between data replacement and portable-sidecar publication."""
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    key = "bucket/explicit-delete-no-xattr.bin"
    cache_path = cache_manager._get_cache_file_path(key)
    transition: queue.Queue[str] = queue.Queue()
    deletion_requested = threading.Event()
    deletion_finished = threading.Event()
    allow_delete = threading.Event()
    errors: list[BaseException] = []
    delete_threads: list[threading.Thread] = []

    class RootPublicationLock:
        def __init__(self) -> None:
            self._lock = threading.Lock()

        def __enter__(self):
            if threading.current_thread().name == "cache-explicit-delete":
                transition.put("root")
            self._lock.acquire()
            return self

        def __exit__(self, *_args) -> None:
            self._lock.release()

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)
    monkeypatch.setattr(cache_manager, "_cache_refresh_lock_file", RootPublicationLock())
    original_delete = cache_manager._delete_cache_file_at_path
    original_replace = cache_module.os.replace

    def observe_delete(path: str) -> None:
        transition.put("body")
        assert allow_delete.wait(timeout=5), "delete was never allowed to continue"
        original_delete(path)

    monkeypatch.setattr(cache_manager, "_delete_cache_file_at_path", observe_delete)

    def delete_cached_file() -> None:
        try:
            deletion_requested.set()
            cache_manager.delete(key)
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)
        finally:
            deletion_finished.set()

    def interleave_after_data_replace(source: str, destination: str) -> None:
        original_replace(source, destination)
        if destination != cache_path:
            return

        delete_thread = threading.Thread(target=delete_cached_file, name="cache-explicit-delete")
        delete_threads.append(delete_thread)
        delete_thread.start()
        assert deletion_requested.wait(timeout=5), "delete did not start"
        transition_kind = transition.get(timeout=5)
        allow_delete.set()
        if transition_kind == "body":
            assert deletion_finished.wait(timeout=5), "unlocked delete did not complete before sidecar publication"

    monkeypatch.setattr(cache_module.os, "replace", interleave_after_data_replace)

    cache_manager.set(key, b"cached", source_version="revision-1")

    assert len(delete_threads) == 1
    delete_threads[0].join(timeout=5)
    assert not delete_threads[0].is_alive()
    assert not errors
    assert not os.path.exists(cache_path)
    assert not os.path.exists(cache_manager._get_chunk_metadata_path(cache_path))


@pytest.mark.parametrize("source_version", [None, ""], ids=["none", "empty"])
def test_required_source_version_rejects_missing_no_xattr_full_and_range_entries(tmpdir, monkeypatch, source_version):
    """Required version checks fail closed when either requested revision is absent or empty."""
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmpdir)),
    )
    key = "bucket/missing-revision.bin"
    cached = b"cached data"
    remote = b"fresh data"

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)
    cache_manager.set(key, cached, source_version="stored-revision")

    assert (
        cache_manager.read(key, source_version=source_version, check_source_version=SourceVersionCheckMode.ENABLE)
        is None
    )
    assert (
        cache_manager.open(key, source_version=source_version, check_source_version=SourceVersionCheckMode.ENABLE)
        is None
    )
    assert (
        cache_manager.read(
            key,
            source_version=source_version,
            byte_range=Range(offset=0, size=3),
            storage_provider=RangeAwareStorageProvider(remote),
            source_size=len(remote),
            check_source_version=SourceVersionCheckMode.ENABLE,
        )
        is None
    )

    assert cache_manager.read(key, check_source_version=SourceVersionCheckMode.DISABLE) == cached
    cached_file = cache_manager.open(key, check_source_version=SourceVersionCheckMode.DISABLE)
    assert cached_file is not None
    with cached_file:
        assert cached_file.read() == cached
    assert (
        cache_manager.read(
            key,
            byte_range=Range(offset=0, size=3),
            storage_provider=RangeAwareStorageProvider(remote),
            source_size=len(remote),
            check_source_version=SourceVersionCheckMode.DISABLE,
        )
        == cached[:3]
    )


@pytest.mark.parametrize(
    "profile",
    [".range-cache-user", ".cache_refresh.lock", "project.v2+preview@west"],
)
def test_cache_profile_names_do_not_collide_with_internal_namespaces(tmpdir, profile):
    """Legal one-component profile names remain ordinary cache-data roots."""
    cache_manager = CacheManager(
        profile=profile,
        cache_config=CacheConfig(
            size="1M",
            cache_line_size="1M",
            check_source_version=False,
            location=str(tmpdir),
            eviction_policy=EvictionPolicyConfig(policy="fifo", purge_factor=0),
        ),
    )
    key = "bucket/object.bin"
    cache_manager.set(key, b"data")
    cache_path = cache_manager._get_cache_file_path(key)

    assert cache_manager.read(key, check_source_version=SourceVersionCheckMode.DISABLE) == b"data"
    assert os.path.commonpath([os.path.realpath(str(tmpdir)), os.path.realpath(cache_path)]) == os.path.realpath(
        str(tmpdir)
    )
    cache_manager._max_cache_size = 0
    cache_manager.evict_files()
    assert not os.path.exists(cache_path)


@pytest.mark.parametrize("profile", [".tmp-user", ".tmp-legacy-profile", ".tmp-"])
def test_cache_manager_rejects_profiles_reserved_for_legacy_temp_downloads(tmpdir, profile):
    """New profiles cannot claim the legacy temporary-directory prefix that eviction must preserve."""
    with pytest.raises(ValueError, match="reserved"):
        CacheManager(
            profile=profile,
            cache_config=CacheConfig(
                size="10M", cache_line_size="1M", check_source_version=False, location=str(tmpdir)
            ),
        )


@pytest.mark.parametrize(
    "profile",
    [".msc-cache-internal", "nested/profile", r"nested\\profile", "nested/./..", ".", ".."],
)
def test_cache_manager_rejects_profiles_that_can_escape_or_alias_internal_paths(tmpdir, profile):
    """Runtime CacheManager construction enforces the same one-component profile boundary as schema loading."""
    with pytest.raises(ValueError, match="profile"):
        CacheManager(
            profile=profile,
            cache_config=CacheConfig(
                size="10M", cache_line_size="1M", check_source_version=False, location=str(tmpdir)
            ),
        )


def test_cache_manager_rejects_logical_keys_that_escape_the_profile_cache_root(tmpdir):
    """A cache key cannot traverse out of the validated profile data root."""
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=False, location=str(tmpdir)),
    )

    with pytest.raises(ValueError, match="escapes"):
        cache_manager.set("../outside.bin", b"outside")


def test_cache_manager_rejects_an_internal_root_symlink_that_escapes_the_cache_root(tmpdir):
    """Internal cache bookkeeping cannot be redirected outside the configured cache location."""
    outside = tmpdir.dirpath().mkdir(f"outside-{uuid.uuid4()}")
    os.symlink(str(outside), os.path.join(str(tmpdir), ".msc-cache-internal"))

    with pytest.raises(ValueError, match="internal.*escapes"):
        CacheManager(
            profile="test",
            cache_config=CacheConfig(
                size="10M", cache_line_size="1M", check_source_version=False, location=str(tmpdir)
            ),
        )


def test_cache_manager_generate_temp_file_path(cache_manager):
    """Test that CacheManager can generate a temporary file path."""
    temp_file_path = cache_manager.generate_temp_file_path()
    assert os.path.exists(temp_file_path) is False
    assert cache_manager._cache_temp_dir in temp_file_path
    assert os.path.commonpath([cache_manager._cache_internal_dir, cache_manager._cache_temp_dir]) == (
        cache_manager._cache_internal_dir
    )


def test_cache_manager_refresh_cache(tmpdir):
    """Test that cache refresh works correctly."""
    # Use a separate cache directory for this test
    cache_dir = os.path.join(str(tmpdir), "refresh_test")
    os.makedirs(cache_dir, exist_ok=True)

    cache_config = CacheConfig(size="10M", cache_line_size="64M", check_source_version=False, location=cache_dir)
    cache_manager = CacheManager(profile="refresh_test", cache_config=cache_config)

    data_10mb = b"*" * 10 * 1024 * 1024
    for i in range(20):
        file_name = f"bucket/test_{i:04d}.bin"
        cache_manager.set(file_name, data_10mb)

    # Force refresh by setting last refresh time to the past
    cache_manager._last_refresh_time = datetime.now() - timedelta(seconds=cache_manager._cache_refresh_interval + 1)

    cache_manager.refresh_cache()
    assert cache_manager.cache_size() <= 10 * 1024 * 1024

    # Clean up
    shutil.rmtree(cache_dir)


def test_chunk_write_schedules_refresh(tmpdir, monkeypatch):
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=False, location=str(tmpdir)),
    )
    storage_provider = RangeAwareStorageProvider(create_test_data(2))
    key = "bucket/scheduled_refresh.bin"
    refresh_called = threading.Event()

    monkeypatch.setattr(cache_manager, "_should_refresh_cache", lambda: True)

    def fake_refresh_cache() -> bool:
        refresh_called.set()
        return True

    monkeypatch.setattr(cache_manager, "refresh_cache", fake_refresh_cache)

    cache_manager.read(
        key,
        byte_range=Range(offset=0, size=16 * 1024),
        storage_provider=storage_provider,  # type: ignore[arg-type]
        source_size=len(storage_provider._data),
    )

    assert refresh_called.wait(timeout=5), "Chunk writes should schedule asynchronous cache refresh"
    refresh_thread = cache_manager._cache_refresh_thread
    if refresh_thread is not None:
        refresh_thread.join(timeout=5)
    assert storage_provider.call_count == 1


def test_partial_chunk_write_triggers_background_eviction(tmpdir):
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="2M", cache_line_size="1M", check_source_version=False, location=str(tmpdir)),
    )
    storage_provider = RangeAwareStorageProvider(create_test_data(5))
    key = "bucket/cleanup.bin"
    cache_path = cache_manager._get_cache_file_path(key)
    chunk0_path = cache_manager._get_chunk_path(cache_path, 0)
    chunk1_path = cache_manager._get_chunk_path(cache_path, 1)
    chunk2_path = cache_manager._get_chunk_path(cache_path, 2)

    assert (
        cache_manager.read(
            key,
            byte_range=Range(offset=0, size=1024 * 1024),
            storage_provider=storage_provider,  # type: ignore[arg-type]
            source_size=len(storage_provider._data),
        )
        == storage_provider._data[: 1024 * 1024]
    )
    assert (
        cache_manager.read(
            key,
            byte_range=Range(offset=1024 * 1024, size=1024 * 1024),
            storage_provider=storage_provider,  # type: ignore[arg-type]
            source_size=len(storage_provider._data),
        )
        == storage_provider._data[1024 * 1024 : 2 * 1024 * 1024]
    )

    assert os.path.exists(chunk0_path)
    assert os.path.exists(chunk1_path)

    cache_manager._last_refresh_time = datetime.now() - timedelta(seconds=cache_manager._cache_refresh_interval + 1)

    assert (
        cache_manager.read(
            key,
            byte_range=Range(offset=2 * 1024 * 1024, size=1024 * 1024),
            storage_provider=storage_provider,  # type: ignore[arg-type]
            source_size=len(storage_provider._data),
        )
        == storage_provider._data[2 * 1024 * 1024 : 3 * 1024 * 1024]
    )

    for _ in range(100):
        if not os.path.exists(chunk0_path):
            break
        time.sleep(0.05)

    assert not os.path.exists(chunk0_path), "Oldest chunk should be evicted by scheduled background refresh"
    assert os.path.exists(chunk1_path)
    assert os.path.exists(chunk2_path)
    assert cache_manager.cache_size() <= 2 * 1024 * 1024


@pytest.fixture
def lru_cache_config(tmpdir):
    cache_dir = os.path.join(str(tmpdir), "lru_cache")
    return CacheConfig(
        size="10M",
        cache_line_size="64M",
        check_source_version=False,
        location=cache_dir,
        eviction_policy=EvictionPolicyConfig(policy="LRU"),
    )


@pytest.fixture
def mru_cache_config(tmpdir):
    cache_dir = os.path.join(str(tmpdir), "mru_cache")
    return CacheConfig(
        size="10M",
        cache_line_size="64M",
        check_source_version=False,
        location=cache_dir,
        eviction_policy=EvictionPolicyConfig(policy="MRU"),
    )


def test_lru_eviction_policy(profile_name, lru_cache_config):
    # Create the CacheManager with the provided lru_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=lru_cache_config)

    test_uuid = str(uuid.uuid4())
    # Add files to the cache (each file is 3 MB)
    cache_manager.set(f"{test_uuid}/file1", b"a" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)
    cache_manager.set(f"{test_uuid}/file2", b"b" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)
    cache_manager.set(f"{test_uuid}/file3", b"c" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)

    # Access file1 to make it the most recently used
    cache_manager.read(f"{test_uuid}/file1")  # force update ts
    time.sleep(1)

    # Add another file to trigger eviction
    cache_manager.set(f"{test_uuid}/file4", b"d" * 3 * 1024 * 1024)  # 3 MB

    time.sleep(1)  # Ensure time difference for LRU
    # Record the current last_refresh_time and set it to past to force refresh
    old_refresh_time = cache_manager._last_refresh_time
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()
    # Verify that refresh occurred by checking last_refresh_time was updated
    assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

    # Verify that file1 is still in the cache (LRU policy)
    assert cache_manager.contains(f"{test_uuid}/file1"), "Most recently used file should be kept"

    # Verify that the least recently used file (file2 or file3) has been evicted
    assert not cache_manager.contains(f"{test_uuid}/file2") or not cache_manager.contains(f"{test_uuid}/file3"), (
        "Least recently used file should be evicted"
    )


def test_mru_eviction_policy(profile_name, mru_cache_config):
    """Test the MRU (Most Recently Used) eviction policy.

    This test verifies that the cache manager correctly implements MRU eviction, where
    the most recently accessed files are evicted first, preserving older files.
    """
    # Create the CacheManager with the provided mru_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=mru_cache_config)

    test_uuid = str(uuid.uuid4())
    # Add files to the cache (each file is 3 MB)
    cache_manager.set(f"{test_uuid}/file1", b"a" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)
    cache_manager.set(f"{test_uuid}/file2", b"b" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)
    cache_manager.set(f"{test_uuid}/file3", b"c" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)

    # Access file1 to make it the most recently used
    cache_manager.read(f"{test_uuid}/file1")  # force update ts
    time.sleep(1)

    # Add another file to trigger eviction
    cache_manager.set(f"{test_uuid}/file4", b"d" * 3 * 1024 * 1024)  # 3 MB

    time.sleep(1)  # Ensure time difference for MRU
    # Record the current last_refresh_time and set it to past to force refresh
    old_refresh_time = cache_manager._last_refresh_time
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()
    # Verify that refresh occurred by checking last_refresh_time was updated
    assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

    # Verify that file1 (most recently used) OR file4 (newly added, also recently used) has been evicted (MRU policy)
    # With MRU, the most recently accessed/added files should be evicted
    file1_present = cache_manager.contains(f"{test_uuid}/file1")
    file4_present = cache_manager.contains(f"{test_uuid}/file4")

    # At least one of the most recent files (file1 or file4) should be evicted
    assert not (file1_present and file4_present), (
        "MRU should evict most recently used files (file1 was accessed, file4 was just added)"
    )

    # Verify that at least one of the older files (file2 or file3) is still in cache
    assert cache_manager.contains(f"{test_uuid}/file2") or cache_manager.contains(f"{test_uuid}/file3"), (
        "Older files should be preserved with MRU policy"
    )


@pytest.fixture
def fifo_cache_config(tmpdir):
    cache_dir = os.path.join(str(tmpdir), "fifo_cache")
    return CacheConfig(
        size="10M",
        cache_line_size="64M",
        check_source_version=False,
        location=cache_dir,
        eviction_policy=EvictionPolicyConfig(policy="FIFO"),
    )


def test_fifo_eviction_policy(profile_name, fifo_cache_config):
    # Create the CacheManager with the provided fifo_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=fifo_cache_config)

    test_uuid = str(uuid.uuid4())
    # Add files to the cache (each file is 3 MB)
    cache_manager.set(f"{test_uuid}/file1", b"a" * 3 * 1024 * 1024)  # 3 MB - First in
    time.sleep(1)  # Ensure files have different timestamps
    cache_manager.set(f"{test_uuid}/file2", b"b" * 3 * 1024 * 1024)  # 3 MB - Second in
    time.sleep(1)  # Ensure files have different timestamps
    cache_manager.set(f"{test_uuid}/file3", b"c" * 3 * 1024 * 1024)  # 3 MB - Third in

    # Access files in different order to verify FIFO is independent of access patterns
    cache_manager.read(f"{test_uuid}/file3")  # Access the newest file
    cache_manager.read(f"{test_uuid}/file2")  # Access the middle file
    cache_manager.read(f"{test_uuid}/file1")  # Access the oldest file

    # Add another file to trigger eviction
    cache_manager.set(f"{test_uuid}/file4", b"d" * 3 * 1024 * 1024)  # 3 MB - Fourth in

    # Force refresh to trigger eviction
    old_refresh_time = cache_manager._last_refresh_time
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()
    assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

    # Verify that file1 (first in) has been evicted
    assert not cache_manager.contains(f"{test_uuid}/file1"), "First file in should be evicted (FIFO)"

    # Verify that later files are still in the cache
    assert cache_manager.contains(f"{test_uuid}/file2"), "Second file in should be kept"
    assert cache_manager.contains(f"{test_uuid}/file3"), "Third file in should be kept"
    assert cache_manager.contains(f"{test_uuid}/file4"), "Newly added file should be in the cache"

    # Add one more file to verify FIFO continues to work
    cache_manager.set(f"{test_uuid}/file5", b"e" * 3 * 1024 * 1024)  # 3 MB - Fifth in

    # Force refresh to trigger eviction
    old_refresh_time = cache_manager._last_refresh_time
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()
    assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

    # Verify that file2 (now the oldest) is evicted
    assert not cache_manager.contains(f"{test_uuid}/file2"), "Second file in should now be evicted"
    assert cache_manager.contains(f"{test_uuid}/file3"), "Third file in should still be kept"
    assert cache_manager.contains(f"{test_uuid}/file4"), "Fourth file in should still be kept"
    assert cache_manager.contains(f"{test_uuid}/file5"), "Most recently added file should be in the cache"


@pytest.fixture
def random_cache_config(tmpdir):
    cache_dir = os.path.join(str(tmpdir), "random_cache")
    return CacheConfig(
        size="10M",
        cache_line_size="64M",
        check_source_version=False,
        location=cache_dir,
        eviction_policy=EvictionPolicyConfig(policy="RANDOM"),
    )


def test_random_eviction_policy(profile_name, random_cache_config):
    """Test the random eviction policy of the cache manager.

    This test verifies that the cache manager correctly implements random eviction when the cache is full.
    The test follows these steps:
    1. Creates a cache with a 10MB limit
    2. Adds three files of 3MB each (total 9MB)
    3. Adds a fourth file to trigger eviction
    4. Verifies that:
       - Exactly one file is evicted
       - Total cache size stays within limits

    The test ensures that:
    - The cache respects its size limit
    - Eviction occurs when needed
    - The random eviction policy works as expected
    - Cache operations maintain consistency

    :param profile_name: The name of the cache profile to use
    :param random_cache_config: Cache configuration with random eviction policy
    """
    # Clean the entire cache directory
    cache_dir = random_cache_config.location
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
    os.makedirs(cache_dir)

    # Create the CacheManager with the provided random_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=random_cache_config)

    test_uuid = str(uuid.uuid4())
    # Add files to the cache (each file is 3 MB)
    cache_manager.set(f"{test_uuid}/file1", b"a" * 3 * 1024 * 1024)  # 3 MB
    cache_manager.set(f"{test_uuid}/file2", b"b" * 3 * 1024 * 1024)  # 3 MB
    cache_manager.set(f"{test_uuid}/file3", b"c" * 3 * 1024 * 1024)  # 3 MB

    # Verify initial state
    assert cache_manager.contains(f"{test_uuid}/file1")
    assert cache_manager.contains(f"{test_uuid}/file2")
    assert cache_manager.contains(f"{test_uuid}/file3")

    # Force a refresh to ensure cache state is up to date
    cache_manager.refresh_cache()

    # Add another file to trigger eviction
    cache_manager.set(f"{test_uuid}/file4", b"d" * 3 * 1024 * 1024)  # 3 MB

    # Force refresh to trigger eviction by setting last refresh time to the past
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()

    # Verify that exactly one file was evicted (could be any of the files)
    all_files = [f"{test_uuid}/file1", f"{test_uuid}/file2", f"{test_uuid}/file3", f"{test_uuid}/file4"]
    remaining_files = sum(1 for f in all_files if cache_manager.contains(f))
    assert remaining_files == 3, "Exactly one file should be evicted"

    # Verify total cache size
    total_size = 0
    for f in all_files:
        if cache_manager.contains(f):
            data = cache_manager.read(f)
            if data is not None:  # Handle potential None return from read()
                total_size += len(data)
    assert total_size <= 10 * 1024 * 1024, "Total cache size should not exceed 10MB"


def verify_cache_operations(cache_manager):
    # Add files to the cache (each file is 3 MB)
    test_uuid = str(uuid.uuid4())
    key1 = f"{test_uuid}/test_file1"
    key2 = f"{test_uuid}/test_file2"
    key3 = f"{test_uuid}/test_file3"
    cache_manager.set(key1, b"a" * 1 * 1024 * 1024, source_version="etag1")  # 1 MB - First in
    cache_manager.set(key2, b"b" * 1 * 1024 * 1024, source_version="etag2")  # 1 MB - Second in
    cache_manager.set(key3, b"c" * 1 * 1024 * 1024, source_version="etag3")  # 1 MB - Third in

    # Access files in different order to verify FIFO is independent of access patterns
    cache_manager.read(key3, source_version="etag3")  # Access the newest file
    cache_manager.read(key2, source_version="etag2")  # Access the middle file
    cache_manager.read(key1, source_version="etag1")  # Access the oldest file

    # Verify that later files are still in the cache with correct ETags
    assert cache_manager.contains(key1, source_version="etag1"), "Second file in should be kept"
    assert cache_manager.contains(key2, source_version="etag2"), "Third file in should be kept"
    assert cache_manager.contains(key3, source_version="etag3"), "Newly added file should be in the cache"


def create_legacy_cache_config(profile_config, tmpdir):
    """Helper function to create legacy cache config."""
    return {
        "profiles": {"s3-local": profile_config},
        "cache": {"size_mb": 10, "check_source_version": False, "location": str(tmpdir), "eviction_policy": "fifo"},
    }


def create_new_cache_config(profile_config, tmpdir):
    """Helper function to create new cache config."""
    profile_config["caching_enabled"] = True
    return {
        "profiles": {"s3-local": profile_config},
        "cache": {
            "size": "10M",
            "cache_line_size": "1M",  # Set explicitly to avoid default 64M exceeding cache size
            "check_source_version": False,
            "eviction_policy": {"policy": "random", "refresh_interval": 300},
        },
    }


def create_mixed_cache_config(profile_config, tmpdir):
    """Helper function to create mixed cache config."""
    return {
        "profiles": {"s3-local": profile_config},
        "cache": {
            "size_mb": 10,
            "check_source_version": False,
            "eviction_policy": {"policy": "random", "refresh_interval": 300},
        },
    }


def create_incorrect_size_cache_config(profile_config, tmpdir):
    """Helper function to create incorrect size cache config."""
    return {"profiles": {"s3-local": profile_config}, "cache": {"size": "one-thousand-gigabytes"}}


@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "config_creator"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket, create_legacy_cache_config],
        [tempdatastore.TemporaryAWSS3Bucket, create_new_cache_config],
    ],
    ids=["legacy_config", "new_config"],
)
def test_storage_provider_cache_configs(config_creator, temp_data_store_type, tmpdir):
    """Test that both legacy and new cache config formats work correctly."""
    with temp_data_store_type() as temp_store:
        config_dict = config_creator(temp_store.profile_config_dict(), tmpdir)

        if config_creator == create_legacy_cache_config:
            with pytest.raises(RuntimeError, match="Failed to validate the config file"):
                StorageClientConfig.from_dict(config_dict)
        else:
            storage_config = StorageClientConfig.from_dict(config_dict, profile="s3-local")
            real_storage_provider = storage_config.storage_provider
            assert real_storage_provider is not None
            tmpdir_path = os.path.abspath(str(tmpdir))
            for obj in real_storage_provider.list_objects(path=tmpdir_path):
                real_storage_provider.delete_object(obj.key)
            cache_manager = storage_config.cache_manager
            verify_cache_operations(cache_manager)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "config_creator", "expected_error", "error_message"],
    argvalues=[
        [
            tempdatastore.TemporaryAWSS3Bucket,
            create_mixed_cache_config,
            ValueError,
            "The 'size_mb' property is no longer supported",
        ],
        [
            tempdatastore.TemporaryAWSS3Bucket,
            create_incorrect_size_cache_config,
            RuntimeError,
            "Failed to validate the config file",
        ],
    ],
    ids=["mixed_config", "incorrect_size"],
)
def test_storage_provider_invalid_cache_configs(
    config_creator, temp_data_store_type, expected_error, error_message, tmpdir
):
    """
    Test that invalid cache configurations raise appropriate errors.

    This test verifies that:
    1. Mixing old and new cache config formats raises a ValueError
    2. Using an incorrect size format raises a RuntimeError
    """
    with temp_data_store_type() as temp_store:
        config_dict = config_creator(temp_store.profile_config_dict(), tmpdir)
        with pytest.raises(expected_error, match=error_message):
            StorageClientConfig.from_dict(config_dict, profile="s3-local")


@pytest.fixture
def storage_provider_partial_cache_config(tmpdir):
    """
    New cache config format
    """

    # Create a config dictionary with profile and cache configuration
    def _config_builder(profile_config):
        return {
            "profiles": {"s3-local": profile_config},
            "cache": {"size": "100M", "eviction_policy": {"policy": "fifo"}},
        }

    return _config_builder


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_storage_provider_partial_cache_config(storage_provider_partial_cache_config, temp_data_store_type):
    with temp_data_store_type() as temp_store:
        config_dict = storage_provider_partial_cache_config(temp_store.profile_config_dict())
        storage_config = StorageClientConfig.from_dict(config_dict, profile="s3-local")
        cache_manager = storage_config.cache_manager

        # Access the CacheManager
        verify_cache_operations(cache_manager)

        cache_config = storage_config.cache_config
        assert cache_config is not None
        assert cache_config.size == "100M"
        assert cache_config.location is not None and isinstance(cache_config.location, str)
        assert cache_config.eviction_policy.policy == "fifo"
        assert cache_config.eviction_policy.refresh_interval == DEFAULT_CACHE_REFRESH_INTERVAL
        assert cache_config.check_source_version
        assert isinstance(cache_manager, CacheManager)


@pytest.fixture
def no_eviction_cache_config(tmpdir):
    cache_dir = os.path.join(str(tmpdir), "no_eviction_cache")
    return CacheConfig(
        size="3M",
        cache_line_size="64M",
        check_source_version=False,
        location=cache_dir,
        eviction_policy=EvictionPolicyConfig(policy="NO_EVICTION"),
    )


@pytest.fixture
def purge_factor_cache_config(tmpdir, request):
    """Parameterized cache config fixture for testing different purge_factor values."""
    purge_factor = request.param
    cache_dir = os.path.join(str(tmpdir), f"purge_{purge_factor}_cache")
    return CacheConfig(
        size="10M",
        cache_line_size="64M",
        check_source_version=False,
        location=cache_dir,
        eviction_policy=EvictionPolicyConfig(policy="LRU", purge_factor=purge_factor),
    )


def test_no_eviction_policy(profile_name, no_eviction_cache_config):
    """Test the NO_EVICTION eviction policy of the cache manager.

    This test verifies that when NO_EVICTION eviction policy is set:
    1. No files are evicted even when cache size limit is exceeded
    2. All files remain in cache regardless of size
    3. Cache refresh does not trigger eviction
    4. No eviction thread is created
    5. No lock file is created

    :param profile_name: The name of the cache profile to use
    :param no_eviction_cache_config: Cache configuration with NO_EVICTION eviction policy
    """
    # Clean the entire cache directory
    cache_dir = no_eviction_cache_config.location
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
    os.makedirs(cache_dir)

    # Create the CacheManager with the provided no_eviction_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=no_eviction_cache_config)

    # Verify no eviction thread is created
    assert not hasattr(cache_manager, "_eviction_thread"), "No eviction thread should be created for NONE policy"
    assert not hasattr(cache_manager, "_eviction_thread_running"), (
        "No eviction thread running flag should exist for NO_EVICTION policy"
    )

    test_uuid = str(uuid.uuid4())
    # Add first 3 files to the cache (each file is 1 MB)
    cache_manager.set(f"{test_uuid}/file1", b"a" * 1 * 1024 * 1024)  # 1 MB
    cache_manager.set(f"{test_uuid}/file2", b"b" * 1 * 1024 * 1024)  # 1 MB
    cache_manager.set(f"{test_uuid}/file3", b"c" * 1 * 1024 * 1024)  # 1 MB

    # Verify first 3 files are in cache
    for i in range(1, 4):
        assert cache_manager.contains(f"{test_uuid}/file{i}"), f"File {i} should be in cache"

    # Add 2 more files to exceed cache size limit
    cache_manager.set(f"{test_uuid}/file4", b"d" * 1 * 1024 * 1024)  # 1 MB
    cache_manager.set(f"{test_uuid}/file5", b"e" * 1 * 1024 * 1024)  # 1 MB

    # Verify no lock file is created
    lock_file_path = os.path.join(cache_dir, ".cache_refresh.lock")
    assert not os.path.exists(lock_file_path), "No lock file should be created for NONE policy"

    # Force refresh to trigger eviction by setting last refresh time to the past
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()

    # Verify all 5 files are still in cache after refresh
    for i in range(1, 6):
        assert cache_manager.contains(f"{test_uuid}/file{i}"), f"File {i} should not be evicted"

    # Verify total cache size exceeds the limit
    total_size = 0
    for i in range(1, 6):
        data = cache_manager.read(f"{test_uuid}/file{i}")
        if data is not None:
            total_size += len(data)
    assert total_size > 3 * 1024 * 1024, "Total cache size should exceed 3MB limit with NONE policy"

    # Verify no eviction thread was created during the test
    assert not hasattr(cache_manager, "_eviction_thread"), "No eviction thread should be created during test"
    assert not hasattr(cache_manager, "_eviction_thread_running"), (
        "No eviction thread running flag should exist during test"
    )


def test_purge_factor_default(profile_name, lru_cache_config):
    """Test that purge_factor defaults to 0 (minimal cleanup).

    With purge_factor=0, cache should delete files until just under max size (current behavior).
    """
    cache_manager = CacheManager(profile=profile_name, cache_config=lru_cache_config)

    test_uuid = str(uuid.uuid4())
    # Add 4 files (3MB each = 12MB total, exceeds 10MB limit)
    for i in range(1, 5):
        cache_manager.set(f"{test_uuid}/file{i}", b"x" * 3 * 1024 * 1024)
        time.sleep(0.1)

    # Force eviction
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()

    # With purge_factor=0, cache should be just under 10MB
    final_size = cache_manager.cache_size()
    assert final_size <= 10 * 1024 * 1024, "Cache should be under max size"
    assert final_size >= 9 * 1024 * 1024, "Cache should be close to max (minimal cleanup)"


@pytest.mark.parametrize(
    "purge_factor_cache_config,expected_target_pct,expected_max_files",
    [
        (20, 0.80, 3),  # 20% purge → 80% kept (8MB) → max 2-3 files
        (50, 0.50, 2),  # 50% purge → 50% kept (5MB) → max 1-2 files
        (100, 0.00, 0),  # 100% purge → 0% kept (0MB) → 0 files
    ],
    indirect=["purge_factor_cache_config"],
    ids=["20_percent", "50_percent", "100_percent"],
)
def test_purge_factor_values(profile_name, purge_factor_cache_config, expected_target_pct, expected_max_files):
    """Test various purge_factor values (20%, 50%, 100%).

    Verifies that cache is evicted down to the correct target size based on purge_factor.
    """
    cache_manager = CacheManager(profile=profile_name, cache_config=purge_factor_cache_config)
    purge_factor = purge_factor_cache_config.eviction_policy.purge_factor

    test_uuid = str(uuid.uuid4())
    # Add 4 files (3MB each = 12MB total, exceeds 10MB limit)
    for i in range(1, 5):
        cache_manager.set(f"{test_uuid}/file{i}", b"x" * 3 * 1024 * 1024)
        time.sleep(0.1)

    # Force eviction
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()

    # Calculate expected target size
    max_cache_size = 10 * 1024 * 1024  # 10MB
    expected_target_size = max_cache_size * expected_target_pct

    # Verify cache size is at or below target
    final_size = cache_manager.cache_size()
    assert final_size <= expected_target_size, (
        f"Cache should be under target size {expected_target_size} with purge_factor={purge_factor}, got {final_size}"
    )

    # Verify file count matches expectation
    remaining_files = sum(1 for i in range(1, 5) if cache_manager.contains(f"{test_uuid}/file{i}"))
    assert remaining_files <= expected_max_files, (
        f"Should have at most {expected_max_files} files remaining with purge_factor={purge_factor}, got {remaining_files}"
    )


def test_concurrent_chunk_creation_with_locking():
    """Test that per-chunk locking prevents race conditions when multiple threads create the same chunk.

    This test verifies:
    1. Two threads can simultaneously request the same byte range
    2. Only one thread successfully creates the chunk file
    3. The other thread either waits for the lock or uses the existing chunk
    4. No file corruption or duplicate chunks occur
    """

    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        with tempfile.TemporaryDirectory() as cache_location:
            # Create configuration with partial file caching enabled
            config = {
                "profiles": {
                    "origin": origin_store.profile_config_dict() | {"caching_enabled": True},
                },
                "cache": {
                    "size": "10M",
                    "location": cache_location,
                    "cache_line_size": "1M",  # 1MB cache lines for testing
                    "check_source_version": True,
                    "eviction_policy": {
                        "policy": "lru",
                        "refresh_interval": 300,
                    },
                },
            }

            client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

            # Create a test file
            file_path = f"test-data-{uuid.uuid4()}/concurrent_test.bin"
            test_content = create_test_data(5)  # 5MB file
            client.write(file_path, test_content)

            # Create two separate clients to test concurrent access
            client1 = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))
            client2 = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

            # Test data
            byte_range = Range(offset=0, size=16 * 1024)  # 16KB starting at beginning

            # Shared variables to track thread execution
            thread_results = []
            thread_errors = []

            def read_range_thread(client_id, client):
                """Thread function that reads a byte range using the client."""
                try:
                    # Read the byte range - this may trigger chunk creation
                    result = client.read(file_path, byte_range=byte_range)
                    thread_results.append((client_id, len(result), "success"))

                except Exception as e:
                    thread_errors.append((client_id, str(e)))

            # Create and start two threads simultaneously, each with its own client
            thread1 = threading.Thread(target=read_range_thread, args=(1, client1))
            thread2 = threading.Thread(target=read_range_thread, args=(2, client2))

            # Start both threads at nearly the same time
            thread1.start()
            thread2.start()

            # Wait for both threads to complete
            thread1.join()
            thread2.join()

            # Verify both threads succeeded
            assert len(thread_errors) == 0, f"Threads encountered errors: {thread_errors}"
            assert len(thread_results) == 2, f"Expected 2 thread results, got {len(thread_results)}"

            # Verify both threads got the same result
            result1 = thread_results[0]
            result2 = thread_results[1]
            assert result1[1] == result2[1], f"Thread results differ: {result1} vs {result2}"
            assert result1[1] == byte_range.size, f"Expected {byte_range.size} bytes, got {result1[1]}"

            # Verify chunk files were created and are valid
            # Check that chunk 0 exists (since we read from offset 0)
            cache_manager = client1._cache_manager
            assert cache_manager is not None
            chunk0_path = cache_manager._get_chunk_path(cache_manager._get_cache_file_path(file_path), 0)
            assert os.path.exists(chunk0_path), "Chunk 0 should exist after range read"

            # Check that the chunk file is valid (not corrupted)
            with open(chunk0_path, "rb") as f:
                chunk_data = f.read()
            assert len(chunk_data) > 0, "Chunk file should contain data"

            etag = xattr.getxattr(chunk0_path, "user.etag").decode("utf-8")
            assert etag, "Chunk should have etag metadata"
            chunk_size = int(xattr.getxattr(chunk0_path, "user.cache_line_size").decode("utf-8"))
            assert chunk_size == 1024 * 1024, f"Expected chunk size 1MB, got {chunk_size}"

            # Verify only one chunk0 exists (no duplicates from race conditions)
            chunk_files = [f for f in os.listdir(os.path.dirname(chunk0_path)) if f == os.path.basename(chunk0_path)]
            assert len(chunk_files) == 1, f"Expected 1 chunk0 file, found {len(chunk_files)}"


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_cache_first_no_head_request_on_hit(temp_data_store_type, tmpdir):
    """
    Test that cache-first approach avoids HEAD requests on cache hit.

    When check_source_version=False and the file is in cache, open() should
    retrieve the file from cache without making any HEAD requests to the remote
    storage. This optimization is critical for data loaders that frequently
    access cached files.
    """
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        file_path = "test_file.txt"
        file_content = b"test data for cache-first optimization"

        profile_config = temp_data_store.profile_config_dict()
        profile_config["caching_enabled"] = True

        config_dict = {
            "profiles": {profile: profile_config},
            "cache": {
                "size": "10M",
                "cache_line_size": "1M",
                "location": str(tmpdir),
                "check_source_version": False,
            },
        }

        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict, profile=profile))

        head_call_count = 0
        original_get_metadata = storage_client._storage_provider.get_object_metadata  # type: ignore

        def counting_get_object_metadata(path: str, strict: bool = True):
            nonlocal head_call_count
            head_call_count += 1
            return original_get_metadata(path, strict=strict)

        storage_client._storage_provider.get_object_metadata = counting_get_object_metadata  # type: ignore

        # Upload a file
        with storage_client.open(path=file_path, mode="wb") as f:
            f.write(file_content)

        # First read - populates cache (will do HEAD request for metadata)
        with storage_client.open(path=file_path, mode="rb") as f:
            content = f.read()
            assert content == file_content

        assert head_call_count == 1, "First read should trigger exactly one HEAD request"

        # Verify file is in cache
        assert storage_client._cache_manager is not None
        assert storage_client._cache_manager.contains(file_path, check_source_version=SourceVersionCheckMode.DISABLE)

        # Second read from cache - with check_source_version=False, this should
        # use cache-first approach and avoid HEAD request
        with storage_client.open(path=file_path, mode="rb", check_source_version=SourceVersionCheckMode.DISABLE) as f:
            content = f.read()
            assert content == file_content

        # Also test read() method for both full and range reads
        content = storage_client.read(file_path, check_source_version=SourceVersionCheckMode.DISABLE)
        assert content == file_content

        # Test range read
        range_content = storage_client.read(
            file_path, byte_range=Range(0, 10), check_source_version=SourceVersionCheckMode.DISABLE
        )
        assert range_content == file_content[:10]

        assert head_call_count == 1, (
            f"No additional HEAD requests should be made for cache-first reads; expected 1 total, got {head_call_count}"
        )
