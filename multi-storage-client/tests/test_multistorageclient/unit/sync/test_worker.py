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

import json
import os
import sys
import tempfile
from datetime import datetime
from typing import Optional
from unittest import mock

import pytest
import xattr

import multistorageclient as msc
from multistorageclient.constants import MEMORY_LOAD_LIMIT
from multistorageclient.sync.worker import (
    FILE_LOCK_SIZE_THRESHOLD,
    FileLock,
    _check_posix_paths,
    _copy_posix_to_posix,
    _copy_posix_to_remote,
    _copy_remote_to_posix,
    _copy_remote_to_remote,
    _create_exclusive_filelock,
    _replace_object_metadata_attributes,
    _update_posix_metadata,
)
from multistorageclient.types import ObjectMetadata, SyncError
from test_multistorageclient.unit.utils import config, tempdatastore


def _setup_test_clients(posix_profile: str, remote_profile: str, temp_posix, temp_remote):
    """Helper to set up test clients with profiles."""
    config.setup_msc_config(
        config_dict={
            "profiles": {
                posix_profile: temp_posix.profile_config_dict(),
                remote_profile: temp_remote.profile_config_dict(),
            }
        }
    )
    posix_client, _ = msc.resolve_storage_client(f"msc://{posix_profile}")
    remote_client, _ = msc.resolve_storage_client(f"msc://{remote_profile}")
    return posix_client, remote_client


class MockStorageClient:
    def list(self, **kwargs):
        raise Exception("No Such Method")

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        pass

    def _is_rust_client_enabled(self) -> bool:
        return False

    def _is_posix_file_storage_provider(self) -> bool:
        return False


def test_replace_object_metadata_attributes():
    """Test metadata replace helper preserves fields and replaces metadata payload."""
    target_metadata = ObjectMetadata(
        key="target/path/file.txt",
        content_length=4,
        last_modified=datetime(2025, 1, 1, 10, 0, 0),
        etag="etag-1",
        metadata={"existing": "value", "version": "1"},
    )

    replaced = _replace_object_metadata_attributes(target_metadata, {"version": "2", "owner": "ml-team"})

    assert replaced.key == target_metadata.key
    assert replaced.content_length == target_metadata.content_length
    assert replaced.last_modified == target_metadata.last_modified
    assert replaced.etag == target_metadata.etag
    assert replaced.metadata == {"version": "2", "owner": "ml-team"}


def test_sync_with_worker_error_fail_fast():
    """Test sync operation with worker error - sync should fail fast."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    source_profile = "source"
    target_profile = "target"

    with (
        tempdatastore.TemporaryPOSIXDirectory() as source_store,
        tempdatastore.TemporaryPOSIXDirectory() as target_store,
    ):
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    source_profile: source_store.profile_config_dict(),
                    target_profile: target_store.profile_config_dict(),
                }
            }
        )

        source_url = f"msc://{source_profile}"
        target_url = f"msc://{target_profile}"

        # Create source files
        msc.write(f"{source_url}/file1.txt", b"content1")
        msc.write(f"{source_url}/file2.txt", b"content2")

        source_client, source_path = msc.resolve_storage_client(source_url)
        target_client, target_path = msc.resolve_storage_client(target_url)

        # Mock shutil.copy2 to raise error on first file (POSIX to POSIX uses shutil.copy2)
        sync_module = sys.modules["multistorageclient.sync"]
        original_copy2 = sync_module.worker.shutil.copy2
        call_count = [0]

        def mock_copy2(src: str, dst: str, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise PermissionError("Simulated permission error")
            return original_copy2(src, dst, **kwargs)

        with mock.patch.object(sync_module.worker.shutil, "copy2", side_effect=mock_copy2):
            # Sync should raise SyncError containing worker errors
            with pytest.raises(SyncError, match="Errors in sync operation"):
                target_client.sync_from(source_client, source_path, target_path)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_check_posix_paths(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test _check_posix_paths correctly identifies POSIX and non-POSIX clients."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_posix,
        temp_data_store_type() as temp_remote,
    ):
        posix_client, remote_client = _setup_test_clients("posix-test", "remote-test", temp_posix, temp_remote)

        # Test POSIX client returns physical path
        posix_file = "test.txt"
        source_physical, target_physical = _check_posix_paths(posix_client, posix_client, posix_file, posix_file)
        assert source_physical is not None
        assert target_physical is not None
        assert posix_file in source_physical
        assert posix_file in target_physical

        # Test remote client returns None
        source_physical, target_physical = _check_posix_paths(remote_client, remote_client, "file.txt", "file.txt")
        assert source_physical is None
        assert target_physical is None

        # Test mixed: POSIX source, remote target
        source_physical, target_physical = _check_posix_paths(posix_client, remote_client, posix_file, "file.txt")
        assert source_physical is not None
        assert target_physical is None


def test_copy_posix_to_posix():
    """Test _copy_posix_to_posix correctly copies files between POSIX locations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        source_file = os.path.join(tmpdir, "source.txt")
        target_file = os.path.join(tmpdir, "subdir", "target.txt")

        # Create source file
        content = "test content"
        with open(source_file, "w") as f:
            f.write(content)

        # Copy file
        _copy_posix_to_posix(source_file, target_file)

        # Verify target file exists and has correct content
        assert os.path.exists(target_file)
        with open(target_file, "r") as f:
            assert f.read() == content


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_copy_posix_to_remote(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test _copy_posix_to_remote correctly uploads files from POSIX to remote storage."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_posix,
        temp_data_store_type() as temp_remote,
    ):
        posix_client, remote_client = _setup_test_clients("posix-test", "remote-test", temp_posix, temp_remote)

        # Create source file in POSIX
        source_file = "source.txt"
        content = b"test content for upload"
        posix_client.write(source_file, content)

        source_physical_path = posix_client.get_posix_path(source_file)
        assert source_physical_path is not None

        # Create metadata
        file_metadata = ObjectMetadata(
            key=source_file,
            content_length=len(content),
            last_modified=datetime.now(),
            metadata={"custom": "value"},
        )

        # Upload to remote storage
        target_file = "target.txt"
        _copy_posix_to_remote(remote_client, source_physical_path, target_file, file_metadata)

        # Verify file exists in remote storage with correct content
        assert remote_client.is_file(target_file)
        retrieved_content = remote_client.read(target_file)
        assert retrieved_content == content


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_copy_remote_to_posix(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test _copy_remote_to_posix correctly downloads files from remote to POSIX storage."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_posix,
        temp_data_store_type() as temp_remote,
    ):
        posix_client, remote_client = _setup_test_clients("posix-test", "remote-test", temp_posix, temp_remote)

        # Create source file in remote storage
        source_file = "source.txt"
        content = b"test content for download"
        remote_client.write(source_file, content)

        # Download to POSIX
        target_file = "target.txt"
        target_physical_path = posix_client.get_posix_path(target_file)
        assert target_physical_path is not None

        _copy_remote_to_posix(remote_client, source_file, target_physical_path)

        # Verify file exists in POSIX with correct content
        assert os.path.exists(target_physical_path)
        with open(target_physical_path, "rb") as f:
            assert f.read() == content


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_single_file_to_directory(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test syncing a single file (source path = object key) to a directory writes to <dir>/<basename>."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_posix,
        temp_data_store_type() as temp_remote,
    ):
        posix_client, remote_client = _setup_test_clients("posix-test", "remote-test", temp_posix, temp_remote)

        single_file_key = "test/dir/file.txt"
        content = b"test content"
        remote_client.write(single_file_key, content)

        source_url = f"msc://remote-test/{single_file_key}"
        target_url = "msc://posix-test/"
        msc.sync(source_url, target_url)

        assert posix_client.is_file("file.txt")
        assert posix_client.read("file.txt") == content


@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "file_size", "file_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket, 100, "small"],  # Small file uses memory
        [tempdatastore.TemporaryAWSS3Bucket, MEMORY_LOAD_LIMIT + 1000, "large"],  # Large file uses temp file
    ],
)
def test_copy_remote_to_remote(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore], file_size: int, file_type: str
):
    """Test _copy_remote_to_remote handles both small and large files correctly."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        temp_data_store_type() as temp_source,
        temp_data_store_type() as temp_target,
    ):
        source_client, target_client = _setup_test_clients("source-test", "target-test", temp_source, temp_target)

        # Create source file
        source_file = f"{file_type}.txt"
        content = b"x" * file_size
        source_client.write(source_file, content)

        file_metadata = ObjectMetadata(
            key=source_file,
            content_length=len(content),
            last_modified=datetime.now(),
            metadata={"type": file_type},
        )

        # Copy to target
        target_file = "target.txt"
        _copy_remote_to_remote(source_client, target_client, source_file, target_file, file_metadata)

        # Verify file exists with correct content
        assert target_client.is_file(target_file)
        assert target_client.read(target_file) == content


@pytest.mark.parametrize(
    argnames=["use_metadata_provider"],
    argvalues=[[True], [False]],
)
def test_update_posix_metadata(use_metadata_provider: bool):
    """Test _update_posix_metadata updates metadata via provider or xattr."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with tempdatastore.TemporaryPOSIXDirectory() as temp_posix:
        profile_config = temp_posix.profile_config_dict()
        if use_metadata_provider:
            profile_config.update({"manifest_base_dir": ".msc_metadata", "use_manifest_metadata": True})

        config.setup_msc_config(config_dict={"profiles": {"posix-test": profile_config}})
        client, _ = msc.resolve_storage_client("msc://posix-test")

        # Create test file and metadata
        test_file = "test.txt"
        content = b"test content"
        client.write(test_file, content)

        target_physical_path = client.get_posix_path(test_file)
        assert target_physical_path is not None

        custom_metadata = {"custom_key": "custom_value", "author": "test"}
        # Use a specific timestamp to verify mtime is set correctly
        expected_mtime = datetime(2025, 6, 15, 10, 30, 45)
        file_metadata = ObjectMetadata(
            key=test_file,
            content_length=len(content),
            last_modified=expected_mtime,
            metadata=custom_metadata,
        )

        # Update metadata
        _update_posix_metadata(client, target_physical_path, test_file, file_metadata)

        # Verify metadata storage
        if use_metadata_provider:
            info = client.info(test_file)
            assert info.metadata == custom_metadata
        else:
            # Verify xattr is set with custom metadata
            try:
                xattr_value = xattr.getxattr(target_physical_path, "user.json")
                stored_metadata = json.loads(xattr_value.decode("utf-8"))
                assert stored_metadata == custom_metadata
            except OSError:
                pytest.skip("xattr not supported on this filesystem")

            # Verify mtime was updated to match file_metadata.last_modified
            actual_mtime = os.path.getmtime(target_physical_path)
            expected_mtime_timestamp = expected_mtime.timestamp()
            assert abs(actual_mtime - expected_mtime_timestamp) < 0.001, (
                f"mtime not updated correctly: {actual_mtime} != {expected_mtime_timestamp}"
            )


def test_create_exclusive_filelock():
    """Test size-based selective file locking behavior.

    Validates that _create_exclusive_filelock:
    - Creates locks for large files (>= threshold) on POSIX storage
    - Skips locks for small files (< threshold) on POSIX storage
    - Never creates locks on non-POSIX storage regardless of size
    """
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        tempdatastore.TemporaryPOSIXDirectory() as posix_store,
        tempdatastore.TemporaryAWSS3Bucket() as s3_store,
    ):
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "posix_target": posix_store.profile_config_dict(),
                    "s3_target": s3_store.profile_config_dict(),
                }
            }
        )

        posix_client, _ = msc.resolve_storage_client("msc://posix_target")
        s3_client, _ = msc.resolve_storage_client("msc://s3_target")
        target_file_path = "test_file.txt"

        # Test 1: Small file on POSIX - should NOT create lock
        small_file_size = FILE_LOCK_SIZE_THRESHOLD - 1
        lock_small = _create_exclusive_filelock(posix_client, target_file_path, small_file_size)
        assert not isinstance(lock_small, FileLock), "Small files on POSIX should not create locks"

        # Test 2: Large file on POSIX - should create lock
        large_file_size = FILE_LOCK_SIZE_THRESHOLD * 2
        lock_large = _create_exclusive_filelock(posix_client, target_file_path, large_file_size)
        assert isinstance(lock_large, FileLock), "Large files on POSIX should create locks"

        # Test 3: Large file on non-POSIX (S3) - should NOT create lock
        lock_s3 = _create_exclusive_filelock(s3_client, target_file_path, large_file_size)
        assert not isinstance(lock_s3, FileLock), "Non-POSIX storage should never create locks"
