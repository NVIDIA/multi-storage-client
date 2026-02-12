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

import concurrent.futures
import os
import sys
import tempfile
import threading
import time
from typing import cast
from unittest import mock

import pytest

import multistorageclient as msc
from multistorageclient.client import StorageClient
from multistorageclient.config import StorageClientConfig
from multistorageclient.constants import MEMORY_LOAD_LIMIT
from multistorageclient.providers.base import BaseStorageProvider
from multistorageclient.providers.manifest_metadata import DEFAULT_MANIFEST_BASE_DIR
from multistorageclient.types import ExecutionMode, ObjectMetadata, PatternType, SyncError
from test_multistorageclient.unit.utils import config, tempdatastore


def get_file_timestamp(uri: str) -> float:
    client, path = msc.resolve_storage_client(uri)
    response = client.info(path=path)
    return response.last_modified.timestamp()


def create_local_test_dataset(target_profile: str, expected_files: dict) -> None:
    """Creates test files based on expected_files dictionary."""
    target_client, target_path = msc.resolve_storage_client(target_profile)
    for rel_path, content in expected_files.items():
        path = os.path.join(target_path, rel_path)
        target_client.write(path, content.encode("utf-8"))


def verify_sync_and_contents(target_url: str, expected_files: dict):
    """Verifies that all expected files exist in the target storage and their contents are correct."""
    for file, expected_content in expected_files.items():
        target_file_url = os.path.join(target_url, file)
        assert msc.is_file(target_file_url), f"Missing file: {target_file_url}"
        actual_content = msc.open(target_file_url).read().decode("utf-8")
        assert actual_content == expected_content, f"Mismatch in file {file}"
    # Ensure there is nothing in target that is not in expected_files
    target_client, target_path = msc.resolve_storage_client(target_url)
    for targetf in target_client.list(prefix=target_path):
        key = targetf.key[len(target_path) :].lstrip("/")
        # Skip temporary files that start with a dot (like .plexihcg)
        if key.startswith(".") or os.path.basename(key).startswith("."):
            continue
        assert key in expected_files


@pytest.mark.serial
@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "sync_kwargs"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket, {}],  # Default settings
        [tempdatastore.TemporaryAWSS3Bucket, {"max_workers": 1}],  # Serial execution
        [tempdatastore.TemporaryAWSS3Bucket, {"max_workers": 2}],  # Parallel with 2 workers
    ],
)
def test_sync_function(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
    sync_kwargs: dict,
):
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    # set environment variables to control multiprocessing
    os.environ["MSC_NUM_PROCESSES"] = str(sync_kwargs.get("max_workers", 1))

    obj_profile = "s3-sync"
    local_profile = "local"
    second_profile = "second"
    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        tempdatastore.TemporaryPOSIXDirectory() as second_local_data_store,
        temp_data_store_type() as temp_data_store,
    ):
        with_manifest_profile_config_dict = second_local_data_store.profile_config_dict() | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                    "writable": True,
                    # allow_overwrites defaults to False
                },
            }
        }

        config.setup_msc_config(
            config_dict={
                "profiles": {
                    obj_profile: temp_data_store.profile_config_dict(),
                    local_profile: temp_source_data_store.profile_config_dict(),
                    second_profile: with_manifest_profile_config_dict,
                }
            }
        )

        target_msc_url = f"msc://{obj_profile}/synced-files"
        source_msc_url = f"msc://{local_profile}"
        second_msc_url = f"msc://{second_profile}/some"

        # Create local dataset
        expected_files = {
            "dir1/file0.txt": "a" * 100,
            "dir1/file1.txt": "b" * 100,
            "dir1/file2.txt": "c" * 100,
            "dir2/file0.txt": "d" * 100,
            "dir2/file1.txt": "e" * 100,
            "dir2/file2.txt": "f" * (MEMORY_LOAD_LIMIT + 1024),  # One large file
            "dir3/file0.txt": "g" * 100,
            "dir3/file1.txt": "h" * 100,
            "dir3/file2.txt": "i" * 100,
        }
        create_local_test_dataset(source_msc_url, expected_files)

        print(f"First sync from {source_msc_url} to {target_msc_url}")
        result = msc.sync(source_url=source_msc_url, target_url=target_msc_url)

        # Verify SyncResult for first sync - should copy all 9 files
        assert result.total_work_units == 9, f"Expected 9 work units, got {result.total_work_units}"
        assert result.total_files_added == 9, f"Expected 9 files added, got {result.total_files_added}"
        assert result.total_files_deleted == 0, f"Expected 0 files deleted, got {result.total_files_deleted}"
        expected_bytes = sum(len(content.encode("utf-8")) for content in expected_files.values())
        assert result.total_bytes_added == expected_bytes, (
            f"Expected {expected_bytes} bytes, got {result.total_bytes_added}"
        )
        assert result.total_bytes_deleted == 0, f"Expected 0 bytes deleted, got {result.total_bytes_deleted}"
        assert result.total_time_seconds > 0, "Expected positive total_time_seconds"

        # Verify contents on target match expectation.
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        print("Deleting a single file at target and syncing again")
        msc.delete(os.path.join(target_msc_url, "dir1/file0.txt"))
        result = msc.sync(source_url=source_msc_url, target_url=target_msc_url)

        # Verify SyncResult - should re-add the deleted file
        assert result.total_work_units == 9
        assert result.total_files_added == 1, f"Expected 1 file added, got {result.total_files_added}"
        assert result.total_files_deleted == 0
        deleted_file_size = len("a" * 100)
        assert result.total_bytes_added == deleted_file_size, (
            f"Expected {deleted_file_size} bytes, got {result.total_bytes_added}"
        )
        assert result.total_time_seconds > 0

        # Verify contents on target match expectation.
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        print("Syncing again and verifying timestamps")
        timestamps_before = {file: get_file_timestamp(os.path.join(target_msc_url, file)) for file in expected_files}
        result = msc.sync(source_url=source_msc_url, target_url=target_msc_url)
        timestamps_after = {file: get_file_timestamp(os.path.join(target_msc_url, file)) for file in expected_files}
        assert timestamps_before == timestamps_after, "Timestamps changed on second sync."

        # Verify SyncResult - no changes, so nothing should be copied
        assert result.total_work_units == 9
        assert result.total_files_added == 0, f"Expected 0 files added, got {result.total_files_added}"
        assert result.total_files_deleted == 0
        assert result.total_bytes_added == 0, f"Expected 0 bytes added, got {result.total_bytes_added}"
        assert result.total_time_seconds > 0

        print("Adding new files and syncing again")
        new_files = {"dir1/new_file.txt": "n" * 100}
        create_local_test_dataset(source_msc_url, expected_files=new_files)
        result = msc.sync(source_url=source_msc_url, target_url=target_msc_url)
        expected_files.update(new_files)

        # Verify SyncResult - should add 1 new file
        assert result.total_work_units == 10, f"Expected 10 work units, got {result.total_work_units}"
        assert result.total_files_added == 1, f"Expected 1 file added, got {result.total_files_added}"
        assert result.total_files_deleted == 0
        new_file_size = len("n" * 100)
        assert result.total_bytes_added == new_file_size, (
            f"Expected {new_file_size} bytes, got {result.total_bytes_added}"
        )
        assert result.total_time_seconds > 0

        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        print("Modifying one of the source files, but keeping size the same, and verifying it's copied.")
        modified_files = {"dir1/file0.txt": "z" * 100}
        create_local_test_dataset(source_msc_url, expected_files=modified_files)
        expected_files.update(modified_files)
        result = msc.sync(source_url=source_msc_url, target_url=target_msc_url)

        # Verify SyncResult - should update 1 modified file
        assert result.total_work_units == 10
        assert result.total_files_added == 1, f"Expected 1 file added (updated), got {result.total_files_added}"
        assert result.total_files_deleted == 0
        modified_file_size = len("z" * 100)
        assert result.total_bytes_added == modified_file_size, (
            f"Expected {modified_file_size} bytes, got {result.total_bytes_added}"
        )
        assert result.total_time_seconds > 0

        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        with pytest.raises(ValueError):
            msc.sync(source_url=source_msc_url, target_url=source_msc_url)
        with pytest.raises(ValueError):
            msc.sync(source_url=target_msc_url, target_url=target_msc_url)
        with pytest.raises(ValueError):
            msc.sync(source_url=source_msc_url, target_url=os.path.join(source_msc_url, "extra"))

        print("Syncing from object to a second posix file location using ManifestProvider.")
        result = msc.sync(source_url=target_msc_url, target_url=second_msc_url)

        # Verify SyncResult - should copy all 10 files to new location
        assert result.total_work_units == 10
        assert result.total_files_added == 10, f"Expected 10 files added, got {result.total_files_added}"
        assert result.total_files_deleted == 0
        expected_bytes = sum(len(content.encode("utf-8")) for content in expected_files.values())
        assert result.total_bytes_added == expected_bytes, (
            f"Expected {expected_bytes} bytes, got {result.total_bytes_added}"
        )
        assert result.total_time_seconds > 0

        verify_sync_and_contents(target_url=second_msc_url, expected_files=expected_files)

        print("Deleting all the files at the target and going again.")
        for key in expected_files.keys():
            msc.delete(os.path.join(target_msc_url, key))

        print("Syncing using prefixes to just copy one subfolder.")
        result = msc.sync(
            source_url=os.path.join(source_msc_url, "dir2"), target_url=os.path.join(target_msc_url, "dir2")
        )
        sub_expected_files = {k: v for k, v in expected_files.items() if k.startswith("dir2")}

        # Verify SyncResult - should copy 3 files from dir2
        assert result.total_work_units == 3, f"Expected 3 work units, got {result.total_work_units}"
        assert result.total_files_added == 3, f"Expected 3 files added, got {result.total_files_added}"
        assert result.total_files_deleted == 0
        dir2_bytes = sum(len(v.encode("utf-8")) for k, v in expected_files.items() if k.startswith("dir2"))
        assert result.total_bytes_added == dir2_bytes, f"Expected {dir2_bytes} bytes, got {result.total_bytes_added}"
        assert result.total_time_seconds > 0

        verify_sync_and_contents(target_url=target_msc_url, expected_files=sub_expected_files)

        result = msc.sync(source_url=source_msc_url, target_url=target_msc_url)

        # Verify SyncResult - should add the remaining 7 files (10 total - 3 from dir2)
        assert result.total_work_units == 10
        assert result.total_files_added == 7, f"Expected 7 files added, got {result.total_files_added}"
        assert result.total_files_deleted == 0
        remaining_bytes = sum(len(v.encode("utf-8")) for k, v in expected_files.items() if not k.startswith("dir2"))
        assert result.total_bytes_added == remaining_bytes, (
            f"Expected {remaining_bytes} bytes, got {result.total_bytes_added}"
        )
        assert result.total_time_seconds > 0

        print("Deleting files at the source and syncing again, verify deletes at target.")
        keys_to_delete = [k for k in expected_files.keys() if k.startswith("dir2")]
        # Save the bytes before deleting
        deleted_files_data = {k: expected_files[k] for k in keys_to_delete}
        # Delete keys at the source.
        for key in keys_to_delete:
            expected_files.pop(key)
            msc.delete(os.path.join(source_msc_url, key))

        # Sync from source to target and expect deletes to happen at the target.
        result = msc.sync(source_url=source_msc_url, target_url=target_msc_url, delete_unmatched_files=True)

        # Verify SyncResult - should delete 3 files from dir2
        # total_work_units includes files from both source and target when checking for deletions
        assert result.total_work_units == 10, f"Expected 10 work units, got {result.total_work_units}"
        assert result.total_files_added == 0, f"Expected 0 files added, got {result.total_files_added}"
        assert result.total_files_deleted == 3, f"Expected 3 files deleted, got {result.total_files_deleted}"
        assert result.total_bytes_added == 0
        deleted_bytes = sum(len(v.encode("utf-8")) for v in deleted_files_data.values())
        assert result.total_bytes_deleted == deleted_bytes, (
            f"Expected {deleted_bytes} bytes deleted, got {result.total_bytes_deleted}"
        )
        assert result.total_time_seconds > 0

        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        # Delete all remaining keys at source and verify the deletes propagate to target.
        remaining_files_count = len(expected_files)
        remaining_bytes = sum(len(v.encode("utf-8")) for v in expected_files.values())
        for key in expected_files.keys():
            msc.delete(os.path.join(source_msc_url, key))
        result = msc.sync(source_url=source_msc_url, target_url=target_msc_url, delete_unmatched_files=True)

        # Verify SyncResult - should delete all remaining 7 files
        # When source is empty, we still iterate over target files to check for deletions
        assert result.total_work_units == remaining_files_count, (
            f"Expected {remaining_files_count} work units, got {result.total_work_units}"
        )
        assert result.total_files_added == 0, f"Expected 0 files added, got {result.total_files_added}"
        assert result.total_files_deleted == remaining_files_count, (
            f"Expected {remaining_files_count} files deleted, got {result.total_files_deleted}"
        )
        assert result.total_bytes_added == 0
        assert result.total_bytes_deleted == remaining_bytes, (
            f"Expected {remaining_bytes} bytes deleted, got {result.total_bytes_deleted}"
        )
        assert result.total_time_seconds > 0

        verify_sync_and_contents(target_url=target_msc_url, expected_files={})


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_from(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    obj_profile = "s3-sync"
    local_profile = "local"
    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        temp_data_store_type() as temp_data_store,
    ):
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    obj_profile: temp_data_store.profile_config_dict(),
                    local_profile: temp_source_data_store.profile_config_dict(),
                }
            }
        )

        source_msc_url = f"msc://{local_profile}/folder"
        target_msc_url = f"msc://{obj_profile}/synced-files"

        # Create local dataset with both regular and hidden files
        source_files = {
            "dir1/file0.txt": "a" * 150,
            "dir1/file1.txt": "b" * 200,
            "dir1/file2.txt": "c" * 1000,
            "dir2/file0.txt": "d" * 1,
            "dir2/file1.txt": "e" * 5,
            "dir2/file2.txt": "f" * (MEMORY_LOAD_LIMIT + 1024),  # One large file
            "dir3/file0.txt": "g" * 10000,
            "dir3/file1.txt": "h" * 800,
            "dir3/file2.txt": "i" * 512,
            # Hidden files that should NOT be synced
            ".hidden_root.txt": "hidden_root",
            "dir1/.hidden_in_dir.txt": "hidden_in_dir1",
            "dir2/.dotfile": "dotfile_content",
            ".git/config": "git_config",
        }
        # Expected files after sync (without hidden files)
        expected_files = {
            "dir1/file0.txt": "a" * 150,
            "dir1/file1.txt": "b" * 200,
            "dir1/file2.txt": "c" * 1000,
            "dir2/file0.txt": "d" * 1,
            "dir2/file1.txt": "e" * 5,
            "dir2/file2.txt": "f" * (MEMORY_LOAD_LIMIT + 1024),  # One large file
            "dir3/file0.txt": "g" * 10000,
            "dir3/file1.txt": "h" * 800,
            "dir3/file2.txt": "i" * 512,
        }
        create_local_test_dataset(source_msc_url, source_files)

        print(f"First sync from {source_msc_url} to {target_msc_url}")
        source_client, source_path = msc.resolve_storage_client(source_msc_url)
        target_client, target_path = msc.resolve_storage_client(target_msc_url)

        # The leading "/" is implied, but a rendundant one should be handled okay.
        target_client.sync_from(source_client, "/folder/", "/synced-files/")

        # Verify contents on target match expectation.
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        # Verify hidden files are NOT synced to target
        hidden_files = [".hidden_root.txt", "dir1/.hidden_in_dir.txt", "dir2/.dotfile", ".git/config"]
        for hidden_file in hidden_files:
            hidden_file_url = os.path.join(target_msc_url, hidden_file)
            assert not msc.is_file(hidden_file_url), f"Hidden file should not be synced: {hidden_file}"


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_replicas(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    obj_profile = "s3-sync"
    local_profile = "local"

    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        temp_data_store_type() as temp_data_store,
    ):
        config_dict = {
            "profiles": {
                obj_profile: temp_data_store.profile_config_dict(),
                local_profile: temp_source_data_store.profile_config_dict(),
            }
        }

        # Make local_profile a replica of obj_profile
        config_dict["profiles"][obj_profile]["replicas"] = [
            {"replica_profile": local_profile, "read_priority": 1},
        ]

        config.setup_msc_config(config_dict=config_dict)

        source_msc_url = f"msc://{obj_profile}/synced-files"
        replica_msc_url = f"msc://{local_profile}/synced-files"

        # Create local dataset
        expected_files = {
            "dir1/file0.txt": "a" * 150,
            "dir1/file1.txt": "b" * 200,
            "dir1/file2.txt": "c" * 1000,
            "dir2/file0.txt": "d" * 1,
            "dir2/file1.txt": "e" * 5,
            "dir2/file2.txt": "f" * (MEMORY_LOAD_LIMIT + 1024),  # One large file
            "dir3/file0.txt": "g" * 10000,
            "dir3/file1.txt": "h" * 800,
            "dir3/file2.txt": "i" * 512,
        }
        create_local_test_dataset(source_msc_url, expected_files)

        source_client, _ = msc.resolve_storage_client(source_msc_url)

        # The leading "/" is implied, but a rendundant one should be handled okay.
        source_client.sync_replicas(source_path="", execution_mode=ExecutionMode.LOCAL)

        # Verify contents on target match expectation.
        verify_sync_and_contents(target_url=replica_msc_url, expected_files=expected_files)

        # Verify that the lock file is created and removed.
        target_client, target_path = msc.resolve_storage_client(replica_msc_url)
        files = list(target_client.list(prefix=target_path))
        assert len([f for f in files if f.key.endswith(".lock")]) == 0


def test_sync_with_attributes_posix():
    """Test that metadata attributes are copied during sync operations with POSIX storage."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    source_profile = "source-local"
    target_profile = "target-local"
    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        tempdatastore.TemporaryPOSIXDirectory() as temp_target_data_store,
    ):
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    source_profile: temp_source_data_store.profile_config_dict(),
                    target_profile: temp_target_data_store.profile_config_dict(),
                }
            }
        )

        source_msc_url = f"msc://{source_profile}/source-with-attrs"
        target_msc_url = f"msc://{target_profile}/target-with-attrs"

        # Create files with custom attributes
        # POSIX supports custom attributes via extended attributes (xattr)
        test_files = {
            "small_file.txt": ("small content", {"env": "test", "version": "1.0"}),
            "medium_file.txt": ("m" * 1000, {"env": "prod", "version": "2.0", "team": "ml"}),
            "large_file.txt": ("l" * (MEMORY_LOAD_LIMIT + 1024), {"env": "staging", "priority": "high"}),
        }

        source_client, source_path = msc.resolve_storage_client(source_msc_url)

        # Write files with attributes
        for filename, (content, attrs) in test_files.items():
            file_path = os.path.join(source_path, filename)
            source_client.write(file_path, content.encode("utf-8"), attributes=attrs)

        time.sleep(1)  # Ensure timestamps are clear

        # Sync from source to target with attributes enabled
        print(f"Syncing from {source_msc_url} to {target_msc_url}")
        source_client, source_path = msc.resolve_storage_client(source_msc_url)
        target_client, target_path = msc.resolve_storage_client(target_msc_url)
        target_client.sync_from(source_client, source_path, target_path, preserve_source_attributes=True)

        # Verify files and their attributes on target
        target_client, target_path = msc.resolve_storage_client(target_msc_url)

        for filename, (content, expected_attrs) in test_files.items():
            target_file_path = os.path.join(target_path, filename)

            # Verify file exists and content is correct
            assert target_client.is_file(target_file_path), f"File {filename} not found at target"
            actual_content = target_client.read(target_file_path).decode("utf-8")
            assert actual_content == content, f"Content mismatch for {filename}"

            # Verify attributes are preserved via xattr
            metadata = target_client.info(target_file_path)
            assert metadata.metadata is not None, f"No metadata found for {filename}"

            for key, expected_value in expected_attrs.items():
                assert key in metadata.metadata, f"Attribute '{key}' missing for {filename}"
                assert metadata.metadata[key] == expected_value, (
                    f"Attribute '{key}' value mismatch for {filename}: "
                    f"expected '{expected_value}', got '{metadata.metadata.get(key)}'"
                )

        print("All attributes verified successfully!")


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_from_with_attributes(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test that metadata attributes are copied during sync_from operations."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    obj_profile = "s3-sync"
    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    obj_profile: temp_data_store.profile_config_dict(),
                }
            }
        )

        source_msc_url = f"msc://{obj_profile}/source-attrs"
        target_msc_url = f"msc://{obj_profile}/target-attrs"

        # Create files with custom attributes
        test_files = {
            "data/file1.txt": ("content1", {"type": "data", "classification": "public"}),
            "data/file2.txt": ("c" * 5000, {"type": "data", "classification": "private"}),
            "logs/file3.txt": ("l" * (MEMORY_LOAD_LIMIT + 500), {"type": "log", "retention": "30d"}),
        }

        source_client, source_path = msc.resolve_storage_client(source_msc_url)
        target_client, target_path = msc.resolve_storage_client(target_msc_url)

        # Write files with attributes
        for filename, (content, attrs) in test_files.items():
            file_path = os.path.join(source_path, filename)
            source_client.write(file_path, content.encode("utf-8"), attributes=attrs)

        time.sleep(1)

        # Use sync_from instead of sync with attributes enabled
        print(f"Using sync_from to sync {source_msc_url} to {target_msc_url}")
        target_client.sync_from(source_client, source_path, target_path, preserve_source_attributes=True)

        # Verify files and their attributes on target
        for filename, (content, expected_attrs) in test_files.items():
            target_file_path = os.path.join(target_path, filename)

            # Verify file exists and content is correct
            assert target_client.is_file(target_file_path), f"File {filename} not found at target"
            actual_content = target_client.read(target_file_path).decode("utf-8")
            assert actual_content == content, f"Content mismatch for {filename}"

            # Verify attributes are preserved
            metadata = target_client.info(target_file_path)
            assert metadata.metadata is not None, f"No metadata found for {filename}"

            for key, expected_value in expected_attrs.items():
                assert key in metadata.metadata, f"Attribute '{key}' missing for {filename}"
                assert metadata.metadata[key] == expected_value, (
                    f"Attribute '{key}' value mismatch for {filename}: "
                    f"expected '{expected_value}', got '{metadata.metadata.get(key)}'"
                )

        print("All attributes verified successfully in sync_from!")


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_from_symlink_files(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test that symlink files are dereferenced and synced correctly when source is local_profile.

    When syncing from a POSIX source that contains symlinks:
    - The synced result should be a file with the symlink's name
    - The content should be the content of the real file that the symlink points to
    """
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    obj_profile = "s3-sync"
    local_profile = "local"

    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        temp_data_store_type() as temp_data_store,
    ):
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    obj_profile: temp_data_store.profile_config_dict(),
                    local_profile: temp_source_data_store.profile_config_dict(),
                }
            }
        )

        source_msc_url = f"msc://{local_profile}/symlink-test"
        target_msc_url = f"msc://{obj_profile}/synced-symlinks"

        source_client, source_path = msc.resolve_storage_client(source_msc_url)
        target_client, target_path = msc.resolve_storage_client(target_msc_url)

        base_path = cast(BaseStorageProvider, source_client._storage_provider)._base_path

        real_file_content = "This is the real file content" * 50
        real_file_path = os.path.join(source_path, "real_file.txt")
        source_client.write(real_file_path, real_file_content.encode("utf-8"))

        physical_real_file_path = os.path.join(base_path, source_path, "real_file.txt")

        physical_symlink_path = os.path.join(base_path, source_path, "symlink_to_real.txt")
        os.symlink(physical_real_file_path, physical_symlink_path)

        regular_file_content = "Regular file content" * 30
        regular_file_path = os.path.join(source_path, "regular_file.txt")
        source_client.write(regular_file_path, regular_file_content.encode("utf-8"))

        subdir_path = os.path.join(source_path, "subdir")
        real_file_subdir_path = os.path.join(subdir_path, "real_in_subdir.txt")
        real_file_subdir_content = "Real file in subdirectory" * 20
        source_client.write(real_file_subdir_path, real_file_subdir_content.encode("utf-8"))

        physical_subdir_path = os.path.join(base_path, subdir_path)
        physical_real_file_subdir_path = os.path.join(base_path, real_file_subdir_path)
        physical_symlink_subdir_path = os.path.join(physical_subdir_path, "symlink_in_subdir.txt")

        os.symlink(physical_real_file_subdir_path, physical_symlink_subdir_path)

        time.sleep(1)  # Ensure timestamps are clear

        target_client.sync_from(source_client, source_path, target_path)

        expected_files = {
            "real_file.txt": real_file_content,
            "symlink_to_real.txt": real_file_content,  # Symlink should be dereferenced
            "regular_file.txt": regular_file_content,
            "subdir/real_in_subdir.txt": real_file_subdir_content,
            "subdir/symlink_in_subdir.txt": real_file_subdir_content,  # Symlink should be dereferenced
        }

        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        target_msc_url = f"msc://{obj_profile}/synced-no-symlinks"
        target_client, target_path = msc.resolve_storage_client(target_msc_url)
        target_client.sync_from(source_client, source_path, target_path, follow_symlinks=False)

        time.sleep(1)

        expected_files = {
            "real_file.txt": real_file_content,
            "regular_file.txt": regular_file_content,
            "subdir/real_in_subdir.txt": real_file_subdir_content,
        }

        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_from_with_source_files(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test sync_from with source_files parameter to sync only specific files."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    obj_profile = "s3-sync"
    local_profile = "local"
    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        temp_data_store_type() as temp_data_store,
    ):
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    obj_profile: temp_data_store.profile_config_dict(),
                    local_profile: temp_source_data_store.profile_config_dict(),
                }
            }
        )

        source_msc_url = f"msc://{local_profile}/folder"
        target_msc_url = f"msc://{obj_profile}/synced-files"

        all_files = {
            "dir1/file0.txt": "a" * 150,
            "dir1/file1.py": "b" * 200,
            "dir2/file2.txt": "f" * (MEMORY_LOAD_LIMIT + 1024),  # One large file
            "dir3/file1.txt": "h" * 800,
        }
        create_local_test_dataset(source_msc_url, all_files)

        source_client, source_path = msc.resolve_storage_client(source_msc_url)
        target_client, target_path = msc.resolve_storage_client(target_msc_url)

        # Test case 0: Sync nothing if source_files is an empty list
        target_client.sync_from(source_client, source_path, target_path, source_files=[])
        verify_sync_and_contents(target_url=target_msc_url, expected_files={})

        # Test case 1: Basic source_files sync
        files_to_sync = ["dir1/file0.txt", "dir2/file2.txt"]
        expected_files = {file: all_files[file] for file in files_to_sync}
        target_client.sync_from(source_client, source_path, target_path, source_files=files_to_sync)
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        # Test case 2: ValueError is raised when both source_files and patterns are provided
        files_to_sync_with_py = ["dir1/file1.py", "dir3/file1.txt"]
        patterns = [(PatternType.EXCLUDE, "*.py")]
        with pytest.raises(ValueError, match="Cannot specify both 'source_files' and 'patterns'"):
            target_client.sync_from(
                source_client, source_path, target_path, source_files=files_to_sync_with_py, patterns=patterns
            )

        # Now sync without patterns to continue with the test
        target_client.sync_from(source_client, source_path, target_path, source_files=files_to_sync_with_py)
        expected_files.update({file: all_files[file] for file in files_to_sync_with_py})
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        # Test case 3: Missing source file - should log warning but continue
        files_with_missing = ["dir1/file0.txt", "dir1/nonexistent.txt"]
        target_client.sync_from(source_client, source_path, target_path, source_files=files_with_missing)
        # Should not raise error, only existing files synced (dir1/file0.txt already exists)
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)


@pytest.mark.serial
@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_posix_large_files_no_temp_optimization(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
):
    """Verify that POSIX sync optimization avoids temp files in sync.py for large files.

    This test specifically validates the optimization by mocking tempfile.NamedTemporaryFile
    and ensuring sync.py doesn't create temp files during large file transfers:
    1. POSIX → POSIX: Direct copy with shutil.copy2 (no temp in sync.py)
    2. POSIX → Cloud: Direct upload with upload_file (no temp in sync.py)

    Note: Cloud → POSIX is not tested because cloud providers' download_file() methods
    internally use temp files for atomic downloads, which is a provider implementation
    detail and not part of the sync.py optimization.

    Only large files (> MEMORY_LOAD_LIMIT) are tested to avoid interference with
    other operations that legitimately use temp files (e.g., atomic writes for small files).
    """
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    source_posix_profile = "source-posix"
    target_posix_profile = "target-posix"
    cloud_profile = "cloud"

    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_posix,
        tempdatastore.TemporaryPOSIXDirectory() as temp_target_posix,
        temp_data_store_type() as temp_cloud,
    ):
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    source_posix_profile: temp_source_posix.profile_config_dict(),
                    target_posix_profile: temp_target_posix.profile_config_dict(),
                    cloud_profile: temp_cloud.profile_config_dict(),
                }
            }
        )

        large_file_content = "X" * (MEMORY_LOAD_LIMIT + 1024 * 1024)
        source_url = f"msc://{source_posix_profile}/large-file.dat"
        target_url = f"msc://{target_posix_profile}/large-file.dat"
        cloud_url = f"msc://{cloud_profile}/large-file.dat"

        msc.write(source_url, large_file_content.encode())
        time.sleep(1)

        sync_module = sys.modules["multistorageclient.sync"]

        # Test POSIX → POSIX: no temp files should be created in sync.py
        with mock.patch.object(sync_module.worker.tempfile, "NamedTemporaryFile") as mock_tempfile:
            msc.sync(source_url=f"msc://{source_posix_profile}", target_url=f"msc://{target_posix_profile}")
            assert not mock_tempfile.called, "POSIX → POSIX should not use temp files in sync.py"

        assert msc.is_file(target_url)
        with msc.open(target_url, mode="rb") as f:
            assert f.read().decode() == large_file_content
        msc.delete(target_url)

        # Test POSIX → Cloud: no temp files should be created in sync.py
        with mock.patch.object(sync_module.worker.tempfile, "NamedTemporaryFile") as mock_tempfile:
            msc.sync(source_url=f"msc://{source_posix_profile}", target_url=f"msc://{cloud_profile}")
            assert not mock_tempfile.called, "POSIX → Cloud should not use temp files in sync.py"

        assert msc.is_file(cloud_url)
        with msc.open(cloud_url, mode="rb") as f:
            assert f.read().decode() == large_file_content


def test_sync_with_manifest_overwrite_behavior():
    """Test sync operations with manifest metadata provider and allow_overwrites True/False."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    source_profile = "source"
    no_overwrite_profile = "no_overwrite"
    with_overwrite_profile = "with_overwrite"

    with (
        tempdatastore.TemporaryPOSIXDirectory() as source_store,
        tempdatastore.TemporaryPOSIXDirectory() as no_overwrite_store,
        tempdatastore.TemporaryPOSIXDirectory() as with_overwrite_store,
    ):
        # Configure profiles
        config_dict = {
            "profiles": {
                source_profile: source_store.profile_config_dict(),
                no_overwrite_profile: no_overwrite_store.profile_config_dict()
                | {
                    "metadata_provider": {
                        "type": "manifest",
                        "options": {
                            "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                            "writable": True,
                            "allow_overwrites": False,  # Explicitly disallow overwrites
                        },
                    }
                },
                with_overwrite_profile: with_overwrite_store.profile_config_dict()
                | {
                    "metadata_provider": {
                        "type": "manifest",
                        "options": {
                            "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                            "writable": True,
                            "allow_overwrites": True,  # Allow overwrites
                        },
                    }
                },
            }
        }
        config.setup_msc_config(config_dict=config_dict)

        source_url = f"msc://{source_profile}"
        no_overwrite_url = f"msc://{no_overwrite_profile}"
        with_overwrite_url = f"msc://{with_overwrite_profile}"

        # Create source files
        msc.write(f"{source_url}/file1.txt", "content1".encode())
        msc.write(f"{source_url}/file2.txt", "content2".encode())
        msc.write(f"{source_url}/dir/file3.txt", "content3".encode())

        # Test sync with allow_overwrites=False
        print("Testing sync with allow_overwrites=False")

        # First sync should succeed
        msc.sync(f"{source_url}/", f"{no_overwrite_url}/synced/")

        # Verify files were synced
        with msc.open(f"{no_overwrite_url}/synced/file1.txt", "rb") as f:
            assert f.read() == b"content1"
        with msc.open(f"{no_overwrite_url}/synced/file2.txt", "rb") as f:
            assert f.read() == b"content2"
        with msc.open(f"{no_overwrite_url}/synced/dir/file3.txt", "rb") as f:
            assert f.read() == b"content3"

        # Modify source files (with different content length to trigger overwrite check)
        msc.write(f"{source_url}/file1.txt", "modified1".encode())
        msc.write(f"{source_url}/file2.txt", "modified2".encode())

        # Second sync with allow_overwrites=False should now fail with SyncError
        # This validates that NGCDP-5748 is fixed - errors from worker threads are now
        # properly propagated to the caller
        with pytest.raises(SyncError, match="Errors in sync operation"):
            msc.sync(f"{source_url}/", f"{no_overwrite_url}/synced/")

        # Original content should be preserved (sync failed before overwrites)
        with msc.open(f"{no_overwrite_url}/synced/file1.txt", "rb") as f:
            assert f.read() == b"content1"
        with msc.open(f"{no_overwrite_url}/synced/file2.txt", "rb") as f:
            assert f.read() == b"content2"

        # Test sync with allow_overwrites=True
        print("Testing sync with allow_overwrites=True")

        # First sync should succeed
        msc.sync(f"{source_url}/", f"{with_overwrite_url}/synced/")

        # Verify files were synced (should have modified content from source)
        with msc.open(f"{with_overwrite_url}/synced/file1.txt", "rb") as f:
            assert f.read() == b"modified1"
        with msc.open(f"{with_overwrite_url}/synced/file2.txt", "rb") as f:
            assert f.read() == b"modified2"
        with msc.open(f"{with_overwrite_url}/synced/dir/file3.txt", "rb") as f:
            assert f.read() == b"content3"

        # Modify source files again
        msc.write(f"{source_url}/file1.txt", "modified_again1".encode())
        msc.write(f"{source_url}/file2.txt", "modified_again2".encode())

        # Second sync should succeed with allow_overwrites=True
        msc.sync(f"{source_url}/", f"{with_overwrite_url}/synced/")

        # Verify files were overwritten
        with msc.open(f"{with_overwrite_url}/synced/file1.txt", "rb") as f:
            assert f.read() == b"modified_again1"
        with msc.open(f"{with_overwrite_url}/synced/file2.txt", "rb") as f:
            assert f.read() == b"modified_again2"

        print("All sync tests passed!")


def test_sync_resume_with_metadata_provider():
    """Test sync resume functionality with metadata provider - existing files should not be overwritten.

    This test verifies that the sync process properly detects files that exist physically but are not
    tracked by the metadata provider. The enhanced sync logic now checks both the metadata provider
    AND the physical storage, ensuring that existing files are not overwritten and are added to the
    metadata provider for tracking.
    """
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    source_profile = "source"
    target_profile = "target_with_metadata"

    with (
        tempdatastore.TemporaryPOSIXDirectory() as source_store,
        tempdatastore.TemporaryPOSIXDirectory() as target_store,
    ):
        # Configure profiles
        config_dict = {
            "profiles": {
                source_profile: source_store.profile_config_dict(),
                target_profile: target_store.profile_config_dict()
                | {
                    "metadata_provider": {
                        "type": "manifest",
                        "options": {
                            "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                            "writable": True,
                            "allow_overwrites": False,  # Don't allow overwrites for this test
                        },
                    }
                },
            }
        }
        config.setup_msc_config(config_dict=config_dict)

        source_url = f"msc://{source_profile}"
        target_url = f"msc://{target_profile}"

        print("Creating source files...")
        source_files = {
            "file1.txt": "content1",
            "file2.txt": "content2",
            "dir/file3.txt": "content3",
            "dir/file4.txt": "content4",
            "dir/subdir/file5.txt": "content5",
        }

        for file_path, content in source_files.items():
            msc.write(f"{source_url}/{file_path}", content.encode())

        print("Manually copying some files to simulate partial sync...")
        manually_copied_files = ["file1.txt", "dir/file3.txt", "dir/subdir/file5.txt"]

        target_client, target_path = msc.resolve_storage_client(target_url)
        source_client, source_path = msc.resolve_storage_client(source_url)

        assert source_client._storage_provider
        assert target_client._storage_provider

        for file_path in manually_copied_files:
            # Use storage provider directly to copy files (bypassing metadata provider)
            # Do NOT tell the metadata provider about these files - sync should discover them
            content = source_client._storage_provider.get_object(file_path)
            target_client._storage_provider.put_object(file_path, content)

        print("Recording timestamps of manually copied files...")
        timestamps_before = {}
        for file_path in manually_copied_files:
            info = target_client._storage_provider.get_object_metadata(file_path)
            timestamps_before[file_path] = info.last_modified
            print(f"File {file_path} timestamp before sync: {info.last_modified}")

        # Wait to ensure timestamp differences would be detectable
        time.sleep(1)

        # Now run sync - this should copy the missing files but not overwrite existing ones
        print("Running sync operation...")
        msc.sync(source_url, target_url)

        print("Verifying all files exist in target...")
        for file_path, expected_content in source_files.items():
            with msc.open(f"{target_url}/{file_path}", "rb") as f:
                actual_content = f.read().decode()
                assert actual_content == expected_content, f"Content mismatch for {file_path}"

            print("Verifying manually copied files were not overwritten...")

            for file_path in manually_copied_files:
                info = target_client._storage_provider.get_object_metadata(file_path)
                timestamp_after = info.last_modified
                timestamp_before = timestamps_before[file_path]

                print(f"File {file_path} timestamp after sync: {timestamp_after}")
                # This assertion will FAIL due to current limitation - files are being overwritten
                assert timestamp_after == timestamp_before, (
                    f"File {file_path} was overwritten during sync! "
                    f"Before: {timestamp_before}, After: {timestamp_after}. "
                    f"This indicates the sync process is not properly detecting existing files that aren't tracked by metadata provider."
                )

        # Verify that files NOT manually copied were created during sync
        print("Verifying new files were created during sync...")
        not_manually_copied = [f for f in source_files.keys() if f not in manually_copied_files]

        for file_path in not_manually_copied:
            info = target_client._storage_provider.get_object_metadata(file_path)
            # These files should exist (we verified content above)
            print(f"New file {file_path} created with timestamp: {info.last_modified}")

        print("Testing that files are now tracked in metadata provider after sync...")

        # After sync, the files should now be tracked in the metadata provider
        for file_path in manually_copied_files:
            try:
                # This should now work because sync added the files to metadata provider
                info = target_client.info(file_path)
                print(f"File {file_path} is now tracked in metadata provider with timestamp: {info.last_modified}")
            except FileNotFoundError:
                assert False, f"File {file_path} should be tracked in metadata provider after sync"

        print("Testing resume behavior with tracked files...")

        # Modify one of the source files
        modified_file = "file1.txt"
        new_content = "modified_content1"
        msc.write(f"{source_url}/{modified_file}", new_content.encode())

        # Run sync again - now the file is tracked, so overwrite protection should raise an error
        print("Running sync after source modification (should be blocked by overwrite protection)...")
        with pytest.raises(SyncError, match="Errors in sync operation"):
            msc.sync(source_url, target_url)

        # Verify the file was NOT updated because it's now tracked and overwrites are disabled
        with msc.open(f"{target_url}/{modified_file}", "rb") as f:
            actual_content = f.read().decode()
            original_content = source_files[modified_file]  # Get original content
            assert actual_content == original_content, (
                f"Modified file {modified_file} should not have been updated due to allow_overwrites=False"
            )

        print("All sync resume tests passed!")


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_rustclient_credentials_refresh_multiple_threads(
    temp_data_store_type: type[tempdatastore.TemporaryAWSS3Bucket],
):
    """
    Test that RustClient handles concurrent credential refresh without deadlock.
    """
    with temp_data_store_type(enable_rust_client=True) as temp_s3_bucket:
        # Create StorageClient directly with configuration
        profile = "data"
        config_dict = {
            "profiles": {
                profile: temp_s3_bucket.profile_config_dict(),
            }
        }
        config_dict["profiles"][profile]["credentials_provider"] = {
            "type": "test_multistorageclient.unit.utils.mocks.SlowRefreshableCredentialsProvider",
            "options": {
                "access_key": temp_s3_bucket.profile_config_dict()["credentials_provider"]["options"]["access_key"],
                "secret_key": temp_s3_bucket.profile_config_dict()["credentials_provider"]["options"]["secret_key"],
            },
        }
        client_config = StorageClientConfig.from_dict(
            config_dict,
            profile=profile,
        )

        source_client = StorageClient(client_config)

        source_path = "test-concurrent-creds"

        # Create 20 random test files
        test_files = {}
        for i in range(20):
            filename = f"file{i:02d}.txt"
            content = f"{i}" * 500000  # ~5MB
            test_files[filename] = content

        # Upload files to S3
        for filename, content in test_files.items():
            file_path = os.path.join(source_path, filename)
            source_client.write(file_path, content.encode("utf-8"))

        # Use 4 threads to download files concurrently
        def download_files_worker(
            thread_id: int, file_subset: list[str], barrier: threading.Barrier
        ) -> tuple[int, float]:
            """Worker function to download a subset of files."""
            barrier.wait()
            start_time = time.time()

            with tempfile.TemporaryDirectory() as local_dir:
                for filename in file_subset:
                    target_path = os.path.join(local_dir, filename)
                    remote_path = os.path.join(source_path, filename)

                    # Download file
                    content = source_client.read(remote_path)
                    with open(target_path, "wb") as f:
                        f.write(content)

                    print(f"[Thread {thread_id}] Downloaded {filename}")

            elapsed = time.time() - start_time
            return thread_id, elapsed

        # Split files across 4 threads
        files_per_thread = len(test_files) // 4
        file_list = list(test_files.keys())
        thread_tasks = [file_list[i * files_per_thread : (i + 1) * files_per_thread] for i in range(4)]

        credentials_provider = source_client._credentials_provider

        # Execute downloads concurrently
        barrier = threading.Barrier(4)
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(download_files_worker, i, tasks, barrier) for i, tasks in enumerate(thread_tasks)
            ]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        total_time_seconds = time.time() - start_time
        final_refresh_count = credentials_provider.refresh_count  # type: ignore

        print("\n[Test Results]")
        print(f"  Total time: {total_time_seconds:.2f}s")
        print(f"  Credential refreshes: {final_refresh_count}")
        for thread_id, elapsed in sorted(results):
            print(f"  Thread {thread_id}: {elapsed:.2f}s")

        # Verify that credentials were refreshed, but not excessively
        assert final_refresh_count == 1, "Credentials should have been refreshed exactly once"


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_between_object_and_posix(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    obj_profile = "s3-sync"
    local_profile = "local"
    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        temp_data_store_type() as temp_data_store,
    ):
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    obj_profile: temp_data_store.profile_config_dict(),
                    local_profile: temp_source_data_store.profile_config_dict(),
                }
            }
        )

        posix_msc_url = f"msc://{local_profile}/"
        object_msc_url = f"msc://{obj_profile}/"

        # Create local dataset
        objects = {
            "dir1/file0.txt": "a" * 150,
            "dir1/file1.txt": "b" * 200,
            "dir1/file2.txt": "c" * 1000,
        }
        create_local_test_dataset(object_msc_url, objects)

        # Insert a delay before sync'ing so that timestamps will be clearer.
        time.sleep(1)

        # Sync from the object to the posix shouldn't do any changes to the object.
        msc.sync(source_url=object_msc_url, target_url=posix_msc_url)
        expected_object_list = list(msc.list(object_msc_url))
        actual_object_list = list(msc.list(posix_msc_url))

        assert len(expected_object_list) == len(actual_object_list)
        for expected_object, actual_object in zip(expected_object_list, actual_object_list):
            assert expected_object.content_length == actual_object.content_length
            assert expected_object.last_modified == actual_object.last_modified

        # Sync from the posix to the object should not update the object.
        msc.sync(source_url=posix_msc_url, target_url=object_msc_url)
        actual_object_list = list(msc.list(object_msc_url))

        assert len(expected_object_list) == len(actual_object_list)
        for expected_object, actual_object in zip(expected_object_list, actual_object_list):
            assert expected_object.content_length == actual_object.content_length
            assert expected_object.last_modified == actual_object.last_modified


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_source_files_should_not_copy_attributes_when_preserve_false(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
):
    """Test that source_files respects preserve_source_attributes=False.

    BUG: When using the source_files parameter, the producer calls info() to get
    file metadata, which always fetches attributes. These attributes then flow to
    the worker and get copied regardless of preserve_source_attributes setting.

    This test demonstrates the bug by:
    1. Creating source files WITH custom attributes
    2. Syncing with source_files and preserve_source_attributes=False (default)
    3. Verifying that attributes should NOT be copied to target

    Expected behavior: No attributes should be copied when preserve_source_attributes=False
    Actual behavior (bug): Attributes ARE copied because info() fetches them
    """
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    obj_profile = "s3-sync"
    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    obj_profile: temp_data_store.profile_config_dict(),
                }
            }
        )

        source_msc_url = f"msc://{obj_profile}/source-attrs"
        target_msc_url = f"msc://{obj_profile}/target-no-attrs"

        source_client, source_path = msc.resolve_storage_client(source_msc_url)
        target_client, target_path = msc.resolve_storage_client(target_msc_url)

        test_attributes = {"env": "test", "version": "1.0", "team": "ml"}
        test_content = "test content for attribute test"

        source_client.write(
            os.path.join(source_path, "file_with_attrs.txt"),
            test_content.encode("utf-8"),
            attributes=test_attributes,
        )

        source_metadata = source_client.info(os.path.join(source_path, "file_with_attrs.txt"))
        assert source_metadata.metadata == test_attributes, "Source file should have attributes"

        time.sleep(1)

        target_client.sync_from(
            source_client,
            source_path,
            target_path,
            source_files=["file_with_attrs.txt"],
            preserve_source_attributes=False,
        )

        target_file_path = os.path.join(target_path, "file_with_attrs.txt")
        assert target_client.is_file(target_file_path), "Target file should exist"

        actual_content = target_client.read(target_file_path).decode("utf-8")
        assert actual_content == test_content, "Content should match"

        target_metadata = target_client.info(target_file_path)

        assert target_metadata.metadata is None or target_metadata.metadata == {}, (
            f"BUG: Attributes were copied even though preserve_source_attributes=False. "
            f"Expected no attributes, got: {target_metadata.metadata}. "
            f"When using source_files, the producer calls info() which fetches attributes, "
            f"and these flow through to the worker regardless of the preserve flag."
        )


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_uses_strict_false_for_get_object_metadata(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
):
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    source_profile = "s3-source"
    target_profile = "s3-target"

    with (
        temp_data_store_type() as temp_source_data_store,
        temp_data_store_type() as temp_target_data_store,
    ):
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    source_profile: temp_source_data_store.profile_config_dict(),
                    target_profile: temp_target_data_store.profile_config_dict(),
                }
            }
        )

        source_url = f"msc://{source_profile}/"
        target_url = f"msc://{target_profile}/"

        test_files = {
            "file1.txt": "test content 1",
            "dir/file2.txt": "test content 2",
        }
        create_local_test_dataset(source_url, test_files)

        target_client, _ = msc.resolve_storage_client(target_url)

        def mock_get_object_metadata(path: str, strict: bool = True) -> ObjectMetadata:
            assert strict is False, f"get_object_metadata should be called with strict=False, but got strict={strict}"
            raise FileNotFoundError(f"Object {path} not found")

        with mock.patch.object(
            target_client._storage_provider, "get_object_metadata", side_effect=mock_get_object_metadata
        ):
            msc.sync(source_url=source_url, target_url=target_url)

        verify_sync_and_contents(target_url, test_files)
