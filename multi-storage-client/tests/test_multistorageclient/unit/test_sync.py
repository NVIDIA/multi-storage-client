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

import copy
import os
import queue
import time
from datetime import datetime
from typing import Optional, cast
from unittest import mock

import pytest

import multistorageclient as msc
from multistorageclient.client import StorageClient
from multistorageclient.constants import MEMORY_LOAD_LIMIT
from multistorageclient.progress_bar import ProgressBar
from multistorageclient.providers.base import BaseStorageProvider
from multistorageclient.providers.manifest_metadata import DEFAULT_MANIFEST_BASE_DIR
from multistorageclient.sync import (
    ProducerThread,
    ResultConsumerThread,
    SyncManager,
    _check_posix_paths,
    _copy_posix_to_posix,
    _copy_posix_to_remote,
    _copy_remote_to_posix,
    _copy_remote_to_remote,
    _SyncOp,
    _update_posix_metadata,
)
from multistorageclient.types import ExecutionMode, ObjectMetadata, PatternType
from multistorageclient.utils import NullStorageClient
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
        with_manifest_profile_config_dict = copy.deepcopy(second_local_data_store.profile_config_dict()) | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                    "writable": True,
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
        # Insert a delay before sync'ing so that timestamps will be clearer.
        time.sleep(1)

        print(f"First sync from {source_msc_url} to {target_msc_url}")
        msc.sync(source_url=source_msc_url, target_url=target_msc_url)

        # Verify contents on target match expectation.
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        print("Deleting file at target and syncing again")
        msc.delete(os.path.join(target_msc_url, "dir1/file0.txt"))
        msc.sync(source_url=source_msc_url, target_url=target_msc_url)
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        print("Syncing again and verifying timestamps")
        timestamps_before = {file: get_file_timestamp(os.path.join(target_msc_url, file)) for file in expected_files}
        msc.sync(source_url=source_msc_url, target_url=target_msc_url)
        timestamps_after = {file: get_file_timestamp(os.path.join(target_msc_url, file)) for file in expected_files}
        assert timestamps_before == timestamps_after, "Timestamps changed on second sync."

        print("Adding new files and syncing again")
        new_files = {"dir1/new_file.txt": "n" * 100}
        create_local_test_dataset(source_msc_url, expected_files=new_files)
        msc.sync(source_url=source_msc_url, target_url=target_msc_url)
        expected_files.update(new_files)
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        print("Modifying one of the source files, but keeping size the same, and verifying it's copied.")
        modified_files = {"dir1/file0.txt": "z" * 100}
        create_local_test_dataset(source_msc_url, expected_files=modified_files)
        expected_files.update(modified_files)
        msc.sync(source_url=source_msc_url, target_url=target_msc_url)
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        with pytest.raises(ValueError):
            msc.sync(source_url=source_msc_url, target_url=source_msc_url)
        with pytest.raises(ValueError):
            msc.sync(source_url=target_msc_url, target_url=target_msc_url)
        with pytest.raises(ValueError):
            msc.sync(source_url=source_msc_url, target_url=os.path.join(source_msc_url, "extra"))

        print("Syncing from object to a second posix file location using ManifestProvider.")
        msc.sync(source_url=target_msc_url, target_url=second_msc_url)
        verify_sync_and_contents(target_url=second_msc_url, expected_files=expected_files)

        print("Deleting all the files at the target and going again.")
        for key in expected_files.keys():
            msc.delete(os.path.join(target_msc_url, key))

        print("Syncing using prefixes to just copy one subfolder.")
        msc.sync(source_url=os.path.join(source_msc_url, "dir2"), target_url=os.path.join(target_msc_url, "dir2"))
        sub_expected_files = {k: v for k, v in expected_files.items() if k.startswith("dir2")}
        verify_sync_and_contents(target_url=target_msc_url, expected_files=sub_expected_files)

        msc.sync(source_url=source_msc_url, target_url=target_msc_url)

        print("Deleting files at the source and syncing again, verify deletes at target.")
        keys_to_delete = [k for k in expected_files.keys() if k.startswith("dir2")]
        # Delete keys at the source.
        for key in keys_to_delete:
            expected_files.pop(key)
            msc.delete(os.path.join(source_msc_url, key))

        # Sync from source to target and expect deletes to happen at the target.
        msc.sync(source_url=source_msc_url, target_url=target_msc_url, delete_unmatched_files=True)
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        # Delete all remaining keys at source and verify the deletes propagate to target.
        for key in expected_files.keys():
            msc.delete(os.path.join(source_msc_url, key))
        msc.sync(source_url=source_msc_url, target_url=target_msc_url, delete_unmatched_files=True)
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
        # Insert a delay before sync'ing so that timestamps will be clearer.
        time.sleep(1)

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
        # Insert a delay before sync'ing so that timestamps will be clearer.
        time.sleep(1)

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


class MockStorageClient:
    def list(self, **kwargs):
        raise Exception("No Such Method")

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        pass

    def _is_rust_client_enabled(self) -> bool:
        return False

    def _is_posix_file_storage_provider(self) -> bool:
        return False


def test_producer_thread_error():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
    )

    producer_thread.start()
    producer_thread.join()

    assert not producer_thread.is_alive()
    assert producer_thread.error is not None


def test_result_consumer_exits_with_stop_signal():
    target_client = MockStorageClient()
    result_queue = queue.Queue()

    result_consumer_thread = ResultConsumerThread(
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        result_queue=result_queue,
    )

    result_consumer_thread.start()
    result_consumer_thread.join(timeout=1)

    assert result_consumer_thread.is_alive()

    result_queue.put((_SyncOp.STOP, None, None))
    result_consumer_thread.join(timeout=1)

    assert not result_consumer_thread.is_alive()


def test_sync_function_return_producer_error():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    manager = SyncManager(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
    )
    with pytest.raises(RuntimeError, match="Errors in sync operation, caused by: .*"):
        manager.sync_objects()


def test_progress_bar_update_in_producer_thread_without_deletion():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(key="file0.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file3.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
    ]

    target_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0)),
        ObjectMetadata(key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0)),
        ObjectMetadata(key="file4.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file5.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file6.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    progress = ProgressBar(desc="Syncing", show_progress=True)
    file_queue = queue.Queue()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=progress,
        file_queue=file_queue,
        num_workers=1,
        delete_unmatched_files=False,
    )

    producer_thread.start()
    producer_thread.join()

    assert producer_thread.error is None
    assert progress.pbar is not None
    assert progress.pbar.total == len(source_files)

    # Because file1.txt and file2.txt are the same, they should be skipped and the progress bar should be updated.
    assert progress.pbar.n == 2


def test_progress_bar_update_in_producer_thread_with_deletion():
    source_client = NullStorageClient()
    target_client = MockStorageClient()

    target_files = [
        ObjectMetadata(key="file0.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file3.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
    ]

    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    progress = ProgressBar(desc="Syncing", show_progress=True)
    file_queue = queue.Queue()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=progress,
        file_queue=file_queue,
        num_workers=1,
        delete_unmatched_files=True,
    )

    producer_thread.start()
    producer_thread.join()

    assert producer_thread.error is None
    assert progress.pbar is not None
    assert progress.pbar.total == len(target_files)
    assert progress.pbar.n == 0


def test_progress_bar_capped_percentage():
    progress = ProgressBar(desc="Syncing", show_progress=True)
    progress.update_total(100_000)
    progress.update_progress(99_999)
    assert "99.9%" in str(progress.pbar)


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
        time.sleep(1)

        source_client, source_path = msc.resolve_storage_client(source_msc_url)
        target_client, target_path = msc.resolve_storage_client(target_msc_url)

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
        time.sleep(0.5)

        import sys

        sync_module = sys.modules["multistorageclient.sync"]

        # Test POSIX → POSIX: no temp files should be created in sync.py
        with mock.patch.object(sync_module.tempfile, "NamedTemporaryFile") as mock_tempfile:
            msc.sync(source_url=f"msc://{source_posix_profile}", target_url=f"msc://{target_posix_profile}")
            assert not mock_tempfile.called, "POSIX → POSIX should not use temp files in sync.py"

        assert msc.is_file(target_url)
        with msc.open(target_url, mode="rb") as f:
            assert f.read().decode() == large_file_content
        msc.delete(target_url)

        # Test POSIX → Cloud: no temp files should be created in sync.py
        with mock.patch.object(sync_module.tempfile, "NamedTemporaryFile") as mock_tempfile:
            msc.sync(source_url=f"msc://{source_posix_profile}", target_url=f"msc://{cloud_profile}")
            assert not mock_tempfile.called, "POSIX → Cloud should not use temp files in sync.py"

        assert msc.is_file(cloud_url)
        with msc.open(cloud_url, mode="rb") as f:
            assert f.read().decode() == large_file_content


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
    import tempfile

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
    import json

    import xattr

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
        file_metadata = ObjectMetadata(
            key=test_file,
            content_length=len(content),
            last_modified=datetime.now(),
            metadata=custom_metadata,
        )

        # Update metadata
        _update_posix_metadata(client, target_physical_path, test_file, file_metadata)

        # Verify metadata storage
        if use_metadata_provider:
            info = client.info(test_file)
            assert info.metadata == custom_metadata
        else:
            try:
                xattr_value = xattr.getxattr(target_physical_path, "user.json")
                stored_metadata = json.loads(xattr_value.decode("utf-8"))
                assert stored_metadata == custom_metadata
            except OSError:
                pytest.skip("xattr not supported on this filesystem")
