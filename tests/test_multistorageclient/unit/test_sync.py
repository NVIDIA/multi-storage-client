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
from typing import Optional

import pytest

import multistorageclient as msc
from multistorageclient.constants import MEMORY_LOAD_LIMIT
from multistorageclient.progress_bar import ProgressBar
from multistorageclient.providers.manifest_metadata import DEFAULT_MANIFEST_BASE_DIR
from multistorageclient.sync import ProducerThread, ResultConsumerThread, SyncManager, _SyncOp
from multistorageclient.types import ExecutionMode, ObjectMetadata
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

        print(f"First sync from {source_msc_url} to {target_msc_url}")
        source_client, source_path = msc.resolve_storage_client(source_msc_url)
        target_client, target_path = msc.resolve_storage_client(target_msc_url)

        # The leading "/" is implied, but a rendundant one should be handled okay.
        target_client.sync_from(source_client, "/folder/", "/synced-files/")

        # Verify contents on target match expectation.
        verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)


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


class MockStorageClient:
    def list(self, **kwargs):
        raise Exception("No Such Method")

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        pass


def test_producer_thread_error():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    producer_thread = ProducerThread(
        source_client=source_client,
        source_path="",
        target_client=target_client,
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
    )

    producer_thread.start()
    producer_thread.join()

    assert not producer_thread.is_alive()
    assert producer_thread.error is not None


def test_result_consumer_error():
    target_client = MockStorageClient()
    result_queue = queue.Queue()

    result_consumer_thread = ResultConsumerThread(
        target_client=target_client,
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        result_queue=result_queue,
    )

    result_consumer_thread.start()
    result_consumer_thread.join(timeout=1)

    assert result_consumer_thread.is_alive()
    assert result_consumer_thread.error is None

    result_queue.put((_SyncOp.ADD, None, None))
    result_consumer_thread.join(timeout=1)

    assert not result_consumer_thread.is_alive()
    assert result_consumer_thread.error is not None


def test_sync_function_return_producer_error():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    manager = SyncManager(source_client=source_client, source_path="", target_client=target_client, target_path="")
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
        source_client=source_client,
        source_path="",
        target_client=target_client,
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
        source_client=source_client,
        source_path="",
        target_client=target_client,
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
