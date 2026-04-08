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
import queue
import sys
from datetime import datetime, timezone
from typing import Optional
from unittest import mock

import pytest
import xattr

import multistorageclient as msc
from multistorageclient.sync import worker as sync_worker_module
from multistorageclient.sync.types import OperationBatch, OperationType
from multistorageclient.sync.worker import (
    check_skip_and_track_with_metadata_provider,
    check_skip_without_metadata_provider,
    update_posix_metadata,
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


def test_object_metadata_replace():
    """Test ObjectMetadata.replace preserves fields and overrides specified ones."""
    target_metadata = ObjectMetadata(
        key="target/path/file.txt",
        content_length=4,
        last_modified=datetime(2025, 1, 1, 10, 0, 0),
        etag="etag-1",
        metadata={"existing": "value", "version": "1"},
    )

    replaced = target_metadata.replace(metadata={"version": "2", "owner": "ml-team"})

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


def test_batch_posix_to_posix():
    """Test the live POSIX-to-POSIX handler path copies files between POSIX locations."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source,
        tempdatastore.TemporaryPOSIXDirectory() as temp_target,
    ):
        source_client, target_client = _setup_test_clients("source-test", "target-test", temp_source, temp_target)

        source_name = "nested/source.txt"
        target_name = "subdir/nested/source.txt"
        content = b"test content"
        source_client.write(source_name, content)

        batch = OperationBatch(
            operation=OperationType.ADD,
            items=[
                (
                    ObjectMetadata(
                        key=source_name,
                        content_length=len(content),
                        last_modified=datetime.now(tz=timezone.utc),
                        metadata=None,
                    ),
                    None,
                )
            ],
        )
        result_queue = queue.Queue()
        error_queue = queue.Queue()

        handler = sync_worker_module.create_sync_handler(
            source_client=source_client,
            source_path="",
            target_client=target_client,
            target_path="subdir",
            preserve_source_attributes=False,
            result_queue=result_queue,
            error_queue=error_queue,
        )
        assert isinstance(handler, sync_worker_module.PosixToPosixHandler)

        handler.process_add_batch(worker_id="test-worker", batch=batch)

        assert error_queue.empty(), f"Unexpected error: {error_queue.get()}"
        assert target_client.is_file(target_name)
        assert target_client.read(target_name) == content


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_batch_posix_to_remote(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test the live POSIX-to-remote handler path uploads files to remote storage."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_posix,
        temp_data_store_type() as temp_remote,
    ):
        posix_client, remote_client = _setup_test_clients("posix-test", "remote-test", temp_posix, temp_remote)

        files = {
            "file1.txt": b"content for file 1",
            "file2.txt": b"content for file 2",
        }
        for name, content in files.items():
            posix_client.write(name, content)

        batch_items = []
        for name, content in files.items():
            batch_items.append(
                (
                    ObjectMetadata(
                        key=name,
                        content_length=len(content),
                        last_modified=datetime.now(tz=timezone.utc),
                        metadata={"source": name},
                    ),
                    None,
                )
            )

        batch = OperationBatch(operation=OperationType.ADD, items=batch_items)
        result_queue = queue.Queue()
        error_queue = queue.Queue()

        handler = sync_worker_module.create_sync_handler(
            source_client=posix_client,
            source_path="",
            target_client=remote_client,
            target_path="uploaded/",
            preserve_source_attributes=False,
            result_queue=result_queue,
            error_queue=error_queue,
        )
        assert isinstance(handler, sync_worker_module.PosixToRemoteHandler)

        handler.process_add_batch(worker_id="test-worker", batch=batch)

        assert error_queue.empty(), f"Unexpected error: {error_queue.get()}"

        for name, content in files.items():
            target_key = f"uploaded/{name}"
            assert remote_client.is_file(target_key)
            assert remote_client.read(target_key) == content


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_batch_remote_to_posix(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test the live remote-to-POSIX handler path downloads files to POSIX storage."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_posix,
        temp_data_store_type() as temp_remote,
    ):
        posix_client, remote_client = _setup_test_clients("posix-test", "remote-test", temp_posix, temp_remote)

        files = {
            "file1.txt": b"remote content 1",
            "file2.txt": b"remote content 2",
        }
        for name, content in files.items():
            remote_client.write(name, content)

        batch_items = []
        for name, content in files.items():
            batch_items.append(
                (
                    ObjectMetadata(
                        key=name,
                        content_length=len(content),
                        last_modified=datetime.now(tz=timezone.utc),
                        metadata={"source": name},
                    ),
                    None,
                )
            )

        batch = OperationBatch(operation=OperationType.ADD, items=batch_items)
        result_queue = queue.Queue()
        error_queue = queue.Queue()

        handler = sync_worker_module.create_sync_handler(
            source_client=remote_client,
            source_path="",
            target_client=posix_client,
            target_path="downloaded/",
            preserve_source_attributes=False,
            result_queue=result_queue,
            error_queue=error_queue,
        )
        assert isinstance(handler, sync_worker_module.RemoteToPosixHandler)

        handler.process_add_batch(worker_id="test-worker", batch=batch)

        assert error_queue.empty(), f"Unexpected error: {error_queue.get()}"

        for name, content in files.items():
            target_key = f"downloaded/{name}"
            assert posix_client.is_file(target_key)
            assert posix_client.read(target_key) == content


def test_check_skip_without_metadata_provider_skips_up_to_date():
    """Test check_skip_without_metadata_provider returns True when target is up-to-date."""
    now = datetime.now(tz=timezone.utc)
    file_metadata = ObjectMetadata(key="source.txt", content_length=100, last_modified=now)
    target_metadata = ObjectMetadata(key="target.txt", content_length=100, last_modified=now)

    assert check_skip_without_metadata_provider("target.txt", file_metadata, target_metadata) is True


def test_check_skip_without_metadata_provider_does_not_skip_outdated():
    """Test check_skip_without_metadata_provider returns False when target is outdated (size mismatch)."""
    now = datetime.now(tz=timezone.utc)
    file_metadata = ObjectMetadata(key="source.txt", content_length=100, last_modified=now)
    target_metadata = ObjectMetadata(key="target.txt", content_length=3, last_modified=now)

    assert check_skip_without_metadata_provider("target.txt", file_metadata, target_metadata) is False


def test_check_skip_without_metadata_provider_does_not_skip_missing():
    """Test check_skip_without_metadata_provider returns False when target_metadata is None."""
    file_metadata = ObjectMetadata(
        key="source.txt",
        content_length=10,
        last_modified=datetime.now(tz=timezone.utc),
    )

    assert check_skip_without_metadata_provider("nonexistent.txt", file_metadata, None) is False


def _make_handler_with_metadata_provider():
    """Create a PosixToPosixHandler with a mocked metadata-provider-enabled target client."""
    from multistorageclient.sync.worker import PosixToPosixHandler

    target_client = mock.MagicMock()
    target_client._metadata_provider = mock.MagicMock()
    target_client._metadata_provider_lock = None

    handler = PosixToPosixHandler(
        source_client=mock.MagicMock(),
        source_path="src/",
        target_client=target_client,
        target_path="dst/",
        preserve_source_attributes=False,
        result_queue=queue.Queue(),
        error_queue=queue.Queue(),
    )
    return handler


def _make_add_batch(n: int):
    items: list[tuple[ObjectMetadata, Optional[ObjectMetadata]]] = [
        (
            ObjectMetadata(
                key=f"src/file{i}.txt", content_length=100, last_modified=datetime(2025, 1, 1, tzinfo=timezone.utc)
            ),
            None,
        )
        for i in range(n)
    ]
    return OperationBatch(operation=OperationType.ADD, items=items)


def test_filter_with_metadata_provider_mixed_skip_and_transfer():
    handler = _make_handler_with_metadata_provider()
    batch = _make_add_batch(6)
    skip_keys = {"src/file0.txt", "src/file2.txt", "src/file4.txt"}

    def side_effect(_client, _path, fm, _preserve):
        return fm.key in skip_keys

    with mock.patch.object(sync_worker_module, "check_skip_and_track_with_metadata_provider", side_effect=side_effect):
        result = handler._filter_with_metadata_provider("w-0", batch)

    assert {fm.key for fm, _ in result} == {"src/file1.txt", "src/file3.txt", "src/file5.txt"}


def test_filter_with_metadata_provider_reports_errors():
    handler = _make_handler_with_metadata_provider()
    batch = _make_add_batch(4)

    def side_effect(_client, _path, fm, _preserve):
        if fm.key == "src/file1.txt":
            raise FileExistsError("overwrites not allowed")
        return False

    with mock.patch.object(sync_worker_module, "check_skip_and_track_with_metadata_provider", side_effect=side_effect):
        result = handler._filter_with_metadata_provider("w-0", batch)

    assert len(result) == 3
    error = handler.error_queue.get()
    assert "overwrites not allowed" in error.exception_message


def test_filter_with_metadata_provider_single_item_no_threadpool():
    handler = _make_handler_with_metadata_provider()
    batch = _make_add_batch(1)

    with (
        mock.patch.object(sync_worker_module, "check_skip_and_track_with_metadata_provider", return_value=False),
        mock.patch.object(sync_worker_module, "ThreadPoolExecutor") as mock_tpe,
    ):
        result = handler._filter_with_metadata_provider("w-0", batch)

    assert len(result) == 1
    mock_tpe.assert_not_called()


def test_check_skip_and_track_with_metadata_provider_skips_up_to_date():
    """Test check_skip_and_track_with_metadata_provider returns True when target is up-to-date."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with tempdatastore.TemporaryPOSIXDirectory() as temp_posix:
        config.setup_msc_config(config_dict={"profiles": {"posix-test": temp_posix.profile_config_dict()}})
        client, _ = msc.resolve_storage_client("msc://posix-test")

        content = b"existing content"
        client.write("target.txt", content)
        info = client.info("target.txt")

        resolved = mock.MagicMock()
        resolved.exists = True
        resolved.physical_path = "target.txt"

        mp = mock.MagicMock()
        mp.realpath.return_value = resolved
        mp.allow_overwrites.return_value = True

        client._metadata_provider = mp
        client._metadata_provider_lock = None

        file_metadata = ObjectMetadata(
            key="source.txt",
            content_length=info.content_length,
            last_modified=info.last_modified,
        )

        skipped = check_skip_and_track_with_metadata_provider(
            client, "target.txt", file_metadata, preserve_source_attributes=False
        )
        assert skipped is True
        mp.add_file.assert_called_once()


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
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_batch_remote_to_remote(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test the live remote-to-remote handler path copies batches between remotes."""
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        temp_data_store_type() as temp_source,
        temp_data_store_type() as temp_target,
    ):
        source_client, target_client = _setup_test_clients("source-test", "target-test", temp_source, temp_target)

        files = {
            "small.txt": b"small content",
            "nested/large.txt": b"large content" * 32,
        }
        batch_items = []
        for name, content in files.items():
            source_client.write(name, content)
            batch_items.append(
                (
                    ObjectMetadata(
                        key=name,
                        content_length=len(content),
                        last_modified=datetime.now(tz=timezone.utc),
                        metadata={"source": name},
                    ),
                    None,
                )
            )

        batch = OperationBatch(operation=OperationType.ADD, items=batch_items)
        result_queue = queue.Queue()
        error_queue = queue.Queue()

        handler = sync_worker_module.create_sync_handler(
            source_client=source_client,
            source_path="",
            target_client=target_client,
            target_path="copied/",
            preserve_source_attributes=False,
            result_queue=result_queue,
            error_queue=error_queue,
        )
        assert isinstance(handler, sync_worker_module.RemoteToRemoteHandler)

        handler.process_add_batch(worker_id="test-worker", batch=batch)

        assert error_queue.empty(), f"Unexpected error: {error_queue.get()}"

        for name, content in files.items():
            target_key = f"copied/{name}"
            assert target_client.is_file(target_key)
            assert target_client.read(target_key) == content


@pytest.mark.parametrize(
    argnames=["use_metadata_provider"],
    argvalues=[[True], [False]],
)
def test_update_posix_metadata(use_metadata_provider: bool):
    """Test update_posix_metadata updates metadata via provider or xattr."""
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
        update_posix_metadata(client, target_physical_path, test_file, file_metadata)

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
