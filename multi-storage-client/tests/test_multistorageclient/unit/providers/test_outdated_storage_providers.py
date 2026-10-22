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

import queue
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import IO
from unittest.mock import Mock

import pytest

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers.base import BaseStorageProvider
from multistorageclient.providers.posix_file import PosixFileStorageProvider
from multistorageclient.sync.transfer_progress import TransferProgress
from multistorageclient.sync.types import OperationBatch, OperationType
from multistorageclient.sync.worker import create_sync_handler
from multistorageclient.types import ObjectMetadata, Range


class OutdatedStorageProvider(BaseStorageProvider):
    """
    Mock storage provider that simulates an old implementation using 'prefix'
    instead of 'path' as the second parameter in _list_objects method.
    This tests backward compatibility with providers that haven't been updated.
    """

    def _put_object(
        self,
        path: str,
        body: bytes,
        if_match: str | None = None,
        if_none_match: str | None = None,
        attributes: dict[str, str] | None = None,
    ) -> int:
        return len(body)

    def _get_object(self, path: str, byte_range: Range | None = None) -> bytes:
        return b""

    def _copy_object(self, src_path: str, dest_path: str) -> int:
        return 0

    def _delete_object(self, path: str, if_match: str | None = None) -> None:
        pass

    def _make_symlink(self, path: str, target: str) -> None:
        pass

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        if not path.endswith("txt"):
            return ObjectMetadata(
                key=path, content_length=0, type="directory", last_modified=datetime.now(tz=timezone.utc)
            )
        else:
            return ObjectMetadata(key=path, content_length=0, type="file", last_modified=datetime.now(tz=timezone.utc))

    def _list_objects(
        self,
        prefix: str,  # Note: using 'prefix' instead of 'path' (old interface)
        start_after: str | None = None,
        end_at: str | None = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
        """
        Simulates an old storage provider implementation that uses 'prefix'
        instead of 'path' as the parameter name. This should still work due
        to Python's duck typing - the parameter is passed positionally.
        """
        # Mock implementation that returns some test objects
        mock_objects = [
            ObjectMetadata(
                key=f"{prefix}/file1.txt", content_length=100, type="file", last_modified=datetime.now(tz=timezone.utc)
            ),
            ObjectMetadata(
                key=f"{prefix}/file2.txt", content_length=200, type="file", last_modified=datetime.now(tz=timezone.utc)
            ),
        ]

        for obj in mock_objects:
            # Apply filtering if specified
            if start_after and obj.key <= start_after:
                continue
            if end_at and obj.key > end_at:
                continue
            yield obj

    def _upload_file(self, remote_path: str, f: str | IO, attributes: dict[str, str] | None = None) -> int:
        return 0

    def _download_file(self, remote_path: str, f: str | IO, metadata: ObjectMetadata | None = None) -> int:
        return 0


@pytest.mark.parametrize("boundary", ["provider", "delegate"])
@pytest.mark.parametrize("notify", [False, True])
def test_legacy_batch_overrides_through_client(tmp_path, monkeypatch, boundary, notify):
    provider = PosixFileStorageProvider(base_path=str(tmp_path / "remote"))
    client = StorageClient(StorageClientConfig(profile="legacy", storage_provider=provider))
    target = provider if boundary == "provider" else client._delegate
    download = target.download_files
    upload = target.upload_files

    def legacy_download(remote_paths, local_paths, metadata=None, max_workers=16):
        download(remote_paths, local_paths, metadata, max_workers)

    def legacy_upload(first_paths, second_paths, attributes=None, max_workers=16):
        upload(first_paths, second_paths, attributes, max_workers)

    monkeypatch.setattr(target, "download_files", legacy_download)
    monkeypatch.setattr(target, "upload_files", legacy_upload)
    observer = Mock()
    source = tmp_path / "source"
    source.write_bytes(b"legacy transfer")
    destination = tmp_path / "destination"

    client.upload_files(["file"], [str(source)], transfer_callback=observer if notify else None)
    client.download_files(["file"], [str(destination)], transfer_callback=observer if notify else None)

    assert destination.read_bytes() == source.read_bytes()
    observer.assert_not_called()


@pytest.mark.parametrize("direction", ["download", "upload", "transfer"])
@pytest.mark.parametrize("notify", [False, True])
def test_sync_supports_legacy_client_batch_overrides(tmp_path, monkeypatch, direction, notify):
    source = StorageClient(
        StorageClientConfig(
            profile="source", storage_provider=PosixFileStorageProvider(base_path=str(tmp_path / "source"))
        )
    )
    target = StorageClient(
        StorageClientConfig(
            profile="target", storage_provider=PosixFileStorageProvider(base_path=str(tmp_path / "target"))
        )
    )
    source.write("file", b"legacy sync")
    download = source.download_files
    upload = target.upload_files

    def legacy_download(remote_paths, local_paths, metadata=None, max_workers=16):
        download(remote_paths, local_paths, metadata, max_workers)

    def legacy_upload(remote_paths, local_paths, attributes=None, max_workers=16):
        upload(remote_paths, local_paths, attributes, max_workers)

    monkeypatch.setattr(source, "download_files", legacy_download)
    monkeypatch.setattr(target, "upload_files", legacy_upload)
    monkeypatch.setattr(source, "_is_posix_file_storage_provider", lambda: direction == "upload")
    monkeypatch.setattr(target, "_is_posix_file_storage_provider", lambda: direction == "download")
    observer = Mock()
    progress = TransferProgress(lambda snapshot: None) if notify else None
    if progress is not None:
        monkeypatch.setattr(progress, "callback", Mock(return_value=observer))
    results = queue.Queue()
    errors = queue.Queue()
    handler = create_sync_handler(source, "", target, "copied/", False, results, errors, str(tmp_path), progress)

    handler.process_add_batch("test-worker", OperationBatch(OperationType.ADD, [(source.info("file"), None)]))

    assert errors.empty(), list(errors.queue)
    assert target.read("copied/file") == b"legacy sync"
    assert results.qsize() == 1
    observer.assert_not_called()


# Integration test using client API
def test_outdated_provider_through_client_api():
    """
    Test that outdated providers work through the client API.
    """
    from multistorageclient.client import StorageClient
    from multistorageclient.config import StorageClientConfig

    # Create a storage client with the outdated provider
    provider = OutdatedStorageProvider(base_path="test-bucket", provider_name="outdated")
    config = StorageClientConfig(profile="test", storage_provider=provider)
    client = StorageClient(config=config)

    # Test case 1: Application that still uses positional arguments (works)
    result = list(client.list("data/files"))
    assert len(result) == 2

    # Test case 2: Application that uses the path keyword (works)
    result = list(client.list(path="data/files"))
    assert len(result) == 2

    # Test case 3: Repeated path keyword calls continue to work
    result = list(client.list(path="data/files"))
    assert len(result) == 2


def test_outdated_provider_list_objects_through_provider_api():
    """Test that an outdated storage provider works correctly through the provider API."""
    provider = OutdatedStorageProvider(base_path="bucket", provider_name="outdated")

    # Test case 1: Use positional arguments (works)
    result = list(provider.list_objects("test/path"))
    assert len(result) == 2

    # Test case 2: Use of the "prefix" parameter would fail because the BaseStorageProvider.list_objects expects "path", not "prefix"
    # We don't expect this API to be used directly by the application, that's why we don't support backward compatibility for this case.
    with pytest.raises(TypeError):
        list(provider.list_objects(prefix="test/path"))  # pyright: ignore [reportCallIssue]

    # Test case 3: Use of the new "path" parameter (works)
    result = list(provider.list_objects(path="test/path"))
    assert len(result) == 2
