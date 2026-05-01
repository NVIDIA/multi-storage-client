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
import shutil
from datetime import datetime
from typing import Optional, cast
from unittest import mock

import pytest

from multistorageclient.client import StorageClient
from multistorageclient.sync import SyncManager
from multistorageclient.types import ObjectMetadata, SyncError


class MockStorageClient:
    def list(self, **kwargs):
        raise Exception("No Such Method")

    def list_recursive(self, **kwargs):
        return self.list(**kwargs)

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        pass

    def _is_rust_client_enabled(self) -> bool:
        return False

    def _is_posix_file_storage_provider(self) -> bool:
        return False


def test_sync_function_return_producer_error():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    manager = SyncManager(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
    )
    with pytest.raises(SyncError, match="Errors in sync operation:"):
        manager.sync_objects()


def test_sync_objects_commits_metadata_by_default():
    """Test that sync_objects calls commit_metadata when commit_metadata is True (default)."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
    ]
    target_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0)),
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    manager = SyncManager(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
    )

    with mock.patch.object(target_client, "commit_metadata") as mock_commit:
        manager.sync_objects(num_worker_processes=1, commit_metadata=True)
        mock_commit.assert_called_once()


def test_sync_objects_skips_commit_when_commit_metadata_false():
    """Test that sync_objects does NOT call commit_metadata when commit_metadata is False."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
    ]
    target_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0)),
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    manager = SyncManager(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
    )

    with mock.patch.object(target_client, "commit_metadata") as mock_commit:
        manager.sync_objects(num_worker_processes=1, commit_metadata=False)
        mock_commit.assert_not_called()


def _read_jsonl_keys(path: str) -> list[str]:
    """Read a JSONL file and return the 'key' field from each line."""
    keys = []
    with open(path) as f:
        for line in f:
            keys.append(json.loads(line)["key"])
    return keys


def _count_jsonl_lines(path: str) -> int:
    with open(path) as f:
        return sum(1 for _ in f)


class TestSyncObjectsDryrun:
    """Tests for dryrun mode in SyncManager.sync_objects."""

    def _cleanup(self, result):
        if result.dryrun:
            shutil.rmtree(os.path.dirname(result.dryrun.files_to_add), ignore_errors=True)

    def test_dryrun_files_to_add(self):
        """Dryrun writes JSONL for files that exist in source but not in target."""
        source_client = MockStorageClient()
        target_client = MockStorageClient()

        source_files = [
            ObjectMetadata(key="a.txt", content_length=100, last_modified=datetime(2025, 1, 1)),
            ObjectMetadata(key="b.txt", content_length=200, last_modified=datetime(2025, 1, 2)),
        ]

        source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
        target_client.list = lambda **kwargs: iter([])  # type: ignore

        manager = SyncManager(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
        )

        result = manager.sync_objects(dryrun=True)
        try:
            assert result.total_files_added == 2
            assert result.total_bytes_added == 300
            assert result.total_files_deleted == 0
            assert result.total_bytes_deleted == 0
            assert result.dryrun is not None
            assert _count_jsonl_lines(result.dryrun.files_to_add) == 2
            assert _count_jsonl_lines(result.dryrun.files_to_delete) == 0
            assert set(_read_jsonl_keys(result.dryrun.files_to_add)) == {"a.txt", "b.txt"}
        finally:
            self._cleanup(result)

    def test_dryrun_files_to_delete(self):
        """Dryrun writes JSONL for files to delete when delete_unmatched_files is True."""
        source_client = MockStorageClient()
        target_client = MockStorageClient()

        target_files = [
            ObjectMetadata(key="old.txt", content_length=50, last_modified=datetime(2025, 1, 1)),
        ]

        source_client.list = lambda **kwargs: iter([])  # type: ignore
        target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

        manager = SyncManager(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
        )

        result = manager.sync_objects(dryrun=True, delete_unmatched_files=True)
        try:
            assert result.total_files_added == 0
            assert result.total_files_deleted == 1
            assert result.total_bytes_deleted == 50
            assert result.dryrun is not None
            assert _count_jsonl_lines(result.dryrun.files_to_add) == 0
            assert _count_jsonl_lines(result.dryrun.files_to_delete) == 1
            assert _read_jsonl_keys(result.dryrun.files_to_delete) == ["old.txt"]
        finally:
            self._cleanup(result)

    def test_dryrun_mixed_add_and_delete(self):
        """Dryrun writes both add and delete JSONL entries in a mixed scenario."""
        source_client = MockStorageClient()
        target_client = MockStorageClient()

        source_files = [
            ObjectMetadata(key="new.txt", content_length=100, last_modified=datetime(2025, 1, 2)),
            ObjectMetadata(key="same.txt", content_length=100, last_modified=datetime(2025, 1, 1)),
        ]
        target_files = [
            ObjectMetadata(key="old.txt", content_length=50, last_modified=datetime(2025, 1, 1)),
            ObjectMetadata(key="same.txt", content_length=100, last_modified=datetime(2025, 1, 1)),
        ]

        source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
        target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

        manager = SyncManager(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
        )

        result = manager.sync_objects(dryrun=True, delete_unmatched_files=True)
        try:
            assert result.total_files_added == 1
            assert result.total_files_deleted == 1
            assert result.total_bytes_added == 100
            assert result.total_bytes_deleted == 50
            assert result.dryrun is not None
            assert _read_jsonl_keys(result.dryrun.files_to_add) == ["new.txt"]
            assert _read_jsonl_keys(result.dryrun.files_to_delete) == ["old.txt"]
        finally:
            self._cleanup(result)

    def test_dryrun_all_up_to_date(self):
        """Dryrun produces empty JSONL files when source and target are in sync."""
        source_client = MockStorageClient()
        target_client = MockStorageClient()

        source_files = [
            ObjectMetadata(key="file.txt", content_length=100, last_modified=datetime(2025, 1, 1)),
        ]
        target_files = [
            ObjectMetadata(key="file.txt", content_length=100, last_modified=datetime(2025, 1, 1)),
        ]

        source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
        target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

        manager = SyncManager(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
        )

        result = manager.sync_objects(dryrun=True)
        try:
            assert result.total_files_added == 0
            assert result.total_files_deleted == 0
            assert result.total_work_units == 1
            assert result.dryrun is not None
            assert _count_jsonl_lines(result.dryrun.files_to_add) == 0
            assert _count_jsonl_lines(result.dryrun.files_to_delete) == 0
        finally:
            self._cleanup(result)

    def test_dryrun_does_not_commit_metadata(self):
        """Dryrun never calls commit_metadata regardless of the commit_metadata flag."""
        source_client = MockStorageClient()
        target_client = MockStorageClient()

        source_client.list = lambda **kwargs: iter([])  # type: ignore
        target_client.list = lambda **kwargs: iter([])  # type: ignore

        manager = SyncManager(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
        )

        with mock.patch.object(target_client, "commit_metadata") as mock_commit:
            result = manager.sync_objects(dryrun=True, commit_metadata=True)
            try:
                mock_commit.assert_not_called()
            finally:
                self._cleanup(result)

    def test_dryrun_propagates_producer_error(self):
        """Dryrun raises SyncError when the producer thread encounters an error."""
        source_client = MockStorageClient()
        target_client = MockStorageClient()

        manager = SyncManager(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
        )

        with pytest.raises(SyncError, match="Errors in dryrun sync operation:") as exc_info:
            manager.sync_objects(dryrun=True)

        if exc_info.value.sync_result.dryrun:
            shutil.rmtree(os.path.dirname(exc_info.value.sync_result.dryrun.files_to_add), ignore_errors=True)

    def test_dryrun_detects_modified_files(self):
        """Dryrun reports files that exist in both but have different size or timestamp."""
        source_client = MockStorageClient()
        target_client = MockStorageClient()

        source_files = [
            ObjectMetadata(key="changed.txt", content_length=200, last_modified=datetime(2025, 1, 2)),
        ]
        target_files = [
            ObjectMetadata(key="changed.txt", content_length=100, last_modified=datetime(2025, 1, 1)),
        ]

        source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
        target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

        manager = SyncManager(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
        )

        result = manager.sync_objects(dryrun=True)
        try:
            assert result.total_files_added == 1
            assert result.total_bytes_added == 200
            assert result.dryrun is not None
            assert _count_jsonl_lines(result.dryrun.files_to_add) == 1
            assert _read_jsonl_keys(result.dryrun.files_to_add) == ["changed.txt"]
        finally:
            self._cleanup(result)

    def test_dryrun_no_delete_without_flag(self):
        """Dryrun does not report deletions unless delete_unmatched_files is True."""
        source_client = MockStorageClient()
        target_client = MockStorageClient()

        target_files = [
            ObjectMetadata(key="extra.txt", content_length=50, last_modified=datetime(2025, 1, 1)),
        ]

        source_client.list = lambda **kwargs: iter([])  # type: ignore
        target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

        manager = SyncManager(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
        )

        result = manager.sync_objects(dryrun=True, delete_unmatched_files=False)
        try:
            assert result.total_files_deleted == 0
            assert result.dryrun is not None
            assert _count_jsonl_lines(result.dryrun.files_to_delete) == 0
        finally:
            self._cleanup(result)

    def test_dryrun_jsonl_roundtrips_through_object_metadata(self):
        """JSONL entries can be deserialized back to ObjectMetadata via from_dict."""
        source_client = MockStorageClient()
        target_client = MockStorageClient()

        source_files = [
            ObjectMetadata(key="data.bin", content_length=4096, last_modified=datetime(2025, 6, 15, 12, 30, 0)),
        ]

        source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
        target_client.list = lambda **kwargs: iter([])  # type: ignore

        manager = SyncManager(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
        )

        result = manager.sync_objects(dryrun=True)
        try:
            assert result.dryrun is not None
            with open(result.dryrun.files_to_add) as f:
                record = json.loads(f.readline())
            restored = ObjectMetadata.from_dict(record)
            assert restored.key == "data.bin"
            assert restored.content_length == 4096
        finally:
            self._cleanup(result)

    def test_dryrun_output_path(self, tmp_path):
        """Dryrun writes JSONL files to the user-specified output directory."""
        source_client = MockStorageClient()
        target_client = MockStorageClient()

        source_files = [
            ObjectMetadata(key="a.txt", content_length=100, last_modified=datetime(2025, 1, 1)),
        ]

        source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
        target_client.list = lambda **kwargs: iter([])  # type: ignore

        output_dir = str(tmp_path / "my_dryrun")

        manager = SyncManager(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
        )

        result = manager.sync_objects(dryrun=True, dryrun_output_path=output_dir)

        assert result.dryrun is not None
        assert result.dryrun.files_to_add == os.path.join(output_dir, "files_to_add.jsonl")
        assert result.dryrun.files_to_delete == os.path.join(output_dir, "files_to_delete.jsonl")
        assert os.path.isfile(result.dryrun.files_to_add)
        assert os.path.isfile(result.dryrun.files_to_delete)
        assert _read_jsonl_keys(result.dryrun.files_to_add) == ["a.txt"]
        assert _count_jsonl_lines(result.dryrun.files_to_delete) == 0
