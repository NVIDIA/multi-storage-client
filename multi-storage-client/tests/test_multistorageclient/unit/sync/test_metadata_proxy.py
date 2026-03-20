# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
from typing import Optional, cast
from unittest.mock import MagicMock

import pytest

import multistorageclient as msc
from multistorageclient.sync.metadata_proxy import QueueBackedMetadataProvider
from multistorageclient.sync.types import OperationType
from multistorageclient.types import MetadataProvider, ObjectMetadata, ResolvedPath, ResolvedPathState
from test_multistorageclient.unit.utils import config, tempdatastore


def _make_metadata(key: str = "file.bin", size: int = 100) -> ObjectMetadata:
    return ObjectMetadata(
        key=key,
        content_length=size,
        last_modified=datetime.now(timezone.utc),
        etag="abc123",
    )


def test_add_file_delegates_and_queues():
    delegate = MagicMock()
    result_queue: queue.Queue = queue.Queue()
    proxy = QueueBackedMetadataProvider(delegate, result_queue)

    metadata = _make_metadata("bucket/prefix/file.bin-uuid", size=42)
    proxy.add_file("file.bin", metadata)

    delegate.add_file.assert_called_once_with("file.bin", metadata)

    op, path, queued_metadata = result_queue.get_nowait()
    assert op == OperationType.ADD
    assert path == "file.bin"
    assert queued_metadata.key == metadata.key
    assert queued_metadata.content_length == metadata.content_length
    assert queued_metadata.last_modified == metadata.last_modified
    assert queued_metadata.metadata == metadata.metadata


def test_remove_file_delegates_without_queuing():
    delegate = MagicMock()
    result_queue: queue.Queue = queue.Queue()
    proxy = QueueBackedMetadataProvider(delegate, result_queue)

    proxy.remove_file("file.bin")

    delegate.remove_file.assert_called_once_with("file.bin")
    assert result_queue.empty()


def test_read_operations_delegate():
    delegate = MagicMock()
    result_queue: queue.Queue = queue.Queue()
    proxy = QueueBackedMetadataProvider(delegate, result_queue)

    proxy.realpath("file.bin")
    assert delegate.realpath.called

    proxy.generate_physical_path("file.bin", for_overwrite=True)
    assert delegate.generate_physical_path.called

    proxy.list_objects("prefix/")
    assert delegate.list_objects.called

    proxy.get_object_metadata("file.bin", include_pending=True)
    assert delegate.get_object_metadata.called

    proxy.glob("*.bin")
    assert delegate.glob.called

    proxy.commit_updates()
    assert delegate.commit_updates.called

    proxy.is_writable()
    assert delegate.is_writable.called

    proxy.allow_overwrites()
    assert delegate.allow_overwrites.called

    proxy.should_use_soft_delete()
    assert delegate.should_use_soft_delete.called

    assert result_queue.empty()


def test_multiple_add_files_queue_in_order():
    delegate = MagicMock()
    result_queue: queue.Queue = queue.Queue()
    proxy = QueueBackedMetadataProvider(delegate, result_queue)

    meta1 = _make_metadata("phys/a.bin", size=10)
    meta2 = _make_metadata("phys/b.bin", size=20)

    proxy.add_file("a.bin", meta1)
    proxy.add_file("b.bin", meta2)

    op1, path1, m1 = result_queue.get_nowait()
    op2, path2, m2 = result_queue.get_nowait()

    assert (op1, path1, m1) == (OperationType.ADD, "a.bin", meta1)
    assert (op2, path2, m2) == (OperationType.ADD, "b.bin", meta2)
    assert result_queue.empty()


class _NoOpMutatingDelegate(MetadataProvider):
    """No-op provider that mutates the metadata dict in add_file (simulates nvdataset-style behavior)."""

    def add_file(self, path: str, metadata: ObjectMetadata) -> None:
        if getattr(metadata, "metadata", None) is not None:
            metadata.metadata = {}

    def remove_file(self, path: str) -> None:
        pass

    def commit_updates(self) -> None:
        pass

    def list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        attribute_filter_expression: Optional[str] = None,
        show_attributes: bool = False,
    ) -> Iterator[ObjectMetadata]:
        return iter([])

    def get_object_metadata(self, path: str, include_pending: bool = False) -> ObjectMetadata:
        raise FileNotFoundError(path)

    def glob(self, pattern: str, attribute_filter_expression: Optional[str] = None) -> list[str]:
        return []

    def realpath(self, logical_path: str) -> ResolvedPath:
        return ResolvedPath(physical_path=logical_path, state=ResolvedPathState.UNTRACKED, profile=None)

    def generate_physical_path(self, logical_path: str, for_overwrite: bool = False) -> ResolvedPath:
        return self.realpath(logical_path)

    def is_writable(self) -> bool:
        return True

    def allow_overwrites(self) -> bool:
        return True

    def should_use_soft_delete(self) -> bool:
        return False


def test_queue_backed_metadata_provider_replay_sees_unmutated_metadata():
    """Reproduces the bug: QueueBackedMetadataProvider puts the same object it passes to the delegate.

    When the delegate mutates that object (e.g. clears metadata.metadata), the same mutated object
    is on the result queue, so the replay sees empty metadata. This test asserts the contract: the
    queued message must contain metadata with custom attributes intact (replay must not see
    delegate mutation). With current code this test fails.
    """
    result_queue = queue.Queue()
    delegate = _NoOpMutatingDelegate()
    proxy = QueueBackedMetadataProvider(delegate, result_queue)

    meta = ObjectMetadata(
        key="dest/file.txt",
        content_length=10,
        last_modified=datetime.now(timezone.utc),
        metadata={"extra_int": "1", "extra_str": "group1"},
    )
    proxy.add_file("dest/file.txt", meta)

    op, path, physical_metadata = result_queue.get_nowait()
    assert op == OperationType.ADD
    assert path == "dest/file.txt"
    assert (physical_metadata.metadata or {}) != {}, (
        "Queued metadata was mutated by delegate; replay would see empty metadata. "
        "QueueBackedMetadataProvider must put a copy so replay is unaffected by delegate mutation."
    )
    assert physical_metadata.metadata.get("extra_int") == "1"
    assert physical_metadata.metadata.get("extra_str") == "group1"


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_overwrite_preserves_physical_path(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Regression test (single-process): sync with overwrite must read back the new content.

    Reproduces the bug documented in METADATA_UPDATE_IN_SYNC.md where the
    monitor's add_file() reconstructed the original (non-UUID) physical path,
    causing reads after overwrite to return stale content.

    Uses S3 storage with a custom metadata provider that generates UUID
    physical paths, exercising the remote write path (write() -> add_file())
    where the proxy intercepts and queues the correct physical path.
    """
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        tempdatastore.TemporaryPOSIXDirectory() as source_store,
        temp_data_store_type() as target_store,
    ):
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "src": source_store.profile_config_dict(),
                    "tgt": target_store.profile_config_dict(),
                }
            }
        )

        source_client, _ = msc.resolve_storage_client("msc://src")
        target_client, _ = msc.resolve_storage_client("msc://tgt")

        target_client._metadata_provider = cast(MetadataProvider, _OverwriteMetadataProvider())

        original_content = b"AAAA"
        updated_content = b"BB"
        file_key = "data.bin"

        target_client.write(file_key, original_content)
        target_client.commit_metadata()
        assert target_client.read(file_key) == original_content

        original_physical = target_client._metadata_provider.realpath(file_key).physical_path

        source_client.write(file_key, updated_content)

        target_client.sync_from(source_client, "", "")

        assert target_client.read(file_key) == updated_content, (
            "After sync overwrite, reading the file must return the new content, not stale data"
        )

        new_physical = target_client._metadata_provider.realpath(file_key).physical_path
        assert new_physical != original_physical, "Physical path must change on overwrite to avoid stale reads"


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_sync_overwrite_multiprocess(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Regression test (multi-process): sync overwrite with num_worker_processes=2.

    Validates that metadata updates survive the process boundary.  In multi-
    process mode, workers get pickled copies of target_client.  The proxy is
    installed after deserialization, intercepts add_file() inside write(), and
    queues the result back to the main process's monitor which replays it on
    the real metadata provider.

    Uses ManifestMetadataProvider (config-driven, survives pickling) with
    allow_overwrites=True so that the overwrite path inside write() is exercised.
    """
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with (
        tempdatastore.TemporaryPOSIXDirectory() as source_store,
        temp_data_store_type() as target_store,
    ):
        target_profile = target_store.profile_config_dict()
        target_profile.update(
            {
                "manifest_base_dir": ".msc_manifests",
                "use_manifest_metadata": True,
                "allow_overwrites": True,
            }
        )

        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "src": source_store.profile_config_dict(),
                    "tgt": target_profile,
                }
            }
        )

        source_client, _ = msc.resolve_storage_client("msc://src")
        target_client, _ = msc.resolve_storage_client("msc://tgt")

        original_content = b"AAAA"
        updated_content = b"BB"
        file_key = "data.bin"

        target_client.write(file_key, original_content)
        target_client.commit_metadata()
        assert target_client.read(file_key) == original_content

        source_client.write(file_key, updated_content)

        result = target_client.sync_from(
            source_client,
            "",
            "",
            num_worker_processes=2,
        )

        assert result.total_files_added == 1
        assert target_client.read(file_key) == updated_content, (
            "After multi-process sync overwrite, reading the file must return the new content"
        )


class _OverwriteMetadataProvider:
    """Minimal metadata provider that generates UUID physical paths for overwrites."""

    def __init__(self):
        self._files: dict[str, ObjectMetadata] = {}
        self._physical_paths: dict[str, str] = {}
        self._pending_adds: dict[str, tuple[str, ObjectMetadata]] = {}
        self._pending_removes: set[str] = set()
        self._deleted: set[str] = set()

    def list_objects(
        self,
        path="",
        start_after=None,
        end_at=None,
        include_directories=False,
        attribute_filter_expression=None,
        show_attributes=False,
    ):
        for key in sorted(self._files):
            if key.startswith(path) and key not in self._deleted:
                yield self._files[key]

    def get_object_metadata(self, path, include_pending=False):
        if include_pending and path in self._pending_adds:
            phys, meta = self._pending_adds[path]
            return ObjectMetadata(
                key=path,
                content_length=meta.content_length,
                last_modified=meta.last_modified,
                etag=meta.etag,
            )
        if path in self._files and path not in self._deleted:
            return self._files[path]
        raise FileNotFoundError(path)

    def glob(self, pattern, **kwargs):
        import fnmatch

        return [k for k in self._files if fnmatch.fnmatch(k, pattern) and k not in self._deleted]

    def realpath(self, logical_path):
        if logical_path in self._deleted:
            return ResolvedPath(physical_path="", state=ResolvedPathState.DELETED, profile=None)
        if logical_path in self._physical_paths:
            return ResolvedPath(
                physical_path=self._physical_paths[logical_path], state=ResolvedPathState.EXISTS, profile=None
            )
        return ResolvedPath(physical_path="", state=ResolvedPathState.UNTRACKED, profile=None)

    def generate_physical_path(self, logical_path, for_overwrite=False):
        import uuid as _uuid

        return ResolvedPath(physical_path=str(_uuid.uuid4()), state=ResolvedPathState.UNTRACKED, profile=None)

    def add_file(self, path, metadata):
        self._pending_adds[path] = (metadata.key, metadata)

    def remove_file(self, path):
        self._pending_removes.add(path)

    def commit_updates(self):
        for path in self._pending_removes:
            self._deleted.add(path)
        for logical, (physical, meta) in self._pending_adds.items():
            self._physical_paths[logical] = physical
            self._files[logical] = ObjectMetadata(
                key=logical,
                content_length=meta.content_length,
                last_modified=meta.last_modified,
                etag=meta.etag,
            )
            self._deleted.discard(logical)
        self._pending_adds.clear()
        self._pending_removes.clear()

    def is_writable(self):
        return True

    def allow_overwrites(self):
        return True

    def should_use_soft_delete(self):
        return False
