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
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

import multistorageclient as msc
from multistorageclient.client import SingleStorageClient, StorageClient
from multistorageclient.client.composite import CompositeStorageClient
from multistorageclient.config import StorageClientConfig
from multistorageclient.sync.producer import (
    MAX_BATCH_SIZE,
    MIN_BATCH_SIZE,
    MSC_SYNC_DISABLE_PARALLEL_LISTING_ENV,
    ProducerThread,
)
from multistorageclient.sync.progress_bar import ProgressBar
from multistorageclient.sync.types import OperationType
from multistorageclient.types import ObjectMetadata, PatternType, SymlinkHandling
from multistorageclient.utils import NullStorageClient, PatternMatcher
from test_multistorageclient.unit.utils import config


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
    def __init__(self):
        self._metadata_provider: object | None = None

    def list(self, **kwargs):
        raise Exception("No Such Method")

    def list_recursive(self, **kwargs):
        return self.list(**kwargs)

    def commit_metadata(self, prefix: str | None = None) -> None:
        pass

    def _is_rust_client_enabled(self) -> bool:
        return False

    def _is_posix_file_storage_provider(self) -> bool:
        return False


class RecordingProgress:
    def __init__(self):
        self.total_updates: list[int] = []

    def update_total(self, new_total: int) -> None:
        self.total_updates.append(new_total)

    def update_progress(self, items_completed: int = 1) -> None:
        pass


def test_match_file_metadata_seconds_resolution():
    """_match_file_metadata compares last_modified at seconds resolution."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()
    producer = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
    )
    base = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    same_size = 100

    # Same second, different microseconds: should match (treated as equal at second resolution)
    source_info = ObjectMetadata(key="a", content_length=same_size, last_modified=base.replace(microsecond=500000))
    target_info = ObjectMetadata(key="a", content_length=same_size, last_modified=base.replace(microsecond=100000))
    assert producer._match_file_metadata(source_info, target_info) is True

    # Target one second newer: should match
    source_info = ObjectMetadata(key="a", content_length=same_size, last_modified=base)
    target_info = ObjectMetadata(
        key="a", content_length=same_size, last_modified=datetime(2025, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
    )
    assert producer._match_file_metadata(source_info, target_info) is True

    # Target one second older: should not match
    source_info = ObjectMetadata(
        key="a", content_length=same_size, last_modified=datetime(2025, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
    )
    target_info = ObjectMetadata(key="a", content_length=same_size, last_modified=base)
    assert producer._match_file_metadata(source_info, target_info) is False

    # Different size: should not match regardless of timestamp
    source_info = ObjectMetadata(key="a", content_length=100, last_modified=base)
    target_info = ObjectMetadata(key="a", content_length=200, last_modified=base)
    assert producer._match_file_metadata(source_info, target_info) is False


@pytest.mark.parametrize(
    argnames=["preserve_source_attributes", "has_metadata_provider", "expected_match"],
    argvalues=[[True, True, False], [True, False, True], [False, True, True]],
)
def test_match_file_metadata_attribute_changes_depend_on_metadata_provider(
    preserve_source_attributes: bool,
    has_metadata_provider: bool,
    expected_match: bool,
):
    """Attribute deltas only affect matching when preserve_source_attributes and metadata provider are enabled."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()
    if has_metadata_provider:
        target_client._metadata_provider = object()

    producer = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
        preserve_source_attributes=preserve_source_attributes,
    )
    base = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    source_info = ObjectMetadata(
        key="a",
        content_length=100,
        last_modified=base.replace(microsecond=600000),
        metadata={"version": "2"},
    )
    target_info = ObjectMetadata(
        key="a",
        content_length=100,
        last_modified=base.replace(microsecond=500000),
        metadata={"version": "1"},
    )
    assert producer._match_file_metadata(source_info, target_info) is expected_match


@pytest.mark.parametrize(
    argnames=["preserve_source_attributes", "has_metadata_provider", "expected_show_attributes"],
    argvalues=[[True, True, True], [True, False, False], [False, True, False]],
)
def test_target_listing_requests_attributes_only_when_needed(
    preserve_source_attributes: bool,
    has_metadata_provider: bool,
    expected_show_attributes: bool,
):
    source_client = MockStorageClient()
    target_client = MockStorageClient()
    if has_metadata_provider:
        target_client._metadata_provider = object()

    source_files = [
        ObjectMetadata(
            key="file1.txt",
            content_length=100,
            last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            metadata={"version": "1"},
        )
    ]
    target_files = [
        ObjectMetadata(
            key="file1.txt",
            content_length=100,
            last_modified=datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
            metadata={"version": "1"},
        )
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    list_kwargs: dict = {}

    def mock_target_list(**kwargs):
        list_kwargs.update(kwargs)
        return iter(target_files)

    target_client.list = mock_target_list  # type: ignore

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
        preserve_source_attributes=preserve_source_attributes,
    )
    producer_thread.start()
    producer_thread.join()

    assert producer_thread.error is None
    assert list_kwargs.get("show_attributes", False) is expected_show_attributes


def test_listing_uses_list_recursive_by_default():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(
            key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        )
    ]
    target_files = [
        ObjectMetadata(
            key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        )
    ]
    list_recursive_calls = []

    def source_list_recursive(**kwargs):
        list_recursive_calls.append(("source", kwargs))
        return iter(source_files)

    def target_list_recursive(**kwargs):
        list_recursive_calls.append(("target", kwargs))
        return iter(target_files)

    source_client.list_recursive = source_list_recursive  # type: ignore[attr-defined]
    target_client.list_recursive = target_list_recursive  # type: ignore[attr-defined]

    producer = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
    )

    assert list(producer._create_listing_iterator(cast(StorageClient, source_client), "source/")) == source_files
    assert list(producer._create_listing_iterator(cast(StorageClient, target_client), "target/")) == target_files
    assert list_recursive_calls == [
        ("source", {"path": "source/", "symlink_handling": producer.symlink_handling}),
        ("target", {"path": "target/", "symlink_handling": producer.symlink_handling}),
    ]


def test_listing_uses_list_when_parallel_listing_disabled(monkeypatch):
    monkeypatch.setenv(MSC_SYNC_DISABLE_PARALLEL_LISTING_ENV, "true")
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(
            key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        )
    ]
    target_files = [
        ObjectMetadata(
            key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        )
    ]
    list_calls = []

    def source_list(**kwargs):
        list_calls.append(("source", kwargs))
        return iter(source_files)

    def target_list(**kwargs):
        list_calls.append(("target", kwargs))
        return iter(target_files)

    def list_recursive(**kwargs):
        raise AssertionError("list_recursive should not be called")

    source_client.list = source_list  # type: ignore[method-assign]
    target_client.list = target_list  # type: ignore[method-assign]
    source_client.list_recursive = list_recursive  # type: ignore[attr-defined]
    target_client.list_recursive = list_recursive  # type: ignore[attr-defined]

    producer = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
    )

    assert list(producer._create_listing_iterator(cast(StorageClient, source_client), "source/")) == source_files
    assert list(producer._create_listing_iterator(cast(StorageClient, target_client), "target/")) == target_files
    assert list_calls == [
        ("source", {"path": "source/", "show_attributes": False, "symlink_handling": producer.symlink_handling}),
        ("target", {"path": "target/", "show_attributes": False, "symlink_handling": producer.symlink_handling}),
    ]


def test_listing_falls_back_to_list_when_attributes_are_requested():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(
            key="file1.txt",
            content_length=100,
            last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            metadata={"version": "1"},
        )
    ]
    target_files = [
        ObjectMetadata(
            key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        )
    ]
    source_list_kwargs = {}

    def source_list(**kwargs):
        source_list_kwargs.update(kwargs)
        return iter(source_files)

    def list_recursive(**kwargs):
        raise AssertionError("source list_recursive should not be called when attributes are requested")

    source_client.list = source_list  # type: ignore[method-assign]
    source_client.list_recursive = list_recursive  # type: ignore[attr-defined]
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore[method-assign]

    producer = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
        preserve_source_attributes=True,
    )

    assert (
        list(producer._create_listing_iterator(cast(StorageClient, source_client), "", show_attributes=True))
        == source_files
    )
    assert source_list_kwargs["show_attributes"] is True


def test_source_files_iterator_is_sorted_and_deduplicated():
    """source_files are yielded in key order (required by the merge loop) without duplicates."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()
    seen: list[str] = []

    def info(path: str, strict: bool = True) -> ObjectMetadata:
        seen.append(path)
        return ObjectMetadata(key=path, content_length=1, last_modified=datetime(2025, 1, 1, tzinfo=timezone.utc))

    source_client.info = info  # type: ignore

    producer = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="data",
        target_client=cast(StorageClient, target_client),
        target_path="backup",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
        source_files=["b.txt", "/dir/c.txt", "a.txt", "b.txt"],
    )

    keys = [metadata.key for metadata in producer._create_source_iterator()]

    assert keys == ["data/a.txt", "data/b.txt", "data/dir/c.txt"]
    assert seen == keys


@pytest.mark.parametrize("symlink_handling", list(SymlinkHandling))
@pytest.mark.parametrize("preserve_source_attributes", [False, True])
def test_source_files_symlink_handling(
    tmp_path: Path, symlink_handling: SymlinkHandling, preserve_source_attributes: bool
):
    source_client = StorageClient(
        StorageClientConfig.from_dict(
            {"profiles": {"source": {"storage_provider": {"type": "file", "options": {"base_path": str(tmp_path)}}}}},
            profile="source",
        )
    )
    source_client.write("src/target.txt", b"target contents", attributes={"label": "target"})
    source_client.write("src/plain.txt", b"plain")
    (tmp_path / "src" / "alias.txt").symlink_to("target.txt")
    (tmp_path / "src" / "link.txt").symlink_to("alias.txt")
    producer = ProducerThread(
        source_client=source_client,
        source_path="src",
        target_client=cast(StorageClient, MockStorageClient()),
        target_path="backup",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
        source_files=["plain.txt", "link.txt", "link.txt"],
        symlink_handling=symlink_handling,
        preserve_source_attributes=preserve_source_attributes,
    )

    entries = list(producer._create_source_iterator())

    if symlink_handling == SymlinkHandling.SKIP:
        assert [entry.key for entry in entries] == ["src/plain.txt"]
    else:
        assert [entry.key for entry in entries] == ["src/link.txt", "src/plain.txt"]
        link = entries[0]
        assert link.metadata == ({"label": "target"} if preserve_source_attributes else None)
        if symlink_handling in (SymlinkHandling.FOLLOW, SymlinkHandling.FOLLOW_STRICT):
            assert link.symlink_target is None
            assert link.content_length == len(b"target contents")
            assert link.last_modified == source_client.info("src/target.txt").last_modified
        else:
            assert link.symlink_target == "alias.txt"


@pytest.mark.parametrize("symlink_handling", list(SymlinkHandling))
def test_source_files_external_symlink_handling(tmp_path: Path, symlink_handling: SymlinkHandling):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (tmp_path / "external.txt").write_bytes(b"external contents")
    (source_dir / "link.txt").symlink_to(tmp_path / "external.txt")
    source_client = StorageClient(
        StorageClientConfig.from_dict(
            {"profiles": {"source": {"storage_provider": {"type": "file", "options": {"base_path": str(source_dir)}}}}},
            profile="source",
        )
    )
    producer = ProducerThread(
        source_client=source_client,
        source_path="",
        target_client=cast(StorageClient, MockStorageClient()),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
        source_files=["link.txt"],
        symlink_handling=symlink_handling,
    )

    if symlink_handling in (SymlinkHandling.FOLLOW_STRICT, SymlinkHandling.PRESERVE_STRICT):
        with pytest.raises(ValueError, match="outside the base directory"):
            list(producer._create_source_iterator())
    else:
        assert list(producer._create_source_iterator()) == []


@pytest.mark.parametrize("symlink_handling", list(SymlinkHandling))
def test_source_files_non_posix_symlinks_are_preserved(symlink_handling: SymlinkHandling):
    source_client = MockStorageClient()

    def info(path: str, strict: bool = True) -> ObjectMetadata:
        return ObjectMetadata(
            key=path,
            content_length=0,
            last_modified=datetime(2025, 1, 1, tzinfo=timezone.utc),
            symlink_target="target.txt",
        )

    source_client.info = info  # type: ignore
    producer = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, MockStorageClient()),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
        source_files=["link.txt"],
        symlink_handling=symlink_handling,
    )

    entries = list(producer._create_source_iterator())

    assert len(entries) == 1
    assert entries[0].key == "link.txt"
    assert entries[0].symlink_target == "target.txt"


def test_batch_size_validation():
    """Test that batch_size must be between MIN_BATCH_SIZE and MAX_BATCH_SIZE."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    # Test batch size too small
    try:
        ProducerThread(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
            progress=ProgressBar(desc="", show_progress=False),
            file_queue=queue.Queue(),
            num_workers=1,
            shutdown_event=threading.Event(),
            batch_size=5,
        )
        assert False, "Expected ValueError for batch_size too small"
    except ValueError as e:
        assert f"batch_size must be between {MIN_BATCH_SIZE} and {MAX_BATCH_SIZE}" in str(e)

    # Test batch size too large
    try:
        ProducerThread(
            source_client=cast(StorageClient, source_client),
            source_path="",
            target_client=cast(StorageClient, target_client),
            target_path="",
            progress=ProgressBar(desc="", show_progress=False),
            file_queue=queue.Queue(),
            num_workers=1,
            shutdown_event=threading.Event(),
            batch_size=2000,
        )
        assert False, "Expected ValueError for batch_size too large"
    except ValueError as e:
        assert f"batch_size must be between {MIN_BATCH_SIZE} and {MAX_BATCH_SIZE}" in str(e)


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
        shutdown_event=threading.Event(),
    )

    producer_thread.start()
    producer_thread.join()

    assert not producer_thread.is_alive()
    assert producer_thread.error is not None


def test_producer_updates_final_total():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(
            key="file0.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(())  # type: ignore

    progress = RecordingProgress()
    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=cast(ProgressBar, progress),
        file_queue=queue.Queue(),
        num_workers=1,
        shutdown_event=threading.Event(),
    )

    producer_thread.start()
    producer_thread.join()

    assert producer_thread.error is None
    assert progress.total_updates[-1] == len(source_files)


def test_progress_bar_update_in_producer_thread_without_deletion():
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(
            key="file0.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file3.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
    ]

    target_files = [
        ObjectMetadata(
            key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file4.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file5.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file6.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    progress = ProgressBar(desc="Syncing", show_progress=True)
    file_queue = queue.Queue()
    shutdown_event = threading.Event()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=progress,
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=shutdown_event,
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
        ObjectMetadata(
            key="file0.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file3.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
    ]

    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    progress = ProgressBar(desc="Syncing", show_progress=True)
    file_queue = queue.Queue()
    shutdown_event = threading.Event()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=progress,
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=shutdown_event,
        delete_unmatched_files=True,
    )

    producer_thread.start()
    producer_thread.join()

    assert producer_thread.error is None
    assert progress.pbar is not None
    assert progress.pbar.total == len(target_files)
    assert progress.pbar.n == 0


def test_progress_total_excludes_pattern_excluded_files_present_on_both_sides():
    """A changed file that the pattern excludes is neither enqueued nor counted as a work unit."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()
    old = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    new = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

    source_files = [
        ObjectMetadata(key="changed.log", content_length=100, last_modified=new),
        ObjectMetadata(key="changed.txt", content_length=100, last_modified=new),
        ObjectMetadata(key="same.log", content_length=100, last_modified=old),
    ]
    target_files = [
        ObjectMetadata(key="changed.log", content_length=100, last_modified=old),
        ObjectMetadata(key="changed.txt", content_length=100, last_modified=old),
        ObjectMetadata(key="same.log", content_length=100, last_modified=old),
    ]
    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    progress = ProgressBar(desc="Syncing", show_progress=True)
    file_queue: queue.Queue = queue.Queue()
    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=progress,
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=threading.Event(),
        pattern_matcher=PatternMatcher([(PatternType.EXCLUDE, "*.log")]),
    )

    producer_thread.start()
    producer_thread.join()

    assert producer_thread.error is None
    enqueued = []
    while True:
        batch = file_queue.get_nowait()
        if batch.operation == OperationType.STOP:
            break
        enqueued.extend(file_metadata.key for file_metadata, _ in batch.items)
    assert enqueued == ["changed.txt"]
    # One ADD plus one unchanged file that was marked complete immediately; the excluded changed file is not work.
    assert producer_thread.total_work_units == 2
    assert progress.pbar is not None
    assert progress.pbar.total == 2
    assert progress.pbar.n == 1


def test_batch_flushing_on_operation_type_change():
    """Test that batches are flushed when operation type changes."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(
            key="file0.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
    ]

    target_files = [
        ObjectMetadata(
            key="file3.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
        ObjectMetadata(
            key="file4.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ),
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    progress = ProgressBar(desc="Syncing", show_progress=False)
    file_queue = queue.Queue()
    shutdown_event = threading.Event()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=progress,
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=shutdown_event,
        delete_unmatched_files=True,
    )

    producer_thread.start()
    producer_thread.join()

    assert producer_thread.error is None

    batches = []
    while not file_queue.empty():
        batches.append(file_queue.get())

    non_stop_batches = [b for b in batches if b.operation != OperationType.STOP]
    assert len(non_stop_batches) == 2

    assert non_stop_batches[0].operation == OperationType.ADD
    assert len(non_stop_batches[0].items) == 3

    assert non_stop_batches[1].operation == OperationType.DELETE
    assert len(non_stop_batches[1].items) == 2


def test_producer_thread_with_shutdown_event():
    """Test ProducerThread respects shutdown event."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    # Create long list of files to ensure producer is interruptible
    source_files = [
        ObjectMetadata(
            key=f"file{i}.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        )
        for i in range(1_000_000)
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter([])  # type: ignore

    progress = ProgressBar(desc="Syncing", show_progress=False)
    file_queue = queue.Queue()
    shutdown_event = threading.Event()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=progress,
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=shutdown_event,
    )

    producer_thread.start()

    # Let it process a few files
    time.sleep(0.01)

    # Signal shutdown
    shutdown_event.set()

    # Wait for producer to stop
    producer_thread.join(timeout=1.0)

    # Producer thread should be stopped
    assert not producer_thread.is_alive()

    # Given that producer thread is not alive, the queue should have some files left
    queue_size = file_queue.qsize()
    assert queue_size > 0


def test_batch_flushing_on_size_bucket_change():
    """Test that batches are flushed when file size bucket changes."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        # SMALL bucket (< 1MB)
        ObjectMetadata(
            key="file0_small1.txt",
            content_length=500 * 1024,
            last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ),
        ObjectMetadata(
            key="file1_small2.txt",
            content_length=800 * 1024,
            last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ),
        # MEDIUM bucket (1MB - 64MB)
        ObjectMetadata(
            key="file2_medium1.txt",
            content_length=2 * 1024 * 1024,
            last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ),
        ObjectMetadata(
            key="file3_medium2.txt",
            content_length=10 * 1024 * 1024,
            last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ),
        # LARGE bucket (64MB - 1GB)
        ObjectMetadata(
            key="file4_large1.txt",
            content_length=100 * 1024 * 1024,
            last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ),
        ObjectMetadata(
            key="file5_large2.txt",
            content_length=500 * 1024 * 1024,
            last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ),
        # VERY_LARGE bucket (> 1GB)
        ObjectMetadata(
            key="file6_vlarge1.txt",
            content_length=2 * 1024 * 1024 * 1024,
            last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ),
        ObjectMetadata(
            key="file7_vlarge2.txt",
            content_length=3 * 1024 * 1024 * 1024,
            last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ),
    ]

    target_files = []

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    progress = ProgressBar(desc="Syncing", show_progress=False)
    file_queue = queue.Queue()
    shutdown_event = threading.Event()

    producer_thread = ProducerThread(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
        progress=progress,
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=shutdown_event,
        batch_size=50,
    )

    producer_thread.start()
    producer_thread.join()

    assert producer_thread.error is None

    batches = []
    while not file_queue.empty():
        batches.append(file_queue.get())

    non_stop_batches = [b for b in batches if b.operation != OperationType.STOP]

    assert len(non_stop_batches) == 4

    assert non_stop_batches[0].operation == OperationType.ADD
    assert len(non_stop_batches[0].items) == 2
    assert all(item.content_length < 1 * 1024 * 1024 for item, _ in non_stop_batches[0].items)

    assert non_stop_batches[1].operation == OperationType.ADD
    assert len(non_stop_batches[1].items) == 2
    assert all(1 * 1024 * 1024 <= item.content_length < 64 * 1024 * 1024 for item, _ in non_stop_batches[1].items)

    assert non_stop_batches[2].operation == OperationType.ADD
    assert len(non_stop_batches[2].items) == 2
    assert all(64 * 1024 * 1024 <= item.content_length < 1024 * 1024 * 1024 for item, _ in non_stop_batches[2].items)

    assert non_stop_batches[3].operation == OperationType.ADD
    assert len(non_stop_batches[3].items) == 2
    assert all(item.content_length >= 1024 * 1024 * 1024 for item, _ in non_stop_batches[3].items)


@pytest.mark.parametrize("ignore_hidden", [False, True])
@pytest.mark.parametrize("marker_kind", ["valid", "missing", "corrupt", "unreadable"])
@pytest.mark.parametrize("explicit_files", [False, True])
@pytest.mark.parametrize("show_attributes", [False, True])
def test_sync_excludes_download_state_from_copy_and_delete(
    tmp_path, monkeypatch, ignore_hidden, marker_kind, explicit_files, show_attributes
):
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()
    reserved_names = [".model.bin.msc-dl", ".msc-dl-leftover.msc-dl"]
    for directory in (source_dir, target_dir):
        for name in reserved_names:
            state_dir = directory / name
            (state_dir / "nested").mkdir(parents=True)
            if marker_kind != "missing":
                (state_dir / "state.json").write_text(
                    "{broken" if marker_kind == "corrupt" else json.dumps({"format": "msc-download-v1"})
                )
            (state_dir / "nested" / "000000.chunk").write_bytes(b"partial")
    source_config = StorageClientConfig.from_dict(
        {"profiles": {"posix": {"storage_provider": {"type": "file", "options": {"base_path": str(source_dir)}}}}},
        profile="posix",
    )
    target_config = StorageClientConfig.from_dict(
        {"profiles": {"posix": {"storage_provider": {"type": "file", "options": {"base_path": str(target_dir)}}}}},
        profile="posix",
    )
    source_client = StorageClient(source_config)
    target_client = StorageClient(target_config)
    source_client.write("file", b"ordinary", attributes={"label": "keep"})
    (target_dir / "extra").write_bytes(b"delete me")
    ordinary_paths = [
        ".user/state.json",
        ".user.msc-dl-extra/file",
        "user.msc-dl/file",
        ".leaf.msc-dl",
    ]
    for relative in ordinary_paths:
        path = source_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"format": "msc-download-v1"}))
    (source_dir / "dir").mkdir()
    (source_dir / "dir" / "file").write_bytes(b"nested ordinary")
    (source_dir / "dir.txt").write_bytes(b"ordinary sibling")

    listed = {item.key for item in source_client.list()}
    recursive = {item.key for item in source_client.list_recursive()}
    assert listed == recursive
    assert set(ordinary_paths).issubset(listed)
    assert all(f"{name}/nested/000000.chunk" in listed for name in reserved_names)

    original_read_text = Path.read_text

    def guarded_read_text(path, *args, **kwargs):
        if marker_kind == "unreadable" and path.name == "state.json" and path.parent.name in reserved_names:
            raise PermissionError("unreadable download marker")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    file_queue = queue.Queue()
    producer = ProducerThread(
        source_client=source_client,
        source_path="",
        target_client=target_client,
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=threading.Event(),
        delete_unmatched_files=True,
        ignore_hidden=ignore_hidden,
        preserve_source_attributes=show_attributes,
        source_files=sorted(listed) if explicit_files else None,
    )
    assert list(producer._create_listing_iterator(source_client, reserved_names[0])) == []
    assert list(producer._create_listing_iterator(source_client, reserved_names[0] + "/nested/000000.chunk")) == []
    producer.run()
    assert producer.error is None
    operations = []
    while not file_queue.empty():
        batch = file_queue.get()
        for source, target in batch.items:
            operations.append((batch.operation, source.key if source is not None else target.key))
            if source is not None and source.key == "file":
                assert source.metadata == ({"label": "keep"} if show_attributes else None)
    assert (OperationType.ADD, "file") in operations
    assert (OperationType.DELETE, "extra") in operations
    staging_operations = {
        (operation, key) for operation, key in operations if any(part in reserved_names for part in Path(key).parts)
    }
    assert staging_operations == (
        {(OperationType.ADD, key) for key in listed if any(part in reserved_names for part in Path(key).parts)}
        if explicit_files and not ignore_hidden
        else set()
    )
    for relative in ordinary_paths:
        assert ((OperationType.ADD, relative) in operations) is (not ignore_hidden or not relative.startswith("."))
    assert (OperationType.ADD, "dir.txt") in operations
    assert (OperationType.ADD, "dir/file") in operations
    assert [key for operation, key in operations if operation == OperationType.ADD] == sorted(
        key for operation, key in operations if operation == OperationType.ADD
    )
    for directory in (source_dir, target_dir):
        assert all((directory / name / "nested" / "000000.chunk").read_bytes() == b"partial" for name in reserved_names)


@pytest.mark.parametrize("symlink_handling", list(SymlinkHandling))
@pytest.mark.parametrize("explicit_files", [False, True])
def test_sync_download_state_aliases_follow_symlink_policy(tmp_path, symlink_handling, explicit_files):
    profiles = {}
    for name in ("source", "target"):
        directory = tmp_path / name
        state_dir = directory / ".model.bin.msc-dl"
        (state_dir / "nested").mkdir(parents=True)
        (state_dir / "nested" / "000000.chunk").write_bytes(b"partial")
        prefix = "target_" if name == "target" else ""
        (directory / f"{prefix}download_alias").symlink_to(state_dir.name, target_is_directory=True)
        (directory / f"{prefix}chunk_alias").symlink_to(".model.bin.msc-dl/nested/000000.chunk")
        profiles[name] = {"storage_provider": {"type": "file", "options": {"base_path": str(directory)}}}
    source_client = StorageClient(StorageClientConfig.from_dict({"profiles": profiles}, profile="source"))
    target_client = StorageClient(StorageClientConfig.from_dict({"profiles": profiles}, profile="target"))
    source_client.write("tree/file", b"ordinary")
    (tmp_path / "source" / "tree_alias").symlink_to("tree", target_is_directory=True)
    (tmp_path / "source" / "file_alias").symlink_to("tree/file")
    target_client.write("extra", b"delete me")
    file_queue = queue.Queue()
    producer = ProducerThread(
        source_client=source_client,
        source_path="",
        target_client=target_client,
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=threading.Event(),
        delete_unmatched_files=True,
        ignore_hidden=False,
        symlink_handling=symlink_handling,
        source_files=["chunk_alias", "download_alias/nested/000000.chunk", "file_alias", "tree/file", "tree_alias/file"]
        if explicit_files
        else None,
    )
    producer.run()
    assert producer.error is None
    operations = []
    while not file_queue.empty():
        batch = file_queue.get()
        operations.extend(
            (batch.operation, source.key if source is not None else target.key) for source, target in batch.items
        )
    expected_add = {"tree/file"}
    expected_delete = {"extra"}
    expected_symlinks = set()
    if explicit_files or symlink_handling in (SymlinkHandling.FOLLOW, SymlinkHandling.FOLLOW_STRICT):
        expected_add.update(("tree_alias/file", "download_alias/nested/000000.chunk"))
    if symlink_handling in (SymlinkHandling.FOLLOW, SymlinkHandling.FOLLOW_STRICT):
        expected_add.update(("file_alias", "chunk_alias"))
        expected_delete.update(("target_chunk_alias", "target_download_alias/nested/000000.chunk"))
    if symlink_handling in (SymlinkHandling.PRESERVE, SymlinkHandling.PRESERVE_STRICT):
        expected_symlinks.update(("chunk_alias", "file_alias"))
        expected_delete.update(("target_chunk_alias", "target_download_alias"))
        if not explicit_files:
            expected_symlinks.update(("download_alias", "tree_alias"))
    assert {key for operation, key in operations if operation == OperationType.ADD} == expected_add
    assert {key for operation, key in operations if operation == OperationType.DELETE} == expected_delete
    assert {key for operation, key in operations if operation == OperationType.SYMLINK} == expected_symlinks
    for name in ("source", "target"):
        assert (tmp_path / name / ".model.bin.msc-dl/nested/000000.chunk").read_bytes() == b"partial"


@pytest.mark.parametrize("explicit_files", [False, True])
@pytest.mark.parametrize("posix_target", [False, True])
def test_sync_keeps_object_keys_named_like_download_staging(tmp_path, explicit_files, posix_target):
    source = MockStorageClient()
    target = MockStorageClient()
    timestamp = datetime.now(timezone.utc)
    source_files = [ObjectMetadata(key, 7, timestamp) for key in (".model.bin.msc-dl/chunk", "ordinary")]
    target_files = [ObjectMetadata(".other.bin.msc-dl/chunk", 7, timestamp)]
    source.list = lambda **kwargs: iter(source_files)  # type: ignore[assignment]
    source.info = lambda path, **kwargs: next(item for item in source_files if item.key == path)  # type: ignore[attr-defined]
    target.list = lambda **kwargs: iter(target_files)  # type: ignore[assignment]
    if posix_target:
        target = StorageClient(
            StorageClientConfig.from_dict(
                {
                    "profiles": {
                        "local": {"storage_provider": {"type": "file", "options": {"base_path": str(tmp_path)}}}
                    }
                },
                profile="local",
            )
        )
        target.write(".model.bin.msc-dl/chunk", b"keep local state")
        target.write(".other.bin.msc-dl/chunk", b"keep local state")
    file_queue = queue.Queue()
    producer = ProducerThread(
        source_client=cast(StorageClient, source),
        source_path="",
        target_client=cast(StorageClient, target),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=threading.Event(),
        delete_unmatched_files=True,
        ignore_hidden=False,
        source_files=[item.key for item in source_files] if explicit_files else None,
    )
    producer.run()
    assert producer.error is None
    operations = []
    while not file_queue.empty():
        batch = file_queue.get()
        operations.extend(
            (batch.operation, item.key if item is not None else target.key) for item, target in batch.items
        )
    expected = [(OperationType.ADD, "ordinary")]
    if explicit_files and posix_target:
        expected.insert(0, (OperationType.ADD, ".model.bin.msc-dl/chunk"))
    elif not posix_target:
        expected = [
            (OperationType.ADD, ".model.bin.msc-dl/chunk"),
            (OperationType.DELETE, ".other.bin.msc-dl/chunk"),
        ] + expected
    assert operations == expected
    if posix_target:
        assert (tmp_path / ".model.bin.msc-dl/chunk").read_bytes() == b"keep local state"
        assert (tmp_path / ".other.bin.msc-dl/chunk").read_bytes() == b"keep local state"


@pytest.mark.parametrize("composite", [False, True])
@pytest.mark.parametrize("explicit_files", [False, True])
def test_sync_checks_posix_path_names_without_metadata_resolution(tmp_path, composite, explicit_files):
    source = StorageClient(
        StorageClientConfig.from_dict(
            {"profiles": {"local": {"storage_provider": {"type": "file", "options": {"base_path": str(tmp_path)}}}}},
            profile="local",
        )
    )
    timestamp = datetime.now(timezone.utc)
    files = {
        key: ObjectMetadata(key, 7, timestamp)
        for key in (".logical.msc-dl/file", "local_chunk", "ordinary", "remote_chunk")
    }
    if composite:
        routed = StorageClient(
            StorageClientConfig.from_dict(
                {
                    "profiles": {
                        "routed": {
                            "provider_bundle": {
                                "type": "test_multistorageclient.unit.utils.mocks.TestProviderBundleV2MultiBackend"
                            }
                        }
                    }
                },
                profile="routed",
            )
        )
        delegate = routed.delegate
        assert isinstance(delegate, CompositeStorageClient)
        delegate._child_clients["loc1"] = cast(SingleStorageClient, source.delegate)
        delegate._child_clients["loc2"] = cast(SingleStorageClient, MockStorageClient())
        source = routed
    else:
        del files["remote_chunk"]
    metadata_provider = MagicMock()
    metadata_provider.realpath.side_effect = AssertionError("Sync must not resolve staging paths")
    metadata_provider.list_objects.side_effect = lambda *args, **kwargs: iter(files.values())
    metadata_provider.get_object_metadata.side_effect = lambda path, **kwargs: files[path]
    source._metadata_provider = metadata_provider
    file_queue = queue.Queue()
    producer = ProducerThread(
        source_client=source,
        source_path="",
        target_client=cast(StorageClient, NullStorageClient()),
        target_path="",
        progress=ProgressBar(desc="", show_progress=False),
        file_queue=file_queue,
        num_workers=1,
        shutdown_event=threading.Event(),
        ignore_hidden=False,
        source_files=list(files) if explicit_files else None,
    )
    producer.run()
    assert producer.error is None
    added = set()
    while not file_queue.empty():
        batch = file_queue.get()
        added.update(item.key for item, _ in batch.items)
    assert added == (set(files) if composite or explicit_files else {"local_chunk", "ordinary"})
    metadata_provider.realpath.assert_not_called()
