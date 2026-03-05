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
import threading
import time
from datetime import datetime
from typing import Optional, cast

import pytest

import multistorageclient as msc
from multistorageclient.client import StorageClient
from multistorageclient.sync.producer import MAX_BATCH_SIZE, MIN_BATCH_SIZE, ProducerThread
from multistorageclient.sync.progress_bar import ProgressBar
from multistorageclient.sync.types import OperationType
from multistorageclient.types import ObjectMetadata
from multistorageclient.utils import NullStorageClient
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
        self._metadata_provider: Optional[object] = None

    def list(self, **kwargs):
        raise Exception("No Such Method")

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        pass

    def _is_rust_client_enabled(self) -> bool:
        return False

    def _is_posix_file_storage_provider(self) -> bool:
        return False


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
    base = datetime(2025, 1, 1, 12, 0, 0)
    same_size = 100

    # Same second, different microseconds: should match (treated as equal at second resolution)
    source_info = ObjectMetadata(key="a", content_length=same_size, last_modified=base.replace(microsecond=500000))
    target_info = ObjectMetadata(key="a", content_length=same_size, last_modified=base.replace(microsecond=100000))
    assert producer._match_file_metadata(source_info, target_info) is True

    # Target one second newer: should match
    source_info = ObjectMetadata(key="a", content_length=same_size, last_modified=base)
    target_info = ObjectMetadata(key="a", content_length=same_size, last_modified=datetime(2025, 1, 1, 12, 0, 1))
    assert producer._match_file_metadata(source_info, target_info) is True

    # Target one second older: should not match
    source_info = ObjectMetadata(key="a", content_length=same_size, last_modified=datetime(2025, 1, 1, 12, 0, 1))
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
    base = datetime(2025, 1, 1, 12, 0, 0)

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
            last_modified=datetime(2025, 1, 1, 0, 0, 0),
            metadata={"version": "1"},
        )
    ]
    target_files = [
        ObjectMetadata(
            key="file1.txt",
            content_length=100,
            last_modified=datetime(2025, 1, 1, 0, 0, 1),
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
    assert list_kwargs.get("show_attributes") is expected_show_attributes


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
        ObjectMetadata(key="file0.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file3.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
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


def test_batch_flushing_on_operation_type_change():
    """Test that batches are flushed when operation type changes."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(key="file0.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file2.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
    ]

    target_files = [
        ObjectMetadata(key="file3.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file4.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
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
        ObjectMetadata(key=f"file{i}.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0))
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
        ObjectMetadata(key="file0_small1.txt", content_length=500 * 1024, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        ObjectMetadata(key="file1_small2.txt", content_length=800 * 1024, last_modified=datetime(2025, 1, 1, 0, 0, 0)),
        # MEDIUM bucket (1MB - 64MB)
        ObjectMetadata(
            key="file2_medium1.txt", content_length=2 * 1024 * 1024, last_modified=datetime(2025, 1, 1, 0, 0, 0)
        ),
        ObjectMetadata(
            key="file3_medium2.txt", content_length=10 * 1024 * 1024, last_modified=datetime(2025, 1, 1, 0, 0, 0)
        ),
        # LARGE bucket (64MB - 1GB)
        ObjectMetadata(
            key="file4_large1.txt", content_length=100 * 1024 * 1024, last_modified=datetime(2025, 1, 1, 0, 0, 0)
        ),
        ObjectMetadata(
            key="file5_large2.txt", content_length=500 * 1024 * 1024, last_modified=datetime(2025, 1, 1, 0, 0, 0)
        ),
        # VERY_LARGE bucket (> 1GB)
        ObjectMetadata(
            key="file6_vlarge1.txt", content_length=2 * 1024 * 1024 * 1024, last_modified=datetime(2025, 1, 1, 0, 0, 0)
        ),
        ObjectMetadata(
            key="file7_vlarge2.txt", content_length=3 * 1024 * 1024 * 1024, last_modified=datetime(2025, 1, 1, 0, 0, 0)
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
    assert all(item.content_length < 1 * 1024 * 1024 for item in non_stop_batches[0].items)

    assert non_stop_batches[1].operation == OperationType.ADD
    assert len(non_stop_batches[1].items) == 2
    assert all(1 * 1024 * 1024 <= item.content_length < 64 * 1024 * 1024 for item in non_stop_batches[1].items)

    assert non_stop_batches[2].operation == OperationType.ADD
    assert len(non_stop_batches[2].items) == 2
    assert all(64 * 1024 * 1024 <= item.content_length < 1024 * 1024 * 1024 for item in non_stop_batches[2].items)

    assert non_stop_batches[3].operation == OperationType.ADD
    assert len(non_stop_batches[3].items) == 2
    assert all(item.content_length >= 1024 * 1024 * 1024 for item in non_stop_batches[3].items)
