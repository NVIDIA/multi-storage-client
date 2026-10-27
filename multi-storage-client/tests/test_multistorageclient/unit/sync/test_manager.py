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
import shutil
import sys
import threading
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest import mock

import pytest

from multistorageclient.client import StorageClient
from multistorageclient.sync import SyncManager
from multistorageclient.sync import manager as manager_module
from multistorageclient.sync import monitors as monitors_module
from multistorageclient.sync import progress_bar as progress_bar_module
from multistorageclient.sync import worker as worker_module
from multistorageclient.sync.transfer_progress import TransferSnapshot
from multistorageclient.sync.types import OperationType
from multistorageclient.sync.worker import sync_worker_process
from multistorageclient.types import ExecutionMode, ObjectMetadata, SyncError


class MockStorageClient:
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


@pytest.fixture(autouse=True)
def network_sampler(monkeypatch):
    monitor = mock.Mock(rate=25_000_000.0, error=None)
    factory = mock.Mock(return_value=monitor)
    monkeypatch.setattr(progress_bar_module, "NetworkThroughputMonitor", factory)
    return factory


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


def test_sync_objects_signals_workers_and_raises_original_result_monitor_error(monkeypatch):
    source_client = MockStorageClient()
    target_client = MockStorageClient()
    monkeypatch.setattr(source_client, "list", lambda **kwargs: iter(()))
    monkeypatch.setattr(target_client, "list", lambda **kwargs: iter(()))
    failure = RuntimeError("metadata write failed")
    provider = mock.Mock()
    provider.add_file.side_effect = failure
    monkeypatch.setattr(target_client, "_metadata_provider", provider, raising=False)
    metadata = ObjectMetadata(key="file", content_length=1, last_modified=datetime.now(UTC))

    def worker(*args, **kwargs):
        result_queue, shutdown_event = args[7], args[9]
        result_queue.put((OperationType.ADD, "file", metadata))
        assert shutdown_event.wait(timeout=2.0), "Result monitor must signal the worker shutdown event"
        result_queue.put((OperationType.PROGRESS, "worker", TransferSnapshot(0, ())))
        result_queue.put((OperationType.ADD, "after-error", metadata))

    monkeypatch.setattr(manager_module, "sync_worker_process", worker)
    manager = SyncManager(cast(StorageClient, source_client), "", cast(StorageClient, target_client), "")

    with pytest.raises(SyncError, match="Result monitor thread error: metadata write failed"):
        manager.sync_objects(num_worker_processes=1)

    provider.add_file.assert_called_once_with("file", metadata)


@pytest.mark.parametrize(
    argnames=["source_path", "target_path", "overlaps"],
    argvalues=[
        ("data", "data/sub", True),
        ("/data", "data/sub", True),
        ("data/sub", "/data", True),
        ("data", "data/", True),
        ("", "data", True),
        ("data", "", True),
        ("", "", True),
        ("data", "data2", True),
        ("data2", "data", True),
        ("data/a", "data/b", False),
    ],
)
def test_sync_manager_rejects_overlapping_paths_on_same_client(source_path: str, target_path: str, overlaps: bool):
    client = cast(StorageClient, MockStorageClient())

    def make_manager() -> SyncManager:
        return SyncManager(
            source_client=client,
            source_path=source_path,
            target_client=client,
            target_path=target_path,
        )

    if overlaps:
        with pytest.raises(ValueError, match="cannot overlap"):
            make_manager()
    else:
        make_manager()


@pytest.mark.parametrize(
    "source_posix, target_posix, delete_unmatched, operation",
    [
        (False, True, False, "download"),
        (True, False, False, "upload"),
        (True, True, False, "copy"),
        (False, False, False, "transfer"),
        (False, True, True, "download"),
        (True, False, True, "upload"),
        (True, True, True, "copy"),
    ],
)
def test_sync_objects_commits_metadata_by_default(source_posix, target_posix, delete_unmatched, operation):
    """Test that sync_objects calls commit_metadata when commit_metadata is True (default)."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)),
    ]
    target_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC)),
    ]

    source_client.list = lambda **kwargs: iter(source_files)  # type: ignore
    target_client.list = lambda **kwargs: iter(target_files)  # type: ignore

    manager = SyncManager(
        source_client=cast(StorageClient, source_client),
        source_path="",
        target_client=cast(StorageClient, target_client),
        target_path="",
    )

    with (
        mock.patch.object(source_client, "_is_posix_file_storage_provider", return_value=source_posix),
        mock.patch.object(target_client, "_is_posix_file_storage_provider", return_value=target_posix),
        mock.patch.object(target_client, "commit_metadata") as mock_commit,
        mock.patch.object(manager_module, "ProgressBar", wraps=progress_bar_module.ProgressBar) as progress_factory,
    ):
        manager.sync_objects(
            num_worker_processes=1,
            commit_metadata=True,
            delete_unmatched_files=delete_unmatched,
            description="Syncing dataset alpha",
        )
        mock_commit.assert_called_once()
        assert progress_factory.call_args.kwargs["transfer_operation"] == operation
        assert progress_factory.call_args.kwargs["include_deletions"] is delete_unmatched
        assert progress_factory.call_args.kwargs["desc"] == "Syncing dataset alpha"


def test_sync_objects_skips_commit_when_commit_metadata_false():
    """Test that sync_objects does NOT call commit_metadata when commit_metadata is False."""
    source_client = MockStorageClient()
    target_client = MockStorageClient()

    source_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)),
    ]
    target_files = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC)),
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


@pytest.mark.parametrize(
    "scenario",
    [
        "single",
        "multi",
        "exited_worker",
        "dryrun",
        "ray",
        "commit_failure",
        "single_interrupt",
        "multi_interrupt",
        "start_interrupt",
        "ignore_sigterm",
    ],
)
def test_network_sampling_scope_and_cleanup(monkeypatch, tmp_path, capsys, network_sampler, scenario):
    source_client = MockStorageClient()
    target_client = MockStorageClient()
    source_client.list = mock.Mock(return_value=iter(()))
    target_client.list = mock.Mock(return_value=iter(()))
    interrupted = scenario.endswith("interrupt") or scenario == "ignore_sigterm"
    multiprocessing = scenario in {"multi", "exited_worker", "multi_interrupt", "start_interrupt", "ignore_sigterm"}
    expected_pids = {os.getpid()}
    shutdown = threading.Event()
    monkeypatch.setattr(manager_module.tempfile, "tempdir", str(tmp_path))
    unrelated = tmp_path / "unrelated"
    unrelated.write_bytes(b"keep")
    staging_dirs = []

    def leave_partial_file(temp_dir):
        path = Path(temp_dir)
        staging_dirs.append(path)
        batch_dir = path / str(len(staging_dirs))
        batch_dir.mkdir()
        (batch_dir / "partial").write_bytes(b"incomplete download")

    if scenario == "single_interrupt":
        monkeypatch.setattr(manager_module, "sync_worker_process", mock.Mock(side_effect=KeyboardInterrupt))
    if multiprocessing:
        queues = [mock.Mock(wraps=queue.Queue(), cancel_join_thread=mock.Mock()) for _ in range(3)]
        processes = [mock.Mock(pid=101), mock.Mock(pid=102)]
        for process in processes:
            process.is_alive.return_value = True
            process.terminate.side_effect = lambda p=process: setattr(p.is_alive, "return_value", False)
            process.kill.side_effect = lambda p=process: setattr(p.is_alive, "return_value", False)

            def join(timeout=None, p=process):
                assert all(path.exists() for path in staging_dirs)
                if timeout is None and scenario in {"multi_interrupt", "ignore_sigterm"}:
                    raise KeyboardInterrupt

            process.join.side_effect = join
        processes[1].is_alive.return_value = scenario != "exited_worker"
        if scenario == "start_interrupt":
            processes[1].start.side_effect = KeyboardInterrupt
            processes[1].pid = None
            processes[1].is_alive.return_value = False
        elif scenario in {"multi_interrupt", "ignore_sigterm"}:
            if scenario == "ignore_sigterm":
                processes[0].terminate.side_effect = None
        context = mock.Mock()
        context.Queue.side_effect = queues
        context.Event.return_value = shutdown

        def create_process(target, args):
            assert args[-1] is True
            leave_partial_file(args[-2])
            return processes[len(staging_dirs) - 1]

        context.Process.side_effect = create_process
        monkeypatch.setattr(manager_module.multiprocessing, "get_context", mock.Mock(return_value=context))
        expected_pids.update({101} if scenario == "exited_worker" else {101, 102})
    if scenario == "ray":
        ray = mock.Mock()
        ray.remote.return_value.options.return_value.remote.side_effect = sync_worker_process
        monkeypatch.setitem(sys.modules, "ray", ray)
        monkeypatch.setitem(
            sys.modules,
            "multistorageclient.contrib.ray.utils",
            SimpleNamespace(SharedQueue=queue.Queue, SharedEvent=threading.Event),
        )
        monkeypatch.setattr(manager_module, "HAVE_RAY", True)

    def commit_metadata():
        if scenario not in {"ray", "dryrun"}:
            network_sampler.return_value.close.assert_called_once()
        if scenario == "commit_failure":
            raise RuntimeError("commit failed")

    target_client.commit_metadata = mock.Mock(side_effect=commit_metadata)
    manager = SyncManager(cast(StorageClient, source_client), "", cast(StorageClient, target_client), "")
    expected_error = (
        pytest.raises(KeyboardInterrupt)
        if interrupted
        else pytest.raises(RuntimeError, match="commit failed")
        if scenario == "commit_failure"
        else nullcontext()
    )
    with expected_error:
        manager.sync_objects(
            execution_mode=ExecutionMode.RAY if scenario == "ray" else ExecutionMode.LOCAL,
            num_worker_processes=2 if multiprocessing else 1,
            dryrun=scenario == "dryrun",
            dryrun_output_path=str(tmp_path),
        )
    if scenario in {"ray", "dryrun", "start_interrupt"}:
        network_sampler.assert_not_called()
    else:
        network_sampler.assert_called_once_with(expected_pids)
        network_sampler.return_value.start.assert_called_once()
        network_sampler.return_value.close.assert_called_once()
    if scenario != "commit_failure":
        assert ("25.0MB/s" in capsys.readouterr().err) == (scenario not in {"ray", "dryrun", "start_interrupt"})
    if interrupted:
        target_client.commit_metadata.assert_not_called()
    assert unrelated.read_bytes() == b"keep"
    assert all(not path.exists() for path in staging_dirs)
    assert len(set(staging_dirs)) == int(multiprocessing)
    if multiprocessing:
        for process in processes:
            process.start.assert_called_once()
            if not interrupted:
                process.join.assert_called_once()
            elif process.pid is not None:
                process.terminate.assert_called_once()
                assert not process.is_alive()
        if interrupted:
            assert shutdown.is_set()
            for channel in queues[1:]:
                channel.cancel_join_thread.assert_called_once()
            assert processes[0].kill.call_count == int(scenario == "ignore_sigterm")


def test_single_process_interrupt_preserves_active_staging(monkeypatch, tmp_path):
    """An SDK interruption must leave staging usable until in-process transfer threads finish."""
    monkeypatch.setenv("MSC_NUM_THREADS_PER_PROCESS", "2")
    monkeypatch.setattr(manager_module.tempfile, "tempdir", str(tmp_path))
    started = threading.Event()
    release = threading.Event()
    staged_files = []
    uploaded = []
    threads = []
    thread_class = threading.Thread
    item = ObjectMetadata(key="large.bin", content_length=4, last_modified=datetime.now(UTC))
    source = mock.Mock(_metadata_provider=None)
    target = mock.Mock(_metadata_provider=None)
    for client in (source, target):
        client._is_posix_file_storage_provider.return_value = False
        client._is_rust_client_enabled.return_value = False
    source.list_recursive.side_effect = lambda **kwargs: iter([item])
    target.list_recursive.side_effect = lambda **kwargs: iter(())

    def download(remote_paths, local_paths, metadata, transfer_callback=None):
        staged_files.extend(Path(path) for path in local_paths)
        for path in staged_files:
            path.write_bytes(b"data")
        started.set()
        assert release.wait(timeout=5)

    def interrupt_join(*args, **kwargs):
        assert started.wait(timeout=5)
        raise KeyboardInterrupt

    def make_thread(*args, **kwargs):
        thread = thread_class(*args, **kwargs)
        threads.append(thread)
        monkeypatch.setattr(thread, "join", interrupt_join)
        return thread

    source.download_files.side_effect = download
    target.upload_files.side_effect = lambda remote_paths, local_paths, attributes, transfer_callback=None: (
        uploaded.extend(Path(path).read_bytes() for path in local_paths)
    )
    monkeypatch.setattr(worker_module.threading, "Thread", make_thread)
    manager = SyncManager(cast(StorageClient, source), "", cast(StorageClient, target), "")
    try:
        with pytest.raises(KeyboardInterrupt):
            manager.sync_objects(num_worker_processes=1)
        assert staged_files and all(path.read_bytes() == b"data" for path in staged_files)
        target.commit_metadata.assert_not_called()
    finally:
        release.set()
        for thread in threads:
            thread_class.join(thread, timeout=5)
            assert not thread.is_alive()
    assert uploaded == [b"data"]
    assert all(not path.parent.exists() for path in staged_files)


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
            ObjectMetadata(key="a.txt", content_length=100, last_modified=datetime(2025, 1, 1, tzinfo=UTC)),
            ObjectMetadata(key="b.txt", content_length=200, last_modified=datetime(2025, 1, 2, tzinfo=UTC)),
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
            ObjectMetadata(key="old.txt", content_length=50, last_modified=datetime(2025, 1, 1, tzinfo=UTC)),
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
            ObjectMetadata(key="new.txt", content_length=100, last_modified=datetime(2025, 1, 2, tzinfo=UTC)),
            ObjectMetadata(key="same.txt", content_length=100, last_modified=datetime(2025, 1, 1, tzinfo=UTC)),
        ]
        target_files = [
            ObjectMetadata(key="old.txt", content_length=50, last_modified=datetime(2025, 1, 1, tzinfo=UTC)),
            ObjectMetadata(key="same.txt", content_length=100, last_modified=datetime(2025, 1, 1, tzinfo=UTC)),
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
            ObjectMetadata(key="file.txt", content_length=100, last_modified=datetime(2025, 1, 1, tzinfo=UTC)),
        ]
        target_files = [
            ObjectMetadata(key="file.txt", content_length=100, last_modified=datetime(2025, 1, 1, tzinfo=UTC)),
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
            ObjectMetadata(key="changed.txt", content_length=200, last_modified=datetime(2025, 1, 2, tzinfo=UTC)),
        ]
        target_files = [
            ObjectMetadata(key="changed.txt", content_length=100, last_modified=datetime(2025, 1, 1, tzinfo=UTC)),
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
            ObjectMetadata(key="extra.txt", content_length=50, last_modified=datetime(2025, 1, 1, tzinfo=UTC)),
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
            ObjectMetadata(
                key="data.bin", content_length=4096, last_modified=datetime(2025, 6, 15, 12, 30, 0, tzinfo=UTC)
            ),
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
            ObjectMetadata(key="a.txt", content_length=100, last_modified=datetime(2025, 1, 1, tzinfo=UTC)),
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


@pytest.mark.parametrize("has_metadata_provider, commit_metadata", [(False, True), (True, False), (True, True)])
def test_sync_progress_finalization_and_metadata_phase(monkeypatch, has_metadata_provider, commit_metadata):
    source = MockStorageClient()
    target = MockStorageClient()
    item = ObjectMetadata(key="file", content_length=1, last_modified=datetime.now(UTC))
    monkeypatch.setattr(source, "list", lambda **kwargs: iter((item,)))
    monkeypatch.setattr(target, "list", lambda **kwargs: iter((item,)))
    if has_metadata_provider:
        monkeypatch.setattr(target, "_metadata_provider", mock.Mock(), raising=False)
    progress = progress_bar_module.ProgressBar("Syncing", True, show_transfer_progress=True)
    progress.set_phase = mock.Mock(wraps=progress.set_phase)
    worker = mock.Mock()
    monkeypatch.setattr(manager_module, "ProgressBar", mock.Mock(return_value=progress))
    monkeypatch.setattr(manager_module, "sync_worker_process", worker)

    pbar = progress.pbar
    assert pbar is not None

    def commit():
        assert progress._phase == ("finalizing metadata" if has_metadata_provider else None)
        assert not pbar.finalized

    target.commit_metadata = mock.Mock(side_effect=commit)
    SyncManager(cast(StorageClient, source), "", cast(StorageClient, target), "").sync_objects(
        num_worker_processes=1, commit_metadata=commit_metadata
    )

    assert worker.call_args.kwargs["track_transfers"] is True
    assert mock.call(None) in progress.set_phase.call_args_list
    assert (mock.call("finalizing metadata") in progress.set_phase.call_args_list) == (
        has_metadata_provider and commit_metadata
    )
    assert target.commit_metadata.call_count == int(commit_metadata)
    assert progress._closed
    assert pbar.finalized
    assert "100.0%" in str(progress.pbar)
    assert progress._phase is None


@pytest.mark.parametrize("delete, suppress", [(False, False), (True, False), (False, True)])
def test_sync_single_worker_transfer_tracking(monkeypatch, delete, suppress):
    from multistorageclient.utils import NullStorageClient

    source = NullStorageClient() if delete else MockStorageClient()
    target = MockStorageClient()
    monkeypatch.setattr(target, "list", lambda **kwargs: iter(()))
    if not delete:
        monkeypatch.setattr(source, "list", lambda **kwargs: iter(()))
    if suppress:
        monkeypatch.setenv("MSC_SUPPRESS_PROGRESS_BAR", "1")
    worker = mock.Mock()
    progress_factory = mock.Mock(wraps=progress_bar_module.ProgressBar)
    monkeypatch.setattr(manager_module, "ProgressBar", progress_factory)
    monkeypatch.setattr(manager_module, "sync_worker_process", worker)

    SyncManager(cast(StorageClient, source), "", cast(StorageClient, target), "").sync_objects(
        num_worker_processes=1, delete_unmatched_files=delete, description="Deleting" if delete else "Syncing"
    )

    assert worker.call_args.kwargs["track_transfers"] is (not delete and not suppress)
    assert progress_factory.call_args.kwargs["show_transfer_progress"] is (not delete)
    assert progress_factory.call_args.kwargs["show_network_throughput"] is True
    assert progress_factory.call_args.kwargs["desc"] == ("Deleting" if delete else "Syncing")


@pytest.mark.parametrize("stage", ["producer_start", "worker", "producer_join", "result_join", "error_join"])
@pytest.mark.parametrize("exception", [RuntimeError, KeyboardInterrupt])
def test_sync_closes_progress_on_setup_worker_and_join_failures(monkeypatch, stage, exception):
    source = MockStorageClient()
    target = MockStorageClient()
    monkeypatch.setattr(source, "list", lambda **kwargs: iter(()))
    monkeypatch.setattr(target, "list", lambda **kwargs: iter(()))
    progress = progress_bar_module.ProgressBar("Syncing", True, show_transfer_progress=True)
    monkeypatch.setattr(manager_module, "ProgressBar", mock.Mock(return_value=progress))
    captured_monitors = []
    for class_name in ("ResultMonitorThread", "ErrorMonitorThread"):
        original = getattr(manager_module, class_name)

        def capture(*args, _original=original, **kwargs):
            thread = _original(*args, **kwargs)
            captured_monitors.append(thread)
            return thread

        monkeypatch.setattr(manager_module, class_name, capture)
    failure = exception("injected failure")
    if stage == "worker":
        monkeypatch.setattr(manager_module, "sync_worker_process", mock.Mock(side_effect=failure))
    elif stage == "producer_start":
        monkeypatch.setattr(manager_module.ProducerThread, "start", mock.Mock(side_effect=failure))
    else:
        thread_class = {
            "producer_join": manager_module.ProducerThread,
            "result_join": monitors_module.ResultMonitorThread,
            "error_join": monitors_module.ErrorMonitorThread,
        }[stage]
        original_join = thread_class.join

        def join(thread, timeout=None):
            if timeout is None:
                raise failure
            return original_join(thread, timeout)

        monkeypatch.setattr(thread_class, "join", join)

    with pytest.raises(exception, match="injected failure"):
        SyncManager(cast(StorageClient, source), "", cast(StorageClient, target), "").sync_objects(
            num_worker_processes=1
        )

    assert progress._closed
    assert all(not thread.is_alive() for thread in captured_monitors)
