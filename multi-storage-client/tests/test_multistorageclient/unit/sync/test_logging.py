# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import io
import logging
import pickle
import queue
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest
from tqdm import tqdm

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers.posix_file import PosixFileStorageProvider
from multistorageclient.sync import logging as logging_module
from multistorageclient.sync import manager as manager_module
from multistorageclient.sync.logging import redirect_sync_logging, worker_logging
from multistorageclient.sync.monitors import ResultMonitorThread
from multistorageclient.sync.types import OperationType


@pytest.mark.parametrize("console", [False, True])
@pytest.mark.parametrize("fail_first", [False, True])
def test_overlapping_logging_redirects_restore_original_handlers(monkeypatch, console, fail_first):
    root = logging.getLogger()
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.ERROR)
    log_filter = logging.Filter("allowed")
    handler.addFilter(log_filter)
    original = [handler] if console else []
    monkeypatch.setattr(root, "handlers", original)
    entered = [threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]

    def run(index):
        with redirect_sync_logging():
            entered[index].set()
            assert release[index].wait(5)
            if index == 0 and fail_first:
                raise RuntimeError("first sync failed")

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(run, 0)
        try:
            assert entered[0].wait(5)
            second = executor.submit(run, 1)
            assert entered[1].wait(5)
            redirected = root.handlers
            release[0].set()
            if fail_first:
                with pytest.raises(RuntimeError, match="first sync failed"):
                    first.result(timeout=5)
            else:
                first.result(timeout=5)
            assert root.handlers == redirected
        finally:
            for event in release:
                event.set()
        second.result(timeout=5)

    assert root.handlers == original
    if console:
        assert root.handlers[0] is handler
        assert handler.level == logging.ERROR
        assert handler.filters == [log_filter]
    else:
        logging.basicConfig(stream=io.StringIO())
        assert len(root.handlers) == 1


def test_disabled_logging_redirect_does_not_touch_handlers(monkeypatch):
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    with (
        mock.patch.object(logging_module, "logging_redirect_tqdm") as redirect,
        redirect_sync_logging(enabled=False),
    ):
        assert root.handlers == []
    redirect.assert_not_called()
    assert root.handlers == []


@pytest.mark.parametrize("display", ["suppressed", "redirected", "dumb"])
def test_concurrent_noninteractive_syncs_do_not_change_logging(tmp_path, monkeypatch, display):
    monkeypatch.setenv("MSC_NUM_THREADS_PER_PROCESS", "1")
    monkeypatch.delenv("MSC_SUPPRESS_PROGRESS_BAR", raising=False)
    if display == "suppressed":
        monkeypatch.setenv("MSC_SUPPRESS_PROGRESS_BAR", "1")
    output = io.StringIO()
    if display == "dumb":
        output.isatty = lambda: True
        monkeypatch.setenv("TERM", "dumb")
    monkeypatch.setattr(sys, "stderr", output)
    source = StorageClient(
        StorageClientConfig(
            profile="source", storage_provider=PosixFileStorageProvider(base_path=str(tmp_path / "source"))
        )
    )
    source.write("file", b"concurrent sync")
    targets = [
        StorageClient(
            StorageClientConfig(
                profile=f"target-{index}",
                storage_provider=PosixFileStorageProvider(base_path=str(tmp_path / f"target-{index}")),
            )
        )
        for index in range(8)
    ]
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    barrier = threading.Barrier(8)

    def sync(target):
        barrier.wait(timeout=5)
        target.sync_from(source, num_worker_processes=1)
        assert target.read("file") == b"concurrent sync"

    with (
        mock.patch.object(logging_module, "logging_redirect_tqdm") as redirect,
        ThreadPoolExecutor(max_workers=8) as executor,
    ):
        futures = [executor.submit(sync, target) for target in targets]
        for future in futures:
            future.result(timeout=10)

    redirect.assert_not_called()
    assert root.handlers == []


def test_worker_logs_replay_through_parent_tqdm_handler(monkeypatch):
    logger = logging.getLogger("multistorageclient.sync.test_worker_logging")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    monkeypatch.setattr(logger, "handlers", [handler])
    monkeypatch.setattr(logger, "propagate", False)
    monkeypatch.setattr(logger, "level", logging.WARNING)
    result_queue = queue.Queue()

    with worker_logging(result_queue):
        try:
            raise ValueError("retry me")
        except ValueError:
            logger.warning("download failed for %s", "file.tar", exc_info=True)
    op, path, record = result_queue.get_nowait()
    record = pickle.loads(pickle.dumps(record))
    assert op == OperationType.LOG
    assert path is None
    assert record.exc_info is None
    assert "ValueError: retry me" in record.getMessage()
    assert logger.handlers == [handler]
    assert logger.propagate is False
    result_queue.put((op, path, record))
    result_queue.put((OperationType.STOP, None, None))
    progress = mock.Mock()
    monitor = ResultMonitorThread(mock.Mock(), "", progress, result_queue)

    with mock.patch.object(tqdm, "write") as write, redirect_sync_logging():
        monitor.run()

    assert monitor.error is None
    assert write.call_count == 1
    assert write.call_args.args[0].startswith("WARNING: download failed for file.tar")
    assert logger.handlers == [handler]
    assert logger.propagate is False
    progress.update_progress.assert_not_called()
    progress.update_transfers.assert_not_called()


def test_progress_logging_keeps_file_handlers_and_restores_console(monkeypatch):
    logger = logging.getLogger("multistorageclient.sync.test_parent_logging")
    logfile = io.StringIO()
    file_handler = logging.StreamHandler(logfile)
    console = logging.StreamHandler(sys.stderr)
    monkeypatch.setattr(logger, "handlers", [file_handler, console])
    monkeypatch.setattr(logger, "propagate", False)
    monkeypatch.setattr(logger, "level", logging.WARNING)

    with mock.patch.object(tqdm, "write") as write, redirect_sync_logging():
        logger.warning("retrying file")

    write.assert_called_once()
    assert logfile.getvalue() == "retrying file\n"
    assert logger.handlers == [file_handler, console]


def test_spawned_worker_wrapper_queues_logs_and_restores_configuration(monkeypatch):
    logger = logging.getLogger("multistorageclient.sync.test_worker_wrapper")
    console = logging.StreamHandler(sys.stderr)
    monkeypatch.setattr(logger, "handlers", [console])
    monkeypatch.setattr(logger, "propagate", False)
    monkeypatch.setattr(logger, "level", logging.WARNING)
    result_queue = queue.Queue()
    args = [None] * 7 + [result_queue, None, None]

    def worker(*args):
        logger.warning("retrying in spawned worker")
        raise RuntimeError("setup failed")

    monkeypatch.setattr(manager_module, "sync_worker_process", worker)
    monkeypatch.setattr(manager_module.signal, "signal", mock.Mock())
    with pytest.raises(RuntimeError, match="setup failed"):
        manager_module._sync_worker_process_local(*args)

    op, _, record = result_queue.get_nowait()
    assert op == OperationType.LOG
    assert record.getMessage() == "retrying in spawned worker"
    assert logger.handlers == [console]
    assert logger.propagate is False


def test_progress_logging_preserves_file_only_root_configuration(monkeypatch):
    logger = logging.getLogger("multistorageclient.sync.test_file_only_logging")
    logfile = io.StringIO()
    handler = logging.StreamHandler(logfile)
    monkeypatch.setattr(logging.getLogger(), "handlers", [handler])
    monkeypatch.setattr(logger, "handlers", [])
    monkeypatch.setattr(logger, "propagate", True)
    monkeypatch.setattr(logger, "level", logging.WARNING)

    with mock.patch.object(tqdm, "write") as write, redirect_sync_logging():
        logger.warning("file log only")

    write.assert_not_called()
    assert logfile.getvalue() == "file log only\n"
    assert logging.getLogger().handlers == [handler]
