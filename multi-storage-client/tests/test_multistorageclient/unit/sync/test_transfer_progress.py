# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import queue

import pytest

from multistorageclient.sync.transfer_progress import TransferProgress, TransferSnapshot


def test_transfer_snapshots_are_bounded_and_remove_finished_files():
    progress = TransferProgress(lambda snapshot: None)
    paths = ["first.tar", *(f"file-{index}.tar" for index in range(127))]
    transfer_callback = progress.callback(dict(zip(paths, paths)))
    for path in paths:
        transfer_callback("remote/" + path, path, "started")
    snapshot = progress.snapshot()
    assert snapshot.count == 128
    assert [item.path for item in snapshot.oldest] == paths[:5]
    transfer_callback("remote/" + paths[0], paths[0], "completed")
    assert progress.snapshot().count == 127
    assert [item.path for item in progress.snapshot().oldest] == paths[1:6]
    for path in paths[1:]:
        transfer_callback("remote/" + path, path, "completed")
    assert progress.snapshot() == TransferSnapshot(0, ())


def test_transfer_failure_and_retry_have_separate_lifetimes():
    progress = TransferProgress(lambda snapshot: None)
    transfer_callback = progress.callback({"local.tar": "file.tar"})
    transfer_callback("physical/file.tar", "local.tar", "started")
    first = progress.snapshot().oldest[0]
    transfer_callback("physical/file.tar", "local.tar", "failed")
    assert progress.snapshot().count == 0
    transfer_callback("physical/file.tar", "local.tar", "started")
    retry = progress.snapshot()
    assert retry.count == 1
    assert retry.oldest[0].started_at >= first.started_at
    transfer_callback("physical/file.tar", "local.tar", "completed")
    assert progress.snapshot().count == 0


def test_transfer_reporting_refreshes_without_completions_and_stops():
    snapshots = queue.Queue()
    progress = TransferProgress(snapshots.put)
    progress.start()
    transfer_callback = progress.callback({"local.tar": "large.tar"})
    try:
        transfer_callback("physical/file.tar", "local.tar", "started")
        snapshot = snapshots.get(timeout=3.0)
        assert snapshot.count == 1
        assert snapshot.oldest[0].path == "large.tar"
        assert snapshots.get(timeout=3.0).count == 1
    finally:
        progress.close()
    assert snapshots.get(timeout=1.0) == TransferSnapshot(0, ())
    assert not progress._thread.is_alive()
    transfer_callback("physical/file.tar", "local.tar", "started")
    assert progress.snapshot().count == 0


@pytest.mark.parametrize("local_source", [False, True])
def test_transfer_callbacks_keep_batches_and_reporters_independent(local_source):
    progress = TransferProgress(lambda snapshot: None)
    first = progress.callback(
        {"local/first": "logical/first.tar", "local/second": "logical/second.tar"}, local_source=local_source
    )
    second = progress.callback({"local/first": "another/file.tar"}, local_source=local_source)
    first_paths = ("local/first", "physical/first") if local_source else ("physical/first", "local/first")
    second_paths = ("local/second", "physical/second") if local_source else ("physical/second", "local/second")
    first(*first_paths, "started")
    first(*second_paths, "started")
    second(*first_paths, "started")
    assert progress.snapshot().count == 3
    first(*first_paths, "completed")
    assert {item.path for item in progress.snapshot().oldest} == {"logical/second.tar", "another/file.tar"}
    second(*first_paths, "failed")
    assert progress.snapshot().count == 1
    first(*second_paths, "failed")
    assert progress.snapshot().count == 0
    first(*second_paths, "started")
    assert progress.snapshot().count == 1
    first(*second_paths, "completed")
    other = TransferProgress(lambda snapshot: None)
    assert progress.snapshot().count == other.snapshot().count == 0
    progress.close()
    first(*first_paths, "started")
    assert progress.snapshot().count == 0
