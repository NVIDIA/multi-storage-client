# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import io
import os
import subprocess
import sys
import textwrap
import threading
import time
from contextlib import nullcontext
from types import SimpleNamespace
from unittest import mock

import pytest

from multistorageclient.sync import progress_bar as progress_bar_module
from multistorageclient.sync.progress_bar import ProgressBar
from multistorageclient.sync.transfer_progress import TransferActivity, TransferSnapshot


@pytest.mark.parametrize("include_deletions", [False, True])
@pytest.mark.parametrize(
    "operation, completed_label, active_label",
    [
        ("download", "downloaded", "downloading"),
        ("upload", "uploaded", "uploading"),
        ("copy", "copied", "copying"),
        ("transfer", "transferred", "transferring"),
    ],
)
def test_inflight_display_keeps_network_rate_and_finalizes_after_metadata(
    monkeypatch, include_deletions, operation, completed_label, active_label
):
    completed_label = "completed" if include_deletions else completed_label
    output = io.StringIO()
    monkeypatch.setattr(output, "isatty", lambda: True)
    monkeypatch.setattr(progress_bar_module.sys, "stderr", output)
    monkeypatch.setattr(progress_bar_module.shutil, "get_terminal_size", lambda **kwargs: os.terminal_size((120, 24)))
    clock = mock.Mock(return_value=100.0)
    monkeypatch.setattr(progress_bar_module, "time", SimpleNamespace(time=clock, monotonic=clock))
    monitor = mock.Mock(rate=25_000_000.0, error=None)
    monkeypatch.setattr(progress_bar_module, "NetworkThroughputMonitor", mock.Mock(return_value=monitor))
    with (
        ProgressBar(
            "Syncing dataset alpha",
            True,
            total_items=1000,
            show_network_throughput=True,
            show_transfer_progress=True,
            transfer_operation=operation,
            include_deletions=include_deletions,
        ) as progress,
        progress.network_throughput({123}),
    ):
        assert progress.pbar is not None
        progress.set_phase(None)
        progress.update_transfers(
            "worker-a",
            TransferSnapshot(
                24,
                (
                    TransferActivity("first.tar", 10.0),
                    TransferActivity("third.tar", 30.0),
                    TransferActivity("fourth.tar", 40.0),
                    TransferActivity("fifth.tar", 50.0),
                    TransferActivity("sixth.tar", 60.0),
                ),
            ),
        )
        progress.update_transfers("worker-b", TransferSnapshot(100, (TransferActivity("second.tar", 20.0),)))
        progress.update_progress(420)
        clock.return_value += 1.0
        progress.refresh()
        assert str(progress.pbar).startswith("Syncing dataset alpha  [")
        assert "Downloading" not in output.getvalue()
        assert "42%" in str(progress.pbar)
        assert progress._summary_row is not None
        assert f"420/1000 files {completed_label}" in progress._summary_row.desc
        assert "25.0 MB/s" in progress._summary_row.desc
        assert "network" not in progress._summary_row.desc
        assert "net:" not in progress._summary_row.desc
        assert f"124 {active_label}" in progress._summary_row.desc
        assert "elapsed" in progress._summary_row.desc
        assert len(progress._detail_rows) == 8
        assert progress._detail_rows[0].desc == ""
        assert f"Currently {active_label}" in progress._detail_rows[1].desc
        assert progress._detail_rows[1].desc.endswith("Elapsed")
        assert "first.tar" in progress._detail_rows[2].desc
        assert "second.tar" in progress._detail_rows[3].desc
        assert "third.tar" in progress._detail_rows[4].desc
        assert "fourth.tar" in progress._detail_rows[5].desc
        assert "fifth.tar" in progress._detail_rows[6].desc
        assert f"+ 119 more {active_label}" in progress._detail_rows[7].desc
        assert progress._detail_rows[2].desc.endswith("01:31")
        assert len({progress_bar_module.disp_len(row.desc) for row in progress._detail_rows[1:7]}) == 1

        monitor.error = OSError("sampler failed")
        progress.update_transfers("worker-a", TransferSnapshot(0, ()))
        progress.update_transfers("worker-b", TransferSnapshot(0, ()))
        clock.return_value += 1.0
        progress.refresh()
        assert "network unavailable" in progress._summary_row.desc
        assert f"0 {active_label}" in progress._summary_row.desc
        assert all(row.desc == "" for row in progress._detail_rows)
        progress.update_progress(580)
        progress.set_phase("finalizing metadata")
        assert "99.9%" in str(progress.pbar)
        assert "finalizing metadata" in progress._summary_row.desc
        progress.close(complete=True)
        assert "100%" in str(progress.pbar)
        assert progress_bar_module.disp_len(str(progress.pbar).split("[", 1)[1].split("]", 1)[0]) <= 20
        assert f"1000/1000 files {completed_label}" in output.getvalue().splitlines()[-1]
        assert "finalizing metadata" not in output.getvalue().splitlines()[-1]
        assert progress._summary_row is None
        assert progress._detail_rows == []
    monitor.close.assert_called_once()


@pytest.mark.parametrize(
    "tty, suppress, show_transfers, term",
    [
        (False, False, True, "xterm"),
        (True, True, True, "xterm"),
        (True, False, False, "xterm"),
        (True, False, True, "dumb"),
    ],
)
def test_file_details_are_disabled_when_inapplicable(monkeypatch, tty, suppress, show_transfers, term):
    output = io.StringIO()
    monkeypatch.setattr(output, "isatty", lambda: tty)
    monkeypatch.setattr(progress_bar_module.sys, "stderr", output)
    monkeypatch.setenv("TERM", term)
    if suppress:
        monkeypatch.setenv("MSC_SUPPRESS_PROGRESS_BAR", "1")
    with ProgressBar("Syncing", True, show_transfer_progress=show_transfers) as progress:
        progress.update_transfers("worker", TransferSnapshot(1, (TransferActivity("file.tar", 0.0),)))
        progress.refresh()
        assert progress._detail_rows == []
        assert progress._summary_row is None
        assert (progress.pbar is None) == suppress
        assert progress.track_transfers == (show_transfers and not suppress)
    assert "\x1b[A" not in output.getvalue()


@pytest.mark.parametrize("suppressed", [False, True])
@pytest.mark.parametrize("stderr_kind", ["none", "write_flush_only"])
def test_progress_handles_stderr_without_terminal_capabilities(monkeypatch, suppressed, stderr_kind):
    output = io.StringIO()
    stderr = None if stderr_kind == "none" else SimpleNamespace(write=output.write, flush=output.flush)
    monkeypatch.setattr(progress_bar_module.sys, "stderr", stderr)
    if suppressed:
        monkeypatch.setenv("MSC_SUPPRESS_PROGRESS_BAR", "1")
    with ProgressBar("Syncing", True, total_items=1, show_transfer_progress=True) as progress:
        progress.update_progress()
        assert progress._detail_rows == []
        assert progress.track_transfers == (not suppressed and stderr_kind != "none")
    assert "\x1b[A" not in output.getvalue()


@pytest.mark.parametrize("variable, value", [("TQDM_DISABLE", "1"), ("TQDM_DELAY", "10")])
def test_tqdm_environment_overrides_do_not_break_progress(variable, value):
    environment = os.environ.copy()
    for name in ("TQDM_DISABLE", "TQDM_DELAY", "MSC_SUPPRESS_PROGRESS_BAR"):
        environment.pop(name, None)
    environment[variable] = value
    environment["TERM"] = "xterm"
    script = textwrap.dedent(
        """
        import importlib
        import io
        import os
        import sys

        progress_module = importlib.import_module("multistorageclient.sync.progress_bar")
        output = io.StringIO()
        output.isatty = lambda: True
        sys.stderr = output
        try:
            with progress_module.ProgressBar(
                "Syncing", True, total_items=1, show_transfer_progress=True, show_network_throughput=True
            ) as progress:
                if os.getenv("TQDM_DISABLE"):
                    assert progress.pbar is None
                    assert not progress.track_transfers
                    with progress_module.CappedProgressBar(total=1, max_bar_width=20) as disabled:
                        assert disabled.format_dict["finalized"]
                    def unexpected_monitor(pids):
                        raise AssertionError("disabled progress started a sampler")
                    progress_module.NetworkThroughputMonitor = unexpected_monitor
                    with progress.network_throughput({123}):
                        progress.update_progress()
                    assert output.getvalue() == ""
                else:
                    assert progress.track_transfers
                    progress.update_progress()
                    progress.close(complete=True)
                    final = output.getvalue().splitlines()[-1]
                    assert final.startswith("1/1 files transferred")
                    assert output.getvalue().endswith("\\n" + final + "\\n")
                    assert "100%" in str(progress.pbar)
        except BaseException:
            sys.stderr = sys.__stderr__
            raise
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], env=environment, capture_output=True, text=True, timeout=10, check=False
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("columns", [0, -1])
def test_unknown_terminal_width_keeps_summary_and_file_details(monkeypatch, columns):
    output = io.StringIO()
    monkeypatch.setattr(output, "isatty", lambda: True)
    monkeypatch.setattr(output, "fileno", lambda: 123)
    monkeypatch.setattr(progress_bar_module.sys, "stderr", output)
    monkeypatch.setattr(progress_bar_module.os, "get_terminal_size", lambda fd: os.terminal_size((columns, 0)))
    monkeypatch.setattr(progress_bar_module.shutil, "get_terminal_size", lambda **kwargs: os.terminal_size((80, 24)))
    with ProgressBar("Syncing", True, total_items=1000, show_transfer_progress=True) as progress:
        progress.update_transfers("worker", TransferSnapshot(1, (TransferActivity("file.tar", time.monotonic()),)))
        progress.update_progress(420)
        assert progress._terminal_width() == 79
        assert progress._summary_row is not None
        assert "420/1000 files transferred" in progress._summary_row.desc
        assert "file.tar" in progress._detail_rows[2].desc


@pytest.mark.parametrize("tty", [False, True])
@pytest.mark.parametrize("phase", ["listing / comparing", "finalizing metadata"])
def test_failed_transfer_clears_activity_without_marking_complete(monkeypatch, tty, phase):
    output = io.StringIO()
    monkeypatch.setattr(output, "isatty", lambda: tty)
    monkeypatch.setattr(progress_bar_module.sys, "stderr", output)
    progress = ProgressBar("Syncing", True, total_items=1, show_transfer_progress=True)
    with pytest.raises(RuntimeError, match="worker failed"), progress:
        progress.update_transfers("worker", TransferSnapshot(1, (TransferActivity("file.tar", 0.0),)))
        progress.update_progress()
        progress.set_phase(phase)
        raise RuntimeError("worker failed")
    assert progress.pbar is not None
    assert "99.9%" in str(progress.pbar)
    assert phase not in output.getvalue().splitlines()[-1]
    assert progress._transfer_count == 0
    assert progress._transfer_snapshots == {}
    assert not progress.track_transfers


@pytest.mark.parametrize("width", [32, 80, 160])
def test_file_details_fit_terminal_width_and_escape_control_characters(monkeypatch, width):
    output = io.StringIO()
    monkeypatch.setattr(output, "isatty", lambda: True)
    monkeypatch.setattr(progress_bar_module.sys, "stderr", output)
    monkeypatch.setattr(progress_bar_module.shutil, "get_terminal_size", lambda **kwargs: os.terminal_size((width, 24)))
    path = "nested/" * 20 + "数据\n\x1b.tar"
    with ProgressBar("Syncing", True, total_items=100, show_transfer_progress=True) as progress:
        assert progress.pbar is not None
        progress.pbar.dynamic_ncols = False
        progress.pbar.ncols = width
        progress.update_transfers("worker", TransferSnapshot(1, (TransferActivity(path, 0.0),)))
        progress.refresh()
        meter = str(progress.pbar)
        bar = meter.split("[", 1)[1].split("]", 1)[0]
        assert progress_bar_module.disp_len(bar) <= 20
        assert progress_bar_module.disp_len(meter) <= width
        if width >= 80:
            assert progress_bar_module.disp_len(bar) == 20
            assert progress_bar_module.disp_len(meter) < width
        progress.update_progress(100)
        assert "99.9%" in str(progress.pbar)
        assert progress_bar_module.disp_len(str(progress.pbar)) <= width
        row = progress._detail_rows[2]
        assert "\n" not in row.desc
        assert "\x1b" not in row.desc
        assert progress_bar_module.disp_len(row.desc) < width
        assert progress_bar_module.disp_len(row.desc) == min(60, width - 1)
        assert ".tar" in row.desc
        assert all(progress_bar_module.disp_len(item.desc) <= min(60, width - 1) for item in progress._detail_rows)
        assert progress_bar_module.disp_len(progress._detail_rows[1].desc) == progress_bar_module.disp_len(row.desc)
        assert progress._detail_rows[1].desc.endswith("Elapsed")
        assert progress._summary_row is not None
        assert progress_bar_module.disp_len(progress._summary_row.desc) < width


def test_progress_bar_capped_percentage():
    progress = ProgressBar(desc="Syncing", show_progress=True)
    assert progress.pbar is not None
    progress.update_total(100_000)
    progress.update_progress(99_999)
    assert "99.9%" in str(progress.pbar)
    for width in [32, 80, 100]:
        meter = progress_bar_module.CappedProgressBar.format_meter(
            100,
            100,
            elapsed=0.0,
            finalized=False,
            ncols=width,
            desc="Syncing",
            bar_format="{desc}  [{bar}] {percentage:.0f}%",
        )
        assert "99.9%" in meter
        assert progress_bar_module.disp_len(meter) <= width


def test_refresh_updates_display_without_changing_counters():
    progress = ProgressBar(desc="Syncing", show_progress=True, total_items=0)
    assert progress.pbar is not None

    refresh_count = {"count": 0}
    original_refresh = progress.pbar.refresh

    def counting_refresh(*args, **kwargs):
        refresh_count["count"] += 1
        return original_refresh(*args, **kwargs)

    progress.pbar.refresh = counting_refresh
    progress.refresh()

    assert progress.pbar.total == 0
    assert progress.pbar.n == 0
    assert refresh_count["count"] == 1

    progress.close()


def test_progress_update_interval():
    progress = ProgressBar(desc="Syncing", show_progress=True, total_items=2000)
    assert progress.pbar is not None

    # Track refresh calls by wrapping the original refresh method
    refresh_count = {"count": 0}
    original_refresh = progress.pbar.refresh

    def counting_refresh(*args, **kwargs):
        refresh_count["count"] += 1
        return original_refresh(*args, **kwargs)

    progress.pbar.refresh = counting_refresh

    # Simulate 500 updates per second for 2000 items over ~2.5 seconds
    # We'll do rapid updates without sleep to simulate high-frequency updates,
    # then add sleeps at key points to verify throttling
    start_time = time.time()
    total_items = 2000

    # Phase 1: Rapid updates without delay (should trigger only 1 refresh)
    for i in range(1, 501):
        progress.update_progress(1)

    refresh_after_phase1 = refresh_count["count"]

    # Phase 2: Wait 1.1 seconds and do more rapid updates (should trigger 1 more refresh)
    time.sleep(1.1)
    for i in range(501, 1001):
        progress.update_progress(1)

    refresh_after_phase2 = refresh_count["count"]

    # Phase 3: Wait another 1.1 seconds and complete remaining items
    time.sleep(1.1)
    for i in range(1001, total_items + 1):
        progress.update_progress(1)

    elapsed_time = time.time() - start_time

    # Verify the internal counter reached the total
    assert progress.pbar.n == total_items, f"Expected {total_items} items, got {progress.pbar.n}"

    # Verify throttling behavior:
    # - Phase 1: Multiple rapid updates should result in at most 1-2 refreshes
    assert refresh_after_phase1 <= 2, f"Too many refreshes in Phase 1: {refresh_after_phase1} for 500 rapid updates"

    # - Phase 2: After 1+ second wait, should have 1-2 more refreshes
    assert refresh_after_phase2 <= refresh_after_phase1 + 2, (
        f"Too many refreshes in Phase 2: {refresh_after_phase2} total"
    )

    # - Overall: Should have much fewer refreshes than total updates
    assert refresh_count["count"] < total_items / 100, (
        f"Refresh not throttled properly: {refresh_count['count']} refreshes for {total_items} updates"
    )

    # Verify elapsed time is reasonable (~2.2 seconds, allowing some overhead)
    assert 2.0 <= elapsed_time <= 3.0, f"Test timing incorrect: {elapsed_time:.2f}s elapsed"

    progress.close()


def test_progress_bar_thread_safety():
    progress = ProgressBar(desc="Syncing", show_progress=True, total_items=10000, show_network_throughput=True)
    assert progress.pbar is not None

    # Track refresh calls
    refresh_count = {"count": 0}
    original_refresh = progress.pbar.refresh

    def counting_refresh(*args, **kwargs):
        refresh_count["count"] += 1
        return original_refresh(*args, **kwargs)

    progress.pbar.refresh = counting_refresh

    errors = []

    def producer_thread():
        try:
            for i in range(100):
                progress.update_total(1000 + i)
                time.sleep(0.01)
        except Exception as e:
            errors.append(("producer", e))

    def consumer_thread():
        try:
            for i in range(500):
                progress.update_progress(1)
                time.sleep(0.001)
        except Exception as e:
            errors.append(("consumer", e))

    # Start multiple threads that concurrently update the progress bar
    threads = []
    threads.append(threading.Thread(target=producer_thread))
    threads.append(threading.Thread(target=consumer_thread))

    start_time = time.time()
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    elapsed_time = time.time() - start_time

    # Verify no exceptions occurred
    assert len(errors) == 0, f"Thread safety errors: {errors}"

    # Verify the progress bar was updated correctly (500 total updates from consumer)
    assert progress.pbar.n == 500, f"Expected 500 updates, got {progress.pbar.n}"

    # Verify refresh was throttled despite concurrent access
    # With 2 threads running concurrently for ~1 second, both update_total and update_progress
    # can each trigger refreshes. We expect roughly 1-2 refreshes per second per method,
    # so around 2-10 refreshes is reasonable (vs 600 unthrottled updates)
    assert refresh_count["count"] < 5, (
        f"Refresh not properly throttled with concurrent access: {refresh_count['count']} refreshes for {elapsed_time:.2f}s"
    )

    # Verify throttling is effective: should be significantly less than total updates
    assert refresh_count["count"] < 600 / 10, (
        f"Throttling not effective enough: {refresh_count['count']} refreshes for 600 total updates"
    )

    progress.close()


def test_network_throughput_display_lifecycle(monkeypatch):
    clock = mock.Mock(return_value=100.0)
    monkeypatch.setattr(progress_bar_module, "time", SimpleNamespace(time=clock))
    monitor = mock.Mock(rate=None, error=None)
    factory = mock.Mock(return_value=monitor)
    monkeypatch.setattr(progress_bar_module, "NetworkThroughputMonitor", factory)

    with (
        ProgressBar(desc="Syncing", show_progress=True, total_items=10, show_network_throughput=True) as progress,
        progress.network_throughput({123, 456}),
    ):
        assert progress.pbar is not None
        assert "--B/s" in str(progress.pbar)
        factory.assert_called_once_with({123, 456})
        monitor.start.assert_called_once()
        monitor.rate = 25_000_000.0
        for method, argument, count in [("update_total", 10, 0), ("update_progress", 1, 1), ("reset_progress", 0, 0)]:
            clock.return_value += 1.0
            getattr(progress, method)(argument)
            assert "25.0MB/s" in str(progress.pbar)
            assert progress.pbar.n == count
            assert progress.pbar.total == 10
        monitor.rate = 0.0
        progress.refresh()
        assert "0.00B/s" in str(progress.pbar)
    monitor.close.assert_called_once()


@pytest.mark.parametrize(
    "show_progress, show_network, suppress",
    [(True, False, False), (False, True, False), (True, True, True)],
)
def test_process_sampler_is_not_started_when_disabled(monkeypatch, show_progress, show_network, suppress):
    if suppress:
        monkeypatch.setenv("MSC_SUPPRESS_PROGRESS_BAR", "1")
    factory = mock.Mock()
    monkeypatch.setattr(progress_bar_module, "NetworkThroughputMonitor", factory)

    with (
        ProgressBar(desc="Syncing", show_progress=show_progress, show_network_throughput=show_network) as progress,
        progress.network_throughput({123}),
    ):
        progress.update_total(1)
        progress.update_progress()
        progress.refresh()

    factory.assert_not_called()


@pytest.mark.parametrize("failure", ["startup", "sampling", "worker"])
def test_network_throughput_closes_on_failure(monkeypatch, failure):
    monitor = mock.Mock(rate=None, error=OSError("unavailable") if failure == "sampling" else None)
    if failure == "startup":
        monitor.start.side_effect = RuntimeError("cannot start thread")
    monkeypatch.setattr(progress_bar_module, "NetworkThroughputMonitor", mock.Mock(return_value=monitor))
    with ProgressBar(desc="Syncing", show_progress=True, total_items=1, show_network_throughput=True) as progress:
        with (
            pytest.raises(RuntimeError, match="worker failed") if failure == "worker" else nullcontext(),
            progress.network_throughput({123}),
        ):
            progress.update_progress()
            assert progress.pbar is not None
            assert progress.pbar.n == 1
            if failure == "worker":
                raise RuntimeError("worker failed")
            assert "network unavailable" in str(progress.pbar)
        monitor.close.assert_called_once()
