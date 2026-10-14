# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import io
import socket
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest

from multistorageclient.sync import network_throughput as network_module
from multistorageclient.sync import progress_bar as progress_module
from multistorageclient.sync.network_throughput import NetworkThroughputMonitor
from multistorageclient.sync.progress_bar import ProgressBar


@pytest.fixture(autouse=True)
def tracked_processes(monkeypatch):
    factory = mock.Mock(side_effect=lambda pid: mock.Mock(is_running=mock.Mock(return_value=True)))
    monkeypatch.setattr(network_module.psutil, "Process", factory)
    return factory


@pytest.mark.parametrize(
    "keep_missing, samples",
    [
        (
            False,
            [
                (0.0, {10: (100, 200), 20: (500, 600)}, None),
                (2.0, {10: (2_000_100, 1_000_200), 30: (1_000_000, 2_000_000)}, 3_000_000.0),
                (3.0, {10: (1, 2)}, 0.0),
            ],
        ),
        (True, [(0.0, {100: (100, 200)}, None), (1.0, {}, 0.0), (2.0, {100: (120, 230)}, 50.0)]),
    ],
)
def test_sample_lifecycle(monkeypatch, keep_missing, samples):
    clock = mock.Mock()
    monkeypatch.setattr(network_module, "time", SimpleNamespace(monotonic=clock))
    monitor = NetworkThroughputMonitor({100})
    for timestamp, counters, expected_rate in samples:
        clock.return_value = timestamp
        monitor._record(counters, keep_missing)
        assert monitor.rate == expected_rate
    clock.return_value += 4.0
    assert monitor.rate is None


@pytest.mark.parametrize("scenario", ["live", "exited", "reused", "denied"])
def test_process_ownership(monkeypatch, tmp_path, tracked_processes, scenario):
    def process_for_pid(pid):
        if scenario == "exited" and pid == 200:
            raise network_module.psutil.NoSuchProcess(pid)
        return mock.Mock(is_running=mock.Mock(return_value=scenario != "reused" or pid != 200))

    tracked_processes.side_effect = process_for_pid
    for pid, inode in ((100, 11), (200, 22), (300, 33)):
        directory = tmp_path / "proc" / str(pid) / "fd"
        directory.mkdir(parents=True)
        (directory / "3").symlink_to(f"socket:[{inode}]")
        (directory / "4").symlink_to("/regular-file")
    monitor = NetworkThroughputMonitor({100, 200, 400})
    if scenario == "denied":
        path = mock.Mock()
        path.iterdir.side_effect = PermissionError("denied")
        monkeypatch.setattr(network_module, "Path", mock.Mock(return_value=path))
        with pytest.raises(PermissionError):
            monitor._socket_inodes()
    else:
        monkeypatch.setattr(network_module, "Path", lambda path: tmp_path / path.lstrip("/"))
        assert monitor._socket_inodes() == ({11, 22} if scenario == "live" else {11})


def _linux_diagnostics(monkeypatch, reply):
    diag = mock.MagicMock()
    diag.__aenter__.return_value = diag
    diag.bind = mock.AsyncMock()
    diag.nlm_request = mock.AsyncMock(side_effect=reply)
    factory = mock.Mock(return_value=diag)
    monkeypatch.setitem(
        sys.modules,
        "pyroute2.netlink",
        SimpleNamespace(NETLINK_SOCK_DIAG=4, NLM_F_REQUEST=1, NLM_F_ROOT=2, NLM_F_MATCH=4),
    )
    monkeypatch.setitem(
        sys.modules,
        "pyroute2.netlink.diag",
        SimpleNamespace(inet_diag_req=dict, MarshalDiag=mock.Mock(), SS_ALL=4095, SOCK_DIAG_BY_FAMILY=20),
    )
    monkeypatch.setitem(sys.modules, "pyroute2.netlink.nlsocket", SimpleNamespace(AsyncNetlinkSocket=factory))
    monkeypatch.setattr(network_module, "sys", SimpleNamespace(platform="linux"))
    return diag, factory


def _socket_stats(inode, cookie, received, sent):
    stats = mock.MagicMock()
    stats.__getitem__.side_effect = {"idiag_inode": inode, "idiag_cookie": cookie}.__getitem__
    stats.get_attr.return_value = {"tcpi_bytes_received": received, "tcpi_bytes_sent": sent}
    return stats


def _macos_command(monkeypatch, process=None, failure=None):
    output = process.stdout if process is not None else io.StringIO("")
    factory = mock.Mock(return_value=process, side_effect=failure)
    monkeypatch.setattr(network_module.subprocess, "Popen", factory)
    monkeypatch.setattr(network_module, "sys", SimpleNamespace(platform="darwin"))
    monkeypatch.setattr(network_module, "os", SimpleNamespace(fdopen=mock.Mock(return_value=output), close=mock.Mock()))
    monkeypatch.setitem(sys.modules, "pty", SimpleNamespace(openpty=mock.Mock(return_value=(101, 102))))
    return factory, output


def test_linux_reuses_socket_and_filters_main_and_worker_traffic(monkeypatch):
    batches = iter(
        [
            [_socket_stats(11, 1, 100, 200), _socket_stats(22, 2, 300, 400), _socket_stats(99, 9, 10**9, 10**9)],
            [_socket_stats(33, 3, 500, 600)],
            [_socket_stats(11, 1, 1_000_100, 200), _socket_stats(22, 2, 300, 2_000_400)],
            [_socket_stats(33, 3, 3_000_500, 600)],
        ]
    )

    async def reply(*args):
        for stats in next(batches):
            yield stats

    diag, factory = _linux_diagnostics(monkeypatch, reply)
    monkeypatch.setattr(network_module.asyncio, "sleep", mock.AsyncMock())
    clock = mock.Mock(return_value=100.0)
    monkeypatch.setattr(network_module, "time", SimpleNamespace(monotonic=clock))
    monitor = NetworkThroughputMonitor({100, 200})
    monkeypatch.setattr(monitor, "_socket_inodes", mock.Mock(return_value={11, 22, 33}))
    record = monitor._record

    def record_then_stop(counters):
        record(counters)
        if clock.return_value == 101.0:
            monitor._stop_event.set()
        clock.return_value = 101.0

    monkeypatch.setattr(monitor, "_record", record_then_stop)
    monitor.run()

    assert monitor.error is None
    assert monitor.rate == 6_000_000.0
    factory.assert_called_once()
    diag.bind.assert_awaited_once()
    diag.__aexit__.assert_awaited_once()
    requests = [call.args[0] for call in diag.nlm_request.call_args_list]
    assert [request["sdiag_family"] for request in requests] == [socket.AF_INET, socket.AF_INET6] * 2
    assert all(request["idiag_ext"] == 2 for request in requests)
    assert all(request["sdiag_protocol"] == socket.IPPROTO_TCP for request in requests)


@pytest.mark.parametrize("stop", [False, True])
def test_linux_stalled_request_times_out_and_releases_socket(monkeypatch, stop):
    entered = threading.Event()

    async def reply(*args):
        entered.set()
        await asyncio.sleep(10)
        yield _socket_stats(11, 1, 100, 200)

    diag, _ = _linux_diagnostics(monkeypatch, reply)
    monitor = NetworkThroughputMonitor({100})
    monkeypatch.setattr(monitor, "_socket_inodes", mock.Mock(return_value={11}))
    monitor.start()
    assert entered.wait(timeout=2.0)
    if stop:
        monitor.close()
    else:
        monitor.join(timeout=2.0)
        assert isinstance(monitor.error, asyncio.TimeoutError)
    assert not monitor.is_alive()
    diag.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("scenario", ["plain", "timestamp", "reused"])
def test_macos_stream_lifecycle(monkeypatch, scenario):
    prefix = "timestamp," if scenario == "timestamp" else ""
    frames = [
        [(100, 100, 200), (200, 300, 400), (999, 99_999_999, 99_999_999)],
        [(100, 1100, 2200), (200, 3300, 4400)],
        [(100, 1100, 2200), (200, 3300, 4400)],
    ]
    header = prefix + ",bytes_in,bytes_out,"
    lines = [header]
    for frame in frames:
        lines.extend(f"{prefix}python.{pid},{rx},{tx}," for pid, rx, tx in frame)
        lines.append(header)
    process = mock.Mock(stdout=io.StringIO("\n".join(lines)))
    process.poll.return_value = None
    factory, _ = _macos_command(monkeypatch, process)
    clock = mock.Mock(return_value=100.0)
    monkeypatch.setattr(network_module, "time", SimpleNamespace(monotonic=clock))
    monitor = NetworkThroughputMonitor({100, 200})
    monkeypatch.setattr(monitor._tracked_processes[200], "is_running", mock.Mock(return_value=scenario != "reused"))
    record = monitor._record
    rates = []

    def record_then_stop(counters, keep_missing):
        record(counters, keep_missing)
        rates.append(monitor.rate)
        clock.return_value += 1.0
        if len(rates) == 3:
            monitor._stop_event.set()

    monkeypatch.setattr(monitor, "_record", record_then_stop)
    monitor.run()
    assert monitor.error is None
    assert rates == [None, 3000.0 if scenario == "reused" else 10_000.0, 0.0]
    factory.assert_called_once()
    command = factory.call_args.args[0]
    assert command[command.index("-L") + 1] == "0"
    assert command[-4:] == ["-p", "100", "-p", "200"]
    assert factory.call_args.kwargs["stdout"] == 102
    network_module.os.close.assert_called_once_with(102)
    assert process.stdout.closed


@pytest.mark.parametrize("scenario", ["missing", "denied", "exit", "unsupported"])
def test_sampling_failure_is_nonfatal(monkeypatch, scenario):
    process = mock.Mock(stdout=io.StringIO(",bytes_in,bytes_out,\n"))
    process.poll.return_value = 1
    failure = {"missing": FileNotFoundError("nettop missing"), "denied": PermissionError("nettop denied")}.get(scenario)
    factory, output = _macos_command(monkeypatch, process, failure)
    if scenario == "unsupported":
        monkeypatch.setattr(network_module, "sys", SimpleNamespace(platform="win32"))
    monitor = NetworkThroughputMonitor({100})
    monitor.run()
    assert monitor.error is not None
    assert monitor.rate is None
    assert output.closed == (scenario != "unsupported")
    if scenario == "unsupported":
        factory.assert_not_called()
    else:
        factory.assert_called_once()


@pytest.mark.parametrize("ignore_sigterm", [False, True])
def test_macos_close_releases_blocked_reader(monkeypatch, ignore_sigterm):
    entered = threading.Event()
    stopped = threading.Event()
    process = mock.Mock(stdout=mock.MagicMock())
    process.poll.return_value = None

    def lines():
        entered.set()
        stopped.wait(timeout=3.0)
        return iter(())

    def release():
        process.poll.return_value = 0
        stopped.set()

    process.stdout.__iter__.side_effect = lines
    process.terminate.side_effect = None if ignore_sigterm else release
    process.kill.side_effect = release
    process.wait.side_effect = [subprocess.TimeoutExpired("nettop", 1.0), 0] if ignore_sigterm else [0]
    _macos_command(monkeypatch, process)
    monitor = NetworkThroughputMonitor({100})
    monitor.start()
    assert entered.wait(timeout=2.0)
    monitor.close()
    assert not monitor.is_alive()
    assert monitor.error is None
    assert process.kill.call_count == int(ignore_sigterm)
    process.stdout.close.assert_called_once()


def test_progress_updates_do_not_wait_for_sampler(monkeypatch):
    entered = threading.Event()
    monitor = NetworkThroughputMonitor({100})

    def slow_sampler():
        entered.set()
        monitor._stop_event.wait(timeout=3.0)

    monkeypatch.setattr(monitor, "_run_macos", slow_sampler)
    monkeypatch.setattr(network_module, "sys", SimpleNamespace(platform="darwin"))
    monkeypatch.setattr(progress_module, "NetworkThroughputMonitor", mock.Mock(return_value=monitor))
    with (
        ProgressBar(desc="Syncing", show_progress=True, show_network_throughput=True) as progress,
        progress.network_throughput({100}),
    ):
        assert entered.wait(timeout=1.0)
        started = time.monotonic()
        for _ in range(1000):
            progress.update_progress(0)
        assert time.monotonic() - started < 1.0
    assert not monitor.is_alive()
