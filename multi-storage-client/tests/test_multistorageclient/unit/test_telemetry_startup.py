# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import atexit
import os
import signal
import time
from multiprocessing import get_context
from multiprocessing.context import SpawnProcess
from multiprocessing.synchronize import Event
from pathlib import Path
from unittest.mock import Mock

import pytest

from multistorageclient import telemetry
from multistorageclient.providers.posix_file import PosixFileStorageProvider


def _stall_before_ready(entered: Event, ignore_terminate: bool) -> None:
    if ignore_terminate:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    entered.set()
    time.sleep(60)


def _exit_before_ready() -> None:
    os._exit(23)


@pytest.mark.timeout(90)
@pytest.mark.parametrize("ignore_terminate", [False, True])
def test_startup_timeout_cleans_up_and_allows_retry(monkeypatch: pytest.MonkeyPatch, ignore_terminate: bool) -> None:
    context = get_context("spawn")
    entered = context.Event()
    manager: telemetry.TelemetryManager | None = None
    original_start = telemetry.TelemetryManager.start
    original_process_start = context.Process.start
    started = 0.0

    def start_and_wait_for_initializer(process: SpawnProcess) -> None:
        nonlocal started
        original_process_start(process)
        # Spawn imports can be slow under coverage and parallel test load.
        assert entered.wait(30)
        started = time.monotonic()

    def start_stalled(child_manager: telemetry.TelemetryManager) -> None:
        nonlocal manager
        manager = child_manager
        original_start(manager, _stall_before_ready, (entered, ignore_terminate))

    monkeypatch.setattr(telemetry, "_TELEMETRY_PROXIES", {})
    monkeypatch.setattr(telemetry, "_MANAGER_START_TIMEOUT_SECONDS", 3.0)
    monkeypatch.setattr(telemetry.TelemetryManager, "start", start_stalled)
    monkeypatch.setattr(context.Process, "start", start_and_wait_for_initializer)

    try:
        with pytest.raises(TimeoutError, match="Telemetry manager did not start within 3 seconds"):
            telemetry.init(mode=telemetry.TelemetryMode.SERVER, address=("127.0.0.1", 0))
        assert manager is not None
        assert time.monotonic() - started < 8
        assert entered.is_set()
        assert not manager._process.is_alive()
        assert manager._process.exitcode == -(signal.SIGKILL if ignore_terminate else signal.SIGTERM)
        assert not telemetry._TELEMETRY_PROXIES
        assert telemetry._TELEMETRY_PROXIES_LOCK.acquire(blocking=False)
        telemetry._TELEMETRY_PROXIES_LOCK.release()
    finally:
        if manager is not None:
            if manager._process.is_alive():
                manager._process.kill()
            manager._process.join(3)

    monkeypatch.setattr(telemetry.TelemetryManager, "start", original_start)
    monkeypatch.setattr(context.Process, "start", original_process_start)
    monkeypatch.setattr(telemetry, "_MANAGER_START_TIMEOUT_SECONDS", 30.0)
    resources = telemetry.init(mode=telemetry.TelemetryMode.SERVER, address=("127.0.0.1", 0))
    healthy_manager = resources._manager  # pyright: ignore [reportAttributeAccessIssue]
    try:
        assert healthy_manager._process.is_alive()
        assert resources is telemetry.init(mode=telemetry.TelemetryMode.SERVER, address=("127.0.0.1", 0))
    finally:
        healthy_manager.shutdown()
        atexit.unregister(healthy_manager.shutdown)
    assert not healthy_manager._process.is_alive()


@pytest.mark.timeout(15)
def test_startup_child_exit_preserves_error_and_reaps_child() -> None:
    manager = telemetry.TelemetryManager(address=("127.0.0.1", 0), ctx=get_context("spawn"))
    try:
        with pytest.raises(EOFError):
            manager.start(initializer=_exit_before_ready)
        assert not manager._process.is_alive()
        assert manager._process.exitcode == 23
    finally:
        if manager._process.is_alive():
            manager._process.kill()
        manager._process.join(3)


def test_storage_operation_continues_after_telemetry_timeout(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    telemetry_provider = Mock(side_effect=TimeoutError("Telemetry manager did not start within 30 seconds"))
    provider = PosixFileStorageProvider(
        base_path=str(tmp_path), config_dict={"opentelemetry": {"metrics": {}}}, telemetry_provider=telemetry_provider
    )
    (tmp_path / "data.txt").write_bytes(b"payload")

    assert provider.get_object("data.txt") == b"payload"
    assert provider.get_object("data.txt") == b"payload"
    telemetry_provider.assert_called_once_with()
    assert "Failed to setup metrics: Telemetry manager did not start within 30 seconds" in caplog.text
