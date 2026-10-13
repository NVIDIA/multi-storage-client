# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import contextlib
import csv
import importlib
import logging
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import TextIO

import psutil

logger = logging.getLogger(__name__)


class NetworkThroughputMonitor(threading.Thread):
    """Sample network traffic for a fixed set of local sync processes in the background."""

    def __init__(self, pids: set[int]):
        super().__init__(daemon=True)
        self._tracked_processes: dict[int, psutil.Process] = {}
        self._stop_event = threading.Event()
        self._process: subprocess.Popen[str] | None = None
        self._output: TextIO | None = None
        self._previous: dict[int, tuple[int, int]] = {}
        self._reading: tuple[float | None, float | None] = (None, None)
        self.error: Exception | None = None
        for pid in pids:
            try:
                self._tracked_processes[pid] = psutil.Process(pid)
            except psutil.NoSuchProcess:
                continue
            except psutil.Error as error:
                self.error = error

    @property
    def rate(self) -> float | None:
        """Return the latest combined receive/send rate, or None while a sample is unavailable."""
        rate, sampled_at = self._reading
        return rate if sampled_at is not None and time.monotonic() - sampled_at <= 3.0 else None

    def _record(self, counters: dict[int, tuple[int, int]], keep_missing: bool = False) -> None:
        now = time.monotonic()
        _, previous_time = self._reading
        rate = None
        if previous_time is not None and now > previous_time:
            transferred = 0
            for key, (received, sent) in counters.items():
                previous_received, previous_sent = self._previous.get(key, (0, 0))
                transferred += max(0, received - previous_received) + max(0, sent - previous_sent)
            rate = transferred / (now - previous_time)
        if keep_missing:
            self._previous.update(counters)
        else:
            self._previous = counters
        self._reading = (rate, now)

    def _socket_inodes(self) -> set[int]:
        inodes = set()
        for pid, process in self._tracked_processes.items():
            if not process.is_running():
                continue
            try:
                entries = list(Path(f"/proc/{pid}/fd").iterdir())
            except FileNotFoundError:
                continue
            for entry in entries:
                try:
                    target = os.readlink(entry)
                except FileNotFoundError:
                    continue
                if target.startswith("socket:[") and target.endswith("]"):
                    inodes.add(int(target[8:-1]))
        return inodes

    async def _sample_linux(self) -> None:
        netlink = importlib.import_module("pyroute2.netlink")
        diagnostics = importlib.import_module("pyroute2.netlink.diag")
        sockets = importlib.import_module("pyroute2.netlink.nlsocket")
        async with sockets.AsyncNetlinkSocket(
            family=netlink.NETLINK_SOCK_DIAG, use_event_loop=asyncio.get_running_loop()
        ) as diag:
            diag.set_marshal(diagnostics.MarshalDiag())
            await asyncio.wait_for(diag.bind(), timeout=1.0)

            async def get_stats(family):
                request = diagnostics.inet_diag_req()
                request.update(
                    sdiag_family=family, sdiag_protocol=socket.IPPROTO_TCP, idiag_states=diagnostics.SS_ALL, idiag_ext=2
                )
                return [
                    stats
                    async for stats in await diag.nlm_request(
                        request,
                        diagnostics.SOCK_DIAG_BY_FAMILY,
                        netlink.NLM_F_REQUEST | netlink.NLM_F_ROOT | netlink.NLM_F_MATCH,
                    )
                ]

            while not self._stop_event.is_set():
                inodes = self._socket_inodes()
                counters = {}
                for family in (socket.AF_INET, socket.AF_INET6):
                    if self._stop_event.is_set():
                        return
                    for stats in await asyncio.wait_for(get_stats(family), timeout=1.0):
                        if stats["idiag_inode"] not in inodes:
                            continue
                        info = stats.get_attr("INET_DIAG_INFO")
                        if info is None or "tcpi_bytes_received" not in info:
                            raise RuntimeError("TCP byte counters are unavailable")
                        counters[stats["idiag_cookie"]] = (
                            info["tcpi_bytes_received"],
                            info.get("tcpi_bytes_sent") or info.get("tcpi_bytes_acked", 0),
                        )
                self._record(counters)
                await asyncio.sleep(1.0)

    def _run_macos(self) -> None:
        import pty

        command = ["nettop", "-P", "-n", "-x", "-L", "0", "-s", "1", "-J", "bytes_in,bytes_out"]
        for pid in sorted(self._tracked_processes):
            command.extend(("-p", str(pid)))
        # nettop buffers CSV output to a pipe; a pseudo-terminal delivers each sample immediately.
        master, slave = pty.openpty()
        try:
            self._output = os.fdopen(master, "r")
            self._process = subprocess.Popen(
                command, stdin=subprocess.DEVNULL, stdout=slave, stderr=subprocess.DEVNULL, text=True
            )
        finally:
            os.close(slave)
            if self._output is None:
                os.close(master)
        if self._stop_event.is_set():
            return
        assert self._output is not None
        columns: tuple[int, int] | None = None
        counters: dict[int, tuple[int, int]] = {}
        for row in csv.reader(self._output):
            if self._stop_event.is_set():
                return
            if "bytes_in" in row and "bytes_out" in row:
                if columns is not None:
                    self._record(counters, keep_missing=True)
                columns = (row.index("bytes_in"), row.index("bytes_out"))
                counters = {}
            elif columns is not None and row:
                _, _, pid = row[columns[0] - 1].rpartition(".")
                process = self._tracked_processes.get(int(pid)) if pid.isdigit() else None
                if process is not None and process.is_running():
                    counters[int(pid)] = (int(row[columns[0]]), int(row[columns[1]]))
        if not self._stop_event.is_set():
            raise RuntimeError("nettop stopped producing network samples")

    def run(self) -> None:
        try:
            if self.error is not None or self._stop_event.is_set():
                return
            if sys.platform == "darwin":
                self._run_macos()
            elif sys.platform == "linux":
                asyncio.run(self._sample_linux())
            else:
                raise RuntimeError("Process network counters are unsupported on this platform")
        except Exception as error:
            if not self._stop_event.is_set():
                self.error = error
                logger.debug("Process network sampling unavailable: %s", error)
        finally:
            try:
                if self._process is not None:
                    self._terminate_process()
            finally:
                if self._output is not None:
                    self._output.close()

    def _terminate_process(self) -> None:
        assert self._process is not None
        if self._process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                self._process.terminate()
            try:
                self._process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    self._process.kill()
                self._process.wait(timeout=1.0)

    def close(self) -> None:
        """Stop sampling and release the owned subprocess and thread."""
        self._stop_event.set()
        if self._process is not None:
            self._terminate_process()
        if self.ident is not None:
            self.join(timeout=3.0)
