# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Internal file activity reporting for local synchronization."""

import heapq
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from ..types import BatchTransferCallback, TransferState

MAX_TRANSFER_DETAILS = 5


@dataclass(frozen=True)
class TransferActivity:
    path: str
    started_at: float


@dataclass(frozen=True)
class TransferSnapshot:
    count: int
    oldest: tuple[TransferActivity, ...]


class TransferProgress:
    """Aggregate file activity locally and publish bounded snapshots once per second."""

    def __init__(self, publish: Callable[[TransferSnapshot], None]):
        self._publish = publish
        self._active: dict[object, TransferActivity] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def snapshot(self) -> TransferSnapshot:
        """Return the active count and a bounded list of oldest file operations."""
        with self._lock:
            oldest = heapq.nsmallest(MAX_TRANSFER_DETAILS, self._active.values(), key=lambda item: item.started_at)
            return TransferSnapshot(len(self._active), tuple(oldest))

    def callback(self, display_paths: Mapping[str, str], *, local_source: bool = False) -> BatchTransferCallback:
        """Create a batch observer with independent keys and logical display paths."""
        batch = object()

        def transfer_callback(source_path: str, destination_path: str, state: TransferState) -> None:
            local_path = source_path if local_source else destination_path
            key = (batch, source_path, destination_path)
            with self._lock:
                if state == "started":
                    if not self._stop.is_set():
                        self._active[key] = TransferActivity(display_paths[local_path], time.monotonic())
                else:
                    self._active.pop(key, None)

        return transfer_callback

    def start(self) -> None:
        """Start background snapshot reporting."""
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(1.0):
            self._publish(self.snapshot())

    def close(self) -> None:
        """Stop reporting and publish an empty final snapshot."""
        self._stop.set()
        if self._thread.ident is not None:
            self._thread.join()
        with self._lock:
            self._active.clear()
        self._publish(self.snapshot())
