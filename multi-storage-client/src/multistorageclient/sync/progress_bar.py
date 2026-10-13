import os
import sys
import time
import types
from collections.abc import Iterator
from contextlib import closing, contextmanager
from typing import Any

from tqdm import tqdm

from .network_throughput import NetworkThroughputMonitor


class CappedProgressBar(tqdm):
    """
    Custom tqdm that caps percentage at 99.9% unless truly complete.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the capped progress bar.
        """
        super().__init__(*args, **kwargs)

    @staticmethod
    def format_meter(n, total, **kwargs):
        """
        Override format_meter to cap percentage at 99.9% unless truly complete.
        """
        result = tqdm.format_meter(n, total, **kwargs)
        if total and total > 0 and n < total:
            result = result.replace("100.0%", "99.9%")
        return result


class ProgressBar:
    def __init__(self, desc: str, show_progress: bool, total_items: int = 0, show_network_throughput: bool = False):
        """Create an item progress bar with optional local process network throughput."""
        if os.getenv("MSC_SUPPRESS_PROGRESS_BAR") is not None:
            show_progress = False

        self._show_network_throughput = show_network_throughput
        self._network_monitor: NetworkThroughputMonitor | None = None

        if not show_progress:
            self.pbar = None
            self._last_total_update_time = 0.0
            return

        # Initialize progress bar based on the 'total_items' provided at creation.
        bar_format = "{desc}: {percentage:3.1f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}{postfix}]"
        self.pbar = CappedProgressBar(
            total=total_items,
            desc=desc,
            bar_format=bar_format,
            file=sys.stderr,
            dynamic_ncols=True,
            position=0,
            mininterval=1.0,  # Throttle update() calls to at most 1 refresh per second
            miniters=1,  # Check throttling on every update
        )
        self._last_total_update_time = time.time()

    @contextmanager
    def network_throughput(self, pids: set[int]) -> Iterator[None]:
        """Sample network traffic for the duration of local worker execution."""
        if not self._show_network_throughput or self.pbar is None:
            yield
            return
        with closing(NetworkThroughputMonitor(pids)) as monitor:
            self._network_monitor = monitor
            try:
                monitor.start()
            except RuntimeError as error:
                monitor.error = error
            self._update_network_throughput()
            yield

    def _update_network_throughput(self) -> None:
        """Read cached process network throughput without blocking progress updates."""
        if self._network_monitor is None or self.pbar is None:
            return
        rate = self._network_monitor.rate
        if self._network_monitor.error is not None:
            postfix = "network unavailable"
        elif rate is None:
            postfix = "--B/s"
        else:
            postfix = f"{tqdm.format_sizeof(rate, suffix='B')}/s"
        self.pbar.set_postfix_str(postfix, refresh=False)

    def update_total(self, new_total: int) -> None:
        """
        Update the progress bar total.

        :param new_total: Total item count to assign to the progress bar.
        """
        if self.pbar is not None:
            self.pbar.total = new_total
            # Custom throttling for update_total since it doesn't go through update()
            current_time = time.time()
            if current_time - self._last_total_update_time >= 1.0:
                self.refresh()
                self._last_total_update_time = current_time

    def update_progress(self, items_completed: int = 1) -> None:
        if self.pbar is not None:
            self._update_network_throughput()
            self.pbar.update(items_completed)

    def reset_progress(self, items_completed: int = 0) -> None:
        if self.pbar is not None:
            self.pbar.n = items_completed
            self.refresh()

    def set_postfix(self, **kwargs: Any) -> None:
        if self.pbar is not None:
            self.pbar.set_postfix(**kwargs)

    def set_postfix_str(self, s: str, refresh: bool = False) -> None:
        if self.pbar is not None:
            self.pbar.set_postfix_str(s, refresh)

    def refresh(self) -> None:
        """
        Refresh the progress bar display without changing its counters.
        """
        if self.pbar is not None:
            self._update_network_throughput()
            self.pbar.refresh()

    def close(self) -> None:
        if self.pbar is not None:
            self.refresh()
            self.pbar.close()

    def __enter__(self) -> "ProgressBar":  # noqa: PYI034
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: types.TracebackType | None
    ) -> None:
        self.close()
