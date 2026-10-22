import heapq
import os
import re
import shutil
import sys
import threading
import time
import types
from collections.abc import Iterator
from contextlib import closing, contextmanager
from typing import Any

from tqdm import tqdm
from tqdm.utils import disp_len, disp_trim

from .network_throughput import NetworkThroughputMonitor
from .transfer_progress import MAX_TRANSFER_DETAILS, TransferSnapshot


class CappedProgressBar(tqdm):
    """
    Custom tqdm that caps percentage at 99.9% unless truly complete.
    """

    def __init__(self, *args, max_bar_width: int | None = None, **kwargs):
        """
        Initialize the capped progress bar.
        """
        self.finalized = True
        self._max_bar_width = max_bar_width
        super().__init__(*args, **kwargs)

    @property
    def format_dict(self):
        """Include finalization state in meter formatting."""
        values = {**super().format_dict, "finalized": self.finalized}
        bar_format = values.get("bar_format")
        if self._max_bar_width is not None and bar_format is not None:
            width = self._max_bar_width
            if values["ncols"] is not None:
                width = max(1, min(width, values["ncols"] - disp_len(self.desc) - len("  [] 99.9%")))
            values["bar_format"] = bar_format.replace("{bar}", f"{{bar:{width}}}")
        return values

    @staticmethod
    def format_meter(n, total, finalized=True, **kwargs):
        """
        Override format_meter to cap percentage at 99.9% unless truly complete.
        """
        result = tqdm.format_meter(n, total, **kwargs)
        if total and total > 0 and (n < total or not finalized):
            capped = result.replace("100.0%", "99.9%").replace("100%", "99.9%")
            extra_width = disp_len(capped) - disp_len(result)
            if extra_width > 0 and kwargs.get("ncols"):
                kwargs["ncols"] = max(1, kwargs["ncols"] - extra_width)
                result = tqdm.format_meter(n, total, **kwargs)
                capped = result.replace("100.0%", "99.9%").replace("100%", "99.9%")
            result = capped
        return result


class ProgressBar:
    def __init__(
        self,
        desc: str,
        show_progress: bool,
        total_items: int = 0,
        show_network_throughput: bool = False,
        show_transfer_progress: bool = False,
        transfer_operation: str = "transfer",
        include_deletions: bool = False,
    ):
        """Create an item progress bar with optional local network and file activity."""
        if os.getenv("MSC_SUPPRESS_PROGRESS_BAR") is not None:
            show_progress = False

        self._show_network_throughput = show_network_throughput
        self._network_monitor: NetworkThroughputMonitor | None = None
        self._show_transfer_progress = show_transfer_progress
        self._completed_label, self._active_label = {
            "download": ("downloaded", "downloading"),
            "upload": ("uploaded", "uploading"),
            "copy": ("copied", "copying"),
            "transfer": ("transferred", "transferring"),
        }[transfer_operation]
        if include_deletions:
            self._completed_label = "completed"
        self._transfer_snapshots: dict[str, TransferSnapshot] = {}
        self._transfer_count = 0
        self._detail_rows: list[tqdm] = []
        self._summary_row: tqdm | None = None
        self._interactive = False
        self._last_render = 0.0
        self._lock = threading.RLock()
        self._closed = False
        self._phase: str | None = "listing / comparing" if show_transfer_progress else None

        if not show_progress or sys.stderr is None:
            self.pbar = None
            self._last_total_update_time = 0.0
            return

        isatty = getattr(sys.stderr, "isatty", None)
        self._interactive = bool(
            show_transfer_progress and callable(isatty) and isatty() and os.getenv("TERM") != "dumb"
        )

        # Initialize progress bar based on the 'total_items' provided at creation.
        bar_format = "{desc}: {percentage:3.1f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}{postfix}]"
        if self._interactive:
            bar_format = "{desc}  [{bar}] {percentage:.0f}%"
        self.pbar = CappedProgressBar(
            total=total_items,
            desc=desc,
            bar_format=bar_format,
            max_bar_width=20 if self._interactive else None,
            file=sys.stderr,
            dynamic_ncols=True,
            position=0,
            mininterval=1.0,  # Throttle update() calls to at most 1 refresh per second
            miniters=1,  # Check throttling on every update
            delay=0,
        )
        self._last_total_update_time = time.time()
        if self.pbar.disable:
            self.pbar = None
            self._interactive = False
            return
        self.pbar.finalized = not show_transfer_progress
        if self._interactive:
            if not self.pbar.ascii:
                self.pbar.ascii = "░▏▎▍▌▋▊▉█"
            self._summary_row = self._text_row(1)
            self._detail_rows = [self._text_row(position) for position in range(2, MAX_TRANSFER_DETAILS + 5)]
        self._update_network_throughput()

    @property
    def track_transfers(self) -> bool:
        """Whether an enabled progress display needs transfer activity updates."""
        return bool(self._show_transfer_progress and self.pbar is not None and not self.pbar.disable)

    @staticmethod
    def _text_row(position: int) -> tqdm:
        return tqdm(
            total=0,
            bar_format="{desc}",
            file=sys.stderr,
            position=position,
            dynamic_ncols=True,
            leave=False,
            delay=0,
        )

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

    def _network_status(self) -> str | None:
        if self._network_monitor is None:
            return "--B/s" if self._show_network_throughput else None
        rate = self._network_monitor.rate
        if self._network_monitor.error is not None:
            return "network unavailable"
        if rate is None:
            return "--B/s"
        value = tqdm.format_sizeof(rate, suffix="B")
        if self._interactive:
            value = re.sub(r"(?<=\d)([a-zA-Z]+)$", r" \1", value)
        return f"{value}/s"

    def _terminal_width(self) -> int:
        if self.pbar is not None:
            try:
                columns = os.get_terminal_size(self.pbar.fp.fileno()).columns
                if columns > 0:
                    return max(1, columns - 1)
            except (AttributeError, OSError, ValueError):
                pass
        columns = shutil.get_terminal_size(fallback=(80, 24)).columns
        return max(1, (columns if columns > 0 else 80) - 1)

    def _update_network_throughput(self) -> None:
        """Compose the summary using cached activity and process network samples."""
        if self.pbar is None:
            return
        rate = self._network_status()
        if self._summary_row is not None:
            completed = str(int(self.pbar.n))
            if self.pbar.total:
                completed += f"/{int(self.pbar.total)}"
            elapsed = tqdm.format_interval(self.pbar.format_dict["elapsed"])
            parts = [f"{completed} files {self._completed_label}", f"{self._transfer_count} {self._active_label}"]
            if rate is not None:
                parts.append(rate)
            parts.append(f"elapsed {elapsed}")
            if self._phase is not None and self._transfer_count == 0:
                parts.append(self._phase)
            width = self._terminal_width()
            summary = " · ".join(parts)
            if disp_len(summary) > width:
                parts = [f"{completed} done", f"{self._transfer_count} active"]
                if rate is not None:
                    parts.append(rate if rate == "network unavailable" else rate.replace(" ", ""))
                parts.append(elapsed)
                if self._phase is not None and self._transfer_count == 0:
                    parts.append(self._phase)
                summary = " · ".join(parts)
                if disp_len(summary) > width:
                    parts.remove(elapsed)
                    summary = " · ".join(parts)
            self._summary_row.set_description_str(disp_trim(summary, width), refresh=False)
            return
        parts = [rate] if rate is not None else []
        if self._show_transfer_progress:
            parts.append(f"in-flight: {self._transfer_count}")
            if self._phase is not None and self._transfer_count == 0:
                parts.append(self._phase)
        if parts:
            self.pbar.set_postfix_str(", ".join(parts), refresh=False)

    def update_transfers(self, worker_id: str, snapshot: TransferSnapshot) -> None:
        """Accept a worker's bounded snapshot without changing completed-file counts."""
        with self._lock:
            if self._closed:
                return
            if snapshot.count:
                self._transfer_snapshots[worker_id] = snapshot
            else:
                self._transfer_snapshots.pop(worker_id, None)
            self._transfer_count = sum(item.count for item in self._transfer_snapshots.values())

    def set_phase(self, phase: str | None) -> None:
        """Describe non-transfer work without treating zero active files as completion."""
        with self._lock:
            self._phase = phase
            self._update_network_throughput()

    def _refresh_details(self) -> None:
        if not self._detail_rows:
            return
        now = time.monotonic()
        oldest = heapq.nsmallest(
            MAX_TRANSFER_DETAILS,
            (activity for snapshot in self._transfer_snapshots.values() for activity in snapshot.oldest),
            key=lambda item: (item.started_at, item.path),
        )
        width = min(60, self._terminal_width())
        ages = [tqdm.format_interval(max(0.0, now - activity.started_at)) for activity in oldest]
        elapsed_width = max(len(value) for value in ["Elapsed", *ages])
        path_width = max(0, width - elapsed_width - 2)

        def table_row(path: str, elapsed: str) -> str:
            path = disp_trim(path, path_width)
            return disp_trim(
                path + " " * max(0, path_width - disp_len(path)) + "  " + elapsed.rjust(elapsed_width), width
            )

        lines = ["", table_row(f"Currently {self._active_label}", "Elapsed")] if oldest else ["", ""]
        for activity, age in zip(oldest, ages):
            path = "".join(character if character.isprintable() else "?" for character in activity.path)
            if disp_len(path) > path_width:
                filename = path.rsplit("/", 1)[-1]
                if disp_len(filename) + 4 <= path_width:
                    path = ".../" + filename
                elif path_width >= 3:
                    tail = disp_trim(filename[::-1], path_width - 3)[::-1]
                    path = "..." + tail
                else:
                    path = "." * path_width
            lines.append(table_row(path, age))
        hidden = self._transfer_count - len(oldest)
        lines.extend([""] * (MAX_TRANSFER_DETAILS - len(oldest)))
        lines.append(disp_trim(f"+ {hidden} more {self._active_label}", width) if hidden else "")
        for row, line in zip(self._detail_rows, lines):
            row.set_description_str(line, refresh=False)
            row.refresh()

    def update_total(self, new_total: int) -> None:
        """
        Update the progress bar total.

        :param new_total: Total item count to assign to the progress bar.
        """
        if self.pbar is not None and not self._closed:
            self.pbar.total = new_total
            # Custom throttling for update_total since it doesn't go through update()
            current_time = time.time()
            if current_time - self._last_total_update_time >= 1.0:
                self.refresh()
                self._last_total_update_time = current_time

    def update_progress(self, items_completed: int = 1) -> None:
        with self._lock:
            if self.pbar is not None and not self._closed:
                if self._show_transfer_progress:
                    self.pbar.n += items_completed
                    self.refresh()
                else:
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

    def refresh(self, force: bool = False) -> None:
        """
        Refresh the progress bar display without changing its counters.
        """
        with self._lock:
            if self.pbar is not None and not self._closed:
                if self._show_transfer_progress:
                    now = time.monotonic()
                    if not force and now - self._last_render < 1.0:
                        return
                    self._last_render = now
                self._update_network_throughput()
                self.pbar.refresh()
                if self._summary_row is not None:
                    self._summary_row.refresh()
                self._refresh_details()

    def close(self, complete: bool = False) -> None:
        with self._lock:
            if self._closed:
                return
            self._phase = None
            self._transfer_snapshots.clear()
            self._transfer_count = 0
            if self.pbar is not None:
                if complete:
                    self.pbar.finalized = True
                self.refresh(force=True)
                for row in reversed(self._detail_rows):
                    row.close()
                self._detail_rows.clear()
                summary = ""
                if self._summary_row is not None:
                    summary = self._summary_row.desc
                    self._summary_row.close()
                    self._summary_row = None
                self.pbar.close()
                if summary:
                    self.pbar.fp.write(summary + "\n")
                    self.pbar.fp.flush()
            self._closed = True

    def __enter__(self) -> "ProgressBar":  # noqa: PYI034
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: types.TracebackType | None
    ) -> None:
        self.close()
