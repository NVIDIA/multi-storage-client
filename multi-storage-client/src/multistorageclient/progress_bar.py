import logging
import os
import sys
from typing import Any

from tqdm import tqdm

logger = logging.getLogger(__name__)


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
    def __init__(self, desc: str, show_progress: bool, total_items: int = 0):
        # If the env var is defined, always suppress the progress bar
        legacy_msc_suppress = os.getenv("SUPPRESS_PROGRESS_BAR") is not None
        msc_suppress = os.getenv("MSC_SUPPRESS_PROGRESS_BAR") is not None

        if msc_suppress or legacy_msc_suppress:
            show_progress = False

        if legacy_msc_suppress:
            logger.warning(
                "Env var 'SUPPRESS_PROGRESS_BAR' is deprecated and will be removed; use 'MSC_SUPPRESS_PROGRESS_BAR'."
            )

        if not show_progress:
            self.pbar = None
            return

        # Initialize progress bar based on the 'total_items' provided at creation.
        bar_format = "{desc}: {percentage:3.1f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}{postfix}]"
        self.pbar = CappedProgressBar(
            total=total_items, desc=desc, bar_format=bar_format, file=sys.stderr, dynamic_ncols=True, position=0
        )

    def update_total(self, new_total: int) -> None:
        # Dynamically update the total work and refresh the progress bar.
        if self.pbar is not None:
            self.pbar.total = new_total
            self.pbar.refresh()

    def update_progress(self, items_completed: int = 1) -> None:
        # Update the progress bar, if it exists.
        if self.pbar is not None:
            self.pbar.update(items_completed)

    def reset_progress(self, items_completed: int = 0) -> None:
        if self.pbar is not None:
            self.pbar.n = items_completed
            self.pbar.refresh()

    def set_postfix(self, **kwargs: Any) -> None:
        if self.pbar is not None:
            self.pbar.set_postfix(**kwargs)

    def set_postfix_str(self, s: str, refresh: bool = False) -> None:
        if self.pbar is not None:
            self.pbar.set_postfix_str(s, refresh)

    def close(self) -> None:
        if self.pbar is not None:
            self.pbar.close()

    def __enter__(self) -> "ProgressBar":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
