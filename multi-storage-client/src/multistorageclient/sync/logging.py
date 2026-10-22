# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import sys
import threading
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from logging.handlers import QueueHandler
from typing import cast

from tqdm.contrib.logging import logging_redirect_tqdm

from .types import OperationType, QueueLike

_redirect_lock = threading.RLock()
_redirect_users = 0
_redirect_stack = ExitStack()


def _msc_loggers() -> list[logging.Logger]:
    return [logging.getLogger("multistorageclient")] + [
        logger
        for name, logger in logging.Logger.manager.loggerDict.copy().items()
        if name.startswith("multistorageclient.") and isinstance(logger, logging.Logger)
    ]


@contextmanager
def redirect_sync_logging(enabled: bool = True) -> Iterator[None]:
    """Share one console redirection across overlapping interactive syncs, restoring it after the last exit."""
    if not enabled:
        yield
        return

    global _redirect_users
    with _redirect_lock:
        if _redirect_users == 0:
            root = logging.getLogger()
            loggers = [
                logger
                for logger in [root, *_msc_loggers()]
                if (not logger.handlers and (logger is root or not logger.propagate))
                or any(
                    isinstance(handler, logging.StreamHandler) and handler.stream in (sys.stdout, sys.stderr)
                    for handler in logger.handlers
                )
            ]
            _redirect_stack.enter_context(logging_redirect_tqdm(loggers=loggers))
        _redirect_users += 1
    try:
        yield
    finally:
        with _redirect_lock:
            _redirect_users -= 1
            if _redirect_users == 0:
                _redirect_stack.close()


class _ResultQueueHandler(QueueHandler):
    def enqueue(self, record: logging.LogRecord) -> None:
        cast(QueueLike, self.queue).put((OperationType.LOG, None, record))


@contextmanager
def worker_logging(result_queue: QueueLike) -> Iterator[None]:
    """Send spawned workers' MSC logs to the process that owns the progress display."""
    loggers = _msc_loggers()
    original = [(logger, logger.handlers, logger.propagate) for logger in loggers]
    handler = _ResultQueueHandler(result_queue)
    try:
        for logger in loggers:
            logger.handlers = []
            logger.propagate = True
        loggers[0].handlers = [handler]
        loggers[0].propagate = False
        yield
    finally:
        for logger, handlers, propagate in original:
            logger.handlers = handlers
            logger.propagate = propagate
        handler.close()
