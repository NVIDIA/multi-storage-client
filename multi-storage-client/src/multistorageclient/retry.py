# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import logging
import random
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import wraps
from typing import Any

from .types import BatchTransferError, BatchTransferFailure, RetryableError


@dataclass
class _RetryDecision:
    """
    Result of classifying an exception raised by a retried operation.
    """

    retryable: bool
    error: Exception
    message: str
    log_non_retryable: bool = True


def sleep_before_retry(retry_config: Any, attempt: int) -> None:
    """
    Sleep before the next retry attempt using MSC exponential backoff and jitter.

    :param retry_config: Retry configuration with delay and backoff multiplier fields.
    :param attempt: Zero-based retry attempt that just failed.
    """
    delay = retry_config.delay * (retry_config.backoff_multiplier**attempt)
    delay += random.uniform(0, 1)
    time.sleep(delay)


def _run_with_retry(
    operation_name: str,
    retry_config: Any,
    call: Callable[[], Any],
    classify_error: Callable[[Exception], _RetryDecision],
) -> Any:
    """
    Run an operation with the common MSC retry loop.

    :param operation_name: Name used in retry log messages.
    :param retry_config: Retry configuration. If ``None``, the operation is called once.
    :param call: Callable that performs one operation attempt.
    :param classify_error: Callable that maps a raised exception to a retry decision.
    :return: The value returned by ``call``.
    :raises Exception: The final classified exception when the operation is not retryable or attempts are exhausted.
    """
    attempts = retry_config.attempts if retry_config is not None else 1
    for attempt in range(attempts):
        try:
            return call()
        except Exception as e:
            decision = classify_error(e)
            if not decision.retryable:
                if decision.log_non_retryable:
                    logging.error("Non-retryable error occurred for %s: %s", operation_name, decision.error)
                raise decision.error

            if retry_config is None:
                raise decision.error

            logging.warning("Attempt %d failed for %s: %s", attempt + 1, operation_name, decision.message)
            if attempt < attempts - 1:
                sleep_before_retry(retry_config, attempt)
            else:
                logging.error("All retry attempts failed for %s", operation_name)
                raise decision.error


def retry(func: Callable) -> Callable:
    """
    Decorator to retry a function call if a retryable error is raised.

    The decorated method must be on an object with an optional ``_retry_config``
    attribute. Only :py:class:`multistorageclient.types.RetryableError` is
    retried; other exceptions are raised immediately.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        storage_client_instance = args[0]
        retry_config = getattr(storage_client_instance, "_retry_config", None)

        def _classify_error(error: Exception) -> _RetryDecision:
            if isinstance(error, RetryableError):
                return _RetryDecision(retryable=True, error=error, message=str(error))
            if isinstance(error, FileNotFoundError):
                # FileNotFoundError is expected in many scenarios (e.g., zarr probing for metadata files)
                # Log at debug level to avoid cluttering logs with expected 404s
                logging.debug("File not found for %s: %s", func.__name__, error)
                return _RetryDecision(
                    retryable=False,
                    error=error,
                    message=str(error),
                    log_non_retryable=False,
                )
            return _RetryDecision(retryable=False, error=error, message=str(error))

        return _run_with_retry(
            operation_name=func.__name__,
            retry_config=retry_config,
            call=lambda: func(*args, **kwargs),
            classify_error=_classify_error,
        )

    return wrapper


def batch_retry(func: Callable | None = None, *, operation_name: str | None = None) -> Callable:
    """
    Decorator to retry batch operations that raise BatchTransferError with item-level failures.

    The decorated method must accept an ``indices`` argument that identifies the
    original item indices to attempt. On retry, only indices associated with
    retryable item failures are attempted again.

    :param func: Decorated batch method when used as ``@batch_retry``.
    :param operation_name: Optional operation name to use in retry log messages.
    :return: Decorated batch method.
    """

    def decorator(batch_func: Callable) -> Callable:
        @wraps(batch_func)
        def wrapper(
            storage_client_instance: Any,
            indices: Sequence[int],
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            retry_config = getattr(storage_client_instance, "_retry_config", None)
            pending_indices = list(indices)
            name = operation_name or batch_func.__name__

            def _call_batch() -> Any:
                return batch_func(storage_client_instance, pending_indices, *args, **kwargs)

            def _classify_error(error: Exception) -> _RetryDecision:
                nonlocal pending_indices
                if isinstance(error, BatchTransferError):
                    batch_error, failed_indices = _remap_batch_transfer_error(error, pending_indices)

                    if any(not isinstance(failure.error, RetryableError) for failure in batch_error.failures):
                        return _RetryDecision(
                            retryable=False,
                            error=batch_error,
                            message=str(batch_error),
                        )

                    pending_indices = failed_indices
                    return _RetryDecision(
                        retryable=True,
                        error=batch_error,
                        message=f"{len(batch_error.failures)} item(s) failed",
                    )
                if isinstance(error, RetryableError):
                    return _RetryDecision(retryable=True, error=error, message=str(error))
                return _RetryDecision(retryable=False, error=error, message=str(error))

            return _run_with_retry(
                operation_name=name,
                retry_config=retry_config,
                call=_call_batch,
                classify_error=_classify_error,
            )

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator


def _remap_batch_transfer_error(
    error: BatchTransferError, pending_indices: Sequence[int]
) -> tuple[BatchTransferError, list[int]]:
    """
    Convert failed subset-relative indices back to the original batch indices.

    :param error: Batch error raised by a provider call for the current pending subset.
    :param pending_indices: Original batch indices included in the provider call.
    :return: A remapped batch error and the original indices that failed.
    """
    remapped_failures: list[BatchTransferFailure] = []
    failed_indices: list[int] = []
    for failure in error.failures:
        original_index = pending_indices[failure.index]
        failed_indices.append(original_index)
        remapped_failures.append(
            BatchTransferFailure(
                index=original_index,
                source_path=failure.source_path,
                destination_path=failure.destination_path,
                error=failure.error,
            )
        )
    return BatchTransferError(remapped_failures), failed_indices
