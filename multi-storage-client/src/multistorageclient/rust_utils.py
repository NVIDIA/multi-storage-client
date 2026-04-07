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

import asyncio
import threading
from collections.abc import Callable, Coroutine
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Optional, TypeVar

if TYPE_CHECKING:
    from multistorageclient_rust import RustRetryConfig

# Retry configuration defaults (matching Rust defaults)
DEFAULT_RETRY_ATTEMPTS = 10
DEFAULT_RETRY_TIMEOUT = 180
DEFAULT_RETRY_INIT_BACKOFF_MS = 100
DEFAULT_RETRY_MAX_BACKOFF = 15
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0

_T = TypeVar("_T")


def parse_retry_config(configs: dict[str, Any]) -> Optional["RustRetryConfig"]:
    """
    Parse retry configuration from rust_client configs dict.

    Extracts 'retry' key from configs and converts it to RustRetryConfig object.
    Removes the 'retry' key from configs dict as a side effect.

    :param configs: The rust_client configuration dictionary.
    :return: RustRetryConfig object if retry config is present, None otherwise.
    """
    if "retry" not in configs:
        return None

    from multistorageclient_rust import RustRetryConfig

    retry_dict = configs.pop("retry")

    # If already a RustRetryConfig object, return it directly
    if isinstance(retry_dict, RustRetryConfig):
        return retry_dict

    # If dict, create RustRetryConfig with values or defaults
    if isinstance(retry_dict, dict):
        return RustRetryConfig(
            attempts=retry_dict.get("attempts", DEFAULT_RETRY_ATTEMPTS),
            timeout=retry_dict.get("timeout", DEFAULT_RETRY_TIMEOUT),
            init_backoff_ms=retry_dict.get("init_backoff_ms", DEFAULT_RETRY_INIT_BACKOFF_MS),
            max_backoff=retry_dict.get("max_backoff", DEFAULT_RETRY_MAX_BACKOFF),
            backoff_multiplier=retry_dict.get("backoff_multiplier", DEFAULT_RETRY_BACKOFF_MULTIPLIER),
        )

    return None


def run_coroutine_sync(coro_factory: Callable[[], Coroutine[Any, Any, _T]]) -> _T:
    """
    Run a coroutine from synchronous code.

    If the current thread already has a running event loop, execute the coroutine
    in a helper thread so nested loop errors are avoided.
    """

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro_factory())

    result: Future[_T] = Future()

    def _runner() -> None:
        try:
            result.set_result(asyncio.run(coro_factory()))
        except BaseException as exc:
            result.set_exception(exc)

    thread = threading.Thread(target=_runner)
    thread.start()
    thread.join()
    return result.result()


def run_async_rust_client_method(rust_client: Any, method_name: str, *args, **kwargs) -> Any:
    """
    Run a method of the rust client asynchronously.

    :param rust_client: The rust client instance.
    :param method_name: The name of the method to call.
    :param args: Positional arguments for the method.
    :param kwargs: Keyword arguments for the method.

    :return: The result of the method call.
    """

    if not hasattr(rust_client, method_name):
        raise AttributeError(f"MSC Rust client has no method '{method_name}'")

    async def _run_method():
        method = getattr(rust_client, method_name)
        return await method(*args, **kwargs)

    return run_coroutine_sync(_run_method)
