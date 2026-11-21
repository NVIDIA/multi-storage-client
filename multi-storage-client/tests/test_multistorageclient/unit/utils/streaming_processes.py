# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

"""
Child-process helpers for the streaming write tests.

Spawned children import this module to load their target, so it must not import torch or other heavy packages.
"""

import multiprocessing
import queue
import time
from collections.abc import Callable
from typing import Any

from multistorageclient import StorageClient, StorageClientConfig, StreamingWriterOptions


def payload(size: int) -> bytes:
    return (bytes(range(251)) * (size // 251 + 1))[:size]


def write_object(config_dict: dict[str, Any], path: str, size: int, options: StreamingWriterOptions) -> int:
    client = StorageClient(StorageClientConfig.from_dict(config_dict, profile="stream"))
    with client.open(path, "wb", writer_options=options) as f:
        f.write(payload(size))
        return f.tell()


def fork_writers(
    config_dict: dict[str, Any], jobs: list[tuple[str, int]], options: StreamingWriterOptions, timeout: float
) -> list[int]:
    # Forked children cannot use a Rust runtime their parent already started, as with buffered Rust uploads,
    # so this parent opens no streams before forking, like NVRx's multiprocess writer.
    StorageClient(StorageClientConfig.from_dict(config_dict, profile="stream"))
    args = [(config_dict, path, size, options) for path, size in jobs]
    return run_in_processes(multiprocessing.get_context("fork"), write_object, args, timeout)


def _report(results: Any, index: int, target: Callable[..., Any], args: tuple) -> None:
    try:
        results.put((index, target(*args), None))
    except BaseException as error:
        results.put((index, None, repr(error)))


def run_in_processes(ctx: Any, target: Callable[..., Any], args_list: list[tuple], timeout: float) -> list[Any]:
    """
    Run ``target(*args)`` in one ``ctx`` process per entry of ``args_list`` and return the results in order.

    Unlike ``multiprocessing.Pool``, which waits forever for a task whose worker died, a child that dies or stalls
    fails this call within ``timeout``.
    """
    results = ctx.Queue()
    processes = [ctx.Process(target=_report, args=(results, i, target, args)) for i, args in enumerate(args_list)]
    for process in processes:
        process.start()
    try:
        deadline = time.monotonic() + timeout
        outcomes: dict[int, Any] = {}
        while len(outcomes) < len(processes):
            try:
                index, value, error = results.get(timeout=1)
            except queue.Empty:
                missing = {i: processes[i].exitcode for i in range(len(processes)) if i not in outcomes}
                if time.monotonic() > deadline or None not in missing.values():
                    raise RuntimeError(
                        f"Child processes did not report a result (index: exit code): {missing}"
                    ) from None
                continue
            if error is not None:
                raise RuntimeError(f"Child process {index} failed: {error}")
            outcomes[index] = value
        return [outcomes[i] for i in range(len(processes))]
    finally:
        for process in processes:
            process.join(timeout=5)
            if process.is_alive():
                # Kill, not terminate: a Python SIGTERM handler never runs while the child is stuck in native code.
                process.kill()
                process.join()
