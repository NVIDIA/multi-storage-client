# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

# pyright: reportCallIssue=false
# pyright: reportGeneralTypeIssues=false


import asyncio
import queue
from typing import Any, Optional

import ray


@ray.remote
class _QueueActor:
    """
    Internal Ray actor that wraps asyncio.Queue for distributed use.

    This actor provides a simple, async-safe queue that can be shared and
    accessed by multiple Ray workers across a cluster. It exposes the common
    queue methods like `put`, `get`, `qsize`, etc., for remote invocation.

    Using asyncio.Queue allows the actor to handle multiple concurrent
    operations without blocking, enabling producers and consumers to work
    simultaneously.

    It is useful for producer-consumer patterns where tasks or data need
    to be passed between different parts of a distributed Ray application.
    """

    def __init__(self, maxsize: int = 0):
        """
        Initializes the queue.

        :param maxsize: The maximum size of the queue. If 0 or negative, the queue size is infinite.
        """
        self._queue = asyncio.Queue(maxsize=maxsize)

    async def put(self, item: Any, block: bool = True, timeout: Optional[float] = None):
        if not block:
            try:
                self._queue.put_nowait(item)
            except asyncio.QueueFull:
                raise queue.Full("Queue is full")
        else:
            try:
                if timeout is None:
                    await self._queue.put(item)
                else:
                    await asyncio.wait_for(self._queue.put(item), timeout=timeout)
            except asyncio.TimeoutError:
                raise queue.Full("Queue is full")

    async def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        if not block:
            try:
                return self._queue.get_nowait()
            except asyncio.QueueEmpty:
                raise queue.Empty("Queue is empty")
        else:
            try:
                if timeout is None:
                    return await self._queue.get()
                else:
                    return await asyncio.wait_for(self._queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                raise queue.Empty("Queue is empty")

    def qsize(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        return self._queue.empty()

    def full(self) -> bool:
        return self._queue.full()


class SharedQueue:
    """
    A shared queue that provides standard queue.Queue interface across Ray workers.

    This class wraps a Ray actor to provide a centralized queue that can be accessed
    by multiple Ray tasks and actors across a cluster. It exposes the same interface
    as Python's standard queue.Queue while hiding Ray implementation details.

    The queue is centralized (single Ray actor) but can be shared between different
    Ray workers, making it useful for producer-consumer patterns in Ray applications.
    """

    def __init__(self, maxsize: int = 0):
        self._actor = _QueueActor.remote(maxsize=maxsize)

    def put(self, item: Any, block: bool = True, timeout: Optional[float] = None) -> None:
        ray.get(self._actor.put.remote(item, block=block, timeout=timeout))

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        return ray.get(self._actor.get.remote(block=block, timeout=timeout))

    def qsize(self) -> int:
        return ray.get(self._actor.qsize.remote())

    def empty(self) -> bool:
        return ray.get(self._actor.empty.remote())

    def full(self) -> bool:
        return ray.get(self._actor.full.remote())
