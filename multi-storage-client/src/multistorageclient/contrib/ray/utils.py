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
import time
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


@ray.remote
class _EventActor:
    """
    Internal Ray actor that provides a distributed Event for synchronization.

    This actor wraps a simple boolean flag that can be set and checked across
    a distributed Ray cluster. It provides similar functionality to threading.Event
    or multiprocessing.Event but works across network boundaries.
    """

    def __init__(self):
        self._is_set = False

    def set(self) -> None:
        """Set the event flag to True."""
        self._is_set = True

    def is_set(self) -> bool:
        """Return True if the event flag is set."""
        return self._is_set

    def clear(self) -> None:
        """Reset the event flag to False."""
        self._is_set = False


class SharedEvent:
    """
    A distributed event that works across Ray workers in a cluster.

    This class wraps a Ray actor to provide an event synchronization primitive
    that can be accessed by multiple Ray tasks and actors across different nodes.
    It provides a similar interface to threading.Event and multiprocessing.Event.

    Unlike multiprocessing.Event which only works on a single machine, SharedEvent
    works across a distributed Ray cluster.

    Performance optimization: To avoid creating a bottleneck with thousands of workers
    checking the event state, this implementation uses local caching with periodic
    refresh. Once the event is set, it remains cached locally, so subsequent checks
    are essentially free. Before the event is set, checks are batched/throttled to
    reduce RPC overhead.
    """

    def __init__(self, cache_ttl: float = 1.0):
        """
        Initialize the shared event.

        :param cache_ttl: Time-to-live for cached 'not set' state in seconds.
            Once set to True, the state is cached permanently. This reduces
            RPC calls when the event is not yet set. Default 1s means at most
            1 check/second per worker, dramatically reducing load on the actor.
        """
        self._actor = _EventActor.remote()
        self._cache_ttl = cache_ttl
        self._local_state = False
        self._last_check = 0.0

    def set(self) -> None:
        """Set the event flag to True, signaling all waiting workers."""
        ray.get(self._actor.set.remote())
        self._local_state = True  # Cache locally

    def is_set(self) -> bool:
        """
        Return True if the event flag is set.

        This method uses intelligent caching to reduce RPC overhead:
        - If locally cached as True: return immediately (free)
        - If recently checked and was False: return cached False (free)
        - Otherwise: check with actor and cache result
        """
        # Once set, always set (no need to check again)
        if self._local_state:
            return True

        # Check if we need to refresh from actor
        now = time.time()
        if now - self._last_check < self._cache_ttl:
            # Cache is still valid, return cached value
            return False

        # Cache expired or first check, query the actor
        self._last_check = now
        result: bool = ray.get(self._actor.is_set.remote())
        self._local_state = result
        return self._local_state

    def clear(self) -> None:
        """Reset the event flag to False."""
        ray.get(self._actor.clear.remote())
        self._local_state = False
        self._last_check = 0.0
