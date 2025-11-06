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

import multiprocessing
import os
import random
import tempfile
import time
from unittest.mock import patch

import pytest

from multistorageclient.cache import CacheManager
from multistorageclient.caching.cache_config import CacheConfig


def worker_write_read(cache_dir, keys, data, barrier, result_queue):
    """
    Worker function that use CacheManager to write and read data at random order.
    """
    try:
        cache_config = CacheConfig(size="10M", cache_line_size="64M", check_source_version=False)
        cache_manager = CacheManager(profile="test", cache_config=cache_config)

        # Synchronize all worker processes at this point
        barrier.wait()

        # Write the files at random
        random.shuffle(keys)
        for key in keys:
            cache_manager.set(key, data)

        # Read the files at random
        random.shuffle(keys)
        for key in keys:
            assert data == cache_manager.read(key)

        # Open the files at random
        random.shuffle(keys)
        for key in keys:
            fp = cache_manager.open(key, "rb")
            assert fp is not None
            assert data == fp.read()
            fp.close()

        # Synchronize all worker processes at this point
        barrier.wait()

        cache_manager.refresh_cache()

        result_queue.put(True)
    except Exception as e:
        import traceback

        traceback.print_exc()
        result_queue.put(e)


def worker_write_refresh(cache_dir, keys, data, barrier, return_dict, result_queue):
    """
    Worker function that use CacheManager to write and read data at random order.
    """
    try:
        cache_config = CacheConfig(size="10M", cache_line_size="64M", check_source_version=False, location=cache_dir)
        cache_manager = CacheManager(profile="test", cache_config=cache_config)

        # Synchronize all worker processes at this point
        barrier.wait()

        # Write the files at random
        random.shuffle(keys)
        for key in keys:
            cache_manager.set(key, data)

        # Synchronize all worker processes at this point
        barrier.wait()

        # Refresh the cache and verify the size
        with patch("multistorageclient.cache.CacheManager.evict_files", new=lambda self: time.sleep(5)):
            cache_refreshed = cache_manager.refresh_cache()

        return_dict[os.getpid()] = cache_refreshed
        result_queue.put(True)
    except Exception as e:
        import traceback

        traceback.print_exc()
        result_queue.put(e)


@pytest.fixture
def cache_dir():
    """
    Pytest fixture to create a temporary cache directory.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.mark.serial
def test_multiprocessing_cache_manager(cache_dir):
    """
    Test the CacheManager with multiple processes reading and writing to the cache.
    """
    num_procs = 8
    max_cache_size = num_procs * 1024 * 1024
    keys = [f"file-{i:04d}.bin" for i in range(num_procs)]
    test_data = b"*" * 1 * 1024 * 1024

    # Queue for capturing the success or failure of each process
    result_queue = multiprocessing.Queue()

    # Create a barrier that will block until all the processes reach it
    barrier = multiprocessing.Barrier(num_procs, timeout=60)

    # Create multiple processes for testing
    processes = []
    for _ in range(num_procs):
        p = multiprocessing.Process(target=worker_write_read, args=(cache_dir, keys, test_data, barrier, result_queue))
        processes.append(p)
        p.start()

    # Wait for all writer processes to finish
    for p in processes:
        p.join()

    # Check the results from the queue for reader processes
    while not result_queue.empty():
        result = result_queue.get()
        if isinstance(result, Exception):
            pytest.fail(f"Worker process failed with error: {result}")

    # Check the final cache size
    cache_config = CacheConfig(size="10M", cache_line_size="64M", check_source_version=False, location=cache_dir)
    cache_manager = CacheManager(profile="test", cache_config=cache_config)
    assert cache_manager.cache_size() <= max_cache_size


@pytest.mark.serial
def test_multiprocessing_cache_manager_single_refresh(cache_dir):
    num_procs = 8
    keys = [f"file-{i:04d}.bin" for i in range(num_procs * 10)]
    test_data = b"*" * 10 * 1024 * 1024

    with multiprocessing.Manager() as manager:
        # Shared dictionary for collecting results from worker processes
        return_dict = manager.dict()

        # Queue for capturing the success or failure of each process
        result_queue = multiprocessing.Queue()

        # Create a barrier that will block until all the processes reach it
        barrier = multiprocessing.Barrier(num_procs, timeout=60)

        # Create multiple processes for testing
        processes = []
        for _ in range(num_procs):
            p = multiprocessing.Process(
                target=worker_write_refresh, args=(cache_dir, keys, test_data, barrier, return_dict, result_queue)
            )
            processes.append(p)
            p.start()

        # Wait for all writer processes to finish
        for p in processes:
            p.join()

        # Check the results from the queue for reader processes
        while not result_queue.empty():
            result = result_queue.get()
            if isinstance(result, Exception):
                pytest.fail(f"Worker process failed with error: {result}")

        # Verify only one process refreshed the cache
        assert len([d for d in return_dict.values() if d is True]) == 1
