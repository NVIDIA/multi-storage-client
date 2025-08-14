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

import queue
import time
from datetime import datetime

import pytest
import ray

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.contrib.ray import SharedQueue
from multistorageclient.sync import _SyncOp
from multistorageclient.types import ExecutionMode, ObjectMetadata

from ..utils import tempdatastore


@pytest.fixture(scope="function")
def ray_cluster():
    ray.init(
        address="127.0.0.1:6379",
        runtime_env={"excludes": [".git", ".git/**"]},
    )
    yield
    ray.shutdown()


@ray.remote
def consumer(input_queue: SharedQueue, output_queue: SharedQueue):
    """
    Read the input queue and put the items into the output queue.
    """
    processed_count = 0
    while True:
        op, metadata = input_queue.get()
        time.sleep(0.01)
        if op == _SyncOp.STOP:
            break
        output_queue.put((op, metadata))
        processed_count += 1

    return processed_count


def test_ray_queue_basic(ray_cluster):
    """
    Test basic Ray queue operations.
    """
    q = SharedQueue(maxsize=2)
    q.put(None)
    result = q.get()
    assert result is None

    # Verify get timeout
    s = time.time()
    with pytest.raises(queue.Empty):
        result = q.get(block=True, timeout=1)
    e = time.time()
    assert e - s >= 1.0

    # Verify put timeout
    q.put(0)
    q.put(1)

    assert q.qsize() == 2
    assert not q.empty()
    assert q.full()

    s = time.time()
    with pytest.raises(queue.Full):
        result = q.put(3, block=True, timeout=1)
    e = time.time()
    assert e - s >= 1.0


def test_ray_queue_producer_consumer(ray_cluster):
    """
    Test producer-consumer pattern with Ray queue, verifying blocking behavior.
    """
    input_queue = SharedQueue(maxsize=500)
    output_queue = SharedQueue(maxsize=500)

    # Producer runs in the main thread
    [
        input_queue.put(
            (_SyncOp.ADD, ObjectMetadata(key=f"test_{i}", content_length=100, last_modified=datetime.now()))
        )
        for i in range(300)
    ]
    [input_queue.put((_SyncOp.STOP, None)) for _ in range(4)]
    assert input_queue.qsize() == 304

    # Initialize 2 consumers
    c1 = consumer.remote(input_queue, output_queue)
    c2 = consumer.remote(input_queue, output_queue)
    c3 = consumer.remote(input_queue, output_queue)
    c4 = consumer.remote(input_queue, output_queue)

    # Wait for all items to be processed
    results = ray.get([c1, c2, c3, c4])
    print(f"Consumer results: {results}")

    # Verify all items were processed
    assert input_queue.qsize() == 0
    assert output_queue.qsize() == 300
    assert sum(results) == 300


def test_storage_client_sync_no_files(ray_cluster):
    """
    Test storage client sync with Ray.
    """
    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        tempdatastore.TemporaryPOSIXDirectory() as temp_target_data_store,
    ):
        config_dict = {
            "profiles": {
                "source": temp_source_data_store.profile_config_dict(),
                "target": temp_target_data_store.profile_config_dict(),
            }
        }

        source_client = StorageClient(StorageClientConfig.from_dict(config_dict=config_dict, profile="source"))
        target_client = StorageClient(StorageClientConfig.from_dict(config_dict=config_dict, profile="target"))

        target_client.sync_from(source_client, "source/", "target/", execution_mode=ExecutionMode.RAY)


def test_storage_client_sync_with_files(ray_cluster):
    """
    Test storage client sync with Ray.
    """
    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        tempdatastore.TemporaryPOSIXDirectory() as temp_target_data_store,
    ):
        config_dict = {
            "profiles": {
                "source": temp_source_data_store.profile_config_dict(),
                "target": temp_target_data_store.profile_config_dict(),
            }
        }

        # Create data files
        source_client = StorageClient(StorageClientConfig.from_dict(config_dict=config_dict, profile="source"))
        source_files = []
        for i in range(100):
            for j in range(5):
                file_path = f"source/dir_{i}/subdir_{j}/file_{i}.txt"
                content = f"Content of file {i}"
                source_files.append(file_path.removeprefix("source/"))
                source_client.write(file_path, content.encode())

        # Sync from source to target using Ray
        target_client = StorageClient(StorageClientConfig.from_dict(config_dict=config_dict, profile="target"))
        target_client.sync_from(source_client, "source/", "target/", execution_mode=ExecutionMode.RAY)

        # Verify sync worked by checking target has the same files
        synced_files = list(target_client.list(prefix="target/"))
        assert set([f.key.removeprefix("target/") for f in synced_files]) == set(source_files), (
            f"Expected {source_files}, found {synced_files}"
        )
        assert len(synced_files) == 500, f"Expected 500 synced files, found {len(synced_files)}"


def test_storage_client_sync_replicas(ray_cluster):
    """
    Test storage client sync replicas with Ray.
    """
    with (
        tempdatastore.TemporaryPOSIXDirectory() as temp_source_data_store,
        tempdatastore.TemporaryPOSIXDirectory() as temp_replica1_data_store,
        tempdatastore.TemporaryPOSIXDirectory() as temp_replica2_data_store,
    ):
        config_dict = {
            "profiles": {
                "source": temp_source_data_store.profile_config_dict(),
                "replica1": temp_replica1_data_store.profile_config_dict(),
                "replica2": temp_replica2_data_store.profile_config_dict(),
            }
        }

        config_dict["profiles"]["source"]["replicas"] = [
            {"replica_profile": "replica1", "read_priority": 1},
            {"replica_profile": "replica2", "read_priority": 2},
        ]

        # Create data files
        source_client = StorageClient(StorageClientConfig.from_dict(config_dict=config_dict, profile="source"))
        source_files = []
        for i in range(100):
            for j in range(5):
                file_path = f"source/dir_{i}/subdir_{j}/file_{i}.txt"
                content = f"Content of file {i}"
                source_files.append(file_path.removeprefix("source/"))
                source_client.write(file_path, content.encode())

        # Sync from source to target using Ray
        source_client.sync_replicas(source_path="source/", execution_mode=ExecutionMode.RAY)

        # Verify sync worked by checking replica1 has the same files
        replica1 = StorageClient(StorageClientConfig.from_dict(config_dict=config_dict, profile="replica1"))
        synced_files = list(replica1.list(prefix=""))
        assert set([f.key.removeprefix("source/") for f in synced_files]) == set(source_files), (
            f"Expected {source_files}, found {synced_files}"
        )
        assert len(synced_files) == 500, f"Expected 500 synced files, found {len(synced_files)}"

        # Verify sync worked by checking replica2 has the same files
        replica2 = StorageClient(StorageClientConfig.from_dict(config_dict=config_dict, profile="replica2"))
        synced_files = list(replica2.list(prefix=""))
        assert set([f.key.removeprefix("source/") for f in synced_files]) == set(source_files), (
            f"Expected {source_files}, found {synced_files}"
        )
        assert len(synced_files) == 500, f"Expected 500 synced files, found {len(synced_files)}"
