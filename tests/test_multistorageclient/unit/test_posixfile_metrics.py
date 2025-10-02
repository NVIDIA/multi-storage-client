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

import os
from unittest.mock import MagicMock

import pytest

from multistorageclient import StorageClient
from multistorageclient.config import StorageClientConfigLoader
from multistorageclient.telemetry import Telemetry


@pytest.fixture
def mocked_metrics():
    mock_latency_gauge = MagicMock()
    mock_data_size_gauge = MagicMock()
    mock_data_rate_gauge = MagicMock()
    mock_request_counter = MagicMock()
    mock_response_counter = MagicMock()
    mock_data_size_counter = MagicMock()

    return {
        "gauges": {
            Telemetry.GaugeName.LATENCY: mock_latency_gauge,
            Telemetry.GaugeName.DATA_SIZE: mock_data_size_gauge,
            Telemetry.GaugeName.DATA_RATE: mock_data_rate_gauge,
        },
        "counters": {
            Telemetry.CounterName.REQUEST_SUM: mock_request_counter,
            Telemetry.CounterName.RESPONSE_SUM: mock_response_counter,
            Telemetry.CounterName.DATA_SIZE_SUM: mock_data_size_counter,
        },
        "latency_gauge": mock_latency_gauge,
        "data_size_gauge": mock_data_size_gauge,
        "data_rate_gauge": mock_data_rate_gauge,
        "request_counter": mock_request_counter,
        "response_counter": mock_response_counter,
        "data_size_counter": mock_data_size_counter,
    }


@pytest.fixture
def storage_client_with_metrics(mocked_metrics, tmp_path):
    config_dict = {
        "profiles": {
            "test": {
                "storage_provider": {
                    "type": "file",
                    "options": {
                        "base_path": str(tmp_path),
                    },
                }
            }
        }
    }

    loader = StorageClientConfigLoader(
        config_dict=config_dict,
        profile="test",
        metric_gauges=mocked_metrics["gauges"],
        metric_counters=mocked_metrics["counters"],
    )
    config = loader.build_config()
    return StorageClient(config=config)


def test_read_operations_emit_metrics(storage_client_with_metrics, mocked_metrics, tmp_path):
    test_file = tmp_path / "test_read.txt"
    test_file.write_bytes(b"Line 1\nLine 2\nLine 3\n")
    filename = "test_read.txt"

    with storage_client_with_metrics.open(filename, "rb") as f:
        data = f.read(10)

    assert len(data) == 10
    assert mocked_metrics["latency_gauge"].set.called
    assert mocked_metrics["data_size_gauge"].set.called
    assert mocked_metrics["request_counter"].add.called
    assert mocked_metrics["response_counter"].add.called

    for mock in mocked_metrics["gauges"].values():
        mock.reset_mock()
    for mock in mocked_metrics["counters"].values():
        mock.reset_mock()

    with storage_client_with_metrics.open(filename, "rb") as f:
        line = f.readline()

    assert line == b"Line 1\n"
    assert mocked_metrics["latency_gauge"].set.called
    assert mocked_metrics["data_size_gauge"].set.called
    assert mocked_metrics["request_counter"].add.called

    for mock in mocked_metrics["gauges"].values():
        mock.reset_mock()
    for mock in mocked_metrics["counters"].values():
        mock.reset_mock()

    with storage_client_with_metrics.open(filename, "rb") as f:
        lines = f.readlines()

    assert len(lines) == 3
    assert mocked_metrics["data_size_gauge"].set.called
    assert mocked_metrics["request_counter"].add.called

    for mock in mocked_metrics["gauges"].values():
        mock.reset_mock()
    for mock in mocked_metrics["counters"].values():
        mock.reset_mock()

    with storage_client_with_metrics.open(filename, "rb") as f:
        buffer = bytearray(10)
        n = f.readinto(buffer)

    assert n == 10
    assert mocked_metrics["data_size_gauge"].set.called
    assert mocked_metrics["request_counter"].add.called

    for mock in mocked_metrics["gauges"].values():
        mock.reset_mock()
    for mock in mocked_metrics["counters"].values():
        mock.reset_mock()

    with storage_client_with_metrics.open(filename, "rb", buffering=0) as f:
        data = f.readall()

    assert len(data) > 0
    assert mocked_metrics["data_size_gauge"].set.called
    assert mocked_metrics["request_counter"].add.called


def test_write_operations_emit_metrics(storage_client_with_metrics, mocked_metrics):
    temp_dir = storage_client_with_metrics._storage_provider._base_path
    test_file = os.path.join(temp_dir, "test_write.txt")

    try:
        with storage_client_with_metrics.open("test_write.txt", "wb") as f:
            n = f.write(b"Hello World")

        assert n == 11
        assert mocked_metrics["latency_gauge"].set.called
        assert mocked_metrics["data_size_gauge"].set.called
        assert mocked_metrics["request_counter"].add.called

        for mock in mocked_metrics["gauges"].values():
            mock.reset_mock()
        for mock in mocked_metrics["counters"].values():
            mock.reset_mock()

        with storage_client_with_metrics.open("test_write.txt", "wb") as f:
            f.writelines([b"Line 1\n", b"Line 2\n"])

        assert mocked_metrics["latency_gauge"].set.called
        assert mocked_metrics["request_counter"].add.called

        for mock in mocked_metrics["gauges"].values():
            mock.reset_mock()
        for mock in mocked_metrics["counters"].values():
            mock.reset_mock()

        with storage_client_with_metrics.open("test_write.txt", "rb+") as f:
            f.truncate(5)

        assert mocked_metrics["latency_gauge"].set.called
        assert mocked_metrics["request_counter"].add.called

    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_io_error_propagates(storage_client_with_metrics):
    with pytest.raises(FileNotFoundError):
        with storage_client_with_metrics.open("non_existent_file.txt", "rb") as f:
            f.read()


def test_text_mode_operations_emit_metrics(storage_client_with_metrics, mocked_metrics, tmp_path):
    test_file = tmp_path / "test_text.txt"
    test_file.write_text("Hello World\nLine 2\nLine 3\n")
    filename = "test_text.txt"

    with storage_client_with_metrics.open(filename, "r") as f:
        data = f.read(5)

    assert data == "Hello"
    assert mocked_metrics["latency_gauge"].set.called
    assert mocked_metrics["data_size_gauge"].set.called
    assert mocked_metrics["request_counter"].add.called

    for mock in mocked_metrics["gauges"].values():
        mock.reset_mock()
    for mock in mocked_metrics["counters"].values():
        mock.reset_mock()

    with storage_client_with_metrics.open(filename, "r") as f:
        line = f.readline()

    assert line == "Hello World\n"
    assert mocked_metrics["data_size_gauge"].set.called
    assert mocked_metrics["request_counter"].add.called
