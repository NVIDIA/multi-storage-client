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

from typing import Optional, cast
from unittest.mock import patch

import pytest

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.retry import retry
from multistorageclient.types import Range, RetryableError, StorageProvider


class FakeStorageProvider:
    def __init__(self, error_count):
        """
        Initializes the fake storage provider to simulate a specified number of connection time out errors.

        Args:
            error_count (int): The number of errors before a successful get_object.
        """
        self.attempts = 0
        self.error_count = error_count

    def get_object(self, path: str, byte_range: Optional[Range] = None):
        # Simulates reading an object from storage, raising a retryable connection time out error
        # for the first 'error_count' attempts before succeeding.
        self.attempts += 1
        if self.attempts < self.error_count:
            raise RetryableError("Simulated connection time out error.")
        return b"File content"

    @retry
    def get_object_outside_storage_client(self, path: str):
        # Simulates reading an object, similar to get_object(), but intended for testing
        # the behavior of the @retry() decorator when used outside of StorageClient.
        self.attempts += 1
        if self.attempts < self.error_count:
            raise RetryableError("Simulated connection time out error.")
        return b"File content"


def test_retry_decorator_in_storage_client():
    config = StorageClientConfig.from_json(
        """{
        "profiles": {
            "default": {
                "storage_provider": {
                    "type": "file",
                    "options": {
                        "base_path": "/"
                    }
                }
            }
        }
    }"""
    )

    storage_client = StorageClient(config)

    # Set the fake storage provider to fail 2 times before succeeding
    storage_provider = FakeStorageProvider(error_count=2)
    storage_client._storage_provider = cast(StorageProvider, storage_provider)

    result = storage_client.read("some_path")
    assert result == b"File content"
    # Ensure we have 2 attempts before succeeding
    assert storage_provider.attempts == 2

    # Another fake storage provider to fail 5 times before succeeding
    storage_provider = FakeStorageProvider(error_count=5)
    storage_client._storage_provider = cast(StorageProvider, storage_provider)

    # Expect error when exceeding the default maximum number (3) of retries
    with pytest.raises(RetryableError) as e:
        result = storage_client.read("some_path")

    assert "Simulated connection time out error." in str(e), f"Unexpected error message: {str(e)}"
    assert storage_provider.attempts == 3


def test_retry_decorator_outside_storage_client():
    # Tests the behavior of the retry decorator when applied to a method outside of StorageClient.
    fake_storage_provider = FakeStorageProvider(error_count=2)

    # Ensure that calling the method raises a RetryableError without retries
    with pytest.raises(RetryableError) as e:
        _ = fake_storage_provider.get_object_outside_storage_client("some_path")

    assert "Simulated connection time out error." in str(e), f"Unexpected error message: {str(e)}"
    assert fake_storage_provider.attempts == 1


def test_retry_with_custom_backoff_multiplier():
    config = StorageClientConfig.from_json(
        """{
        "profiles": {
            "default": {
                "storage_provider": {
                    "type": "file",
                    "options": {
                        "base_path": "/"
                    }
                },
                "retry": {
                    "attempts": 3,
                    "delay": 1.0,
                    "backoff_multiplier": 3.0
                }
            }
        }
    }"""
    )

    storage_client = StorageClient(config)
    storage_client._storage_provider = cast(StorageProvider, FakeStorageProvider(error_count=4))

    sleep_times = []

    def mock_sleep(seconds):
        sleep_times.append(seconds)

    with patch("time.sleep", side_effect=mock_sleep):
        with pytest.raises(RetryableError):
            storage_client.read("some_path")

    # We should have 2 sleep calls (attempts 0 and 1, but not on the final failure at attempt 2)
    assert len(sleep_times) == 2

    # First retry: delay * (3.0 ** 0) + jitter = 1.0 + jitter
    # Second retry: delay * (3.0 ** 1) + jitter = 3.0 + jitter
    # Jitter is between 0 and 1 second
    assert 1.0 <= sleep_times[0] <= 2.0, f"First sleep time {sleep_times[0]} should be between 1.0 and 2.0"
    assert 3.0 <= sleep_times[1] <= 4.0, f"Second sleep time {sleep_times[1]} should be between 3.0 and 4.0"


def test_file_not_found_error_logging(caplog):
    """Test that FileNotFoundError is logged at DEBUG level, not ERROR level."""
    import logging

    config = StorageClientConfig.from_json(
        """{
        "profiles": {
            "default": {
                "storage_provider": {
                    "type": "file",
                    "options": {
                        "base_path": "/"
                    }
                }
            }
        }
    }"""
    )

    storage_client = StorageClient(config)

    class FileNotFoundStorageProvider:
        def __init__(self):
            self._retry_config = storage_client._retry_config

        @retry
        def get_object(self, path: str, byte_range: Optional[Range] = None):
            raise FileNotFoundError(f"Object {path} not found")

    storage_client._storage_provider = cast(StorageProvider, FileNotFoundStorageProvider())

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(FileNotFoundError):
            storage_client.read("nonexistent_file.txt")

    error_logs = [record for record in caplog.records if record.levelname == "ERROR"]
    assert len(error_logs) == 0, "FileNotFoundError should not be logged at ERROR level"

    debug_logs = [record for record in caplog.records if record.levelname == "DEBUG" and "not found" in record.message]
    assert len(debug_logs) > 0, "FileNotFoundError should be logged at DEBUG level"
