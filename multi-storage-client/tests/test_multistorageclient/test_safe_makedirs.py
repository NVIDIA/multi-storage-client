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

import threading
from unittest.mock import patch

import pytest

from multistorageclient.utils import safe_makedirs


def test_basic_directory_creation(tmp_path):
    """Test basic directory creation."""
    test_dir = tmp_path / "test" / "nested" / "dirs"
    safe_makedirs(str(test_dir))
    assert test_dir.exists()
    assert test_dir.is_dir()


def test_already_exists(tmp_path):
    """Test that safe_makedirs doesn't fail if directory already exists."""
    test_dir = tmp_path / "existing"
    test_dir.mkdir()
    safe_makedirs(str(test_dir))
    assert test_dir.exists()


def test_concurrent_creation(tmp_path):
    """Test concurrent directory creation from multiple threads."""
    test_dir = tmp_path / "concurrent" / "test" / "directory"
    errors = []
    num_threads = 10

    def create_dir():
        try:
            safe_makedirs(str(test_dir))
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=create_dir) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Expected no errors, got: {errors}"
    assert test_dir.exists()


def test_deep_nested_paths(tmp_path):
    """Test creating deeply nested directory structures."""
    deep_path = tmp_path / "a" / "b" / "c" / "d" / "e" / "f" / "g"
    safe_makedirs(str(deep_path))
    assert deep_path.exists()


def test_race_condition_simulation(tmp_path):
    """Simulate race condition by having multiple threads create nested directories."""
    base_dir = tmp_path / "race_test"
    errors = []
    created_dirs = []
    num_threads = 20
    barrier = threading.Barrier(num_threads)

    def create_nested_dir(thread_id):
        try:
            barrier.wait()
            thread_dir = base_dir / "shared" / "parent" / f"thread_{thread_id}"
            safe_makedirs(str(thread_dir))
            created_dirs.append(thread_dir)
        except Exception as e:
            errors.append((thread_id, e))

    threads = [threading.Thread(target=create_nested_dir, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Expected no errors, got: {errors}"
    assert len(created_dirs) == num_threads
    for dir_path in created_dirs:
        assert dir_path.exists()


def test_exist_ok_false(tmp_path):
    """Test that exist_ok=False raises error when directory exists."""
    test_dir = tmp_path / "test_exist_ok"
    test_dir.mkdir()

    with pytest.raises(FileExistsError):
        safe_makedirs(str(test_dir), exist_ok=False)


def test_high_concurrency_stress(tmp_path):
    """Stress test with many concurrent threads creating overlapping directory structures."""
    base_dir = tmp_path / "stress_test"
    num_threads = 50
    errors = []

    def worker(worker_id):
        try:
            for i in range(5):
                path = base_dir / f"level1_{i % 3}" / f"level2_{i % 2}" / f"worker_{worker_id}"
                safe_makedirs(str(path))
        except Exception as e:
            errors.append((worker_id, e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Expected no errors in stress test, got: {errors}"
    assert base_dir.exists()


def test_retry_on_file_not_found_error():
    """Test that safe_makedirs retries on FileNotFoundError and eventually succeeds."""
    call_count = 0

    def mock_makedirs(path, mode=0o777, exist_ok=False):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise FileNotFoundError(f"Simulated failure on attempt {call_count}")
        # Success on 3rd attempt

    with patch("os.makedirs", side_effect=mock_makedirs):
        safe_makedirs("/test/path")

    assert call_count == 3, f"Expected 3 attempts, got {call_count}"


def test_retry_exhaustion_raises_error():
    """Test that safe_makedirs raises FileNotFoundError after exhausting all retries."""
    call_count = 0

    def mock_makedirs(path, mode=0o777, exist_ok=False):
        nonlocal call_count
        call_count += 1
        raise FileNotFoundError(f"Persistent failure on attempt {call_count}")

    with (
        patch("os.makedirs", side_effect=mock_makedirs),
        pytest.raises(FileNotFoundError, match="Persistent failure on attempt 5"),
    ):
        safe_makedirs("/test/path")

    assert call_count == 5, f"Expected 5 attempts (max retries), got {call_count}"


def test_no_retry_on_other_errors():
    """Test that safe_makedirs does not retry on non-FileNotFoundError exceptions."""
    call_count = 0

    def mock_makedirs(path, mode=0o777, exist_ok=False):
        nonlocal call_count
        call_count += 1
        raise PermissionError("Access denied")

    with patch("os.makedirs", side_effect=mock_makedirs), pytest.raises(PermissionError, match="Access denied"):
        safe_makedirs("/test/path")

    assert call_count == 1, f"Expected 1 attempt (no retry), got {call_count}"


def test_file_exists_error_with_exist_ok_true():
    """Test that FileExistsError returns successfully when exist_ok=True."""
    call_count = 0

    def mock_makedirs(path, mode=0o777, exist_ok=False):
        nonlocal call_count
        call_count += 1
        raise FileExistsError("Directory already exists")

    with patch("os.makedirs", side_effect=mock_makedirs):
        safe_makedirs("/test/path", exist_ok=True)

    assert call_count == 1, f"Expected 1 attempt, got {call_count}"


def test_file_exists_error_with_exist_ok_false():
    """Test that FileExistsError is raised when exist_ok=False."""
    call_count = 0

    def mock_makedirs(path, mode=0o777, exist_ok=False):
        nonlocal call_count
        call_count += 1
        raise FileExistsError("Directory already exists")

    with (
        patch("os.makedirs", side_effect=mock_makedirs),
        pytest.raises(FileExistsError, match="Directory already exists"),
    ):
        safe_makedirs("/test/path", exist_ok=False)

    assert call_count == 1, f"Expected 1 attempt, got {call_count}"
