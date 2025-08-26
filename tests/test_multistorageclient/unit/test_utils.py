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
from datetime import datetime
from unittest.mock import patch

import pytest

import multistorageclient as msc
from multistorageclient.types import ExecutionMode, ObjectMetadata
from multistorageclient.utils import (
    AttributeFilterEvaluator,
    calculate_worker_processes_and_threads,
    create_attribute_filter_evaluator,
    expand_env_vars,
    extract_prefix_from_glob,
    glob,
    insert_directories,
    join_paths,
    matches_attribute_filter_expression,
    merge_dictionaries_no_overwrite,
)


def test_basic_glob():
    keys = ["file1.txt", "file2.txt", "image1.jpg", "doc1.pdf"]
    pattern = "*.txt"
    expected = ["file1.txt", "file2.txt"]
    assert glob(keys, pattern) == expected


def test_wildcard_glob():
    keys = ["file1.txt", "file2.txt", "file3.log", "file4.txt"]
    pattern = "file?.txt"
    expected = ["file1.txt", "file2.txt", "file4.txt"]
    assert glob(keys, pattern) == expected


def test_recursive_glob():
    keys = ["logs/app1/file1.log", "logs/app1/subdir/file2.log", "logs/app2/file3.log", "logs/app2/subdir/file4.txt"]
    pattern = "**/*.log"
    expected = ["logs/app1/file1.log", "logs/app1/subdir/file2.log", "logs/app2/file3.log"]
    assert glob(keys, pattern) == expected


def test_invalid_glob():
    keys = ["file1.txt", "file2.txt", "file3.log", "file4.txt"]
    pattern = "**/***/file/**.txt"
    expected = []
    assert glob(keys, pattern) == expected


def test_join_paths():
    assert "msc://profile/bucket/prefix" == join_paths("msc://profile", "bucket/prefix")
    assert "msc://profile/bucket/prefix" == join_paths("msc://profile", "/bucket/prefix")
    assert "msc://profile/bucket/prefix" == join_paths("msc://profile/", "/bucket/prefix")


def test_expand_env_vars():
    os.environ["VAR"] = "value"
    options = {
        "key1": "${VAR}",
        "key2": 42,
        "key3": ["list_item", "$VAR"],
        "key4": {"nested_key": "${VAR}"},
        "key5": "PREFIX_${VAR}",
    }
    expected = {
        "key1": "value",
        "key2": 42,
        "key3": ["list_item", "value"],
        "key4": {"nested_key": "value"},
        "key5": "PREFIX_value",
    }
    assert expand_env_vars(options) == expected


def test_expand_env_vars_unresolved_var():
    os.environ.clear()
    with pytest.raises(ValueError):
        options = {"key1": "${VAR}"}

        options = expand_env_vars(options)


def test_extract_prefix_from_glob():
    assert extract_prefix_from_glob("bucket/prefix1/**/*.txt") == "bucket/prefix1"
    assert extract_prefix_from_glob("bucket/prefix1/subprefix2/*my_file") == "bucket/prefix1/subprefix2"
    assert extract_prefix_from_glob("bucket/*.log") == "bucket"
    assert extract_prefix_from_glob("bucket/folder/**/*") == "bucket/folder"
    assert extract_prefix_from_glob("bucket/deep/**/*.csv") == "bucket/deep"
    assert extract_prefix_from_glob("bucket/prefix1") == "bucket/prefix1"
    assert extract_prefix_from_glob("bucket") == "bucket"
    assert extract_prefix_from_glob("**/*.json") == ""
    assert extract_prefix_from_glob("*.pdf") == ""
    # Absolute paths
    assert extract_prefix_from_glob("/") == "/"
    assert extract_prefix_from_glob("/bucket/prefix1/**/*.txt") == "/bucket/prefix1"
    assert extract_prefix_from_glob("") == ""
    # Riva use case
    assert extract_prefix_from_glob("bucket/deep/folder/struct/**/*dataset_info.json") == "bucket/deep/folder/struct"
    # Earth-2
    assert extract_prefix_from_glob("bucket/prefix1/subprefix2/my_file.0.*.mdlus") == "bucket/prefix1/subprefix2"
    assert extract_prefix_from_glob("bucket/prefix1/**/my_file.0.*.mdlus") == "bucket/prefix1"
    assert extract_prefix_from_glob("bucket/**/my_file.0.*.mdlus") == "bucket"


def test_merge_dictionaries_no_overwrite_no_conflicts():
    dict_a = {
        "profiles": {
            "s3-local": {
                "storage_provider": {
                    "type": "s3",
                    "options": {
                        "region_name": "us-east-1",
                    },
                },
                "credentials_provider": {
                    "type": "S3Credentials",
                    "options": {
                        "access_key": "foo",
                        "secret_key": "bar",
                    },
                },
            }
        }
    }

    dict_b = {
        "profiles": {
            # Same profile name "s3-local" only sets "endpoint_url" which was missing in dict_a.
            "s3-local": {
                "storage_provider": {
                    "options": {
                        "endpoint_url": "http://localhost:9000",
                    },
                },
            },
            # New profile name "s3-remote" won't conflict with dict_a
            "s3-remote": {
                "storage_provider": {
                    "type": "s3",
                    "options": {
                        "endpoint_url": "https://s3.amazonaws.com",
                        "region_name": "us-west-2",
                    },
                },
                "credentials_provider": {
                    "type": "S3Credentials",
                    "options": {
                        "access_key": "remote-foo",
                        "secret_key": "remote-bar",
                    },
                },
            },
        },
        "cache": {"location": "/tmp/"},
    }

    merged, conflicts = merge_dictionaries_no_overwrite(dict_a, dict_b)
    assert conflicts == [], f"Expected no conflicts, but found: {conflicts}"

    # Check that both profiles exist
    assert "s3-local" in merged["profiles"]
    assert "s3-remote" in merged["profiles"]
    # Check that data was merged properly
    assert merged["profiles"]["s3-remote"]["storage_provider"]["options"]["endpoint_url"] == "https://s3.amazonaws.com"
    assert merged["profiles"]["s3-local"]["storage_provider"]["options"]["endpoint_url"] == "http://localhost:9000"
    assert merged["cache"]["location"] == "/tmp/"


def test_merge_dictionaries_no_overwrite_with_conflict():
    dict_a = {
        "profiles": {
            "s3-local": {
                "storage_provider": {
                    "type": "s3",
                    "options": {
                        "endpoint_url": "http://localhost:9000",
                        "region_name": "us-east-1",
                    },
                },
            }
        }
    }

    dict_b = {
        "profiles": {
            "s3-local": {
                # same profile "s3-local" => potential conflict
                "storage_provider": {
                    "type": "s3",  # type is already defined in dict_a so conflict!
                }
            }
        }
    }

    _, conflicts = merge_dictionaries_no_overwrite(dict_a, dict_b)
    assert "type" in conflicts, "Expected a conflict on 'type' but it wasn't recorded."


def test_insert_directories():
    """Test directory insertion with nested folder structure."""
    keys = ["folder1/file1.txt", "folder1/subfolder/file2.txt", "folder2/file3.txt"]
    expected = [
        "folder1",
        "folder1/file1.txt",
        "folder1/subfolder",
        "folder1/subfolder/file2.txt",
        "folder2",
        "folder2/file3.txt",
    ]
    result = insert_directories(keys)
    assert result == expected


def test_version():
    assert msc.__version__ != "0.1.0"


@patch("multiprocessing.cpu_count")
def test_calculate_worker_processes_and_threads_low_cpu(mock_cpu_count, monkeypatch):
    # Test with 4 CPUs (should use all CPUs for processes)
    mock_cpu_count.return_value = 4
    monkeypatch.delenv("MSC_NUM_PROCESSES", raising=False)
    monkeypatch.delenv("MSC_NUM_THREADS_PER_PROCESS", raising=False)

    processes, threads = calculate_worker_processes_and_threads()
    assert processes == 4  # Default processes should equal CPU count
    assert threads == 16  # Default minimum threads is 16


@patch("multiprocessing.cpu_count")
def test_calculate_worker_processes_and_threads_high_cpu(mock_cpu_count, monkeypatch):
    # Test with 16 CPUs (should cap at 8 processes)
    mock_cpu_count.return_value = 16
    monkeypatch.delenv("MSC_NUM_PROCESSES", raising=False)
    monkeypatch.delenv("MSC_NUM_THREADS_PER_PROCESS", raising=False)

    processes, threads = calculate_worker_processes_and_threads()
    assert processes == 8  # Default processes should cap at 8
    assert threads == 16  # Default threads should be 16 for 8 processes on 16 CPUs


@patch("multiprocessing.cpu_count")
def test_calculate_worker_processes_and_threads_custom_processes(mock_cpu_count, monkeypatch):
    mock_cpu_count.return_value = 8
    monkeypatch.setenv("MSC_NUM_PROCESSES", "4")
    monkeypatch.delenv("MSC_NUM_THREADS_PER_PROCESS", raising=False)

    processes, threads = calculate_worker_processes_and_threads()
    assert processes == 4  # Should use environment variable
    assert threads == 16  # Should calculate based on CPU and processes


@patch("multiprocessing.cpu_count")
def test_calculate_worker_processes_and_threads_custom_threads(mock_cpu_count, monkeypatch):
    mock_cpu_count.return_value = 8
    monkeypatch.delenv("MSC_NUM_PROCESSES", raising=False)
    monkeypatch.setenv("MSC_NUM_THREADS_PER_PROCESS", "10")

    processes, threads = calculate_worker_processes_and_threads()
    assert processes == 8  # Should use default
    assert threads == 10  # Should use environment variable


@patch("multiprocessing.cpu_count")
def test_calculate_worker_processes_and_threads_both_custom(mock_cpu_count, monkeypatch):
    mock_cpu_count.return_value = 16
    monkeypatch.setenv("MSC_NUM_PROCESSES", "2")
    monkeypatch.setenv("MSC_NUM_THREADS_PER_PROCESS", "8")

    processes, threads = calculate_worker_processes_and_threads()
    assert processes == 2  # Should use environment variable
    assert threads == 8  # Should use environment variable


@patch("multistorageclient.StorageClient")
@patch("multistorageclient.StorageClient")
def test_calculate_worker_processes_and_threads_use_single_process(source_client, target_client):
    # Both clients are using the Rust client.
    source_client._is_rust_client_enabled.return_value = True
    target_client._is_rust_client_enabled.return_value = True

    processes, threads = calculate_worker_processes_and_threads(
        execution_mode=ExecutionMode.LOCAL, source_client=source_client, target_client=target_client
    )
    assert processes == 1
    assert threads == multiprocessing.cpu_count()

    # One client is using the Rust client and the other is using the POSIX file storage provider.
    source_client._is_rust_client_enabled.return_value = True
    source_client._is_posix_file_storage_provider.return_value = False
    target_client._is_rust_client_enabled.return_value = False
    target_client._is_posix_file_storage_provider.return_value = True
    processes, threads = calculate_worker_processes_and_threads(
        execution_mode=ExecutionMode.LOCAL, source_client=source_client, target_client=target_client
    )
    assert processes == 1
    assert threads == multiprocessing.cpu_count()

    source_client._is_rust_client_enabled.return_value = False
    source_client._is_posix_file_storage_provider.return_value = True
    target_client._is_rust_client_enabled.return_value = False
    target_client._is_posix_file_storage_provider.return_value = True
    processes, threads = calculate_worker_processes_and_threads(
        execution_mode=ExecutionMode.LOCAL, source_client=source_client, target_client=target_client
    )
    assert processes == 1
    assert threads == multiprocessing.cpu_count()


def test_attribute_filter_evaluator_comparison():
    """Test basic comparison operations in AttributeFilterEvaluator."""
    evaluator = AttributeFilterEvaluator()

    # Test string comparison
    assert evaluator._compare_values("test", "=", "test")
    assert not evaluator._compare_values("test", "=", "other")
    assert evaluator._compare_values("test", "!=", "other")
    assert not evaluator._compare_values("test", "!=", "test")

    # Test numeric comparison
    assert evaluator._compare_values("1.5", ">", "1.0")
    assert evaluator._compare_values("1.0", ">=", "1.0")
    assert evaluator._compare_values("1.0", "<", "1.5")
    assert evaluator._compare_values("1.0", "<=", "1.0")

    # Test string fallback for numeric comparison
    assert evaluator._compare_values("b", ">", "a")
    assert evaluator._compare_values("a", "<", "b")

    # Test invalid operator
    with pytest.raises(ValueError):
        evaluator._compare_values("test", "invalid", "test")


def test_create_attribute_filter_evaluator():
    """Test creation of attribute filter evaluators."""
    # Test empty expression
    evaluator = create_attribute_filter_evaluator("")
    assert evaluator({"any": "value"})  # Empty expression always returns True

    # Test simple comparison
    evaluator = create_attribute_filter_evaluator('model_name = "gpt"')
    assert evaluator({"model_name": "gpt"})
    assert not evaluator({"model_name": "bert"})
    assert not evaluator({})  # Missing key returns False

    # Test complex expression
    evaluator = create_attribute_filter_evaluator('(model_name = "gpt" OR model_name = "bert") AND version > 1.0')
    assert evaluator({"model_name": "gpt", "version": "1.5"})
    assert evaluator({"model_name": "bert", "version": "2.0"})
    assert not evaluator({"model_name": "gpt", "version": "0.5"})
    assert not evaluator({"model_name": "other", "version": "2.0"})

    # Test invalid expression
    with pytest.raises(ValueError):
        create_attribute_filter_evaluator("invalid expression")


def test_matches_attribute_filter_expression():
    """Test matching objects against attribute filter expressions."""
    # Create test metadata
    metadata = ObjectMetadata(
        key="test.txt",
        content_length=100,
        last_modified=datetime.now(),
        metadata={"model_name": "gpt", "version": "1.5", "type": "text"},
    )

    # Test empty expression
    evaluator = create_attribute_filter_evaluator("")
    assert matches_attribute_filter_expression(metadata, evaluator)

    # Test simple comparison
    evaluator = create_attribute_filter_evaluator('model_name = "gpt"')
    assert matches_attribute_filter_expression(metadata, evaluator)

    # Test complex expression
    evaluator = create_attribute_filter_evaluator('(model_name = "gpt" OR model_name = "bert") AND version > 1.0')
    assert matches_attribute_filter_expression(metadata, evaluator)

    # Test non-matching expression
    evaluator = create_attribute_filter_evaluator('model_name = "bert"')
    assert not matches_attribute_filter_expression(metadata, evaluator)

    # Test empty metadata
    empty_metadata = ObjectMetadata(key="test.txt", content_length=100, last_modified=datetime.now(), metadata={})
    evaluator = create_attribute_filter_evaluator('model_name = "gpt"')
    assert not matches_attribute_filter_expression(empty_metadata, evaluator)
