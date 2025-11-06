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

import argparse
import json
import os
import shutil
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Manager, Pool
from multiprocessing.managers import ListProxy
from typing import Any, Optional, Union

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.schema import BENCHMARK_SCHEMA, validate

# Default configuration
DEFAULT_CONFIG = {
    "processes": [8],
    "threads": [4],
    "test_object_sizes": {
        "4MB": 12800,
        "64MB": 800,
    },
}

DEFAULT_TEST_DIR = "/tmp/msc_benchmark"


def load_config(config_path: Optional[str]) -> dict[str, Any]:
    """
    Load configuration from a JSON file.

    :param config_path: Path to the configuration file
    :return: Configuration dictionary
    """
    if config_path is None:
        print(f"Config file {config_path} not found. Using default configuration.")
        return DEFAULT_CONFIG
    elif os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                config = json.load(f)

            # Validate config against benchmark schema
            validate(instance=config, schema=BENCHMARK_SCHEMA)

            return config
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Error parsing config file {config_path}", e.doc, e.pos)
        except Exception as e:
            raise Exception(f"Encountered an exception loading config file: {e}")
    else:
        raise FileNotFoundError(f"No file found, config_path incorrect {config_path}")


def size_to_bytes(size: str) -> int:
    return int(size[:-2]) * 1024 ** {"KB": 1, "MB": 2, "GB": 3, "TB": 4, "PB": 5}[size[-2:]]


class PerformanceMetrics:
    def __init__(
        self,
        start_times: Union[list[Any], ListProxy],
        end_times: Union[list[Any], ListProxy],
        response_times: Union[list[Any], ListProxy],
        object_sizes: Union[list[Any], ListProxy],
    ) -> None:
        self.start_times = start_times
        self.end_times = end_times
        self.response_times = response_times
        self.object_sizes = object_sizes

    def record(self, start_time: float, end_time: float, size: int) -> None:
        self.start_times.append(start_time)
        self.end_times.append(end_time)
        self.response_times.append(end_time - start_time)
        self.object_sizes.append(size)

    def calculate(self) -> None:
        total_size = sum(self.object_sizes)
        total_time = max(self.end_times) - min(self.start_times)
        avg_response_time = sum(self.response_times) / len(self.response_times)

        response_time_percentiles = {
            "50%": statistics.median(self.response_times),
            "90%": statistics.quantiles(self.response_times, n=10)[-1],
            "99%": statistics.quantiles(self.response_times, n=100)[-1],
        }

        # Results summary
        print(f"Total data transferred: {pretty_print_bytes(total_size)}")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Throughput: {pretty_print_bytes(total_size / total_time)}/s")
        print(f"Average response time: {avg_response_time * 1000:.2f} ms")
        print(
            f"Response time percentiles: 50% Median: {response_time_percentiles['50%'] * 1000:.2f} ms, "
            f"90%: {response_time_percentiles['90%'] * 1000:.2f} ms, "
            f"99%: {response_time_percentiles['99%'] * 1000:.2f} ms\n"
        )


def pretty_print_bytes(byte_value: float) -> str:
    suffixes = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while byte_value >= 1024 and i < len(suffixes) - 1:
        byte_value /= 1024.0
        i += 1
    return f"{byte_value:.2f}".rstrip("0").rstrip(".") + " " + suffixes[i]


class BenchmarkRunner:
    def __init__(
        self,
        storage_client: StorageClient,
        test_sizes: Optional[dict[str, int]] = None,
        processes: Optional[list[int]] = None,
        threads: Optional[list[int]] = None,
        prefix: str = "",
        include_file_tests: bool = False,
        file_tests_dir: str = DEFAULT_TEST_DIR,
    ) -> None:
        """Initialize the benchmark runner with a storage client and test parameters.

        Args:
            storage_client: The storage client to use for benchmarking
            test_sizes: Dictionary mapping size strings to number of objects, e.g. {"4MB": 12800}
            processes: List of process counts to test with
            threads: List of thread counts to test with
            prefix: Path prefix to use for storing test objects
            include_file_tests: Whether to run upload_file and download_file tests
            file_tests_dir: Path to the directory to be used for upload_file/download_file test
        """
        self.storage_client = storage_client
        self.test_sizes = test_sizes or DEFAULT_CONFIG["test_object_sizes"]
        self.processes = processes or DEFAULT_CONFIG["processes"]
        self.threads = threads or DEFAULT_CONFIG["threads"]
        self.prefix = prefix
        self.include_file_tests = include_file_tests
        self.file_tests_dir = file_tests_dir
        # Pre-generate random data
        self.random_data = {k: os.urandom(size_to_bytes(k)) for k in self.test_sizes.keys()}

    def upload_object(self, size: str, path: str, metrics: PerformanceMetrics) -> None:
        """Upload a single object of the specified size."""
        data = self.random_data[size]  # Get pre-generated random data for this size
        start_time = time.time()
        try:
            self.storage_client.write(path=path, body=data)
        except Exception as e:
            print(f"Error uploading {path}: {e}")
        end_time = time.time()
        metrics.record(start_time, end_time, size_to_bytes(size))

    def download_object(self, path: str, metrics: PerformanceMetrics) -> None:
        """Download a single object and record metrics."""
        start_time = time.time()
        size = 0
        try:
            size = len(self.storage_client.read(path=path))
        except Exception as e:
            print(f"Error downloading {path}: {e}")
        end_time = time.time()
        metrics.record(start_time, end_time, size)

    def delete_object(self, path: str) -> None:
        """Delete a single object."""
        try:
            self.storage_client.delete(path=path)
        except Exception as e:
            print(f"Error deleting {path}: {e}")

    def create_files(self, size: str, num_files: int) -> None:
        """Create files of the specified size with random data."""
        os.makedirs(self.file_tests_dir, exist_ok=True)
        # Guardrail: abort early if required space exceeds 90% of free space
        required = size_to_bytes(size) * num_files
        free = shutil.disk_usage(self.file_tests_dir).free
        if required > free * 0.9:
            raise RuntimeError(
                f"Refusing to generate {required / 1_048_576:.1f} MiB testing files - "
                f"only {free / 1_048_576:.1f} MiB free in {self.file_tests_dir}"
            )

        print(f"Generating {num_files} test files of size {size} in {self.file_tests_dir}")
        for i in range(num_files):
            file_path = os.path.join(self.file_tests_dir, f"test-{size}-{i}")
            with open(file_path, "wb") as f:
                f.write(self.random_data[size])

    def upload_file(self, size: str, remote_path: str, local_path: str, metrics: PerformanceMetrics) -> None:
        """Upload a file from local filesystem to storage and record metrics."""
        start_time = time.time()
        try:
            self.storage_client.upload_file(remote_path=remote_path, local_path=local_path)
        except Exception as e:
            print(f"Error uploading file {local_path} to {remote_path}: {e}")
        end_time = time.time()
        metrics.record(start_time, end_time, size_to_bytes(size))

    def download_file(self, size: str, remote_path: str, local_path: str, metrics: PerformanceMetrics) -> None:
        """Download a file from storage to local filesystem and record metrics."""
        start_time = time.time()
        try:
            self.storage_client.download_file(remote_path=remote_path, local_path=local_path)
        except Exception as e:
            print(f"Error downloading file {remote_path} to {local_path}: {e}")
        end_time = time.time()
        metrics.record(start_time, end_time, size_to_bytes(size))

    def cleanup_test_dir(self) -> None:
        """Delete the test directory with all files inside."""
        abs_dir = os.path.abspath(self.file_tests_dir)
        if abs_dir in {"/", "", "/home", "/var", "/etc", os.path.expanduser("~")}:
            raise ValueError(f"Refusing to delete unsafe path: {abs_dir}")
        try:
            shutil.rmtree(abs_dir)
        except Exception as e:
            print(f"Error deleting test directory {self.file_tests_dir}: {e}")

    def task(self, test_type: str, bucket: str, size: str, i: int, metrics: PerformanceMetrics) -> None:
        """Execute a single task (upload, download, delete, upload_file, download_file)."""
        object_name_prefix = f"test-{size}"
        object_name = f"{object_name_prefix}-{i}"
        object_path = os.path.join(bucket, object_name)
        local_path = os.path.join(self.file_tests_dir, f"test-{size}-{i}")

        if test_type == "upload":
            self.upload_object(size, object_path, metrics)
        elif test_type == "download":
            self.download_object(object_path, metrics)
        elif test_type == "delete":
            self.delete_object(object_path)
        elif test_type == "upload_file":
            self.upload_file(size, object_path, local_path, metrics)
        elif test_type == "download_file":
            self.download_file(size, object_path, local_path, metrics)
        else:
            raise ValueError(f"Invalid task type: {test_type}")

    def process_task(
        self,
        test_type: str,
        bucket: str,
        size: str,
        batch_range: range,
        metrics: PerformanceMetrics,
        threads: int,
    ) -> None:
        """Process a batch of tasks using thread pool."""
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for i in batch_range:
                executor.submit(self.task, test_type, bucket, size, i, metrics)

    def run_test(
        self,
        test_type: str,
        bucket: str,
        size: str,
        num_objects: int,
        processes: int,
        threads: int,
    ) -> None:
        """Run a benchmark test with the specified parameters."""
        print(
            f"--- Running {test_type} test for {num_objects} x {size} objects with {processes} processes x {threads} threads ---"
        )

        with Manager() as manager:
            # Create shared lists for metrics
            start_times = manager.list()
            end_times = manager.list()
            response_times = manager.list()
            object_sizes = manager.list()

            metrics = PerformanceMetrics(start_times, end_times, response_times, object_sizes)  # type: ignore

            # Split files into batches for each process
            batch_size = num_objects // processes + 1
            batches = [range(i, min(i + batch_size, num_objects)) for i in range(0, num_objects, batch_size)]

            with Pool(processes=processes, maxtasksperchild=1) as pool:
                pool.starmap(
                    self.process_task,
                    [(test_type, bucket, size, batches[i], metrics, threads) for i in range(len(batches))],
                )

            if test_type != "delete":
                metrics.calculate()
            else:
                print("Delete complete")

    def run_all_tests(self) -> None:
        """Run all benchmark tests with the configured parameters."""
        for size_str, objects in self.test_sizes.items():
            try:
                if self.include_file_tests:
                    self.create_files(size_str, objects)
                for processes in self.processes:
                    for threads in self.threads:
                        self.run_test("upload", self.prefix, size_str, objects, processes, threads)
                        self.run_test("download", self.prefix, size_str, objects, processes, threads)
                        if self.include_file_tests:
                            self.run_test("upload_file", self.prefix, size_str, objects, processes, threads)
                            self.run_test("download_file", self.prefix, size_str, objects, processes, threads)
                        self.run_test("delete", self.prefix, size_str, objects, processes, threads)
            finally:
                if self.include_file_tests:
                    self.cleanup_test_dir()


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload/Download performance tests with Multi-Storage Client")
    parser.add_argument("--prefix", type=str, default="", help="The path prefix to use for the test")
    parser.add_argument("--config", type=str, help="Path to configuration file")
    parser.add_argument("--profile", type=str, required=True, help="MSC profile to use")
    parser.add_argument(
        "--include-file-tests",
        action="store_true",
        help="Include additional file-based tests (use upload_file and download_file)",
    )
    parser.add_argument(
        "--file-tests-dir",
        type=str,
        default=DEFAULT_TEST_DIR,
        help=f"Directory for file-based tests (if --include-file-tests is used). This directory will be deleted after test finishes, do NOT specify a directory containing important files). Default: {DEFAULT_TEST_DIR}",
    )
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    processes = config["processes"]
    threads = config["threads"]
    test_object_sizes = config["test_object_sizes"]

    # Initialize storage client
    storage_client_config = StorageClientConfig.from_file(profile=args.profile)
    storage_client = StorageClient(config=storage_client_config)

    # Create and run the benchmark
    benchmark = BenchmarkRunner(
        storage_client,
        prefix=args.prefix,
        processes=processes,
        threads=threads,
        test_sizes=test_object_sizes,
        include_file_tests=args.include_file_tests,
        file_tests_dir=args.file_tests_dir,
    )
    benchmark.run_all_tests()


if __name__ == "__main__":
    main()
