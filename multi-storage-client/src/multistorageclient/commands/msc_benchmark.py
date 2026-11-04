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
import hashlib
import importlib.metadata
import json
import math
import os
import shutil
import statistics
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Manager, Pool
from multiprocessing.managers import ListProxy
from pathlib import Path
from typing import Any

from jsonschema import validate

import multistorageclient as msc
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.schema import BENCHMARK_SCHEMA

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

REPORT_SCHEMA = {
    "type": "object",
    "required": ["schema_version", "version", "profile", "workload", "results", "complete"],
    "properties": {
        "schema_version": {"const": 1},
        "version": {"type": "string"},
        "profile": {"type": "string"},
        "workload": {"type": "object"},
        "complete": {"const": True},
        "results": {
            "type": "object",
            "minProperties": 1,
            "additionalProperties": {
                "type": "array",
                "minItems": 3,
                "items": {
                    "type": "object",
                    "required": ["elapsed_seconds", "throughput_bytes_per_second"],
                    "properties": {
                        "elapsed_seconds": {"type": "number", "exclusiveMinimum": 0},
                        "throughput_bytes_per_second": {"type": "number", "exclusiveMinimum": 0},
                    },
                },
            },
        },
    },
}


def compare_reports(baseline: dict[str, Any], candidate: dict[str, Any], threshold: float) -> dict[str, float]:
    """Compare schema-v1 file reports, rejecting incomplete or incompatible data.

    Unknown additive report fields are allowed. Existing field meanings and units
    remain stable within schema version 1. Throughput is bytes per second; return
    cases whose median drops strictly beyond the fractional threshold.
    """
    for report in (baseline, candidate):
        validate(instance=report, schema=REPORT_SCHEMA)
    if baseline["workload"] != candidate["workload"] or baseline["profile"] != candidate["profile"]:
        raise ValueError("Baseline and candidate workloads and profiles must match")
    if baseline["results"].keys() != candidate["results"].keys():
        raise ValueError("Baseline and candidate benchmark cases must match")
    regressions = {}
    for case, before in baseline["results"].items():
        after = candidate["results"][case]
        if len(before) != len(after):
            raise ValueError("Baseline and candidate repetition counts must match")
        for sample in [*before, *after]:
            if not all(math.isfinite(sample[key]) for key in ("elapsed_seconds", "throughput_bytes_per_second")):
                raise ValueError("Benchmark samples must be finite")
        drop = 1 - statistics.median(s["throughput_bytes_per_second"] for s in after) / statistics.median(
            s["throughput_bytes_per_second"] for s in before
        )
        if drop > threshold:
            regressions[case] = drop
    return regressions


def run_file_benchmarks(
    client: StorageClient, config: dict[str, Any], repeats: int, prefix: str
) -> dict[str, list[dict[str, float]]]:
    """Measure direct file transfers with one warmup and bounded memory.

    Test sizes and thread counts use the existing benchmark workload config.
    This mode uses one process and fresh remote keys for every sample. Timings
    include completed downloads and multipart assembly. Generation, streaming
    SHA-256 verification, and cleanup are outside timed sections.
    """
    if config["processes"] != [1] or repeats < 3:
        raise ValueError("File-only reports require one process and at least three repetitions")
    sizes = {size_to_bytes(size): count for size, count in config["test_object_sizes"].items()}
    if not sizes or not config["threads"] or min(*sizes, *sizes.values(), *config["threads"]) < 1:
        raise ValueError("Object sizes, counts, and thread counts must be positive")
    results: dict[str, list[dict[str, float]]] = {}
    with tempfile.TemporaryDirectory(prefix="msc-benchmark-") as directory:
        root = Path(directory)
        for size, count in sizes.items():
            source = root / "source"
            with source.open("wb") as stream:
                remaining = size
                while remaining:
                    block = os.urandom(min(8 * 1024 * 1024, remaining))
                    stream.write(block)
                    remaining -= len(block)
            with source.open("rb") as stream:
                expected = hashlib.file_digest(stream, "sha256").hexdigest()
            for threads in config["threads"]:
                for repeat in range(-1, repeats):
                    sample_prefix = f"{prefix.rstrip('/')}/file-benchmark-{uuid.uuid4().hex}".lstrip("/")
                    keys = [f"{sample_prefix}/{i}" for i in range(count)]
                    destinations = [root / f"download-{i}" for i in range(count)]
                    print(f"{size} bytes x {count}, threads={threads}, sample={repeat + 1}", flush=True)
                    try:
                        with ThreadPoolExecutor(max_workers=threads) as executor:
                            for operation in ("upload_file", "download_file"):
                                paths = (
                                    [str(source)] * count
                                    if operation == "upload_file"
                                    else list(map(str, destinations))
                                )
                                start = time.perf_counter()
                                list(executor.map(getattr(client, operation), keys, paths))
                                elapsed = time.perf_counter() - start
                                case = f"{operation}/{size}/{count}/{threads}"
                                if repeat >= 0:
                                    results.setdefault(case, []).append(
                                        {
                                            "elapsed_seconds": elapsed,
                                            "throughput_bytes_per_second": size * count / elapsed,
                                        }
                                    )
                                if operation == "download_file":
                                    for destination in destinations:
                                        with destination.open("rb") as stream:
                                            if hashlib.file_digest(stream, "sha256").hexdigest() != expected:
                                                raise RuntimeError(f"Download checksum mismatch: {destination}")
                                        destination.unlink()
                    finally:
                        for key in keys:
                            try:
                                client.delete(key)
                            except FileNotFoundError:
                                pass
    return results


def load_config(config_path: str | None) -> dict[str, Any]:
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
        start_times: list[Any] | ListProxy,
        end_times: list[Any] | ListProxy,
        response_times: list[Any] | ListProxy,
        object_sizes: list[Any] | ListProxy,
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
        test_sizes: dict[str, int] | None = None,
        processes: list[int] | None = None,
        threads: list[int] | None = None,
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
        self.random_data = {k: os.urandom(size_to_bytes(k)) for k in self.test_sizes}

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

                        # Commit metadata after upload to ensure metadata provider has the file records
                        # This is required when using metadata providers (e.g., ManifestMetadataProvider)
                        self.storage_client.commit_metadata()

                        self.run_test("download", self.prefix, size_str, objects, processes, threads)
                        if self.include_file_tests:
                            self.run_test("upload_file", self.prefix, size_str, objects, processes, threads)

                            # Commit metadata after upload_file to ensure metadata provider has the file records
                            self.storage_client.commit_metadata()

                            self.run_test("download_file", self.prefix, size_str, objects, processes, threads)
                        self.run_test("delete", self.prefix, size_str, objects, processes, threads)

                        # Commit metadata after delete to ensure metadata provider records the deletions
                        self.storage_client.commit_metadata()
            finally:
                if self.include_file_tests:
                    self.cleanup_test_dir()


def main() -> None:
    """Run existing object benchmarks or uncached file-only release comparisons.

    File-only mode supports --output-file JSON reports and --baseline-file
    comparisons. Reports follow REPORT_SCHEMA version 1, with raw timings in
    seconds and throughput in bytes per second. One warmup is discarded before
    three measured repetitions. A drop above --threshold is checked again
    against the same stored baseline before failing. MSC_PERF_OVERRIDE_REASON
    accepts confirmed regressions only; transfer and report errors always fail.

    For a small file workload, use --config .msc/benchmark.json with the
    local-s3 profile from .msc/config.benchmark.yaml. Start the test storage
    services and create its bucket first. For published releases without report
    support, this CLI file can run inside uvx's released-package environment.
    Use an uncontended machine and the same workload for relative comparisons.
    """
    parser = argparse.ArgumentParser(description="Upload/Download performance tests with Multi-Storage Client")
    parser.add_argument("--prefix", type=str, default="", help="The path prefix to use for the test")
    parser.add_argument("--config", type=str, help="Path to configuration file")
    parser.add_argument("--profile", type=str, required=True, help="MSC profile to use")
    parser.add_argument(
        "--file-only",
        action="store_true",
        help="Measure direct, uncached file transfers (requires a one-process config)",
    )
    parser.add_argument("--output-file", type=Path, help="Write a schema-v1 JSON file-only benchmark report")
    parser.add_argument("--baseline-file", type=Path, help="Fail on confirmed regressions against a JSON report")
    parser.add_argument("--repeats", type=int, default=3, help="File-only measured repetitions after one warmup")
    parser.add_argument("--threshold", type=float, default=float(os.getenv("MSC_PERF_THRESHOLD") or 0.10))
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
    if not 0 <= args.threshold < 1 or args.repeats < 3:
        parser.error("threshold must be in [0, 1) and repeats at least 3")
    if (args.output_file or args.baseline_file) and not args.file_only:
        parser.error("JSON reports and comparisons require --file-only")

    # Load configuration
    config = load_config(args.config)
    processes = config["processes"]
    threads = config["threads"]
    test_object_sizes = config["test_object_sizes"]

    # Initialize storage client
    storage_client_config = StorageClientConfig.from_file(profile=args.profile)
    if args.file_only and storage_client_config.cache_manager is not None:
        parser.error("File-only benchmarks require an uncached storage profile")
    storage_client = StorageClient(config=storage_client_config)

    if args.file_only:
        report = {
            "schema_version": 1,
            "version": importlib.metadata.version("multi-storage-client"),
            "package_path": str(Path(msc.__file__).resolve()),
            "profile": args.profile,
            "workload": {**config, "repeats": args.repeats},
            "results": {},
            "complete": False,
        }
        if args.output_file:
            args.output_file.parent.mkdir(parents=True, exist_ok=True)
            args.output_file.write_text(json.dumps(report, indent=2))
        baseline = json.loads(args.baseline_file.read_text()) if args.baseline_file else None
        if baseline is not None:
            validate(instance=baseline, schema=REPORT_SCHEMA)
            if baseline["workload"] != report["workload"] or baseline["profile"] != report["profile"]:
                raise ValueError("Baseline and candidate workloads and profiles must match")
        report["results"] = run_file_benchmarks(storage_client, config, args.repeats, args.prefix)
        report["complete"] = True
        validate(instance=report, schema=REPORT_SCHEMA)
        regressions = {}
        reason = os.getenv("MSC_PERF_OVERRIDE_REASON", "").strip()
        if baseline is not None:
            regressions = compare_reports(baseline, report, args.threshold)
            report.update(baseline_version=baseline["version"], initial_regressions=regressions)
            if regressions:
                confirmation = run_file_benchmarks(storage_client, config, args.repeats, args.prefix)
                failures = compare_reports(baseline, {**report, "results": confirmation}, args.threshold)
                regressions = {case: drop for case, drop in failures.items() if case in regressions}
                report["confirmation_results"] = confirmation
        report.update(regressions=regressions, threshold=args.threshold, override_reason=reason)
        if args.output_file:
            args.output_file.write_text(json.dumps(report, indent=2))
        else:
            print(json.dumps(report, indent=2))
        if regressions and not reason:
            raise SystemExit(f"Confirmed performance regressions: {regressions}")
        return

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
