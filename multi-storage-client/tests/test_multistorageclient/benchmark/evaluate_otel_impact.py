# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

"""
Benchmark script to measure OpenTelemetry performance impact on MSC operations.

This script simulates training workload I/O patterns and measures the overhead
of different telemetry configurations.
"""

import argparse
import os
import shutil
import statistics
import sys
import tempfile
import time
from contextlib import contextmanager


@contextmanager
def redirect_stdout_fd(to_file):
    """
    Redirect stdout at the file descriptor level.
    This catches output from background threads (like OTel console exporter).
    """
    stdout_fd = sys.stdout.fileno()
    # Save the original stdout fd
    saved_stdout_fd = os.dup(stdout_fd)

    try:
        # Redirect stdout fd to the file
        os.dup2(to_file.fileno(), stdout_fd)
        yield
    finally:
        # Restore original stdout
        os.dup2(saved_stdout_fd, stdout_fd)
        os.close(saved_stdout_fd)


def create_config(
    base_path: str,
    enable_otel: bool = True,
    collect_interval_ms: int = 10,
    export_interval_ms: int = 1000,
    use_process_attrs: bool = True,
    use_env_attrs: bool = True,
    use_msc_config_attrs: bool = True,
    async_recording: bool = False,
) -> dict:
    """Create MSC configuration with specified telemetry settings."""
    config = {"profiles": {"test": {"storage_provider": {"type": "file", "options": {"base_path": base_path}}}}}

    if enable_otel:
        # Use console exporter but redirect to file to suppress output
        exporter_config = {"type": "console", "options": {}}

        reader_config = {
            "collect_interval_millis": collect_interval_ms,
            "collect_timeout_millis": min(100, collect_interval_ms * 10),
            "export_interval_millis": export_interval_ms,
            "export_timeout_millis": min(500, export_interval_ms // 2),
        }

        if async_recording:
            reader_config["async"] = True

        otel_config = {"metrics": {"attributes": [], "reader": reader_config, "exporter": exporter_config}}

        if use_process_attrs:
            otel_config["metrics"]["attributes"].append(
                {"type": "process", "options": {"attributes": {"msc.process": "pid"}}}
            )

        if use_env_attrs:
            otel_config["metrics"]["attributes"].append(
                {
                    "type": "environment_variables",
                    "options": {
                        "attributes": {
                            "msc.test_var1": "TEST_VAR_1",
                            "msc.test_var2": "TEST_VAR_2",
                            "msc.test_var3": "TEST_VAR_3",
                            "msc.test_var4": "TEST_VAR_4",
                            "msc.test_var5": "TEST_VAR_5",
                            "msc.test_var6": "TEST_VAR_6",
                            "msc.test_var7": "TEST_VAR_7",
                            "msc.test_var8": "TEST_VAR_8",
                            "msc.test_var9": "TEST_VAR_9",
                        }
                    },
                }
            )
            # Set env vars for testing
            for i in range(1, 10):
                os.environ[f"TEST_VAR_{i}"] = f"value_{i}"

        if use_msc_config_attrs:
            otel_config["metrics"]["attributes"].append(
                {
                    "type": "msc_config",
                    "options": {
                        "attributes": {"msc.test_value": {"expression": "profiles.test.storage_provider.type"}}
                    },
                }
            )

        config["opentelemetry"] = otel_config

    return config


def benchmark_workload(
    provider,
    test_dir: str,
    num_files: int = 100,
    file_size_kb: int = 64,
    num_reads: int = 10,
) -> dict[str, float]:
    """
    Simulate a training workload with many small I/O operations.

    Args:
        provider: MSC storage provider instance
        test_dir: Directory to create test files in
        num_files: Number of files to create and read
        file_size_kb: Size of each file in KB
        num_reads: Number of times to read each file

    Returns:
        Dictionary with timing results
    """
    results = {}

    # Generate test data
    test_data = b"x" * (file_size_kb * 1024)

    # Benchmark: Write operations
    write_times = []
    print(f"  Writing {num_files} files ({file_size_kb}KB each)...")
    for i in range(num_files):
        path = os.path.join(test_dir, f"test_file_{i:04d}.bin")
        start = time.perf_counter()
        provider.put_object(path, test_data)
        write_times.append(time.perf_counter() - start)

    results["write_total_time"] = sum(write_times)
    results["write_mean_latency"] = statistics.mean(write_times) * 1000  # ms
    results["write_p50_latency"] = statistics.median(write_times) * 1000  # ms
    results["write_p95_latency"] = statistics.quantiles(write_times, n=20)[18] * 1000  # ms
    results["write_throughput_ops"] = num_files / results["write_total_time"]
    results["write_throughput_mbps"] = (num_files * file_size_kb / 1024) / results["write_total_time"]

    # Benchmark: Read operations
    read_times = []
    print(f"  Reading {num_files} files {num_reads} times each...")
    for _ in range(num_reads):
        for i in range(num_files):
            path = os.path.join(test_dir, f"test_file_{i:04d}.bin")
            start = time.perf_counter()
            _ = provider.get_object(path)
            read_times.append(time.perf_counter() - start)

    total_reads = num_files * num_reads
    results["read_total_time"] = sum(read_times)
    results["read_mean_latency"] = statistics.mean(read_times) * 1000  # ms
    results["read_p50_latency"] = statistics.median(read_times) * 1000  # ms
    results["read_p95_latency"] = statistics.quantiles(read_times, n=20)[18] * 1000  # ms
    results["read_throughput_ops"] = total_reads / results["read_total_time"]
    results["read_throughput_mbps"] = (total_reads * file_size_kb / 1024) / results["read_total_time"]

    # Benchmark: List operations
    list_times = []
    print("  Listing directory 50 times...")
    for _ in range(50):
        start = time.perf_counter()
        list(provider.list_objects(""))
        list_times.append(time.perf_counter() - start)

    results["list_mean_latency"] = statistics.mean(list_times) * 1000  # ms
    results["list_p50_latency"] = statistics.median(list_times) * 1000  # ms
    results["list_p95_latency"] = statistics.quantiles(list_times, n=20)[18] * 1000  # ms

    # Benchmark: Metadata operations
    info_times = []
    print(f"  Getting metadata for {num_files} files...")
    for i in range(num_files):
        path = os.path.join(test_dir, f"test_file_{i:04d}.bin")
        start = time.perf_counter()
        provider.get_object_metadata(path)
        info_times.append(time.perf_counter() - start)

    results["info_mean_latency"] = statistics.mean(info_times) * 1000  # ms
    results["info_p50_latency"] = statistics.median(info_times) * 1000  # ms
    results["info_p95_latency"] = statistics.quantiles(info_times, n=20)[18] * 1000  # ms

    return results


def run_benchmark_scenario(
    scenario_name: str,
    config: dict,
    test_dir: str,
    num_files: int = 100,
    file_size_kb: int = 64,
    num_reads: int = 10,
) -> dict[str, float]:
    """Run a single benchmark scenario with given configuration."""
    print(f"\n{'=' * 70}")
    print(f"Running: {scenario_name}")
    print(f"{'=' * 70}")
    sys.stdout.flush()  # Ensure output is visible before redirection

    # Create data directory
    data_dir = os.path.join(test_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    # Update config to use data_dir
    config["profiles"]["test"]["storage_provider"]["options"]["base_path"] = data_dir

    # Redirect telemetry console output to a file
    metrics_file_path = os.path.join(test_dir, "metrics.json")

    with open(metrics_file_path, "w") as metrics_file:
        with redirect_stdout_fd(metrics_file):
            # Initialize MSC directly from dict
            from multistorageclient import StorageClient, StorageClientConfig

            config_obj = StorageClientConfig.from_dict(config_dict=config, profile="test")
            client = StorageClient(config_obj)
            provider = client._storage_provider

            # Run benchmark (console exporter output goes to metrics_file)
            results = benchmark_workload(
                provider, test_dir, num_files=num_files, file_size_kb=file_size_kb, num_reads=num_reads
            )

            # Shutdown async telemetry if enabled
            if hasattr(provider, "_shutdown_async_telemetry"):
                provider._shutdown_async_telemetry()  # type: ignore

            # Give telemetry a moment to flush final exports
            time.sleep(0.2)

    print(f"\nResults for {scenario_name}:")
    print(f"  Write: {results['write_throughput_ops']:.1f} ops/s, {results['write_mean_latency']:.3f}ms mean latency")
    print(f"  Read:  {results['read_throughput_ops']:.1f} ops/s, {results['read_mean_latency']:.3f}ms mean latency")
    print(f"  List:  {results['list_mean_latency']:.3f}ms mean latency")
    print(f"  Info:  {results['info_mean_latency']:.3f}ms mean latency")

    return results


def calculate_overhead(baseline: dict, test: dict) -> dict[str, float]:
    """Calculate percentage overhead compared to baseline."""
    overhead = {}
    for key in baseline:
        if "latency" in key or "time" in key:
            overhead[key] = ((test[key] / baseline[key]) - 1.0) * 100
        elif "throughput" in key:
            overhead[key] = ((baseline[key] / test[key]) - 1.0) * 100
    return overhead


def print_comparison_table(scenarios: dict[str, dict[str, float]], baseline_name: str):
    """Print a comparison table of all scenarios."""
    print(f"\n{'=' * 100}")
    print("PERFORMANCE COMPARISON TABLE")
    print(f"{'=' * 100}")
    print(f"Baseline: {baseline_name}")
    print(f"{'-' * 100}")

    baseline = scenarios[baseline_name]

    metrics = [
        ("write_mean_latency", "Write Latency (ms)", "latency"),
        ("read_mean_latency", "Read Latency (ms)", "latency"),
        ("list_mean_latency", "List Latency (ms)", "latency"),
        ("info_mean_latency", "Info Latency (ms)", "latency"),
        ("write_throughput_ops", "Write Throughput (ops/s)", "throughput"),
        ("read_throughput_ops", "Read Throughput (ops/s)", "throughput"),
    ]

    for metric_key, metric_name, metric_type in metrics:
        print(f"\n{metric_name}:")
        print(f"  {'Scenario':<40} {'Value':>15} {'vs Baseline':>20}")
        print(f"  {'-' * 40} {'-' * 15} {'-' * 20}")

        baseline_val = baseline[metric_key]
        print(f"  {baseline_name:<40} {baseline_val:>15.3f} {'(baseline)':>20}")

        for scenario_name, results in scenarios.items():
            if scenario_name == baseline_name:
                continue

            val = results[metric_key]
            if metric_type == "latency":
                overhead = ((val / baseline_val) - 1.0) * 100
                diff_str = f"+{overhead:.1f}% slower" if overhead > 0 else f"{overhead:.1f}% faster"
            else:  # throughput
                overhead = ((baseline_val / val) - 1.0) * 100
                diff_str = f"-{overhead:.1f}% slower" if overhead > 0 else f"+{abs(overhead):.1f}% faster"

            print(f"  {scenario_name:<40} {val:>15.3f} {diff_str:>20}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark OpenTelemetry performance impact on MSC")
    parser.add_argument("--num-files", type=int, default=1000, help="Number of files to test with (default: 100)")
    parser.add_argument("--file-size-kb", type=int, default=16, help="Size of each file in KB (default: 64)")
    parser.add_argument("--num-reads", type=int, default=10, help="Number of times to read each file (default: 10)")
    parser.add_argument("--quick", action="store_true", help="Run quick test with fewer files")
    args = parser.parse_args()

    if args.quick:
        num_files = 20
        num_reads = 3
        print("Running QUICK test mode (fewer operations)")
    else:
        num_files = args.num_files
        num_reads = args.num_reads

    file_size_kb = args.file_size_kb

    # Create temporary directory for testing
    test_base = tempfile.mkdtemp(prefix="msc_otel_benchmark_")
    print(f"Test directory: {test_base}")

    try:
        scenarios = {}

        # Scenario 1: No telemetry (baseline)
        test_dir = os.path.join(test_base, "scenario_1")
        os.makedirs(test_dir)
        config = create_config(test_dir, enable_otel=False)
        scenarios["1. No Telemetry (Baseline)"] = run_benchmark_scenario(
            "1. No Telemetry (Baseline)", config, test_dir, num_files, file_size_kb, num_reads
        )
        shutil.rmtree(test_dir)

        # Scenario 2: User's original config (100 Hz, all attributes)
        test_dir = os.path.join(test_base, "scenario_2")
        os.makedirs(test_dir)
        config = create_config(
            test_dir,
            enable_otel=True,
            collect_interval_ms=10,
            export_interval_ms=1000,
            use_process_attrs=True,
            use_env_attrs=True,
            use_msc_config_attrs=True,
        )
        scenarios["2. Original Config (100 Hz, All Attrs)"] = run_benchmark_scenario(
            "2. Original Config (100 Hz, All Attrs)", config, test_dir, num_files, file_size_kb, num_reads
        )
        shutil.rmtree(test_dir)

        # Scenario 3: Optimized frequency (1 Hz, all attributes)
        test_dir = os.path.join(test_base, "scenario_3")
        os.makedirs(test_dir)
        config = create_config(
            test_dir,
            enable_otel=True,
            collect_interval_ms=1000,
            export_interval_ms=5000,
            use_process_attrs=True,
            use_env_attrs=True,
            use_msc_config_attrs=True,
        )
        scenarios["3. Optimized Frequency (1 Hz, All Attrs)"] = run_benchmark_scenario(
            "3. Optimized Frequency (1 Hz, All Attrs)", config, test_dir, num_files, file_size_kb, num_reads
        )
        shutil.rmtree(test_dir)

        # Scenario 4: Optimized frequency + minimal attributes
        test_dir = os.path.join(test_base, "scenario_4")
        os.makedirs(test_dir)
        config = create_config(
            test_dir,
            enable_otel=True,
            collect_interval_ms=1000,
            export_interval_ms=5000,
            use_process_attrs=False,
            use_env_attrs=False,
            use_msc_config_attrs=True,
        )
        scenarios["4. Optimized Frequency + Minimal Attrs"] = run_benchmark_scenario(
            "4. Optimized Frequency + Minimal Attrs", config, test_dir, num_files, file_size_kb, num_reads
        )
        shutil.rmtree(test_dir)

        # Scenario 5: Original frequency but no attributes
        test_dir = os.path.join(test_base, "scenario_5")
        os.makedirs(test_dir)
        config = create_config(
            test_dir,
            enable_otel=True,
            collect_interval_ms=10,
            export_interval_ms=1000,
            use_process_attrs=False,
            use_env_attrs=False,
            use_msc_config_attrs=False,
        )
        scenarios["5. Original Frequency (100 Hz), No Attrs"] = run_benchmark_scenario(
            "5. Original Frequency (100 Hz), No Attrs", config, test_dir, num_files, file_size_kb, num_reads
        )
        shutil.rmtree(test_dir)

        # Scenario 6: Async recording (100 Hz, all attributes)
        test_dir = os.path.join(test_base, "scenario_6")
        os.makedirs(test_dir)
        config = create_config(
            test_dir,
            enable_otel=True,
            collect_interval_ms=10,
            export_interval_ms=1000,
            use_process_attrs=True,
            use_env_attrs=True,
            use_msc_config_attrs=True,
            async_recording=True,
        )
        scenarios["6. Async Recording (100 Hz, All Attrs)"] = run_benchmark_scenario(
            "6. Async Recording (100 Hz, All Attrs)", config, test_dir, num_files, file_size_kb, num_reads
        )
        shutil.rmtree(test_dir)

        # Print comparison table
        print_comparison_table(scenarios, "1. No Telemetry (Baseline)")

        # Print summary recommendations
        print(f"\n{'=' * 100}")
        print("SUMMARY & RECOMMENDATIONS")
        print(f"{'=' * 100}")

        baseline = scenarios["1. No Telemetry (Baseline)"]
        original = scenarios["2. Original Config (100 Hz, All Attrs)"]
        optimized = scenarios["4. Optimized Frequency + Minimal Attrs"]
        async_mode = scenarios["6. Async Recording (100 Hz, All Attrs)"]

        read_overhead_original = ((original["read_mean_latency"] / baseline["read_mean_latency"]) - 1.0) * 100
        read_overhead_optimized = ((optimized["read_mean_latency"] / baseline["read_mean_latency"]) - 1.0) * 100
        read_overhead_async = ((async_mode["read_mean_latency"] / baseline["read_mean_latency"]) - 1.0) * 100

        print("\n1. ORIGINAL CONFIG IMPACT:")
        print(f"   - Read latency overhead: +{read_overhead_original:.1f}%")
        print(f"   - Per-operation overhead: ~{(original['read_mean_latency'] - baseline['read_mean_latency']):.3f}ms")

        print("\n2. ASYNC RECORDING IMPACT:")
        print(f"   - Read latency overhead: +{read_overhead_async:.1f}%")
        print(
            f"   - Per-operation overhead: ~{(async_mode['read_mean_latency'] - baseline['read_mean_latency']):.3f}ms"
        )
        print(f"   - Improvement over original: {read_overhead_original - read_overhead_async:.1f} percentage points")
        print(
            f"   - Throughput: {async_mode['read_throughput_ops']:.1f} ops/s ({async_mode['read_throughput_ops'] / baseline['read_throughput_ops'] * 100:.1f}% of baseline)"
        )

        print("\n3. OPTIMIZED CONFIG IMPACT:")
        print(f"   - Read latency overhead: +{read_overhead_optimized:.1f}%")
        print(f"   - Per-operation overhead: ~{(optimized['read_mean_latency'] - baseline['read_mean_latency']):.3f}ms")
        print(
            f"   - Improvement over original: {read_overhead_original - read_overhead_optimized:.1f} percentage points"
        )

        print("\n4. RECOMMENDATIONS:")
        if read_overhead_async < 10:
            print("   🏆 BEST SOLUTION: Async Recording")
            print(f"   ✓ Overhead: +{read_overhead_async:.1f}% (vs +{read_overhead_original:.1f}% with sync)")
            print("   ✓ Full observability maintained (100% of operations tracked)")
            print(
                f"   ✓ Near-baseline performance ({async_mode['read_throughput_ops'] / baseline['read_throughput_ops'] * 100:.1f}% of baseline)"
            )
            print("   ✓ Simply add 'async: true' to reader options")
        elif read_overhead_original > 10:
            print(f"   ⚠️  CRITICAL: Original config adds {read_overhead_original:.1f}% overhead!")
            print("   ✓ Enable async recording: add 'async: true' to reader options")
            print(
                f"   ✓ Expected improvement: ~{read_overhead_original - read_overhead_async:.1f}% reduction in overhead"
            )
            print("   ✓ Alternative: Change collect_interval_millis from 10 to 1000")
            print("   ✓ Alternative: Remove process and environment_variables attributes")
        elif read_overhead_original > 5:
            print(f"   ⚠️  WARNING: Original config adds {read_overhead_original:.1f}% overhead")
            print("   ✓ Consider enabling async recording or other optimizations to reduce to <5%")
        else:
            print(f"   ✓ Overhead is acceptable at {read_overhead_original:.1f}%")

        print("\n4. FOR TRAINING WORKLOADS:")
        total_ops = num_files + (num_files * num_reads)  # writes + reads
        overhead_ms_per_op = original["read_mean_latency"] - baseline["read_mean_latency"]
        print(f"   - With {total_ops:,} I/O operations in this test:")
        print(f"   - Original config added: ~{overhead_ms_per_op * total_ops / 1000:.2f}s total overhead")
        print("   - For a training job with millions of I/O ops, this multiplies significantly!")

    finally:
        # Cleanup
        shutil.rmtree(test_base)
        print(f"\nCleaned up test directory: {test_base}")


if __name__ == "__main__":
    main()
