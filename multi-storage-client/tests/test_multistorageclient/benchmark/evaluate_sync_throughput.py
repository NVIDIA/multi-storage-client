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
#
# Benchmark script to evaluate msc.sync upload and download throughput
# across a realistic mix of file sizes totaling ~10 GB.
#
# Usage (from repo root, with MSC config having a profile backed by object storage):
#   python -m tests.test_multistorageclient.benchmark.evaluate_sync_throughput
#
# Environment variables:
#   MSC_BENCH_TARGET_URL  - Target URL for uploads (default: msc://datasource/bench_sync_throughput)
#   MSC_BENCH_SCALE       - Multiplier for data volume; 1.0 = ~10 GB (default: 1.0)
#   MSC_BENCH_DIR         - Base directory for temporary benchmark data (default: cwd)
#
# Prerequisites:
#   - MSC config with a profile referenced by the target URL (e.g. "datasource" backed by S3).
#   - Credentials for that profile.

import os
import shutil
import tempfile
import time

import multistorageclient as msc

TARGET_URL = os.environ.get("MSC_BENCH_TARGET_URL", "msc://datasource/bench_sync_throughput")
SCALE = float(os.environ.get("MSC_BENCH_SCALE", "1.0"))
BENCH_DIR = os.environ.get("MSC_BENCH_DIR", os.getcwd())

FILE_TIERS: list[tuple[str, int, int]] = [
    ("1MB", 1 * 1024 * 1024, 10000),
]


def _human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024  # type: ignore[assignment]
    return f"{n:.1f} PB"


def _human_rate(byte_count: int, seconds: float) -> str:
    if seconds <= 0:
        return "N/A"
    return f"{_human_bytes(int(byte_count / seconds))}/s"


def create_test_data(base_dir: str) -> dict[str, dict]:
    """Generate files organised into sub-directories per size tier."""
    tier_info: dict[str, dict] = {}
    total_bytes = 0

    for label, size, base_count in FILE_TIERS:
        count = max(1, int(base_count * SCALE))
        tier_dir = os.path.join(base_dir, label)
        os.makedirs(tier_dir, exist_ok=True)

        tier_bytes = size * count
        total_bytes += tier_bytes
        tier_info[label] = {"size": size, "count": count, "total_bytes": tier_bytes}

        print(f"  Generating {count} × {label} files ({_human_bytes(tier_bytes)}) ...")
        chunk = os.urandom(min(size, 1 * 1024 * 1024))
        for i in range(count):
            path = os.path.join(tier_dir, f"{i:06d}.bin")
            with open(path, "wb") as f:
                remaining = size
                while remaining > 0:
                    write_size = min(remaining, len(chunk))
                    f.write(chunk[:write_size])
                    remaining -= write_size

    print(f"  Total test data: {_human_bytes(total_bytes)}")
    tier_info["_total"] = {"total_bytes": total_bytes}
    return tier_info


def run_upload(local_dir: str, target_url: str) -> msc.types.SyncResult:
    return msc.sync(os.path.abspath(local_dir), target_url)


def run_download(target_url: str, local_dir: str) -> msc.types.SyncResult:
    return msc.sync(target_url, os.path.abspath(local_dir))


def print_result_table(results: list[tuple[str, int, float, float]]) -> None:
    """Print a formatted results table.

    Each entry is (phase, bytes_transferred, elapsed_seconds, throughput_bytes_per_sec).
    """
    header = f"{'Phase':<20} {'Data':<12} {'Time (s)':<12} {'Throughput':<16}"
    print()
    print(header)
    print("-" * len(header))
    for phase, nbytes, elapsed, _ in results:
        print(f"{phase:<20} {_human_bytes(nbytes):<12} {elapsed:<12.2f} {_human_rate(nbytes, elapsed):<16}")
    print()


def main() -> None:
    upload_dir = tempfile.mkdtemp(prefix="msc_bench_upload_", dir=BENCH_DIR)
    download_dir = tempfile.mkdtemp(prefix="msc_bench_download_", dir=BENCH_DIR)
    results: list[tuple[str, int, float, float]] = []

    try:
        # --- Generate data ---
        print(f"\n=== Generating test data (scale={SCALE:.1f}) ===")
        tier_info = create_test_data(upload_dir)
        total_bytes = tier_info["_total"]["total_bytes"]

        # --- Upload (local → remote) ---
        print(f"\n=== Upload: local → {TARGET_URL} ===")
        t0 = time.perf_counter()
        upload_result = run_upload(upload_dir, TARGET_URL)
        upload_elapsed = time.perf_counter() - t0
        print(upload_result)
        results.append(("Upload", total_bytes, upload_elapsed, total_bytes / max(upload_elapsed, 1e-9)))

        # --- Download (remote → local) ---
        print(f"\n=== Download: {TARGET_URL} → local ===")
        t0 = time.perf_counter()
        download_result = run_download(TARGET_URL, download_dir)
        download_elapsed = time.perf_counter() - t0
        print(download_result)
        results.append(("Download", total_bytes, download_elapsed, total_bytes / max(download_elapsed, 1e-9)))

        # --- Per-tier breakdown (informational) ---
        for label in [t[0] for t in FILE_TIERS]:
            info = tier_info[label]
            print(f"  Tier {label}: {info['count']} files, {_human_bytes(info['total_bytes'])} total")

        # --- Summary ---
        print_result_table(results)

    finally:
        shutil.rmtree(upload_dir, ignore_errors=True)
        shutil.rmtree(download_dir, ignore_errors=True)
        print("Cleaning up remote objects ...")
        try:
            msc.delete(TARGET_URL.rstrip("/") + "/", recursive=True)
        except Exception as exc:
            print(f"  Warning: remote cleanup failed: {exc}")


if __name__ == "__main__":
    main()
