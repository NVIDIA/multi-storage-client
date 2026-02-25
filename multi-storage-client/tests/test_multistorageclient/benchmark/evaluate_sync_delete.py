# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Benchmark script to evaluate msc.delete(..., recursive=True) performance
# across different MSC_SYNC_BATCH_SIZE values.
#
# Usage (from repo root, with MSC config having profile "datasource" backed by S3):
#   python -m tests.test_multistorageclient.benchmark.evaluate_sync_delete
#
# Prerequisites:
#   - MSC config with a profile named "datasource" backed by S3.
#   - AWS credentials (or equivalent) for that profile.

import os
import shutil
import tempfile
import time

import multistorageclient as msc

NUM_FILES = 1_000
FILE_SIZE_BYTES = 512
TARGET_URL = "msc://datasource/test_001"

# Sync producer enforces batch_size in [10, 100, 1000]. We compare minimum (10) vs maximum (1000).
BATCH_SIZES = (10, 100, 1000)


def create_local_dir_with_files(dir_path: str, num_files: int, file_size: int) -> None:
    os.makedirs(dir_path, exist_ok=True)
    for i in range(num_files):
        path = os.path.join(dir_path, f"file_{i:05d}.bin")
        with open(path, "wb") as f:
            f.write(os.urandom(file_size))


def upload_directory(local_dir: str, target_url: str) -> None:
    local_path = os.path.abspath(local_dir)
    msc.sync(local_path, target_url)


def run_delete_benchmark(target_url: str, batch_size: int) -> float:
    os.environ["MSC_SYNC_BATCH_SIZE"] = str(batch_size)
    prefix = target_url.rstrip("/") + "/"
    start = time.perf_counter()
    msc.delete(prefix, recursive=True)
    elapsed = time.perf_counter() - start
    return elapsed


def main() -> None:
    tmpdir = tempfile.mkdtemp(prefix="msc_eval_sync_delete_")
    try:
        local_dir = os.path.join(tmpdir, "src")
        print(f"Creating {NUM_FILES} small files in {local_dir} ...")
        create_local_dir_with_files(local_dir, NUM_FILES, FILE_SIZE_BYTES)
        print(f"Uploading to {TARGET_URL} ...")
        upload_directory(local_dir, TARGET_URL)

        results = {}
        for batch_size in BATCH_SIZES:
            print(f"\nDeleting with MSC_SYNC_BATCH_SIZE={batch_size} ...")
            elapsed = run_delete_benchmark(TARGET_URL, batch_size)
            results[batch_size] = elapsed
            print(f"  Elapsed: {elapsed:.2f}s")
            print("  Re-uploading for next run ...")
            upload_directory(local_dir, TARGET_URL)

        print(f"{'MSC_SYNC_BATCH_SIZE':<20} {'Elapsed (s)':<12}")
        print("-" * 32)
        for batch_size in BATCH_SIZES:
            print(f"{batch_size:<20} {results[batch_size]:<12.2f}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
        try:
            msc.delete(TARGET_URL.rstrip("/") + "/", recursive=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
