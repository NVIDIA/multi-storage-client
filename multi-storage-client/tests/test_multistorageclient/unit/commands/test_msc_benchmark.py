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

import copy
import json
import sys
from pathlib import Path

import pytest
from jsonschema.exceptions import ValidationError

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.commands.msc_benchmark import BenchmarkRunner, compare_reports, main, run_file_benchmarks
from multistorageclient.providers.manifest_metadata import DEFAULT_MANIFEST_BASE_DIR
from test_multistorageclient.unit.utils import tempdatastore


@pytest.fixture
def small_test_sizes():
    """Fixture for small test sizes (faster tests)."""
    return {"1KB": 2}


@pytest.fixture
def benchmark_config(small_test_sizes):
    """Fixture for benchmark configuration."""
    return {
        "processes": [1],
        "threads": [1],
        "test_object_sizes": small_test_sizes,
    }


def file_report(workload, throughput=100):
    """Build a valid schema-v1 fixture, including fields added by future producers."""
    return {
        "schema_version": 1,
        "version": "1.2.0",
        "profile": "test",
        "workload": {**workload, "repeats": 3},
        "complete": True,
        "future_metadata": {"ignored": True},
        "results": {
            "download_file/1024/2/1": [
                {"throughput_bytes_per_second": throughput, "elapsed_seconds": 1.0} for _ in range(3)
            ]
        },
    }


@pytest.mark.parametrize("throughput,regression", [(100, False), (90, False), (89, True)])
def test_compare_file_reports(benchmark_config, throughput, regression):
    baseline = file_report(benchmark_config)
    candidate = file_report(benchmark_config, throughput)
    assert bool(compare_reports(baseline, candidate, 0.10)) is regression


@pytest.mark.parametrize("invalid", ["schema", "incomplete", "workload", "cases", "samples", "nonfinite"])
def test_compare_file_reports_rejects_invalid_baselines(benchmark_config, invalid):
    baseline = file_report(benchmark_config)
    candidate = copy.deepcopy(baseline)
    if invalid == "schema":
        baseline["schema_version"] = 2
    elif invalid == "incomplete":
        baseline["complete"] = False
    elif invalid == "workload":
        baseline["workload"]["repeats"] = 4
    elif invalid == "cases":
        baseline["results"]["unexpected"] = baseline["results"].pop("download_file/1024/2/1")
    elif invalid == "samples":
        baseline["results"]["download_file/1024/2/1"].pop()
    else:
        baseline["results"]["download_file/1024/2/1"][0]["throughput_bytes_per_second"] = float("nan")
    with pytest.raises((ValueError, ValidationError)):
        compare_reports(baseline, candidate, 0.10)


@pytest.mark.parametrize("corrupt", [False, True])
def test_direct_file_benchmarks_verify_content_and_cleanup(monkeypatch, corrupt):
    with tempdatastore.TemporaryPOSIXDirectory() as store:
        client = StorageClient(
            StorageClientConfig.from_dict({"profiles": {"test": store.profile_config_dict()}}, "test")
        )
        download = client.download_file

        def download_file(remote_path, local_path):
            download(remote_path, local_path)
            if corrupt:
                Path(local_path).write_bytes(b"corrupted")

        monkeypatch.setattr(client, "download_file", download_file)
        monkeypatch.setattr(client, "read", lambda *args, **kwargs: pytest.fail("Buffered reads must not run"))
        monkeypatch.setattr(client, "write", lambda *args, **kwargs: pytest.fail("Buffered writes must not run"))
        workload = {"processes": [1], "threads": [2], "test_object_sizes": {"1KB": 2, "2KB": 1}}
        if corrupt:
            with pytest.raises(RuntimeError, match="checksum mismatch"):
                run_file_benchmarks(client, workload, 3, "test")
        else:
            results = run_file_benchmarks(client, workload, 3, "test")
            assert len(results) == 4
            assert all(len(samples) == 3 for samples in results.values())
        assert list(client.list("test")) == []


@pytest.mark.parametrize("confirmation,override", [(75, ""), (100, ""), (75, "approved regression")])
def test_file_report_cli_confirmation_and_override(tmp_path, monkeypatch, benchmark_config, confirmation, override):
    config = tmp_path / "workload.json"
    config.write_text(json.dumps(benchmark_config))
    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps(file_report(benchmark_config)))
    output = tmp_path / "candidate.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "msc-benchmark",
            "--profile",
            "test",
            "--file-only",
            "--config",
            str(config),
            "--baseline-file",
            str(baseline),
            "--output-file",
            str(output),
        ],
    )
    monkeypatch.setenv("MSC_PERF_OVERRIDE_REASON", override)
    with tempdatastore.TemporaryPOSIXDirectory() as store:
        client_config = StorageClientConfig.from_dict({"profiles": {"test": store.profile_config_dict()}}, "test")
        monkeypatch.setattr(StorageClientConfig, "from_file", lambda **kwargs: client_config)
        batches = iter(
            [file_report(benchmark_config, 80)["results"], file_report(benchmark_config, confirmation)["results"]]
        )
        monkeypatch.setattr(
            "multistorageclient.commands.msc_benchmark.run_file_benchmarks", lambda *args: next(batches)
        )
        if confirmation == 75 and not override:
            with pytest.raises(SystemExit, match="Confirmed performance regressions"):
                main()
        else:
            main()
    report = json.loads(output.read_text())
    assert report["schema_version"] == 1
    assert report["complete"] is True
    assert report["override_reason"] == override
    assert "confirmation_results" in report
    assert bool(report["regressions"]) is (confirmation == 75)


def test_file_report_cli_execution_errors_are_not_overridden(tmp_path, monkeypatch, benchmark_config):
    config = tmp_path / "workload.json"
    config.write_text(json.dumps(benchmark_config))
    output = tmp_path / "incomplete.json"
    monkeypatch.setattr(
        sys,
        "argv",
        ["msc-benchmark", "--profile", "test", "--file-only", "--config", str(config), "--output-file", str(output)],
    )
    monkeypatch.setenv("MSC_PERF_OVERRIDE_REASON", "approved throughput regression")
    with tempdatastore.TemporaryPOSIXDirectory() as store:
        client_config = StorageClientConfig.from_dict({"profiles": {"test": store.profile_config_dict()}}, "test")
        monkeypatch.setattr(StorageClientConfig, "from_file", lambda **kwargs: client_config)

        def fail_transfer(*args):
            raise OSError("transfer failed")

        monkeypatch.setattr("multistorageclient.commands.msc_benchmark.run_file_benchmarks", fail_transfer)
        with pytest.raises(OSError, match="transfer failed"):
            main()
    assert json.loads(output.read_text())["complete"] is False


@pytest.mark.parametrize(
    "temp_data_store_type",
    [
        tempdatastore.TemporaryPOSIXDirectory,
        tempdatastore.TemporaryAWSS3Bucket,
    ],
)
def test_benchmark_without_metadata_provider(temp_data_store_type, benchmark_config, small_test_sizes):
    """Test benchmark runner works without metadata provider."""
    with temp_data_store_type() as temp_data_store:
        profile_config_dict = temp_data_store.profile_config_dict()

        storage_client_config_dict = {
            "profiles": {
                "test": profile_config_dict,
            }
        }

        # Create storage client without metadata provider
        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(config_dict=storage_client_config_dict, profile="test")
        )
        assert storage_client._metadata_provider is None

        # Create and run benchmark
        benchmark = BenchmarkRunner(
            storage_client,
            prefix="benchmark_test",
            processes=benchmark_config["processes"],
            threads=benchmark_config["threads"],
            test_sizes=small_test_sizes,
            include_file_tests=False,
        )

        # Run all tests
        benchmark.run_all_tests()

        # Verify files were created and can be listed
        files = list(storage_client.list(path="benchmark_test"))
        assert len(files) == 0  # Files are deleted at the end


@pytest.mark.parametrize(
    "temp_data_store_type",
    [
        tempdatastore.TemporaryPOSIXDirectory,
        tempdatastore.TemporaryAWSS3Bucket,
    ],
)
def test_benchmark_with_metadata_provider(temp_data_store_type, benchmark_config, small_test_sizes):
    """Test benchmark runner works correctly with metadata provider."""
    with temp_data_store_type() as temp_data_store:
        data_profile_config_dict = temp_data_store.profile_config_dict()

        # Add metadata provider to profile
        data_with_manifest_profile_config_dict = copy.deepcopy(data_profile_config_dict) | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                    "writable": True,
                },
            }
        }

        storage_client_config_dict = {
            "profiles": {
                "test_with_manifest": data_with_manifest_profile_config_dict,
            }
        }

        # Create storage client with metadata provider
        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(config_dict=storage_client_config_dict, profile="test_with_manifest")
        )
        assert storage_client._metadata_provider is not None

        # Create and run benchmark
        benchmark = BenchmarkRunner(
            storage_client,
            prefix="benchmark_test",
            processes=benchmark_config["processes"],
            threads=benchmark_config["threads"],
            test_sizes=small_test_sizes,
            include_file_tests=False,
        )

        # Run all tests - this should now work with the commit_metadata() calls
        benchmark.run_all_tests()

        # Verify that all files were deleted
        files = list(storage_client.list(path="benchmark_test"))
        assert len(files) == 0
