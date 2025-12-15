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

import pytest

import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.commands.msc_benchmark import BenchmarkRunner
from multistorageclient.providers.manifest_metadata import DEFAULT_MANIFEST_BASE_DIR


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
