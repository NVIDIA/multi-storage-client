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

import pytest

import multistorageclient as msc
import test_multistorageclient.e2e.common as common
from multistorageclient.types import PreconditionFailedError


@pytest.mark.parametrize("profile_name", ["test-s3-iad", "test-s3-iad-base-path-with-prefix"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_s3_shortcuts(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_shortcuts(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_s3_storage_client(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_storage_client(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_conditional_put(profile_name):
    """Test conditional PUT operations in S3 using if-match and if-none-match conditions."""
    profile = profile_name
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")

    # S3 uses PreconditionFailedError for both if_none_match="*" and if_match failures
    # and NotImplementedError for if_none_match with specific etag
    common.test_conditional_put(
        storage_client=client,
        if_none_match_error_type=PreconditionFailedError,
        if_match_error_type=PreconditionFailedError,
        if_none_match_specific_error_type=NotImplementedError,
    )


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_open_with_source_version_check(profile_name):
    profile = profile_name
    common.test_open_with_source_version_check(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_attributes(profile_name):
    """Test S3 attributes functionality - storing custom metadata with msc_ prefix."""
    profile = profile_name
    common.test_attributes(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad-rust"])
def test_s3_shortcuts_rust(profile_name):
    profile = profile_name
    common.test_shortcuts(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad-rust"])
def test_s3_storage_client_rust(profile_name):
    profile = profile_name
    common.test_storage_client(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_list_exact_file_no_prefix_match(profile_name):
    """Test that listing exact file path doesn't match files with same prefix."""
    profile = profile_name
    common.test_list_exact_file_no_prefix_match(profile)


@pytest.mark.skip(reason="Temporarily disable due to hangs in CI")
@pytest.mark.parametrize("profile_name", ["test-s3-iad-with-replica"])
def test_s3_replica_read_using_msc_open(profile_name):
    profile = profile_name
    common.test_replica_read_using_msc_open(profile)


@pytest.mark.skip(reason="Temporarily disable due to hangs in CI")
@pytest.mark.parametrize("profile_name", ["test-s3-iad-with-replica"])
def test_s3_replica_read_using_msc_read(profile_name):
    profile = profile_name
    common.test_replica_read_using_msc_read(profile)


@pytest.mark.skip(reason="Temporarily disable due to hangs in CI")
@pytest.mark.parametrize("profile_name", ["test-s3-iad-with-replica"])
def test_s3_on_demand_replica_fetch_with_cache_using_open(profile_name):
    profile = profile_name
    common.test_on_demand_replica_fetch_with_cache_using_open(profile)


@pytest.mark.skip(reason="Temporarily disable due to hangs in CI")
@pytest.mark.parametrize("profile_name", ["test-s3-iad-with-replica"])
def test_s3_on_demand_replica_fetch_with_cache_using_read(profile_name):
    profile = profile_name
    common.test_on_demand_replica_fetch_with_cache_using_read(profile)
