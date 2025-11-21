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


@pytest.mark.parametrize("profile_name", ["test-azure-uswest"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_azure_shortcuts(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_shortcuts(profile)


@pytest.mark.parametrize("profile_name", ["test-azure-uswest"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_azure_storage_client(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_storage_client(profile)


@pytest.mark.parametrize("profile_name", ["test-azure-uswest"])
def test_azure_conditional_put(profile_name):
    """Test conditional PUT operations in Azure using if-match and if-none-match conditions."""
    profile = profile_name
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")

    # Azure uses PreconditionFailedError for if_match failures (412)
    # Azure uses PreconditionFailedError for if_none_match with specific etag (412)
    # Azure does not support if_none_match="*" and raises RuntimeError
    common.test_conditional_put(
        storage_client=client,
        if_none_match_error_type=RuntimeError,
        if_match_error_type=PreconditionFailedError,
        if_none_match_specific_error_type=PreconditionFailedError,
        supports_if_none_match_star=False,
    )


@pytest.mark.parametrize("profile_name", ["test-azure-uswest"])
def test_azure_open_with_source_version_check(profile_name):
    profile = profile_name
    common.test_open_with_source_version_check(profile)


@pytest.mark.parametrize("profile_name", ["test-azure-uswest"])
def test_azure_attributes(profile_name):
    """Test Azure attributes functionality - storing custom metadata with msc_ prefix."""
    profile = profile_name
    common.test_attributes(profile)


@pytest.mark.parametrize("profile_name", ["test-azure-uswest"])
def test_azure_list_exact_file_no_prefix_match(profile_name):
    """Test that listing exact file path doesn't match files with same prefix."""
    profile = profile_name
    common.test_list_exact_file_no_prefix_match(profile)
