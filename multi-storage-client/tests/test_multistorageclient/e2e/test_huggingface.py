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


import os
import re
from unittest.mock import patch

import pytest


def enable_xet():
    """
    Enable HuggingFace XET mode and disable hf_transfer.

    Sets environment variables to:
    - Enable XET (by setting HF_HUB_DISABLE_XET=0 or removing it)
    - Disable hf_transfer (by setting HF_HUB_ENABLE_HF_TRANSFER=0)
    """

    os.environ.pop("HF_HUB_DISABLE_XET", None)
    os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "0"


def enable_transfer():
    """
    Enable HuggingFace hf_transfer mode and disable XET.

    Sets environment variables to:
    - Disable XET (by setting HF_HUB_DISABLE_XET=1)
    - Enable hf_transfer (by setting HF_HUB_ENABLE_HF_TRANSFER=1)
    """

    os.environ["HF_HUB_DISABLE_XET"] = "1"
    os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"


@pytest.mark.parametrize("profile_name", ["test-hf-private-dataset"])
def test_hf_shortcuts_xet(profile_name):
    """Test HuggingFace using common shortcuts test pattern."""
    # enable_xet()
    # common.test_shortcuts(profile_name)


@pytest.mark.parametrize("profile_name", ["test-hf-private-dataset"])
def test_hf_storage_client_xet(profile_name):
    """Test HuggingFace using common storage client test pattern."""
    # Commented out until HF rate limiting issue is resolved.
    # enable_xet()
    # common.test_storage_client(profile_name)


@pytest.mark.parametrize("profile_name", ["test-hf-private-dataset"])
def test_hf_shortcuts_transfer(profile_name):
    """Test HuggingFace using common shortcuts test pattern."""
    # Commented out until HF rate limiting issue is resolved.
    # enable_transfer()
    # common.test_shortcuts(profile_name)


@pytest.mark.parametrize("profile_name", ["test-hf-private-dataset"])
def test_hf_transfer_failure_when_unavailable(profile_name):
    """Test that HuggingFaceStorageProvider throws ValueError when hf_transfer is enabled but not available."""

    enable_transfer()

    # Save the original find_spec before mocking
    import importlib.util

    original_find_spec = importlib.util.find_spec

    with patch("importlib.util.find_spec") as mock_find_spec:

        def mock_find_spec_func(name):
            if name == "hf_transfer":
                return None
            # Use the saved original function
            return original_find_spec(name)

        mock_find_spec.side_effect = mock_find_spec_func

        from multistorageclient.providers.huggingface import (
            HF_TRANSFER_UNAVAILABLE_ERROR_MESSAGE,
            HuggingFaceStorageProvider,
        )

        with pytest.raises(ValueError, match=re.escape(HF_TRANSFER_UNAVAILABLE_ERROR_MESSAGE)):
            HuggingFaceStorageProvider(repository_id="test/repo", repo_type="dataset")


def test_parse_rate_limit_headers_basic():
    """Test basic rate limit header parsing."""
    from unittest.mock import Mock

    from multistorageclient.providers.huggingface import HuggingFaceStorageProvider

    provider = HuggingFaceStorageProvider(repository_id="test/repo", repo_type="model")

    # Test with full rate limit info
    mock_response = Mock()
    mock_response.headers = {
        "RateLimit": '"api";r=50;t=142',
        "RateLimit-Policy": '"fixed window";"api";q=10000;w=300',
    }

    result = provider._parse_rate_limit_headers(mock_response)

    assert "Requests remaining in current window: 50" in result
    assert "Rate limit resets in: 142 seconds" in result
    assert "10000 requests per 5-minute window" in result


def test_parse_rate_limit_headers_zero_remaining():
    """Test rate limit header parsing when quota is exhausted."""
    from unittest.mock import Mock

    from multistorageclient.providers.huggingface import HuggingFaceStorageProvider

    provider = HuggingFaceStorageProvider(repository_id="test/repo", repo_type="model")

    mock_response = Mock()
    mock_response.headers = {
        "RateLimit": '"api";r=0;t=299',
        "RateLimit-Policy": '"fixed window";"api";q=2500;w=300',
    }

    result = provider._parse_rate_limit_headers(mock_response)

    assert "Requests remaining in current window: 0" in result
    assert "Rate limit resets in: 299 seconds" in result


def test_parse_rate_limit_headers_none_response():
    """Test rate limit header parsing with None response."""
    from multistorageclient.providers.huggingface import HuggingFaceStorageProvider

    provider = HuggingFaceStorageProvider(repository_id="test/repo", repo_type="model")

    result = provider._parse_rate_limit_headers(None)

    assert result == ""
