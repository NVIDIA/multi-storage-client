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

    with patch("importlib.util.find_spec") as mock_find_spec:

        def mock_find_spec_func(name):
            if name == "hf_transfer":
                return None
            import importlib.util

            return importlib.util.find_spec.__wrapped__(name)

        mock_find_spec.side_effect = mock_find_spec_func

        from multistorageclient.providers.huggingface import (
            HF_TRANSFER_UNAVAILABLE_ERROR_MESSAGE,
            HuggingFaceStorageProvider,
        )

        with pytest.raises(ValueError, match=re.escape(HF_TRANSFER_UNAVAILABLE_ERROR_MESSAGE)):
            HuggingFaceStorageProvider(repository_id="test/repo", repo_type="dataset")
