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

from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.instrumentation.auth import AzureAccessTokenProvider


@pytest.fixture
def mock_azure_provider():
    """Fixture to create a mocked AzureAccessTokenProvider."""
    with patch("multistorageclient.instrumentation.auth.AzureAccessTokenProvider.__init__") as mock_init:
        mock_init.return_value = None
        provider = AzureAccessTokenProvider({})
        provider.auth_options = {}
        provider.msal_client = MagicMock()
        provider.azure_scopes = ["scope1", "scope2"]
        yield provider


def test_azure_get_token_success(mock_azure_provider):
    mock_azure_provider.msal_client.acquire_token_for_client.return_value = {
        "access_token": "new_token",
        "expires_in": 3600,
    }

    assert mock_azure_provider.get_token() == "new_token"
    mock_azure_provider.msal_client.acquire_token_for_client.assert_called_once_with(scopes=["scope1", "scope2"])


def test_azure_get_token_no_token(mock_azure_provider):
    mock_azure_provider.msal_client.acquire_token_for_client.return_value = {
        "error": "some_error",
        "error_description": "some_description",
    }
    assert mock_azure_provider.get_token() is None


def test_azure_get_token_exception(mock_azure_provider):
    mock_azure_provider.msal_client.acquire_token_for_client.side_effect = Exception("Test Exception")
    assert mock_azure_provider.get_token() is None
