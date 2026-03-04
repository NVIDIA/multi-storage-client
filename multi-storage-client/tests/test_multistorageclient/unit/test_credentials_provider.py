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
from google.auth import identity_pool

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers.azure import (
    AZURE_CREDENTIAL_KEY,
    AzureBlobStorageProvider,
    DefaultAzureCredentialsProvider,
)
from multistorageclient.providers.gcs import GoogleIdentityPoolCredentialsProvider, GoogleStorageProvider


def test_default_azure_credentials_provider():
    config = StorageClientConfig.from_dict(
        {
            "profiles": {
                "test-azure": {
                    "storage_provider": {
                        "type": "azure",
                        "options": {
                            "endpoint_url": "https://myaccount.blob.core.windows.net",
                            "base_path": "container",
                        },
                    },
                    "credentials_provider": {
                        "type": "DefaultAzureCredentials",
                        "options": {
                            "authority": "https://login.microsoftonline.com/common",
                        },
                    },
                }
            }
        },
        profile="test-azure",
    )
    storage_client = StorageClient(config)
    assert isinstance(storage_client._credentials_provider, DefaultAzureCredentialsProvider)
    assert storage_client._credentials_provider.get_credentials().get_custom_field(AZURE_CREDENTIAL_KEY) is not None
    assert isinstance(storage_client._storage_provider, AzureBlobStorageProvider)


def test_default_azure_credentials_provider_with_invalid_options():
    with pytest.raises(TypeError, match="'tenant_id' is not supported in DefaultAzureCredential."):
        config = StorageClientConfig.from_dict(
            {
                "profiles": {
                    "test-azure": {
                        "storage_provider": {
                            "type": "azure",
                            "options": {
                                "endpoint_url": "https://myaccount.blob.core.windows.net",
                                "base_path": "container",
                            },
                        },
                        "credentials_provider": {
                            "type": "DefaultAzureCredentials",
                            "options": {"tenant_id": "invalid_value"},
                        },
                    }
                }
            },
            profile="test-azure",
        )
        StorageClient(config)


def test_google_identity_pool_credentials():
    config = StorageClientConfig.from_dict(
        {
            "profiles": {
                "test-gcs": {
                    "storage_provider": {
                        "type": "gcs",
                        "options": {
                            "project_id": "test-project-id",
                            "base_path": "test-base-path",
                        },
                    },
                    "credentials_provider": {
                        "type": "GoogleIdentityPoolCredentialsProvider",
                        "options": {"audience": "test-audience", "token_supplier": "test-token"},
                    },
                }
            }
        },
        profile="test-gcs",
    )
    storage_client = StorageClient(config)
    assert isinstance(storage_client._credentials_provider, GoogleIdentityPoolCredentialsProvider)
    assert storage_client._credentials_provider.get_credentials().get_custom_field("audience") == "test-audience"
    assert storage_client._credentials_provider.get_credentials().get_custom_field("token") == "test-token"
    assert isinstance(storage_client._storage_provider, GoogleStorageProvider)
    assert isinstance(storage_client._storage_provider._gcs_client._credentials, identity_pool.Credentials)
