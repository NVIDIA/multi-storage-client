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

import pytest

from multistorageclient.providers.azure import AzureBlobStorageProvider, StaticAzureCredentialsProvider


@pytest.mark.parametrize(
    ("endpoint_url", "connection", "expected"),
    [
        (
            "account.blob.core.windows.net/?sv=2024-11-04&sig=fake-test-value",
            None,
            "https://account.blob.core.windows.net/",
        ),
        (
            "https://account.blob.core.windows.net",
            "BlobEndpoint=https://other.blob.core.windows.net/;SharedAccessSignature=sv=2024-11-04&sig=fake-test-value",
            "https://other.blob.core.windows.net/",
        ),
    ],
)
def test_root_metadata_reports_blob_client_endpoint_without_sas_token(endpoint_url, connection, expected):
    """The blob client connects to a connection string's endpoint over endpoint_url, and a SAS token is a credential."""
    credentials_provider = StaticAzureCredentialsProvider(connection=connection) if connection else None
    provider = AzureBlobStorageProvider(
        endpoint_url=endpoint_url, base_path="container", credentials_provider=credentials_provider
    )

    assert provider.get_root_metadata()["options"]["endpoint_url"] == expected
