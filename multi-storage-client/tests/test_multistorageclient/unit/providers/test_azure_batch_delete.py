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

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.providers.azure import AzureBlobStorageProvider


@pytest.fixture
def azure_provider():
    with patch.object(AzureBlobStorageProvider, "_create_blob_service_client", return_value=MagicMock()):
        yield AzureBlobStorageProvider(endpoint_url="https://account.blob.core.windows.net", base_path="container")


def test_batch_delete_error_includes_response_body(azure_provider):
    """Batch sub-responses expose text() as a method; the error must carry the body, not a bound-method repr."""
    container_client = azure_provider._blob_service_client.get_container_client.return_value
    # Use a plain object rather than a Mock so that a bare attribute access cannot mask the bug.
    container_client.delete_blobs.return_value = [
        SimpleNamespace(status_code=202, text=lambda: ""),
        SimpleNamespace(status_code=500, text=lambda: "internal server error body"),
    ]

    with pytest.raises(RuntimeError, match="status_code: 500, response: internal server error body"):
        azure_provider._delete_objects(["container/a.txt", "container/b.txt"])

    container_client.delete_blobs.assert_called_once_with("a.txt", "b.txt", raise_on_any_failure=False)


def test_batch_delete_treats_missing_blobs_as_success(azure_provider):
    container_client = azure_provider._blob_service_client.get_container_client.return_value
    container_client.delete_blobs.return_value = [
        SimpleNamespace(status_code=202, text=lambda: ""),
        SimpleNamespace(status_code=404, text=lambda: "not found"),
    ]

    azure_provider._delete_objects(["container/a.txt", "container/missing.txt"])
