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

import builtins
import io
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.providers.azure import AzureBlobStorageProvider
from multistorageclient.types import ObjectMetadata

TEXT = "h\u00e9llo w\u00f6rld \u2014 \u65e5\u672c\u8a9e\n"


@pytest.fixture
def azure_provider():
    with patch.object(AzureBlobStorageProvider, "_create_blob_service_client", return_value=MagicMock()):
        yield AzureBlobStorageProvider(endpoint_url="https://account.blob.core.windows.net", base_path="container")


def test_large_stringio_download_decodes_as_utf8(azure_provider):
    """The chunked StringIO download path must decode the staging file as UTF-8, like the small-object path."""
    payload = TEXT.encode("utf-8")
    azure_provider._multipart_threshold = 0  # force the chunked download path
    stream = azure_provider._blob_service_client.get_blob_client.return_value.download_blob.return_value
    stream.readinto.side_effect = lambda fp: fp.write(payload)
    metadata = ObjectMetadata(
        key="container/text.txt", content_length=len(payload), last_modified=datetime(2025, 1, 1, tzinfo=timezone.utc)
    )

    text_encodings: list[str | None] = []
    real_open = builtins.open

    def recording_open(file, mode="r", *args, **kwargs):
        if "b" not in mode and "r" in mode:
            text_encodings.append(kwargs.get("encoding"))
        return real_open(file, mode, *args, **kwargs)

    f = io.StringIO()
    with patch("multistorageclient.providers.azure.open", recording_open, create=True):
        assert azure_provider._download_file("container/text.txt", f, metadata) == len(payload)

    assert f.getvalue() == TEXT
    assert text_encodings == ["utf-8"]
