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

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from aistore.sdk.errors import AISError
from google.api_core.exceptions import from_http_status

from multistorageclient.providers.ais import AIStoreStorageProvider
from multistorageclient.providers.gcs import GoogleStorageProvider
from multistorageclient.types import ObjectMetadata, Range, SuffixRange


def _metadata(key: str, content_length: int, symlink_target: str | None = None) -> ObjectMetadata:
    return ObjectMetadata(
        key=key,
        content_length=content_length,
        last_modified=datetime(2025, 1, 1, tzinfo=UTC),
        symlink_target=symlink_target,
    )


def _gcs_provider() -> tuple[GoogleStorageProvider, MagicMock]:
    gcs_client = MagicMock()
    with patch.object(GoogleStorageProvider, "_create_gcs_client", return_value=gcs_client):
        provider = GoogleStorageProvider(project_id="project", base_path="bucket")
    blob = gcs_client.bucket.return_value.blob.return_value
    blob.download_as_bytes.side_effect = from_http_status(416, "Requested range not satisfiable")
    return provider, blob


def _ais_provider() -> tuple[AIStoreStorageProvider, MagicMock]:
    provider = AIStoreStorageProvider(endpoint="http://localhost:51080", base_path="bucket")
    client = MagicMock()
    provider.client = client
    reader = client.bucket.return_value.object.return_value.get_reader
    reader.side_effect = AISError(416, "Requested range not satisfiable", "", MagicMock())
    return provider, reader


@pytest.mark.parametrize("create_provider", [_gcs_provider, _ais_provider])
def test_suffix_range_unsatisfiable_falls_back_to_metadata(create_provider) -> None:
    """HTTP 416 (empty objects, e.g. MSC symlinks) falls back to the object metadata, following symlinks."""
    provider, _ = create_provider()
    objects = {"bucket/empty.bin": b"", "bucket/link.bin": b"", "bucket/data.bin": b"0123456789"}
    metadata = {
        "bucket/empty.bin": _metadata("bucket/empty.bin", 0),
        "bucket/link.bin": _metadata("bucket/link.bin", 0, symlink_target="data.bin"),
        "bucket/data.bin": _metadata("bucket/data.bin", 10),
    }

    def get_object(path: str, byte_range: Range | SuffixRange | None = None) -> bytes:
        if isinstance(byte_range, SuffixRange):
            return original_get_object(path, byte_range)
        content = objects[path]
        return content[byte_range.offset : byte_range.offset + byte_range.size] if byte_range else content

    original_get_object = provider._get_object
    with (
        patch.object(provider, "_get_object", side_effect=get_object),
        patch.object(provider, "_get_object_metadata", side_effect=lambda path, strict=True: metadata[path]),
    ):
        assert provider.get_object("empty.bin", byte_range=SuffixRange(size=4)) == b""
        assert provider.get_object("link.bin", byte_range=SuffixRange(size=4)) == b"6789"


@pytest.mark.parametrize("create_provider", [_gcs_provider, _ais_provider])
def test_suffix_range_fallback_errors_are_translated_once(create_provider) -> None:
    """A missing object in the metadata fallback raises FileNotFoundError, not a RuntimeError."""
    provider, _ = create_provider()
    with (
        patch.object(provider, "_get_object_metadata", side_effect=FileNotFoundError("missing")),
        pytest.raises(FileNotFoundError),
    ):
        provider.get_object("missing.bin", byte_range=SuffixRange(size=4))
