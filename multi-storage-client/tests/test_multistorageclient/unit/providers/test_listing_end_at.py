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

"""end_at handling when a listing page mixes directory prefixes and objects (S3 and OCI)."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.providers.oci import OracleStorageProvider
from multistorageclient.providers.s3 import S3StorageProvider, StaticS3CredentialsProvider

LAST_MODIFIED = datetime(2025, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def s3_provider():
    with patch.object(S3StorageProvider, "_create_s3_client", return_value=MagicMock()):
        yield S3StorageProvider(
            endpoint_url="http://localhost:9000",
            base_path="bucket",
            credentials_provider=StaticS3CredentialsProvider(access_key="a", secret_key="b"),
        )


@pytest.fixture
def oci_provider():
    with patch.object(OracleStorageProvider, "_create_oci_client", return_value=MagicMock()):
        yield OracleStorageProvider(namespace="test-ns", base_path="bucket")


def test_s3_end_at_keeps_objects_on_page_with_out_of_range_prefix(s3_provider):
    """A CommonPrefix beyond end_at must not drop the in-range objects returned on the same page."""
    # Every object on the first page is within range, so only the out-of-range prefix can stop the
    # listing; the second page must never be requested.
    pages_fetched = 0

    def pages():
        nonlocal pages_fetched
        pages_fetched += 1
        yield {
            "CommonPrefixes": [{"Prefix": "z/"}],
            "Contents": [
                {"Key": "a", "Size": 1, "LastModified": LAST_MODIFIED, "ETag": '"1"'},
                {"Key": "b", "Size": 1, "LastModified": LAST_MODIFIED, "ETag": '"2"'},
            ],
        }
        pages_fetched += 1
        yield {"Contents": [{"Key": "d", "Size": 1, "LastModified": LAST_MODIFIED, "ETag": '"4"'}]}

    s3_provider._s3_client.get_paginator.return_value.paginate.return_value = pages()

    keys = [obj.key for obj in s3_provider.list_objects("", end_at="b", include_directories=True)]

    assert keys == ["a", "b"]
    assert pages_fetched == 1


def test_oci_end_at_keeps_objects_on_page_with_out_of_range_prefix(oci_provider):
    """A prefix beyond end_at must not drop the in-range objects returned in the same response."""

    def obj(name: str):
        return SimpleNamespace(name=name, size=1, time_modified=LAST_MODIFIED, etag=name)

    # Every object in the first response is within range; the second response must never be requested.
    oci_provider._oci_client.list_objects.side_effect = [
        SimpleNamespace(data=SimpleNamespace(prefixes=["z/"], objects=[obj("a"), obj("b")], next_start_with="d")),
        SimpleNamespace(data=SimpleNamespace(prefixes=[], objects=[obj("d")], next_start_with=None)),
    ]

    keys = [obj.key for obj in oci_provider.list_objects("", end_at="b", include_directories=True)]

    assert keys == ["a", "b"]
    oci_provider._oci_client.list_objects.assert_called_once()
