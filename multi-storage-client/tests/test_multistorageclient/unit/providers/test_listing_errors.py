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
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import HttpResponseError
from botocore.exceptions import ClientError
from google.api_core.exceptions import NotFound
from oci.exceptions import ServiceError
from requests import HTTPError, Response

from multistorageclient.providers.ais import AIStoreStorageProvider
from multistorageclient.providers.azure import AzureBlobStorageProvider
from multistorageclient.providers.gcs import GoogleStorageProvider
from multistorageclient.providers.oci import OracleStorageProvider
from multistorageclient.providers.s3 import S3StorageProvider
from multistorageclient.types import RetryableError
from multistorageclient_rust import RustRetryableError

LAST_MODIFIED = datetime(2025, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize("provider_type", ["s3", "gcs", "oci", "azure", "ais"])
@pytest.mark.parametrize("failure", ["first_page", "later_page", None])
def test_cloud_listing_translates_errors_during_iteration(provider_type, failure):
    def items(item, error):
        if failure != "first_page":
            yield item
        if failure is not None:
            raise error

    if provider_type == "s3":
        provider = S3StorageProvider.__new__(S3StorageProvider)
        provider._s3_client = MagicMock()
        api = provider._s3_client.get_paginator.return_value.paginate
        error = ClientError(
            {"Error": {"Code": "NoSuchBucket"}, "ResponseMetadata": {"HTTPStatusCode": 404}}, "ListObjectsV2"
        )
        api.return_value = items(
            {"Contents": [{"Key": "a", "Size": 1, "LastModified": LAST_MODIFIED, "ETag": '"1"'}]}, error
        )
    elif provider_type == "gcs":
        provider = GoogleStorageProvider.__new__(GoogleStorageProvider)
        provider._refresh_gcs_client_if_needed = MagicMock()
        provider._gcs_client = MagicMock()
        api = provider._gcs_client.bucket.return_value.list_blobs
        error = NotFound("missing bucket")
        api.return_value = items(
            SimpleNamespace(name="a", size=1, updated=LAST_MODIFIED, content_type=None, etag="1", metadata=None), error
        )
    elif provider_type == "oci":
        provider = OracleStorageProvider.__new__(OracleStorageProvider)
        provider._refresh_oci_client_if_needed = MagicMock()
        provider._namespace = "namespace"
        provider._oci_client = MagicMock()
        api = provider._oci_client.list_objects
        error = ServiceError(status=404, code="NotFound", headers={}, message="missing bucket")
        page = SimpleNamespace(
            data=SimpleNamespace(
                prefixes=[],
                objects=[SimpleNamespace(name="a", size=1, time_modified=LAST_MODIFIED, etag="1")],
                next_start_with="b" if failure == "later_page" else None,
            )
        )
        api.side_effect = [error] if failure == "first_page" else [page, error]
    elif provider_type == "azure":
        provider = AzureBlobStorageProvider.__new__(AzureBlobStorageProvider)
        provider._refresh_blob_service_client_if_needed = MagicMock()
        provider._blob_service_client = MagicMock()
        api = provider._blob_service_client.get_container_client.return_value.list_blobs
        error = HttpResponseError(message="missing container")
        error.status_code = 404
        api.return_value = items(
            SimpleNamespace(
                name="a",
                size=1,
                last_modified=LAST_MODIFIED,
                content_settings=SimpleNamespace(content_type=None),
                metadata=None,
                etag='"1"',
            ),
            error,
        )
    else:
        provider = AIStoreStorageProvider.__new__(AIStoreStorageProvider)
        provider.client = MagicMock()
        provider.provider = "ais"
        api = provider.client.bucket.return_value.list_objects_iter
        response = Response()
        response.status_code = 404
        error = HTTPError("missing bucket", response=response)
        entry = MagicMock()
        entry.object.name = "a"
        entry.generate_object_props.return_value = SimpleNamespace(size=1, access_time="", checksum_value="1")
        api.return_value = items(entry, error)

    listing = provider._list_objects("bucket/")
    api.assert_not_called()
    if failure != "first_page":
        assert next(listing).key.endswith("a")
        api.assert_called_once()
    if failure is None:
        assert list(listing) == []
    else:
        with pytest.raises(FileNotFoundError, match="bucket") as raised:
            next(listing)
        assert raised.value.__context__ is error


def test_rust_recursive_listing_translates_errors_during_iteration():
    provider = S3StorageProvider.__new__(S3StorageProvider)
    provider._rust_client = MagicMock()
    error = RustRetryableError("connection reset")
    with patch("multistorageclient.providers.s3.run_async_rust_client_method", side_effect=error) as run_rust:
        listing = provider._list_objects_recursive_rust("", "bucket/", "bucket", None, None, 1)
        run_rust.assert_not_called()
        with pytest.raises(RetryableError, match="bucket") as raised:
            next(listing)
    assert raised.value.__cause__ is error
