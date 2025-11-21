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

from collections.abc import Iterator
from datetime import datetime
from typing import IO, Optional, Union

import pytest

from multistorageclient.providers.base import BaseStorageProvider
from multistorageclient.types import ObjectMetadata, Range


class OutdatedStorageProvider(BaseStorageProvider):
    """
    Mock storage provider that simulates an old implementation using 'prefix'
    instead of 'path' as the second parameter in _list_objects method.
    This tests backward compatibility with providers that haven't been updated.
    """

    def _put_object(
        self,
        path: str,
        body: bytes,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
    ) -> int:
        return len(body)

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        return b""

    def _copy_object(self, src_path: str, dest_path: str) -> int:
        return 0

    def _delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        pass

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        if not path.endswith("txt"):
            return ObjectMetadata(key=path, content_length=0, type="directory", last_modified=datetime.now())
        else:
            return ObjectMetadata(key=path, content_length=0, type="file", last_modified=datetime.now())

    def _list_objects(
        self,
        prefix: str,  # Note: using 'prefix' instead of 'path' (old interface)
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
        """
        Simulates an old storage provider implementation that uses 'prefix'
        instead of 'path' as the parameter name. This should still work due
        to Python's duck typing - the parameter is passed positionally.
        """
        # Mock implementation that returns some test objects
        mock_objects = [
            ObjectMetadata(key=f"{prefix}/file1.txt", content_length=100, type="file", last_modified=datetime.now()),
            ObjectMetadata(key=f"{prefix}/file2.txt", content_length=200, type="file", last_modified=datetime.now()),
        ]

        for obj in mock_objects:
            # Apply filtering if specified
            if start_after and obj.key <= start_after:
                continue
            if end_at and obj.key > end_at:
                continue
            yield obj

    def _upload_file(self, remote_path: str, f: Union[str, IO], attributes: Optional[dict[str, str]] = None) -> int:
        return 0

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> int:
        return 0


# Integration test using client API
def test_outdated_provider_through_client_api():
    """
    Test that outdated providers work through the client API.
    """
    from multistorageclient.client import StorageClient
    from multistorageclient.config import StorageClientConfig

    # Create a storage client with the outdated provider
    provider = OutdatedStorageProvider(base_path="test-bucket", provider_name="outdated")
    config = StorageClientConfig(profile="test", storage_provider=provider)
    client = StorageClient(config=config)

    # Test case 1: Application that still uses positional arguments (works)
    result = list(client.list("data/files"))
    assert len(result) == 2

    # Test case 2: Application that still uses the prefix parameter (works)
    result = list(client.list(prefix="data/files"))
    assert len(result) == 2

    # Test case 3: Application that uses the new path parameter (works)
    result = list(client.list(path="data/files"))
    assert len(result) == 2


def test_outdated_provider_list_objects_through_provider_api():
    """Test that an outdated storage provider works correctly through the provider API."""
    provider = OutdatedStorageProvider(base_path="bucket", provider_name="outdated")

    # Test case 1: Use positional arguments (works)
    result = list(provider.list_objects("test/path"))
    assert len(result) == 2

    # Test case 2: Use of the "prefix" parameter would fail because the BaseStorageProvider.list_objects expects "path", not "prefix"
    # We don't expect this API to be used directly by the application, that's why we don't support backward compatibility for this case.
    with pytest.raises(TypeError):
        list(provider.list_objects(prefix="test/path"))  # pyright: ignore [reportCallIssue]

    # Test case 3: Use of the new "path" parameter (works)
    result = list(provider.list_objects(path="test/path"))
    assert len(result) == 2
