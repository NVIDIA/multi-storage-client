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
from unittest.mock import MagicMock

from multistorageclient.providers.base import BaseStorageProvider
from multistorageclient.types import ObjectMetadata, Range


class MockBaseStorageProvider(BaseStorageProvider):
    def _put_object(self, path: str, body: bytes) -> None:
        pass

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        return b""

    def _copy_object(self, src_path: str, dest_path: str) -> None:
        pass

    def _delete_object(self, path: str, etag: Optional[str] = None) -> None:
        pass

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        if not path.endswith("txt"):
            return ObjectMetadata(key=path, content_length=0, type="directory", last_modified=datetime.now())
        else:
            return ObjectMetadata(key=path, content_length=0, type="file", last_modified=datetime.now())

    def _list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
        return iter([])

    def _upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        pass

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        pass


def test_list_objects_with_base_path():
    mock_objects = [
        ObjectMetadata(key="prefix/dir/file1.txt", content_length=0, type="file", last_modified=datetime.now()),
        ObjectMetadata(key="prefix/dir/file2.txt", content_length=0, type="file", last_modified=datetime.now()),
        ObjectMetadata(key="prefix/dir", content_length=0, type="directory", last_modified=datetime.now()),
    ]
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider._list_objects = MagicMock(return_value=iter(mock_objects))
    response = list(provider.list_objects(path="prefix/dir"))
    assert len(response) == 3

    for m in response:
        assert m.key.startswith("prefix/dir")


def test_list_objects_with_prefix_in_base_path():
    mock_objects = [
        ObjectMetadata(key="bucket/prefix/dir/file1.txt", content_length=0, type="file", last_modified=datetime.now()),
        ObjectMetadata(key="bucket/prefix/dir/file2.txt", content_length=0, type="file", last_modified=datetime.now()),
        ObjectMetadata(key="bucket/prefix/dir", content_length=0, type="directory", last_modified=datetime.now()),
    ]
    provider = MockBaseStorageProvider(base_path="bucket/prefix", provider_name="mock")
    provider._list_objects = MagicMock(return_value=iter(mock_objects))
    response = list(provider.list_objects(path="dir/"))
    assert len(response) == 3

    for m in response:
        assert m.key.startswith("dir")
