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
from datetime import datetime, timezone
from typing import Optional

from multistorageclient.types import (
    Credentials,
    CredentialsProvider,
    MetadataProvider,
    ObjectMetadata,
    ProviderBundle,
    Replica,
    StorageProviderConfig,
)
from multistorageclient.utils import glob as glob_util


class TestCredentialsProvider(CredentialsProvider):
    def __init__(self):
        pass

    def get_credentials(self) -> Credentials:
        return Credentials(access_key="*****", secret_key="*****", token="ooooo", expiration="")

    def refresh_credentials(self) -> None:
        pass


class TestScopedCredentialsProvider(CredentialsProvider):
    def __init__(self, base_path: Optional[str] = None, endpoint_url: Optional[str] = None, expiry: int = 1000):
        self._base_path = base_path
        self._endpoint_url = endpoint_url
        self._expiry = expiry

    def get_credentials(self) -> Credentials:
        return Credentials(access_key="*****", secret_key="*****", token="ooooo", expiration="")

    def refresh_credentials(self) -> None:
        pass


class TestMetadataProvider(MetadataProvider):
    def __init__(self):
        self._files = {
            key: ObjectMetadata(key=key, content_length=19283, last_modified=datetime.now(tz=timezone.utc))
            for key in ["webdataset-00001.tar", "webdataset-00002.tar"]
        }
        self._pending = {}
        self._pending_removes = set()

    def list_objects(
        self,
        prefix: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        attribute_filter_expression: Optional[str] = None,
    ) -> Iterator[ObjectMetadata]:
        assert not include_directories, "Directories are not supported in the test metadata provider"
        return iter(
            [
                file
                for file in self._files.values()
                if (start_after is None or start_after < file.key) and (end_at is None or file.key <= file.key)
            ]
        )

    def get_object_metadata(self, path: str, include_pending: bool = False) -> ObjectMetadata:
        assert not include_pending, "Not supported in tests"
        return ObjectMetadata(key=path, content_length=19283, last_modified=datetime.now(tz=timezone.utc))

    def glob(self, pattern: str, attribute_filter_expression: Optional[str] = None) -> list[str]:
        return glob_util(list(self._files.keys()), pattern)

    def realpath(self, path: str) -> tuple[str, bool]:
        exists = path in self._files
        return path, exists

    def add_file(self, path: str, metadata: ObjectMetadata) -> None:
        assert path not in self._files
        self._pending[path] = ObjectMetadata(
            key=path, content_length=19283, last_modified=datetime.now(tz=timezone.utc)
        )

    def remove_file(self, path: str) -> None:
        assert path in self._files
        self._pending_removes.add(path)

    def commit_updates(self) -> None:
        self._files |= self._pending
        for path in self._pending_removes:
            del self._files[path]
        self._pending.clear()
        self._pending_removes.clear()

    def is_writable(self) -> bool:
        # Writable by default to support generation and updates
        return True


class TestProviderBundle(ProviderBundle):
    @property
    def storage_provider_config(self) -> StorageProviderConfig:
        return StorageProviderConfig(type="file", options={"base_path": "/"})

    @property
    def credentials_provider(self) -> Optional[CredentialsProvider]:
        return TestCredentialsProvider()

    @property
    def metadata_provider(self) -> Optional[MetadataProvider]:
        """
        Returns the metadata provider responsible for retrieving metadata about objects in the storage service.
        """
        return TestMetadataProvider()

    @property
    def replicas(self) -> list[Replica]:
        """
        Returns the replicas configuration for this provider bundle, if any.
        """
        return []
