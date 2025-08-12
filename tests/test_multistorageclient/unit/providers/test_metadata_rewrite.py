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

import fnmatch
import tempfile
import uuid
from collections.abc import Iterator
from typing import Optional

import pytest

import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.types import MetadataProvider, ObjectMetadata


class UuidMetadataProvider(MetadataProvider):
    def __init__(self):
        # Remap the paths to random uuid filenames
        self._path_to_uuid: dict[str, str] = {}
        self._uuid_to_info: dict[str, ObjectMetadata] = {}
        self._pending_adds: dict[str, ObjectMetadata] = {}
        self._pending_deletes: set[str] = set()

    def list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        attribute_filter_expression: Optional[str] = None,
        show_attributes: bool = False,
    ) -> Iterator[ObjectMetadata]:
        assert not include_directories
        sorted_paths = sorted(self._path_to_uuid.keys())
        for path_key in sorted_paths:
            if start_after is not None and path_key < start_after:
                continue
            if end_at is not None and path_key > end_at:
                return

            u = self._path_to_uuid[path_key]
            if path_key.startswith(path):
                yield self._uuid_to_info[u]

    def get_object_metadata(self, path: str, include_pending: bool = False) -> ObjectMetadata:
        assert not include_pending, "Not supported in tests"
        u = self._path_to_uuid.get(path)
        if u is None:
            raise FileNotFoundError(f"Object {path} does not exist.")
        return self._uuid_to_info[u]

    def glob(self, pattern: str, attribute_filter_expression: Optional[str] = None) -> list[str]:
        return [path for path in self._path_to_uuid.keys() if fnmatch.fnmatch(path, pattern)]

    def realpath(self, path: str) -> tuple[str, bool]:
        u = self._path_to_uuid.get(path)
        if u is None:
            return str(uuid.uuid4()), False
        return u, True

    def add_file(self, path: str, metadata: ObjectMetadata) -> None:
        # Keep a dictionary of pending adds
        if path in self._pending_adds:
            raise ValueError(f"Object {path} already exists")
        self._pending_adds[path] = metadata

    def remove_file(self, path: str) -> None:
        if path not in self._path_to_uuid:
            raise ValueError(f"Object {path} does not exist")
        self._pending_deletes.add(path)

    def commit_updates(self) -> None:
        # Move entries in pending_adds to path_to_uuid and remove entries in pending_deletes from path_to_uuid
        for path in self._pending_deletes:
            u = self._path_to_uuid.pop(path)
            del self._uuid_to_info[u]
        for vpath, metadata in self._pending_adds.items():
            u = metadata.key
            metadata.key = vpath
            self._path_to_uuid[vpath] = u
            self._uuid_to_info[u] = metadata
        self._pending_adds.clear()
        self._pending_deletes.clear()

    def is_writable(self) -> bool:
        return True


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryPOSIXDirectory], [tempdatastore.TemporaryAWSS3Bucket]],
)
def test_uuid_metadata_provider(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        data_with_uuid_profile = "uuid"

        data_profile_config_dict = temp_data_store.profile_config_dict()

        storage_client_config_dict = {
            "profiles": {
                data_with_uuid_profile: data_profile_config_dict,
            }
        }

        # Create the storage clients
        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(config_dict=storage_client_config_dict, profile=data_with_uuid_profile)
        )
        storage_client._metadata_provider = UuidMetadataProvider()

        # Dictionary of filepaths to content
        content_dict = {
            "file1.txt": b"hello world",
            "dir1/file2.txt": b"hello world",
            "dir2/file3.txt": b"different content",
            "dir1/file4.txt": b"hello world",
            "dir2/file5.txt": b"different content",
            "dir2/file6.txt": b"different content",
            "dir2/dir3/file7.txt": b"different content",
            "dir2/dir3/file8.txt": b"different content",
            "dir2/dir3/file9.txt": b"different content",
        }
        for path, content in content_dict.items():
            storage_client.write(path, content)

        # Nothing visible until commit_metadata
        assert len(list(storage_client.list(prefix=""))) == 0

        with pytest.raises(FileNotFoundError):
            _ = storage_client.info("file1.txt")
        with pytest.raises(FileNotFoundError):
            _ = storage_client.read("file1.txt")

        storage_client.commit_metadata()

        assert set([f.key for f in storage_client.list(prefix="")]) == set(content_dict.keys())

        # Verify content via info, read, download_file, is_file
        for path, content in content_dict.items():
            metadata = storage_client.info(path)
            assert metadata.key == path
            assert metadata.content_length == len(content)
            assert metadata.last_modified is not None
            assert metadata.type == "file"

            assert storage_client.read(path) == content

            with tempfile.NamedTemporaryFile() as temp_file:
                storage_client.download_file(path, temp_file.name)
                with open(temp_file.name, "rb") as f:
                    assert f.read() == content

            assert storage_client.is_file(path)

            with storage_client.open(path, "rb") as f:
                assert f.read() == content

        assert storage_client.is_empty("not_a_directory")
        assert not storage_client.is_empty("dir1/")

        # Test glob with *.txt returns all files with .txt extension
        assert sorted(storage_client.glob("*.txt")) == sorted(
            [path for path in content_dict.keys() if path.endswith(".txt")]
        )

        # Test copy and upload_file APIs.
        storage_client.copy("file1.txt", "file1_copy.txt")
        assert not storage_client.is_file("file1_copy.txt")

        upload_filename = "uploaded_file.txt"
        upload_content = "This is a fixed content file."
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as temp_file:
            temp_file.write(upload_content)
            temp_file.flush()
            storage_client.upload_file(upload_filename, temp_file.name)

        # Not visible until commit_metadata
        with pytest.raises(FileNotFoundError):
            _ = storage_client.info("file1_copy.txt")
        with pytest.raises(FileNotFoundError):
            _ = storage_client.read("file1_copy.txt")

        storage_client.commit_metadata()

        assert storage_client.is_file("file1_copy.txt")
        assert storage_client.is_file(upload_filename)

        content_dict["file1_copy.txt"] = content_dict["file1.txt"]
        content_dict[upload_filename] = upload_content.encode("utf-8")

        for path, content in content_dict.items():
            metadata = storage_client.info(path)
            assert metadata.key == path
            assert metadata.content_length == len(content)

            assert storage_client.read(path) == content

        # Test delete API
        storage_client.delete("file1_copy.txt")
        storage_client.commit_metadata()
        del content_dict["file1_copy.txt"]

        assert not storage_client.is_file("file1_copy.txt")
        with pytest.raises(FileNotFoundError):
            _ = storage_client.info("file1_copy.txt")
        with pytest.raises(FileNotFoundError):
            _ = storage_client.read("file1_copy.txt")

        # call commit_metadata again, should be a no-op
        storage_client.commit_metadata()
        assert set([f.key for f in storage_client.list(prefix="")]) == set(content_dict.keys())

        # Test delete API with recursive=True, which auto-commits.
        storage_client.delete(path="", recursive=True)
        # Assert that all files are deleted
        assert len(list(storage_client.list(prefix=""))) == 0
