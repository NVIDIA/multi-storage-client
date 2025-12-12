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
from multistorageclient.types import MetadataProvider, ObjectMetadata, ResolvedPath, ResolvedPathState


class UuidMetadataProvider(MetadataProvider):
    def __init__(self):
        # Remap the paths to random uuid filenames
        self._path_to_uuid: dict[str, str] = {}
        self._uuid_to_info: dict[str, ObjectMetadata] = {}
        self._pending_adds: dict[str, ObjectMetadata] = {}
        self._pending_deletes: set[str] = set()
        self._deleted_files: set[str] = set()  # Track soft-deleted files
        self._allow_overwrites: bool = False  # Control overwrite behavior

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
            # Skip deleted files
            if path_key in self._deleted_files:
                continue

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
        if u is None or path in self._deleted_files:
            raise FileNotFoundError(f"Object {path} does not exist.")
        return self._uuid_to_info[u]

    def glob(self, pattern: str, attribute_filter_expression: Optional[str] = None) -> list[str]:
        return [
            path
            for path in self._path_to_uuid.keys()
            if fnmatch.fnmatch(path, pattern) and path not in self._deleted_files
        ]

    def realpath(self, logical_path: str) -> ResolvedPath:
        """Resolves a logical path to its UUID-based physical path."""
        u = self._path_to_uuid.get(logical_path)
        if u:
            if logical_path in self._deleted_files:
                # File exists in metadata but is soft-deleted
                return ResolvedPath(physical_path=u, state=ResolvedPathState.DELETED, profile=None)
            else:
                # File exists and is not deleted
                return ResolvedPath(physical_path=u, state=ResolvedPathState.EXISTS, profile=None)
        # File never existed
        return ResolvedPath(physical_path=logical_path, state=ResolvedPathState.UNTRACKED, profile=None)

    def generate_physical_path(self, logical_path: str, for_overwrite: bool = False) -> ResolvedPath:
        """Generates a new UUID-based physical path."""
        return ResolvedPath(physical_path=str(uuid.uuid4()), state=ResolvedPathState.UNTRACKED, profile=None)

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
        # Handle pending deletes: mark as soft-deleted instead of removing
        for path in self._pending_deletes:
            if path in self._path_to_uuid:
                self._deleted_files.add(path)

        # Handle pending adds: add new files or restore deleted files
        for vpath, metadata in self._pending_adds.items():
            u = metadata.key
            metadata.key = vpath
            self._path_to_uuid[vpath] = u
            self._uuid_to_info[u] = metadata
            # If this was a deleted file, remove it from deleted set (restore)
            self._deleted_files.discard(vpath)

        self._pending_adds.clear()
        self._pending_deletes.clear()

    def is_writable(self) -> bool:
        return True

    def allow_overwrites(self) -> bool:
        return self._allow_overwrites


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

        # Test ResolvedPath enum functionality
        # Test EXISTS state
        resolved = ResolvedPath(physical_path="/test/path", state=ResolvedPathState.EXISTS)
        assert resolved.state == ResolvedPathState.EXISTS

        # Test DELETED state
        resolved = ResolvedPath(physical_path="/test/path", state=ResolvedPathState.DELETED)
        assert resolved.state == ResolvedPathState.DELETED

        # Test UNTRACKED state
        resolved = ResolvedPath(physical_path="/test/path", state=ResolvedPathState.UNTRACKED)
        assert resolved.state == ResolvedPathState.UNTRACKED

        # Test with profile parameter
        resolved = ResolvedPath(physical_path="/test/path", state=ResolvedPathState.DELETED, profile="test")
        assert resolved.state == ResolvedPathState.DELETED
        assert resolved.profile == "test"

        # Test delete-then-overwrite scenario
        # Create a test file for delete-overwrite testing
        test_file_path = "delete_overwrite_test.txt"
        test_content_original = b"original content"
        test_content_new = b"new content after delete"

        # Write the original file
        storage_client.write(test_file_path, test_content_original)
        storage_client.commit_metadata()

        # Verify file exists
        assert storage_client.is_file(test_file_path)
        assert storage_client.read(test_file_path) == test_content_original

        # Delete the file
        storage_client.delete(test_file_path)
        storage_client.commit_metadata()

        # Verify file is deleted
        assert not storage_client.is_file(test_file_path)
        with pytest.raises(FileNotFoundError):
            storage_client.read(test_file_path)

        # At this point, the metadata provider should return DELETED state for this path
        # Test the soft delete behavior
        resolved = storage_client._metadata_provider.realpath(test_file_path)
        assert resolved.state == ResolvedPathState.DELETED

        # Test that overwrite protection works with deleted files
        # The UuidMetadataProvider doesn't allow overwrites by default
        assert not storage_client._metadata_provider.allow_overwrites()

        # Attempt to write to the deleted file - this should raise FileExistsError
        # because the file was previously deleted and overwrites are not allowed
        with pytest.raises(FileExistsError, match="already exists"):
            storage_client.write(test_file_path, b"attempt to overwrite deleted file")

        # Now test that we can overwrite if we enable overwrites
        # Create a new provider that allows overwrites
        overwrite_provider = UuidMetadataProvider()
        overwrite_provider._allow_overwrites = True  # Enable overwrites

        # Copy the state from the original provider
        overwrite_provider._path_to_uuid = storage_client._metadata_provider._path_to_uuid.copy()
        overwrite_provider._uuid_to_info = storage_client._metadata_provider._uuid_to_info.copy()
        overwrite_provider._deleted_files = storage_client._metadata_provider._deleted_files.copy()

        # Replace the provider temporarily
        original_provider = storage_client._metadata_provider
        storage_client._metadata_provider = overwrite_provider

        try:
            # Now overwrite the deleted file with new content (should succeed)
            storage_client.write(test_file_path, test_content_new)
            storage_client.commit_metadata()

            # Verify the file exists again with new content
            assert storage_client.is_file(test_file_path)
            assert storage_client.read(test_file_path) == test_content_new

            # Verify the file is no longer marked as deleted
            resolved = storage_client._metadata_provider.realpath(test_file_path)
            assert resolved.state == ResolvedPathState.EXISTS
        finally:
            # Restore original provider
            storage_client._metadata_provider = original_provider
