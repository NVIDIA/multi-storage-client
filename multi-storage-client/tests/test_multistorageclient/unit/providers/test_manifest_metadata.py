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

import copy
import os
import tempfile
import time
from datetime import datetime, timezone

import pytest

import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers.manifest_metadata import (
    DEFAULT_MANIFEST_BASE_DIR,
    ManifestMetadataProvider,
)
from multistorageclient.providers.manifest_object_metadata import ManifestObjectMetadata
from multistorageclient.types import ObjectMetadata


@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "replace_base_path"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory, False],
        [tempdatastore.TemporaryAWSS3Bucket, False],
        [tempdatastore.TemporaryPOSIXDirectory, True],
        [tempdatastore.TemporaryAWSS3Bucket, True],
    ],
)
def test_manifest_metadata(temp_data_store_type: type[tempdatastore.TemporaryDataStore], replace_base_path: bool):
    with temp_data_store_type() as temp_data_store:
        data_profile = "data"
        data_with_manifest_profile = "data_with_manifest"

        data_profile_config_dict = temp_data_store.profile_config_dict()

        base_path = ""
        if replace_base_path:
            base_path = data_profile_config_dict["storage_provider"]["options"]["base_path"].removeprefix("/")

        data_with_manifest_profile_config_dict = copy.deepcopy(data_profile_config_dict) | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": os.path.join(base_path, DEFAULT_MANIFEST_BASE_DIR),
                    "writable": True,
                },
            }
        }
        if replace_base_path:
            data_with_manifest_profile_config_dict["storage_provider"]["options"]["base_path"] = "/"

        storage_client_config_dict = {
            "profiles": {
                data_profile: data_profile_config_dict,
                data_with_manifest_profile: data_with_manifest_profile_config_dict,
            }
        }

        file_path = os.path.join(base_path, "dir/file.txt")
        file_content_length = 1
        file_body_bytes = b"\x00" * file_content_length

        # Create the data storage client.
        data_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(config_dict=storage_client_config_dict, profile=data_profile)
        )
        assert data_storage_client._metadata_provider is None

        # Create the data with manifest storage client.
        data_with_manifest_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict=storage_client_config_dict, profile=data_with_manifest_profile
            )
        )
        assert data_with_manifest_storage_client._metadata_provider is not None

        # Check if the manifest metadata tracks no files.
        assert len(list(data_with_manifest_storage_client.list())) == 0
        assert data_with_manifest_storage_client.is_empty(path="dir")

        # Write a file.
        data_with_manifest_storage_client.write(path=file_path, body=file_body_bytes)
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 0
        assert data_with_manifest_storage_client.is_empty(path="dir")

        data_with_manifest_storage_client.commit_metadata()
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 1

        # Check if the manifest is persisted.
        data_with_manifest_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict=storage_client_config_dict, profile=data_with_manifest_profile
            )
        )
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 1
        assert not data_with_manifest_storage_client.is_empty(path=os.path.join(base_path, "dir"))

        # Check the file metadata.
        file_info = data_with_manifest_storage_client.info(path=file_path)
        assert file_info is not None
        assert file_info.key.endswith(file_path)
        assert file_info.content_length == file_content_length
        assert file_info.type == "file"
        assert file_info.last_modified is not None

        file_info_list = list(data_with_manifest_storage_client.list(path=base_path))
        assert len(file_info_list) == 1
        listed_file_info = file_info_list[0]
        assert listed_file_info is not None
        assert listed_file_info.key.endswith(file_path)
        assert listed_file_info.content_length == file_info.content_length
        assert listed_file_info.type == file_info.type
        assert listed_file_info.last_modified == file_info.last_modified

        # Check that info() detects directories too.
        for dir_path in ["dir", "dir/"]:
            dir_info = data_with_manifest_storage_client.info(path=os.path.join(base_path, dir_path), strict=False)
            assert dir_info.type == "directory"
            assert dir_info.key == os.path.join(base_path, "dir/")
            assert dir_info.content_length == 0

        # But "di" is not a valid directory, even though it is a valid prefix.
        with pytest.raises(FileNotFoundError):
            data_with_manifest_storage_client.info(path="di", strict=False)

        # Delete the file.
        data_with_manifest_storage_client.delete(path=file_path)
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 1
        data_with_manifest_storage_client.commit_metadata()
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 0

        # Upload the file.
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_body_bytes)
            temp_file.close()
            data_with_manifest_storage_client.upload_file(remote_path=file_path, local_path=temp_file.name)
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 0
        data_with_manifest_storage_client.commit_metadata()
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 1

        # Check the file metadata.
        file_info = data_with_manifest_storage_client.info(path=file_path)
        assert file_info is not None
        assert file_info.key.endswith(file_path)
        assert file_info.content_length == file_content_length
        assert file_info.type == "file"

        # Copy the file.
        file_copy_path = os.path.join(base_path, "copy-" + file_path)
        data_with_manifest_storage_client.copy(src_path=file_path, dest_path=file_copy_path)
        assert len(data_with_manifest_storage_client.glob(pattern=file_copy_path)) == 0
        data_with_manifest_storage_client.commit_metadata()
        assert len(data_with_manifest_storage_client.glob(pattern=file_copy_path)) == 1

        # Check the file copy metadata.
        file_copy_info = data_with_manifest_storage_client.info(path=file_copy_path)
        assert file_copy_info is not None
        assert file_copy_info.key.endswith(file_copy_path)
        assert file_copy_info.content_length == file_content_length
        assert file_copy_info.type == "file"

        # Delete the file and its copy.
        for path in [file_path, file_copy_path]:
            data_with_manifest_storage_client.delete(path=path)
        data_with_manifest_storage_client.commit_metadata()

        # Write files.
        file_directory = os.path.join(base_path, "directory")
        file_count = 10
        for i in range(file_count):
            data_storage_client.write(path=os.path.join("directory", f"{i}.txt"), body=file_body_bytes)
        assert len(list(data_with_manifest_storage_client.list(path=file_directory + "/"))) == 0

        data_with_manifest_storage_client.commit_metadata(prefix=f"{file_directory}/")
        assert len(list(data_with_manifest_storage_client.list(path=file_directory + "/"))) == file_count

        # Test listing with directories
        with_dirs = list(data_with_manifest_storage_client.list(path=base_path, include_directories=True))
        assert len(with_dirs) == 1
        assert with_dirs[0].key == file_directory + "/"


def test_nonexistent_and_read_only():
    with tempdatastore.TemporaryPOSIXDirectory() as temp_data_store:
        data_with_manifest_profile = "data_with_manifest"
        data_with_read_only_manifest_profile = "data_with_read_only_manifest"

        data_with_manifest_profile_config_dict = temp_data_store.profile_config_dict() | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                    "writable": True,
                },
            }
        }
        data_with_read_only_manifest_profile_config_dict = temp_data_store.profile_config_dict() | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                    "writable": False,
                },
            }
        }

        storage_client_config_dict = {
            "profiles": {
                data_with_manifest_profile: data_with_manifest_profile_config_dict,
                data_with_read_only_manifest_profile: data_with_read_only_manifest_profile_config_dict,
            }
        }

        file_path = "file.txt"
        file_body_bytes = b"\x00"

        # Create the data with manifest storage client.
        data_with_manifest_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict=storage_client_config_dict, profile=data_with_manifest_profile
            )
        )

        # Write a file.
        data_with_manifest_storage_client.write(path=file_path, body=file_body_bytes)
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 0
        data_with_manifest_storage_client.commit_metadata()
        assert len(data_with_manifest_storage_client.glob(pattern=file_path)) == 1

        # Create the data with read-only manifest storage client.
        data_with_read_only_manifest_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict=storage_client_config_dict, profile=data_with_read_only_manifest_profile
            )
        )

        # Attempt an overwrite.
        with pytest.raises(FileExistsError):
            data_with_read_only_manifest_storage_client.write(path=file_path, body=file_body_bytes)

        # Attempt a write.
        with pytest.raises(RuntimeError):
            data_with_read_only_manifest_storage_client.write(path=f"nonexistent-{file_path}", body=file_body_bytes)

        # Attempt a non-existent delete.
        with pytest.raises(FileNotFoundError):
            data_with_read_only_manifest_storage_client.delete(path=f"nonexistent-{file_path}")

        # Attempt a delete.
        with pytest.raises(RuntimeError):
            data_with_read_only_manifest_storage_client.delete(path=file_path)


@pytest.mark.parametrize(
    argnames="temp_data_store_type",
    argvalues=[
        tempdatastore.TemporaryAWSS3Bucket,
    ],
)
def test_autocommit(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        manifest_profile = "manifest"

        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={
                    "profiles": {
                        manifest_profile: {
                            **temp_data_store.profile_config_dict(),
                            "metadata_provider": {
                                "type": "manifest",
                                "options": {
                                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                                    "writable": True,
                                },
                            },
                            "autocommit": {
                                "interval_minutes": 0.05,  # 0.05 minutes = 3 seconds
                                "at_exit": False,
                            },
                        }
                    }
                },
                profile=manifest_profile,
            )
        )

        file_count = 10
        for i in range(file_count):
            fname = f"folder/filename-{i}.txt"
            if not storage_client.is_file(fname):
                storage_client.write(fname, f"contents for {i}")

        # Wait 3 seconds for the autocommit to commit the files.
        time.sleep(5)

        assert len(list(storage_client.list(path="folder/"))) == file_count
        for i in range(file_count):
            fname = f"folder/filename-{i}.txt"
            assert storage_client.is_file(fname)
            assert storage_client.open(fname, mode="r").read() == f"contents for {i}"


def test_manifest_metadata_attribute_filtering():
    """Test attribute filter support in manifest metadata provider."""
    with tempdatastore.TemporaryPOSIXDirectory() as temp_data_store:
        manifest_profile = "manifest"

        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={
                    "profiles": {
                        manifest_profile: {
                            **temp_data_store.profile_config_dict(),
                            "metadata_provider": {
                                "type": "manifest",
                                "options": {
                                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                                    "writable": True,
                                },
                            },
                        }
                    }
                },
                profile=manifest_profile,
            )
        )

        # Create test files with different attributes
        test_files = [
            {
                "path": "models/model_v1.bin",
                "content": b"model_v1_content",
                "attributes": {
                    "model_name": "gpt",
                    "version": "1.0",
                    "environment": "prod",
                    "size": "large",
                    "priority": "10",
                },
            },
            {
                "path": "models/model_v2.bin",
                "content": b"model_v2_content",
                "attributes": {
                    "model_name": "gpt",
                    "version": "2.0",
                    "environment": "dev",
                    "size": "small",
                    "priority": "5",
                },
            },
            {
                "path": "data/dataset.bin",
                "content": b"dataset_content",
                "attributes": {
                    "model_name": "bert",
                    "version": "1.5",
                    "environment": "test",
                    "size": "medium",
                    "priority": "8",
                },
            },
            {
                "path": "data/data_v1.bin",
                "content": b"data_v1_content",
                "attributes": {
                    "model_name": "bert",
                    "version": "1.0",
                    "environment": "prod",
                    "size": "large",
                    "priority": "15",
                },
            },
            {
                "path": "config/settings.txt",
                "content": b"settings_content",
                "attributes": {
                    "type": "config",
                    "version": "0.5",
                    "environment": "prod",
                    "size": "small",
                    "priority": "20",
                },
            },
            {
                "path": "cache/cache.tmp",
                "content": b"cache_content",
                "attributes": {
                    "type": "cache",
                    "version": "1.2",
                    "environment": "dev",
                    "size": "medium",
                    "priority": "12",
                },
            },
        ]

        # Write all test files with attributes
        for test_file in test_files:
            storage_client.write(test_file["path"], test_file["content"], attributes=test_file["attributes"])

        # Commit metadata to manifest
        storage_client.commit_metadata()

        # Test 1: list_objects with attribute filter expressions

        # Test multiple filters (AND logic) - bert model in prod environment
        results = list(
            storage_client.list(attribute_filter_expression='(model_name = "bert" AND environment = "prod")')
        )
        assert len(results) == 1
        result_paths = [r.key for r in results]
        assert "data/data_v1.bin" in result_paths

        # Test multiple filters (OR logic) - gpt or bert models
        results = list(storage_client.list(attribute_filter_expression='(model_name = "gpt" OR model_name = "bert")'))
        assert len(results) == 4  # All model files
        result_paths = [r.key for r in results]
        assert "models/model_v1.bin" in result_paths
        assert "models/model_v2.bin" in result_paths
        assert "data/dataset.bin" in result_paths
        assert "data/data_v1.bin" in result_paths

        # Test inequality filter - find files not in prod environment
        results = list(storage_client.list(attribute_filter_expression='environment != "prod"'))
        assert len(results) == 3  # dev + test + dev files
        result_paths = [r.key for r in results]
        assert "models/model_v2.bin" in result_paths  # dev
        assert "data/dataset.bin" in result_paths  # test
        assert "cache/cache.tmp" in result_paths  # dev

        # Test string comparison (greater than) - priority > 10
        results = list(storage_client.list(attribute_filter_expression='priority > "10"'))
        assert len(results) == 3
        result_paths = [r.key for r in results]
        assert "data/data_v1.bin" in result_paths  # priority: 15
        assert "config/settings.txt" in result_paths  # priority: 20
        assert "cache/cache.tmp" in result_paths  # priority: 12

        # Test numeric comparison (less than or equal) - priority <= 8
        results = list(storage_client.list(attribute_filter_expression="priority <= 8.0"))
        assert len(results) == 2
        result_paths = [r.key for r in results]
        assert "models/model_v2.bin" in result_paths  # priority: 5
        assert "data/dataset.bin" in result_paths  # priority: 8

        # Test empty filter (should return all files)
        results = list(storage_client.list(attribute_filter_expression=""))
        assert len(results) == 6

        # Test 2: glob with attribute filter expressions

        # Test glob with multiple filters (AND logic) - large files in prod
        results = storage_client.glob("**/*", attribute_filter_expression='(size = "large" AND environment = "prod")')
        assert len(results) == 2
        result_paths = [path for path in results]
        assert "models/model_v1.bin" in result_paths
        assert "data/data_v1.bin" in result_paths

        # Test filtering with mixed numeric and string comparisons
        results = storage_client.glob("**/*", attribute_filter_expression='(priority > 7 AND size != "large")')
        # Should return files with priority > 7 AND size != large
        # That's: cache.tmp (12, medium), settings.txt (20, small)
        assert len(results) >= 1
        result_paths = [path for path in results]
        assert "cache/cache.tmp" in result_paths or "config/settings.txt" in result_paths

        # Test glob pattern specificity with filters - only .bin files that are small
        results = storage_client.glob("**/*.bin", attribute_filter_expression='size = "small"')
        assert len(results) == 1
        assert "models/model_v2.bin" in results

        # Test complex glob pattern with attribute filters
        results = storage_client.glob("**/model_*.bin", attribute_filter_expression='environment != "test"')
        assert len(results) == 2  # model_v1.bin (prod) and model_v2.bin (dev)
        result_paths = [path for path in results]
        assert "models/model_v1.bin" in result_paths
        assert "models/model_v2.bin" in result_paths

        # Test 3: Edge cases and error handling

        # Test invalid filter format should raise error
        with pytest.raises(ValueError, match="Invalid attribute filter expression"):
            list(storage_client.list(attribute_filter_expression="incomplete_filter"))

        # Test unsupported operator should raise error
        with pytest.raises(ValueError, match="Invalid attribute filter expression"):
            list(storage_client.list(attribute_filter_expression='model_name ~= "value"'))


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_manifest_metadata_allow_overwrites(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test the allow_overwrites functionality in manifest metadata provider."""
    with temp_data_store_type() as temp_data_store:
        # Test with allow_overwrites=False (default)
        profile_no_overwrite = "no_overwrite"
        storage_client_no_overwrite = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={
                    "profiles": {
                        profile_no_overwrite: {
                            **temp_data_store.profile_config_dict(),
                            "metadata_provider": {
                                "type": "manifest",
                                "options": {
                                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                                    "writable": True,
                                    "allow_overwrites": False,  # Explicitly set to False
                                },
                            },
                        }
                    }
                },
                profile=profile_no_overwrite,
            )
        )

        # Write a file
        test_path = "test_file.txt"
        test_content = b"original content"
        storage_client_no_overwrite.write(test_path, test_content)
        storage_client_no_overwrite.commit_metadata()

        # Verify file exists
        assert storage_client_no_overwrite.read(test_path) == test_content

        # Try to overwrite with write() - should fail
        with pytest.raises(FileExistsError, match=f"The file at path '{test_path}' already exists"):
            storage_client_no_overwrite.write(test_path, b"new content")

        # Try to overwrite with upload_file() - should fail
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(b"new content from file")
            tmp_path = tmp.name

        try:
            with pytest.raises(FileExistsError, match=f"The file at path '{test_path}' already exists"):
                storage_client_no_overwrite.upload_file(test_path, tmp_path)
        finally:
            os.unlink(tmp_path)

        # Try to copy to existing file - should fail
        test_path2 = "test_file2.txt"
        storage_client_no_overwrite.write(test_path2, b"second file")
        storage_client_no_overwrite.commit_metadata()

        with pytest.raises(FileExistsError, match=f"The file at path '{test_path2}' already exists"):
            storage_client_no_overwrite.copy(test_path, test_path2)

        # Test with allow_overwrites=True
        profile_with_overwrite = "with_overwrite"
        storage_client_with_overwrite = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={
                    "profiles": {
                        profile_with_overwrite: {
                            **temp_data_store.profile_config_dict(),
                            "metadata_provider": {
                                "type": "manifest",
                                "options": {
                                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                                    "writable": True,
                                    "allow_overwrites": True,  # Enable overwrites
                                },
                            },
                        }
                    }
                },
                profile=profile_with_overwrite,
            )
        )

        # Write a file
        test_path3 = "test_file3.txt"
        original_content = b"original content for overwrite test"
        storage_client_with_overwrite.write(test_path3, original_content)
        storage_client_with_overwrite.commit_metadata()

        # Verify file exists
        assert storage_client_with_overwrite.read(test_path3) == original_content

        # Overwrite with write() - should succeed
        new_content = b"overwritten content"
        storage_client_with_overwrite.write(test_path3, new_content)
        storage_client_with_overwrite.commit_metadata()
        assert storage_client_with_overwrite.read(test_path3) == new_content

        # Overwrite with upload_file() - should succeed
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(b"overwritten from file")
            tmp_path = tmp.name

        try:
            storage_client_with_overwrite.upload_file(test_path3, tmp_path)
            storage_client_with_overwrite.commit_metadata()
            assert storage_client_with_overwrite.read(test_path3) == b"overwritten from file"
        finally:
            os.unlink(tmp_path)

        # Copy to existing file - should succeed
        test_path4 = "test_file4.txt"
        test_path5 = "test_file5.txt"
        storage_client_with_overwrite.write(test_path4, b"source file")
        storage_client_with_overwrite.write(test_path5, b"dest file")
        storage_client_with_overwrite.commit_metadata()

        storage_client_with_overwrite.copy(test_path4, test_path5)
        storage_client_with_overwrite.commit_metadata()
        assert storage_client_with_overwrite.read(test_path5) == b"source file"


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_manifest_metadata_realpath_for_write(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test the realpath method with for_write parameter in manifest metadata provider."""
    with temp_data_store_type() as temp_data_store:
        # Create storage client to get storage_provider
        config = StorageClientConfig.from_dict(
            config_dict={"profiles": {"test": temp_data_store.profile_config_dict()}},
            profile="test",
        )
        storage_client = StorageClient(config)

        # Create manifest provider with allow_overwrites=False
        provider = ManifestMetadataProvider(
            storage_provider=storage_client._storage_provider,
            manifest_path=DEFAULT_MANIFEST_BASE_DIR,
            writable=True,
            allow_overwrites=False,
        )

        # Test realpath for non-existing file
        resolved = provider.realpath("new_file.txt")
        assert not resolved.exists
        assert resolved.physical_path == "new_file.txt"

        # Test generate_physical_path for non-existing file
        resolved = provider.generate_physical_path("new_file.txt", for_overwrite=False)
        assert resolved.physical_path == "new_file.txt"
        assert not resolved.exists

        # Add a file to the manifest
        metadata = ObjectMetadata(
            key="existing_file.txt",
            content_length=1024,
            last_modified=datetime.now(tz=timezone.utc),
            content_type="text/plain",
            etag="abc123",
        )
        provider.add_file("existing_file.txt", metadata)

        # Test realpath for pending file - should NOT be found (only checks committed)
        resolved = provider.realpath("existing_file.txt")
        assert not resolved.exists  # Pending files are not found by realpath

        # Test generate_physical_path for pending file
        resolved = provider.generate_physical_path("existing_file.txt", for_overwrite=False)
        assert resolved.physical_path == "existing_file.txt"
        assert not resolved.exists

        # Commit the file
        provider.commit_updates()

        # Now realpath should find the committed file
        resolved = provider.realpath("existing_file.txt")
        assert resolved.exists
        assert resolved.physical_path == "existing_file.txt"

        # Test generate_physical_path for existing file (overwrite scenario)
        resolved = provider.generate_physical_path("existing_file.txt", for_overwrite=True)
        assert resolved.physical_path == "existing_file.txt"
        assert not resolved.exists  # generate always returns exists=False

        # Create manifest provider with allow_overwrites=True
        provider_with_overwrite = ManifestMetadataProvider(
            storage_provider=storage_client._storage_provider,
            manifest_path=DEFAULT_MANIFEST_BASE_DIR,
            writable=True,
            allow_overwrites=True,
        )

        # Add the same file
        provider_with_overwrite.add_file("existing_file.txt", metadata)

        # Test realpath for existing file with overwrites allowed
        resolved = provider_with_overwrite.realpath("existing_file.txt")
        assert resolved.exists
        assert resolved.physical_path == "existing_file.txt"


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_manifest_physical_path_tracking(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test that ManifestMetadataProvider properly tracks logical and physical paths."""
    with temp_data_store_type() as temp_data_store:
        # Create storage client to get storage_provider
        config = StorageClientConfig.from_dict(
            config_dict={"profiles": {"test": temp_data_store.profile_config_dict()}},
            profile="test",
        )
        storage_client = StorageClient(config)

        # Create manifest provider
        provider = ManifestMetadataProvider(
            storage_provider=storage_client._storage_provider,
            manifest_path=DEFAULT_MANIFEST_BASE_DIR,
            writable=True,
            allow_overwrites=True,
        )

        # Test realpath for non-existing file
        resolved = provider.realpath("new_file.txt")
        assert not resolved.exists

        # Test generate_physical_path for new file
        resolved = provider.generate_physical_path("new_file.txt", for_overwrite=False)
        assert resolved.physical_path == "new_file.txt"  # Currently, physical = logical

        # Add a file - note that metadata.key is the physical path from storage
        metadata = ObjectMetadata(
            key="storage/path/file.txt",  # This is the actual storage path
            content_length=1024,
            last_modified=datetime.now(tz=timezone.utc),
            content_type="text/plain",
            etag="abc123",
        )
        provider.add_file("logical/file.txt", metadata)  # Logical path

        # Test that realpath returns exists=False for uncommitted files
        resolved = provider.realpath("logical/file.txt")
        assert not resolved.exists  # File not yet committed

        # Test generate_physical_path for uncommitted file
        resolved = provider.generate_physical_path("logical/file.txt", for_overwrite=False)
        assert resolved.physical_path == "logical/file.txt"
        assert not resolved.exists

        # Test that get_object_metadata returns logical path in key
        obj_metadata = provider.get_object_metadata("logical/file.txt", include_pending=True)
        assert obj_metadata.key == "logical/file.txt"

        # Commit the pending changes
        provider.commit_updates()

        # Now realpath should find the committed file
        resolved = provider.realpath("logical/file.txt")
        assert resolved.exists
        assert resolved.physical_path == "storage/path/file.txt"

        # Test list_objects returns logical paths
        objects = list(provider.list_objects(""))
        assert len(objects) == 1
        assert objects[0].key == "logical/file.txt"

        # Test overwrite scenario
        new_metadata = ObjectMetadata(
            key="storage/path/file-v2.txt",  # New physical path
            content_length=2048,
            last_modified=datetime.now(tz=timezone.utc),
            content_type="text/plain",
            etag="def456",
        )
        provider.add_file("logical/file.txt", new_metadata)  # Same logical path

        # realpath should still see old committed version
        resolved_path = provider.realpath("logical/file.txt")
        assert resolved_path.exists
        assert resolved_path.physical_path == "storage/path/file.txt"  # Old committed version

        # But get_object_metadata with include_pending should see new version
        obj_metadata = provider.get_object_metadata("logical/file.txt", include_pending=True)
        # Note: ManifestObjectMetadata stores physical path separately, but returns logical path in key
        assert obj_metadata.key == "logical/file.txt"

        # Commit and verify physical path is updated
        provider.commit_updates()
        resolved_path = provider.realpath("logical/file.txt")
        assert resolved_path.exists
        assert resolved_path.physical_path == "storage/path/file-v2.txt"  # Now committed with new physical path

        # Test generate_physical_path for overwrite
        # Currently returns same path, but in future could generate unique path
        overwrite_path = provider.generate_physical_path("logical/file.txt", for_overwrite=True)
        assert overwrite_path.physical_path == "logical/file.txt"

        # Test that add_file can accept ManifestObjectMetadata directly
        manifest_obj = ManifestObjectMetadata(
            key="direct/logical.txt",
            content_length=512,
            last_modified=datetime.now(tz=timezone.utc),
            content_type="text/plain",
            etag="xyz789",
            physical_path="direct/physical/path.txt",
        )
        provider.add_file("direct/logical.txt", manifest_obj)

        # Verify it was added correctly
        resolved = provider.realpath("direct/logical.txt")
        assert not resolved.exists  # Not committed yet

        provider.commit_updates()
        resolved = provider.realpath("direct/logical.txt")
        assert resolved.exists
        assert resolved.physical_path == "direct/physical/path.txt"
