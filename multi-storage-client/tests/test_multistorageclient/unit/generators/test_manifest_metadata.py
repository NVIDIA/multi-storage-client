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

import json
import os
import random
import re
import time
from datetime import datetime, timezone

import pytest

import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.generators import ManifestMetadataGenerator
from multistorageclient.providers.manifest_formats import ManifestFormat, get_format_handler
from multistorageclient.providers.manifest_metadata import (
    DEFAULT_MANIFEST_BASE_DIR,
    MANIFEST_INDEX_FILENAME,
    MANIFEST_PART_PREFIX,
    MANIFEST_PARTS_CHILD_DIR,
    SEQUENCE_PADDING,
)
from multistorageclient.types import ObjectMetadata

try:
    import pyarrow  # noqa: F401

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryAzureBlobStorageContainer],
        [tempdatastore.TemporaryGoogleCloudStorageBucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
def test_manifest_metadata(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        data_profile = "data"
        data_with_manifest_profile = "data_with_manifest"

        data_profile_config_dict = temp_data_store.profile_config_dict()
        data_with_manifest_profile_config_dict = temp_data_store.profile_config_dict() | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                },
            }
        }

        storage_client_config_dict = {
            "profiles": {
                data_profile: data_profile_config_dict,
                data_with_manifest_profile: data_with_manifest_profile_config_dict,
            }
        }

        # Create the data storage client.
        data_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(config_dict=storage_client_config_dict, profile=data_profile)
        )

        # Generate files.
        placeholder_last_modified = datetime.now(tz=timezone.utc)
        expected_files_info = {
            key: ObjectMetadata(
                key=key,
                content_length=random.randint(0, 100),
                last_modified=placeholder_last_modified,
                metadata={"key1": key},
            )
            for key in [f"{i}.txt" for i in range(2)]
        }

        for key, placeholder_file_info in expected_files_info.items():
            data_storage_client.write(
                path=key, body=b"\x00" * placeholder_file_info.content_length, attributes=placeholder_file_info.metadata
            )

        # Generate a manifest.
        manifest_format = ManifestFormat.JSONL
        ManifestMetadataGenerator.generate_and_write_manifest(
            data_storage_client=data_storage_client,
            manifest_storage_client=data_storage_client,
            manifest_format=manifest_format,
        )

        manifest_format_suffix = get_format_handler(manifest_format).get_file_suffix()
        assert manifest_format_suffix == ".jsonl"

        # List the manifest.
        manifest_directories_info = [
            metadata
            for metadata in data_storage_client.list(path=f"{DEFAULT_MANIFEST_BASE_DIR}/", include_directories=True)
            if metadata.type == "directory"
        ]
        assert len(manifest_directories_info) == 1

        # Check if the manifest timestamp is parseable and has second-level precision.
        datetime.fromisoformat(os.path.basename(manifest_directories_info[0].key)).isoformat(timespec="seconds")

        # Check the manifest index.
        manifest_index_path = os.path.join(manifest_directories_info[0].key, MANIFEST_INDEX_FILENAME)
        manifest_index_info = data_storage_client.info(path=manifest_index_path)
        assert manifest_index_info is not None
        assert manifest_index_info.key.endswith(manifest_index_path)
        assert manifest_index_info.type == "file"

        # List the manifest parts directory.
        manifest_parts_directories_info = [
            metadata
            for metadata in data_storage_client.list(
                path=f"{manifest_directories_info[0].key}/", include_directories=True
            )
            if metadata.type == "directory" and metadata.key.endswith(MANIFEST_PARTS_CHILD_DIR)
        ]
        assert len(manifest_parts_directories_info) == 1

        # Check the manifest parts.
        manifest_parts_info = [
            metadata for metadata in data_storage_client.list(path=f"{manifest_parts_directories_info[0].key}/")
        ]
        assert len(manifest_parts_info) > 0
        for manifest_part_info in manifest_parts_info:
            assert manifest_part_info is not None
            assert (
                re.search(
                    pattern=f"\\/{MANIFEST_PART_PREFIX}\\d{{{SEQUENCE_PADDING}}}\\{manifest_format_suffix}$",
                    string=manifest_part_info.key,
                )
                is not None
            )
            assert manifest_part_info.type == "file"
            with data_storage_client.open(path=manifest_part_info.key, mode="r") as manifest_part:
                for line in manifest_part:
                    assert DEFAULT_MANIFEST_BASE_DIR not in json.loads(line)["key"]

        # Create the data with manifest storage client.
        data_with_manifest_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict=storage_client_config_dict, profile=data_with_manifest_profile
            )
        )

        # Check the manifest.
        assert set(expected_files_info.keys()) == {
            file_info.key for file_info in data_with_manifest_storage_client.list()
        }
        for key, expected_file_info in expected_files_info.items():
            # Not all object metadata is preserved in manifests.
            #
            # Timestamp precision depends on the storage service, so skipping that too.
            actual_file_info = data_with_manifest_storage_client.info(path=key)
            assert actual_file_info.key == expected_file_info.key
            assert actual_file_info.type == expected_file_info.type
            assert actual_file_info.content_length == expected_file_info.content_length
            assert actual_file_info.metadata == expected_file_info.metadata

        # Generate a manifest with a later timestamp.
        time.sleep(1)
        ManifestMetadataGenerator.generate_and_write_manifest(
            data_storage_client=data_storage_client, manifest_storage_client=data_storage_client
        )

        # List the later manifest.
        later_manifest_directories_info = [
            metadata
            for metadata in data_storage_client.list(path=f"{DEFAULT_MANIFEST_BASE_DIR}/", include_directories=True)
            if metadata.type == "directory" and metadata.key != manifest_directories_info[0].key
        ]
        assert len(later_manifest_directories_info) == 1

        # Check if the later manifest timestamp is parseable and has second-level precision.
        datetime.fromisoformat(os.path.basename(later_manifest_directories_info[0].key)).isoformat(timespec="seconds")


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not installed")
@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_manifest_metadata_parquet_format(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        data_profile = "data"
        data_with_manifest_profile = "data_with_manifest"

        data_profile_config_dict = temp_data_store.profile_config_dict()
        data_with_manifest_profile_config_dict = temp_data_store.profile_config_dict() | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                },
            }
        }

        storage_client_config_dict = {
            "profiles": {
                data_profile: data_profile_config_dict,
                data_with_manifest_profile: data_with_manifest_profile_config_dict,
            }
        }

        data_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(config_dict=storage_client_config_dict, profile=data_profile)
        )

        placeholder_last_modified = datetime.now(tz=timezone.utc)
        expected_files_info = {
            key: ObjectMetadata(
                key=key,
                content_length=random.randint(0, 100),
                last_modified=placeholder_last_modified,
                metadata={"key1": key},
            )
            for key in [f"{i}.txt" for i in range(2)]
        }

        for key, placeholder_file_info in expected_files_info.items():
            data_storage_client.write(
                path=key, body=b"\x00" * placeholder_file_info.content_length, attributes=placeholder_file_info.metadata
            )

        ManifestMetadataGenerator.generate_and_write_manifest(
            data_storage_client=data_storage_client,
            manifest_storage_client=data_storage_client,
            manifest_format=ManifestFormat.PARQUET,
        )

        manifest_directories_info = [
            metadata
            for metadata in data_storage_client.list(path=f"{DEFAULT_MANIFEST_BASE_DIR}/", include_directories=True)
            if metadata.type == "directory"
        ]
        assert len(manifest_directories_info) == 1

        manifest_index_path = os.path.join(manifest_directories_info[0].key, MANIFEST_INDEX_FILENAME)
        manifest_index_content = data_storage_client.read(path=manifest_index_path).decode("utf-8")
        manifest_index = json.loads(manifest_index_content)
        assert manifest_index["format"] == "parquet"

        manifest_parts_info = [
            metadata
            for metadata in data_storage_client.list(
                path=os.path.join(manifest_directories_info[0].key, MANIFEST_PARTS_CHILD_DIR, "")
            )
        ]
        assert len(manifest_parts_info) > 0
        for manifest_part_info in manifest_parts_info:
            assert manifest_part_info.key.endswith(".parquet")

        data_with_manifest_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict=storage_client_config_dict, profile=data_with_manifest_profile
            )
        )

        assert set(expected_files_info.keys()) == {
            file_info.key for file_info in data_with_manifest_storage_client.list()
        }
        for key, expected_file_info in expected_files_info.items():
            actual_file_info = data_with_manifest_storage_client.info(path=key)
            assert actual_file_info.key == expected_file_info.key
            assert actual_file_info.type == expected_file_info.type
            assert actual_file_info.content_length == expected_file_info.content_length
            assert actual_file_info.metadata == expected_file_info.metadata
