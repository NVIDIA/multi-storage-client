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

import os
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Type

import pytest
import test_multistorageclient.unit.utils.tempdatastore as tempdatastore

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.constants import MEMORY_LOAD_LIMIT
from multistorageclient.providers.s3 import StaticS3CredentialsProvider
from multistorageclient_rust import RustClient  # pyright: ignore[reportAttributeAccessIssue]

from .utils import RefreshableTestCredentialsProvider


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
@pytest.mark.asyncio
async def test_rustclient_basic_operations(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        # Create a Rust client from the temp data store profile config dict
        config_dict = temp_data_store.profile_config_dict()
        credentials_provider = StaticS3CredentialsProvider(
            access_key=config_dict["credentials_provider"]["options"]["access_key"],
            secret_key=config_dict["credentials_provider"]["options"]["secret_key"],
        )
        rust_client = RustClient(
            provider="s3",
            configs={
                "bucket": config_dict["storage_provider"]["options"]["base_path"],
                "endpoint_url": config_dict["storage_provider"]["options"]["endpoint_url"],
                "allow_http": config_dict["storage_provider"]["options"]["endpoint_url"].startswith("http://"),
                "max_concurrency": 16,
                "multipart_chunksize": 10 * 1024 * 1024,
            },
            credentials_provider=credentials_provider,
        )

        # Create a storage client as well for operations that are not supported by the Rust client
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        file_extension = ".txt"
        # add a random string to the file path below so concurrent tests don't conflict
        file_path_fragments = [f"{uuid.uuid4().hex}-prefix", "infix", f"suffix{file_extension}"]
        file_path = os.path.join(*file_path_fragments)
        file_body_bytes = b"\x00\x01\x02" * 3

        # Test put
        await rust_client.put(file_path, file_body_bytes)

        # Test get
        result = await rust_client.get(file_path)
        assert result == file_body_bytes

        # Test range get
        result = await rust_client.get(file_path, 1, 4)
        assert result == file_body_bytes[1:4]

        result = await rust_client.get(file_path, 0, len(file_body_bytes))
        assert result == file_body_bytes

        # Test upload the file.
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_body_bytes)
            temp_file.close()
            await rust_client.upload(temp_file.name, file_path)

        # Verify the file was uploaded successfully using multi-storage client
        assert storage_client.is_file(path=file_path)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.close()
            storage_client.download_file(remote_path=file_path, local_path=temp_file.name)
            with open(temp_file.name, "rb") as f:
                assert f.read() == file_body_bytes

        # Test upload_multipart with a large file
        large_file_size = MEMORY_LOAD_LIMIT + 1
        large_file_body = os.urandom(large_file_size)
        large_file_path_fragments = [f"{uuid.uuid4().hex}-prefix", "infix", f"multipart_suffix{file_extension}"]
        large_file_path = os.path.join(*large_file_path_fragments)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(large_file_body)
            temp_file.close()
            await rust_client.upload_multipart(temp_file.name, large_file_path)

        # Verify the large file was uploaded successfully using multi-storage client
        assert storage_client.is_file(path=large_file_path)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.close()
            storage_client.download_file(remote_path=large_file_path, local_path=temp_file.name)
            assert os.path.getsize(temp_file.name) == large_file_size
            # Assert file content is the same
            with open(temp_file.name, "rb") as f:
                downloaded = f.read()
            assert downloaded == large_file_body

        # Test upload_multipart with a large file
        large_file_size = MEMORY_LOAD_LIMIT + 1
        large_file_body = os.urandom(large_file_size)
        large_file_path_fragments = [f"{uuid.uuid4().hex}-prefix", "infix", f"multipart_suffix{file_extension}"]
        large_file_path = os.path.join(*large_file_path_fragments)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(large_file_body)
            temp_file.close()
            await rust_client.upload_multipart(temp_file.name, large_file_path)

        # Verify the large file was uploaded successfully using multi-storage client
        assert storage_client.is_file(path=large_file_path)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.close()
            storage_client.download_file(remote_path=large_file_path, local_path=temp_file.name)
            assert os.path.getsize(temp_file.name) == large_file_size
            # Assert file content is the same
            with open(temp_file.name, "rb") as f:
                downloaded = f.read()
            assert downloaded == large_file_body

        # Test download the file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.close()
            await rust_client.download(file_path, temp_file.name)
            with open(temp_file.name, "rb") as f:
                assert f.read() == file_body_bytes

        # Test download_multipart with a large file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.close()
            await rust_client.download_multipart(large_file_path, temp_file.name)
            assert os.path.getsize(temp_file.name) == large_file_size
            # Assert file content is the same
            with open(temp_file.name, "rb") as f:
                downloaded = f.read()
            assert downloaded == large_file_body

        # Delete the file.
        storage_client.delete(path=file_path)
        storage_client.delete(path=large_file_path)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
@pytest.mark.asyncio
async def test_rustclient_with_refreshable_credentials(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        config_dict = temp_data_store.profile_config_dict()
        # The credentials are valid for 5 seconds before the refresh
        # After refresh, the credentials are invalid.
        credentials_provider = RefreshableTestCredentialsProvider(
            access_key=config_dict["credentials_provider"]["options"]["access_key"],
            secret_key=config_dict["credentials_provider"]["options"]["secret_key"],
            expiration=(datetime.now(timezone.utc) + timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        rust_client = RustClient(
            provider="s3",
            configs={
                "bucket": config_dict["storage_provider"]["options"]["base_path"],
                "endpoint_url": config_dict["storage_provider"]["options"]["endpoint_url"],
                "allow_http": config_dict["storage_provider"]["options"]["endpoint_url"].startswith("http://"),
                "max_concurrency": 16,
                "multipart_chunksize": 10 * 1024 * 1024,
            },
            credentials_provider=credentials_provider,
        )

        # Create a storage client as well for operations that are not supported by the Rust client
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        file_extension = ".txt"
        # add a random string to the file path below so concurrent tests don't conflict
        file_path_fragments = [f"{uuid.uuid4().hex}-prefix", "infix", f"suffix{file_extension}"]
        file_path = os.path.join(*file_path_fragments)
        file_body_bytes = b"\x00\x01\x02" * 3

        # Test before valid credentials expire
        await rust_client.put(file_path, file_body_bytes)
        result = await rust_client.get(file_path)
        assert result == file_body_bytes
        assert credentials_provider.refresh_count == 0

        # Test after valid credentials expire, should call refresh_credentials and fail
        time.sleep(6)
        with pytest.raises(RuntimeError):
            await rust_client.get(file_path)
        assert credentials_provider.refresh_count == 1

        # Delete the file.
        storage_client.delete(path=file_path)
