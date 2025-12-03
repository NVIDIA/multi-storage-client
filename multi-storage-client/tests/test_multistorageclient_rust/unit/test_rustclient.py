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

import io
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
from multistorageclient.types import Range
from multistorageclient_rust import (  # pyright: ignore[reportAttributeAccessIssue]
    RustClient,
    RustClientError,
    RustRetryableError,
)

from .utils import RefreshableTestCredentialsProvider


async def run_rust_client_operations(rust_client: RustClient, storage_client: StorageClient):
    file_extension = ".txt"
    # add a random string to the file path below so concurrent tests don't conflict
    file_path_fragments = [f"{uuid.uuid4().hex}-prefix", "infix", f"suffix{file_extension}"]
    file_path = os.path.join(*file_path_fragments)
    file_body_bytes = b"\x00\x01\x02" * 3

    # Test put
    result = await rust_client.put(file_path, file_body_bytes)
    assert result == len(file_body_bytes)

    # Test get
    result = await rust_client.get(file_path)
    assert result == file_body_bytes

    # Test range get
    result = await rust_client.get(file_path, range=Range(1, 4))
    assert len(result) == 4
    assert result == file_body_bytes[1:5]

    result = await rust_client.get(file_path, range=Range(0, len(file_body_bytes)))
    assert result == file_body_bytes

    # Test upload the file.
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(file_body_bytes)
        temp_file.close()
        result = await rust_client.upload(temp_file.name, file_path)
        assert result == len(file_body_bytes)

    # Verify the file was uploaded successfully using multi-storage client
    assert storage_client.is_file(path=file_path)
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.close()
        storage_client.download_file(remote_path=file_path, local_path=temp_file.name)
        with open(temp_file.name, "rb") as f:
            assert f.read() == file_body_bytes

    # Test upload_multipart_from_file with a large file
    large_file_size = MEMORY_LOAD_LIMIT + 1
    large_file_body = os.urandom(large_file_size)
    large_file_path_fragments = [f"{uuid.uuid4().hex}-prefix", "infix", f"multipart_suffix{file_extension}"]
    large_file_path = os.path.join(*large_file_path_fragments)
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(large_file_body)
        temp_file.close()
        result = await rust_client.upload_multipart_from_file(temp_file.name, large_file_path)
        assert result == large_file_size

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
        result = await rust_client.download(file_path, temp_file.name)
        assert result == len(file_body_bytes)
        with open(temp_file.name, "rb") as f:
            assert f.read() == file_body_bytes

    # Test download_multipart_to_file with a large file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.close()
        result = await rust_client.download_multipart_to_file(large_file_path, temp_file.name)
        assert result == large_file_size
        assert os.path.getsize(temp_file.name) == large_file_size
        # Assert file content is the same
        with open(temp_file.name, "rb") as f:
            downloaded = f.read()
        assert downloaded == large_file_body

    # Delete the file.
    storage_client.delete(path=file_path)
    storage_client.delete(path=large_file_path)

    # Test get a non-existent file
    with pytest.raises(RustClientError) as exc_info:
        await rust_client.get(file_path)
    assert exc_info.value.args[1] == 404


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

        # Run tests
        await run_rust_client_operations(rust_client, storage_client)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
@pytest.mark.asyncio
async def test_rustclient_with_refreshable_credentials(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        config_dict = temp_data_store.profile_config_dict()
        # The credentials are valid for 905 seconds before the refresh, refresh threshold is 15 minutes for Rust Client.
        # After refresh, the credentials are invalid.
        credentials_provider = RefreshableTestCredentialsProvider(
            access_key=config_dict["credentials_provider"]["options"]["access_key"],
            secret_key=config_dict["credentials_provider"]["options"]["secret_key"],
            expiration=(datetime.now(timezone.utc) + timedelta(seconds=905)).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
        py_client_config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        py_storage_client = StorageClient(
            config=StorageClientConfig.from_dict(config_dict=py_client_config_dict, profile=profile)
        )

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

        # Test Rust client proactively refreshes credentials 15 minutes before expiration, should call refresh_credentials and fail
        time.sleep(6)
        with pytest.raises(RustClientError) as exc_info:
            await rust_client.get(file_path)
        assert exc_info.value.args[1] == 403
        assert credentials_provider.refresh_count == 1

        error_message = str(exc_info.value)
        assert "The operation lacked the necessary privileges" in error_message or "403 Forbidden" in error_message, (
            f"Expected access error in message, but got: {error_message}"
        )

        # Delete the file.
        py_storage_client.delete(path=file_path)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
@pytest.mark.asyncio
async def test_rustclient_with_refreshable_credentials_expect_error(
    temp_data_store_type: Type[tempdatastore.TemporaryDataStore],
):
    with temp_data_store_type() as temp_data_store:
        config_dict = temp_data_store.profile_config_dict()

        # The credentials are valid for 899 seconds before the refresh, refresh threshold is 15 minutes for Rust Client.
        credentials_provider = RefreshableTestCredentialsProvider(
            access_key=config_dict["credentials_provider"]["options"]["access_key"],
            secret_key=config_dict["credentials_provider"]["options"]["secret_key"],
            expiration=(datetime.now(timezone.utc) + timedelta(seconds=899)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            refresh_error=True,
        )

        rust_client = RustClient(
            provider="s3",
            configs={
                "bucket": config_dict["storage_provider"]["options"]["base_path"],
                "endpoint_url": config_dict["storage_provider"]["options"]["endpoint_url"],
                "allow_http": config_dict["storage_provider"]["options"]["endpoint_url"].startswith("http://"),
            },
            credentials_provider=credentials_provider,
        )

        # Create a file
        file_extension = ".txt"
        file_path_fragments = [f"{uuid.uuid4().hex}-prefix", "infix", f"suffix{file_extension}"]
        file_path = os.path.join(*file_path_fragments)
        file_body_bytes = b"\x00\x01\x02" * 3

        # Test Rust client proactively refreshes credentials 15 minutes before expiration, should call refresh_credentials and fail
        with pytest.raises(RustRetryableError) as exc_info:
            await rust_client.put(file_path, file_body_bytes)
        assert credentials_provider.refresh_count == 1

        # Verify the error message indicates refresh failure (Python-side exception)
        error_message = str(exc_info.value)
        assert "Failed to refresh credentials" in error_message, (
            f"Expected refresh failure error in message, but got: {error_message}"
        )


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
@pytest.mark.asyncio
async def test_rustclient_list_recursive(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
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

        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        test_prefix = f"test-list-recursive-{uuid.uuid4().hex}"

        test_files = [
            f"{test_prefix}/file1.txt",
            f"{test_prefix}/file2.txt",
            f"{test_prefix}/subdir1/file3.txt",
            f"{test_prefix}/subdir1/file4.txt",
            f"{test_prefix}/subdir2/file5.txt",
            f"{test_prefix}/subdir1/nested/file6.txt",
        ]

        file_content = b"test content for list_recursive"

        for file_path in test_files:
            await rust_client.put(file_path, file_content)

        result = rust_client.list_recursive([test_prefix])

        assert hasattr(result, "objects")
        assert hasattr(result, "prefixes")
        assert isinstance(result.objects, list)
        assert isinstance(result.prefixes, list)

        assert len(result.objects) == 6

        for obj in result.objects:
            assert hasattr(obj, "key")
            assert hasattr(obj, "content_length")
            assert hasattr(obj, "last_modified")
            assert hasattr(obj, "object_type")
            assert hasattr(obj, "etag")
            assert obj.object_type == "file"
            assert obj.content_length == len(file_content)
            assert obj.key in test_files

        expected_dirs = [
            f"{test_prefix}/subdir1",
            f"{test_prefix}/subdir2",
            f"{test_prefix}/subdir1/nested",
        ]

        dir_keys = [obj.key for obj in result.prefixes]
        assert len(dir_keys) == len(expected_dirs)
        for expected_dir in expected_dirs:
            assert any(dir_key == expected_dir for dir_key in dir_keys), (
                f"Expected directory {expected_dir} not found in {dir_keys}"
            )

        limited_result = rust_client.list_recursive([test_prefix], limit=3)
        assert len(limited_result.objects) == 3

        txt_result = rust_client.list_recursive([test_prefix], suffix=".txt")
        assert all(obj.key.endswith(".txt") for obj in txt_result.objects)

        depth_1_result = rust_client.list_recursive([test_prefix], max_depth=1)
        for obj in depth_1_result.objects:
            path_parts = obj.key.replace(test_prefix + "/", "").split("/")
            assert len(path_parts) <= 2, f"File {obj.key} exceeds max_depth=1"

        multi_prefix_result = rust_client.list_recursive([f"{test_prefix}/subdir1", f"{test_prefix}/subdir2"])
        assert len(multi_prefix_result.objects) == 4

        concurrency_result = rust_client.list_recursive([test_prefix], max_concurrency=4)
        assert len(concurrency_result.objects) == 6

        for file_path in test_files:
            storage_client.delete(path=file_path)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
@pytest.mark.asyncio
async def test_rustclient_explicit_multipart_chunksize(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
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

        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        large_file_size = MEMORY_LOAD_LIMIT + 1
        large_data_bytes = os.urandom(large_file_size)
        file_extension = ".txt"
        large_file_path_fragments = [f"{uuid.uuid4().hex}-prefix", "infix", f"multipart_suffix{file_extension}"]
        large_file_path = os.path.join(*large_file_path_fragments)

        # Test upload_multipart_from_bytes with large data bytes
        chunk_size = 10 * 1024 * 1024
        max_concurrency = 4
        result = await rust_client.upload_multipart_from_bytes(
            large_file_path, large_data_bytes, multipart_chunksize=chunk_size, max_concurrency=max_concurrency
        )
        assert result == large_file_size
        # Test upload_multipart_from_bytes with BytesIO object
        with io.BytesIO(large_data_bytes) as bytes_io:
            result = await rust_client.upload_multipart_from_bytes(large_file_path, bytes_io.getbuffer())
        assert result == large_file_size

        # Test download_multipart_to_bytes with large data bytes
        result = await rust_client.download_multipart_to_bytes(
            large_file_path, multipart_chunksize=chunk_size, max_concurrency=max_concurrency
        )
        assert result == large_data_bytes

        # Test download_multipart_to_bytes with range
        result = await rust_client.download_multipart_to_bytes(
            large_file_path,
            range=Range(10, chunk_size * max_concurrency + 1),
            multipart_chunksize=chunk_size,
            max_concurrency=max_concurrency,
        )
        assert result == large_data_bytes[10 : 10 + chunk_size * max_concurrency + 1]

        # Delete the file.
        storage_client.delete(path=large_file_path)

        # Test upload_multipart_from_file with explicit chunk size and concurrency
        large_file_path_fragments = [f"{uuid.uuid4().hex}-prefix", "infix", f"multipart_suffix{file_extension}"]
        large_file_path = os.path.join(*large_file_path_fragments)
        chunk_size = 10 * 1024 * 1024
        max_concurrency = 4
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(large_data_bytes)
            temp_file.close()
            result = await rust_client.upload_multipart_from_file(
                temp_file.name, large_file_path, multipart_chunksize=chunk_size, max_concurrency=max_concurrency
            )
            assert result == large_file_size

        # Test download_multipart_to_file with explicit chunk size and concurrency
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.close()
            result = await rust_client.download_multipart_to_file(
                large_file_path, temp_file.name, multipart_chunksize=chunk_size, max_concurrency=max_concurrency
            )
            assert result == large_file_size
            assert os.path.getsize(temp_file.name) == large_file_size
            # Assert file content is the same
            with open(temp_file.name, "rb") as f:
                downloaded = f.read()
            assert downloaded == large_data_bytes

        # Delete the file.
        storage_client.delete(path=large_file_path)


@pytest.mark.asyncio
async def test_rustclient_public_bucket():
    # Create a RustClient with skip_signature enabled on a public bucket
    rust_client = RustClient(
        provider="s3",
        configs={
            "bucket": "noaa-ghcn-pds",
            "region_name": "us-east-1",
            "skip_signature": True,
        },
        credentials_provider=None,
    )

    # Test that we can list objects using the Rust client directly
    objects = rust_client.list_recursive(["csv/"], limit=10).objects
    assert len(objects) > 0, "Should be able to list objects from public bucket"

    if objects:
        first_object = objects[0]
        data = await rust_client.get(first_object.key, range=Range(0, 100))
        assert len(data) == 100
        assert len(data) > 0, "Should be able to read data from public bucket"


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
@pytest.mark.asyncio
async def test_rustclient_with_aws_credentials(
    temp_data_store_type: Type[tempdatastore.TemporaryDataStore], monkeypatch: pytest.MonkeyPatch
):
    with temp_data_store_type() as temp_data_store:
        # Create a Rust client from the temp data store profile config dict
        config_dict = temp_data_store.profile_config_dict()

        monkeypatch.setenv("AWS_ACCESS_KEY_ID", config_dict["credentials_provider"]["options"]["access_key"])
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", config_dict["credentials_provider"]["options"]["secret_key"])

        rust_client = RustClient(
            provider="s3",
            configs={
                "bucket": config_dict["storage_provider"]["options"]["base_path"],
                "endpoint_url": config_dict["storage_provider"]["options"]["endpoint_url"],
                "allow_http": config_dict["storage_provider"]["options"]["endpoint_url"].startswith("http://"),
                "max_concurrency": 16,
                "multipart_chunksize": 10 * 1024 * 1024,
            },
        )

        # Create a storage client as well for operations that are not supported by the Rust client
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        await run_rust_client_operations(rust_client, storage_client)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
@pytest.mark.asyncio
async def test_rustclient_with_aws_credentials_file(
    temp_data_store_type: Type[tempdatastore.TemporaryDataStore], monkeypatch: pytest.MonkeyPatch
):
    with temp_data_store_type() as temp_data_store:
        # Create a Rust client from the temp data store profile config dict
        config_dict = temp_data_store.profile_config_dict()

        # Create a temporary AWS credentials file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".credentials", delete=False) as creds_file:
            creds_file.write("[default]\n")
            creds_file.write(f"aws_access_key_id = {config_dict['credentials_provider']['options']['access_key']}\n")
            creds_file.write(
                f"aws_secret_access_key = {config_dict['credentials_provider']['options']['secret_key']}\n"
            )
            creds_file_path = creds_file.name

        try:
            monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", creds_file_path)

            rust_client = RustClient(
                provider="s3",
                configs={
                    "bucket": config_dict["storage_provider"]["options"]["base_path"],
                    "endpoint_url": config_dict["storage_provider"]["options"]["endpoint_url"],
                    "allow_http": config_dict["storage_provider"]["options"]["endpoint_url"].startswith("http://"),
                    "max_concurrency": 16,
                    "multipart_chunksize": 10 * 1024 * 1024,
                },
            )

            # Create a storage client as well for operations that are not supported by the Rust client
            profile = "data"
            config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
            storage_client = StorageClient(
                config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile)
            )

            await run_rust_client_operations(rust_client, storage_client)
        finally:
            if os.path.exists(creds_file_path):
                os.unlink(creds_file_path)
