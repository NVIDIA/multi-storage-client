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

import asyncio
import io
import json
import os
import stat
import sys
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from test_multistorageclient.unit.utils import tempdatastore

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.constants import MEMORY_LOAD_LIMIT
from multistorageclient.providers.s3 import StaticS3CredentialsProvider
from multistorageclient.types import BatchTransferError, Range, RetryableError
from multistorageclient_rust import (  # pyright: ignore[reportAttributeAccessIssue]
    RustClient,
    RustClientError,
    RustRetryableError,
    RustRetryConfig,
)

from .utils import RefreshableTestCredentialsProvider


@pytest.mark.parametrize("multipart", [False, True])
@pytest.mark.parametrize("stalled", [False, True])
@pytest.mark.asyncio
async def test_rustclient_download_read_timeout(tmp_path, multipart, stalled):
    body = b"abcdef"
    tasks: list[asyncio.Task] = []

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        task = asyncio.current_task()
        assert task is not None
        tasks.append(task)
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            is_head = request.startswith(b"HEAD ")
            is_range = b"range: " in request.lower()
            status = "206 Partial Content" if is_range else "200 OK"
            headers = (
                f"HTTP/1.1 {status}\r\nContent-Length: {len(body)}\r\n"
                'ETag: "test"\r\nLast-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\n'
                "Connection: close\r\n"
            )
            if is_range:
                headers += f"Content-Range: bytes 0-{len(body) - 1}/{len(body)}\r\n"
            writer.write((headers + "\r\n").encode())
            await writer.drain()
            if not is_head:
                if stalled:
                    await asyncio.sleep(1.5)
                    writer.write(body)
                    await writer.drain()
                else:
                    for byte in body:
                        await asyncio.sleep(0.3)
                        writer.write(bytes([byte]))
                        await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with server:
            client = RustClient(
                provider="s3",
                configs={
                    "bucket": "test-bucket",
                    "endpoint_url": f"http://127.0.0.1:{port}",
                    "allow_http": True,
                    "skip_signature": True,
                    "read_timeout": 1,
                    "multipart_chunksize": len(body),
                },
                retry=RustRetryConfig(attempts=0),
            )
            path = tmp_path / "download"

            async def download():
                if multipart:
                    return await client.download_multipart_to_file("key", str(path))
                return await client.download("key", str(path))

            if stalled:
                with pytest.raises(RustRetryableError, match="timed out"):
                    await download()
            else:
                assert await download() == len(body)
                assert await asyncio.to_thread(path.read_bytes) == body
    finally:
        await asyncio.gather(*tasks)


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
    os.unlink(temp_file.name)

    # Verify the file was uploaded successfully using multi-storage client
    assert storage_client.is_file(path=file_path)
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.close()
        storage_client.download_file(remote_path=file_path, local_path=temp_file.name)
        with open(temp_file.name, "rb") as f:  # noqa: ASYNC230
            assert f.read() == file_body_bytes
    os.unlink(temp_file.name)

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
    os.unlink(temp_file.name)

    # Verify the large file was uploaded successfully using multi-storage client
    assert storage_client.is_file(path=large_file_path)
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.close()
        storage_client.download_file(remote_path=large_file_path, local_path=temp_file.name)
        assert os.path.getsize(temp_file.name) == large_file_size
        # Assert file content is the same
        with open(temp_file.name, "rb") as f:  # noqa: ASYNC230
            downloaded = f.read()
        assert downloaded == large_file_body
    os.unlink(temp_file.name)

    # Test download the file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.close()
        result = await rust_client.download(file_path, temp_file.name)
        assert result == len(file_body_bytes)
        with open(temp_file.name, "rb") as f:  # noqa: ASYNC230
            assert f.read() == file_body_bytes
    os.unlink(temp_file.name)

    # Test download_multipart_to_file with a large file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.close()
        result = await rust_client.download_multipart_to_file(large_file_path, temp_file.name)
        assert result == large_file_size
        assert os.path.getsize(temp_file.name) == large_file_size
        # Assert file content is the same
        with open(temp_file.name, "rb") as f:  # noqa: ASYNC230
            downloaded = f.read()
        assert downloaded == large_file_body
    os.unlink(temp_file.name)

    # Delete the file.
    storage_client.delete(path=file_path)
    storage_client.delete(path=large_file_path)

    # Test get a non-existent file
    with pytest.raises(RustClientError) as exc_info:
        await rust_client.get(file_path)
    assert exc_info.value.args[1] == 404

    # Test with special characters in file path (URL encoded)
    prefix = f"{uuid.uuid4().hex}"
    special_chars_path = f"{prefix}/%28sici%291096-8628%2819960122%29test{file_extension}"
    special_chars_body = b"test content with special chars in path"

    result = await rust_client.put(special_chars_path, special_chars_body)
    assert result == len(special_chars_body)
    result = await rust_client.get(special_chars_path)
    assert result == special_chars_body

    storage_client.delete(path=special_chars_path)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
@pytest.mark.asyncio
async def test_rustclient_basic_operations(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        # Create a Rust client from the temp data store profile config dict
        config_dict = temp_data_store.profile_config_dict()
        credentials_provider = StaticS3CredentialsProvider(
            access_key=config_dict["credentials_provider"]["options"]["access_key"],
            secret_key=config_dict["credentials_provider"]["options"]["secret_key"],
        )

        retry_config = RustRetryConfig(
            attempts=5,
            timeout=60,
            init_backoff_ms=1000,
            max_backoff=10,
            backoff_multiplier=2.0,
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
            retry=retry_config,
        )

        # Create a storage client as well for operations that are not supported by the Rust client
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        # Run tests
        await run_rust_client_operations(rust_client, storage_client)


@pytest.mark.parametrize(
    "temp_data_store_type",
    [tempdatastore.TemporaryAWSS3Bucket, tempdatastore.TemporaryGoogleCloudStorageS3Bucket],
)
@pytest.mark.asyncio
async def test_rustclient_upload_attributes_s3_api(tmp_path, temp_data_store_type):
    with temp_data_store_type() as temp_data_store:
        config = temp_data_store.profile_config_dict()
        provider = config["storage_provider"]["type"]
        bucket = config["storage_provider"]["options"]["base_path"]
        endpoint = config["storage_provider"]["options"]["endpoint_url"]
        credentials = config["credentials_provider"]["options"]
        rust_client = RustClient(
            provider=provider,
            configs={"bucket": bucket, "endpoint_url": endpoint, "region_name": "us-east-1", "allow_http": True},
            credentials_provider=StaticS3CredentialsProvider(
                access_key=credentials["access_key"], secret_key=credentials["secret_key"]
            ),
        )
        attributes = {"test-key": "test-value"}
        small_file = tmp_path / "small.bin"
        small_file.write_bytes(b"file")
        multipart_data = b"x" * (5 * 1024 * 1024 + 1)
        large_file = tmp_path / "large.bin"
        large_file.write_bytes(multipart_data)

        assert await rust_client.put("attributes/put", b"put", attributes=attributes) == 3
        assert await rust_client.upload(str(small_file), "attributes/upload", attributes=attributes) == 4
        assert (
            await rust_client.upload_multipart_from_bytes(
                "attributes/bytes-small", b"small", multipart_chunksize=5 * 1024 * 1024, attributes=attributes
            )
            == 5
        )
        assert await rust_client.upload_multipart_from_bytes(
            "attributes/bytes-large", multipart_data, multipart_chunksize=5 * 1024 * 1024, attributes=attributes
        ) == len(multipart_data)
        assert await rust_client.upload_multipart_from_file(
            str(large_file), "attributes/file-large", multipart_chunksize=5 * 1024 * 1024, attributes=attributes
        ) == len(multipart_data)

        for key, size in (
            ("attributes/put", 3),
            ("attributes/upload", 4),
            ("attributes/bytes-small", 5),
            ("attributes/bytes-large", len(multipart_data)),
            ("attributes/file-large", len(multipart_data)),
        ):
            result = temp_data_store._client.head_object(Bucket=bucket, Key=key)
            assert result["ContentLength"] == size
            assert result["Metadata"] == attributes

        for invalid_attributes, error in (
            ({"my key": "value"}, "Invalid metadata key"),
            ({"key": "line\nfeed"}, "Invalid metadata value"),
        ):
            with pytest.raises(ValueError, match=error):
                await rust_client.put("attributes/invalid-put", b"put", attributes=invalid_attributes)
            with pytest.raises(ValueError, match=error):
                await rust_client.upload(str(small_file), "attributes/invalid-upload", attributes=invalid_attributes)
            with pytest.raises(ValueError, match=error):
                await rust_client.upload_multipart_from_bytes(
                    "attributes/invalid-bytes", b"bytes", attributes=invalid_attributes
                )
            with pytest.raises(ValueError, match=error):
                await rust_client.upload_multipart_from_file(
                    str(large_file), "attributes/invalid-file", attributes=invalid_attributes
                )


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
@pytest.mark.asyncio
async def test_rustclient_basic_operations_with_sha256_checksum(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
):
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
                "checksum_algorithm": "sha256",
            },
            credentials_provider=credentials_provider,
        )

        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        await run_rust_client_operations(rust_client, storage_client)


def test_rustclient_invalid_checksum_algorithm_raises():
    with pytest.raises(ValueError, match="checksum_algorithm"):
        RustClient(
            provider="s3",
            configs={
                "bucket": "test-bucket",
                "endpoint_url": "http://localhost:7070",
                "region_name": "us-east-1",
                "allow_http": True,
                "checksum_algorithm": "md5",
            },
            credentials_provider=StaticS3CredentialsProvider(access_key="a", secret_key="b"),
        )


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
@pytest.mark.asyncio
async def test_rustclient_with_refreshable_credentials(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        config_dict = temp_data_store.profile_config_dict()
        # The credentials are valid for 605 seconds before the refresh, refresh threshold is 10 minutes for Rust Client.
        # After refresh, the credentials are invalid.
        credentials_provider = RefreshableTestCredentialsProvider(
            access_key=config_dict["credentials_provider"]["options"]["access_key"],
            secret_key=config_dict["credentials_provider"]["options"]["secret_key"],
            expiration=(datetime.now(UTC) + timedelta(seconds=605)).strftime("%Y-%m-%dT%H:%M:%SZ"),
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

        # Test Rust client proactively refreshes credentials 10 minutes before expiration, should call refresh_credentials and fail
        time.sleep(6)  # noqa: ASYNC251
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
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
):
    with temp_data_store_type() as temp_data_store:
        config_dict = temp_data_store.profile_config_dict()

        # The credentials are valid for 599 seconds before the refresh, refresh threshold is 10 minutes for Rust Client.
        credentials_provider = RefreshableTestCredentialsProvider(
            access_key=config_dict["credentials_provider"]["options"]["access_key"],
            secret_key=config_dict["credentials_provider"]["options"]["secret_key"],
            expiration=(datetime.now(UTC) + timedelta(seconds=599)).strftime("%Y-%m-%dT%H:%M:%SZ"),
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

        # Test Rust client proactively refreshes credentials 10 minutes before expiration, should call refresh_credentials and fail
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
async def test_rustclient_list_recursive(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
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

        result = await rust_client.list_recursive([test_prefix])

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

        limited_result = await rust_client.list_recursive([test_prefix], limit=3)
        assert len(limited_result.objects) == 3

        txt_result = await rust_client.list_recursive([test_prefix], suffix=".txt")
        assert all(obj.key.endswith(".txt") for obj in txt_result.objects)

        depth_1_result = await rust_client.list_recursive([test_prefix], max_depth=1)
        for obj in depth_1_result.objects:
            path_parts = obj.key.replace(test_prefix + "/", "").split("/")
            assert len(path_parts) <= 2, f"File {obj.key} exceeds max_depth=1"

        multi_prefix_result = await rust_client.list_recursive([f"{test_prefix}/subdir1", f"{test_prefix}/subdir2"])
        assert len(multi_prefix_result.objects) == 4

        concurrency_result = await rust_client.list_recursive([test_prefix], max_concurrency=4)
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
async def test_rustclient_explicit_multipart_chunksize(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
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
        os.unlink(temp_file.name)

        # Test download_multipart_to_file with explicit chunk size and concurrency
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.close()
            result = await rust_client.download_multipart_to_file(
                large_file_path, temp_file.name, multipart_chunksize=chunk_size, max_concurrency=max_concurrency
            )
            assert result == large_file_size
            assert os.path.getsize(temp_file.name) == large_file_size
            # Assert file content is the same
            with open(temp_file.name, "rb") as f:  # noqa: ASYNC230
                downloaded = f.read()
            assert downloaded == large_data_bytes
        os.unlink(temp_file.name)

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
    result = await rust_client.list_recursive(["csv/"], limit=10)
    objects = result.objects
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
    temp_data_store_type: type[tempdatastore.TemporaryDataStore], monkeypatch: pytest.MonkeyPatch
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
    temp_data_store_type: type[tempdatastore.TemporaryDataStore], monkeypatch: pytest.MonkeyPatch
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


class _DownloadServer:
    def __init__(self):
        self.body = b"abcdefghijk"
        self.etag = '"revision-1"'
        self.fail_at: int | None = 4
        self.mutate_at: int | None = None
        self.requests: list[int] = []
        self.conditions: list[str | None] = []
        self.pause_zero = False
        self.release_zero = asyncio.Event()
        self.tasks: list[asyncio.Task] = []

    async def __aenter__(self):
        self.server = await asyncio.start_server(self.serve, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *_):
        self.release_zero.set()
        self.server.close()
        await self.server.wait_closed()
        await asyncio.gather(*self.tasks)

    def configs(self, chunk_size=4):
        return {
            "bucket": "test-bucket",
            "endpoint_url": f"http://127.0.0.1:{self.port}",
            "allow_http": True,
            "skip_signature": True,
            "multipart_chunksize": chunk_size,
            "max_concurrency": 1,
        }

    def client(self, chunk_size=4):
        return RustClient(configs=self.configs(chunk_size), retry=RustRetryConfig(attempts=0))

    async def serve(self, reader, writer):
        task = asyncio.current_task()
        assert task is not None
        self.tasks.append(task)
        try:
            request = (await reader.readuntil(b"\r\n\r\n")).decode()
            headers = {}
            for line in request.split("\r\n")[1:]:
                if ":" in line:
                    key, value = line.split(":", 1)
                    headers[key.lower()] = value.strip()
            is_head = request.startswith("HEAD ")
            start, end = 0, len(self.body)
            status = "200 OK"
            if not is_head:
                start, last = map(int, headers["range"].removeprefix("bytes=").split("-"))
                end = last + 1
                self.requests.append(start)
                self.conditions.append(headers.get("if-match"))
                if start == 0 and self.pause_zero:
                    await self.release_zero.wait()
                status = "206 Partial Content"
                if start == self.mutate_at:
                    self.etag = '"revision-2"'
                if headers.get("if-match") is not None and headers["if-match"] != self.etag:
                    writer.write(b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    await writer.drain()
                    return
            content = self.body[start:end]
            response = (
                f"HTTP/1.1 {status}\r\nContent-Length: {len(content)}\r\n"
                "Last-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\nConnection: close\r\n"
            )
            if self.etag:
                response += f"ETag: {self.etag}\r\n"
            if not is_head:
                response += f"Content-Range: bytes {start}-{end - 1}/{len(self.body)}\r\n"
            writer.write((response + "\r\n").encode())
            if not is_head:
                writer.write(content[:1] if start == self.fail_at else content)
            await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass


_DOWNLOAD_SCRIPT = """
import asyncio, json, sys
from multistorageclient_rust import RustClient, RustRetryConfig
async def main():
    client = RustClient(configs=json.loads(sys.argv[1]), retry=RustRetryConfig(attempts=0))
    await client.download_multipart_to_file('key', sys.argv[2])
asyncio.run(main())
"""


@pytest.mark.asyncio
async def test_rustclient_download_resume_new_process(tmp_path):
    async with _DownloadServer() as server:
        path = tmp_path / "download"
        path.write_bytes(b"original destination")
        with pytest.raises(RustRetryableError):
            await server.client().download_multipart_to_file("key", str(path))
        state_dir = tmp_path / ".download.msc-dl"
        assert json.loads((state_dir / "state.json").read_text())["key"] == "key"
        assert (state_dir / "000000.chunk").read_bytes() == server.body[:4]
        assert path.read_bytes() == b"original destination"
        (state_dir / "000001.chunk.Abc123.tmp").write_bytes(b"incomplete")
        server.fail_at = None
        server.requests.clear()
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            _DOWNLOAD_SCRIPT,
            json.dumps(server.configs(8)),
            str(path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await process.communicate()
        assert process.returncode == 0, stderr.decode()
        assert server.requests == [4, 8]
        assert all(condition == server.etag for condition in server.conditions)
        assert path.read_bytes() == server.body
        assert not state_dir.exists()


@pytest.mark.parametrize("change", ["invalid_chunk", "key", "revision"])
@pytest.mark.asyncio
async def test_rustclient_download_resume_invalid_chunk_and_source(tmp_path, change):
    async with _DownloadServer() as server:
        path = tmp_path / "download"
        with pytest.raises(RustRetryableError):
            await server.client().download_multipart_to_file("key", str(path))
        if change == "revision":
            server.etag = '"revision-2"'
            server.body = server.body.upper()
        elif change == "invalid_chunk":
            (tmp_path / ".download.msc-dl" / "000000.chunk").write_bytes(b"short")
        server.fail_at = None
        server.requests.clear()
        key = "other-key" if change == "key" else "key"
        assert await server.client().download_multipart_to_file(key, str(path)) == len(server.body)
        assert server.requests == [0, 4, 8]
        assert path.read_bytes() == server.body


@pytest.mark.asyncio
async def test_rustclient_download_resume_remote_replacement(tmp_path):
    async with _DownloadServer() as server:
        server.fail_at = None
        server.mutate_at = 4
        path = tmp_path / "download"
        with pytest.raises(RustClientError) as exc:
            await server.client().download_multipart_to_file("key", str(path))
        assert exc.value.args[1] == 412
        assert not path.exists()
        assert (tmp_path / ".download.msc-dl" / "000000.chunk").read_bytes() == server.body[:4]


@pytest.mark.asyncio
async def test_rustclient_download_resume_publication_failure(tmp_path):
    async with _DownloadServer() as server:
        server.fail_at = None
        path = tmp_path / "download"
        path.mkdir()
        with pytest.raises(RuntimeError):
            await server.client().download_multipart_to_file("key", str(path))
        assert (tmp_path / ".download.msc-dl" / "000002.chunk").read_bytes() == server.body[8:]
        path.rmdir()
        server.requests.clear()
        assert await server.client().download_multipart_to_file("key", str(path)) == len(server.body)
        assert server.requests == []
        assert path.read_bytes() == server.body
        assert not (tmp_path / ".download.msc-dl").exists()


@pytest.mark.parametrize("marker_kind", ["missing", "corrupt", "unrecognized", "unreadable"])
@pytest.mark.asyncio
async def test_rustclient_download_resume_directory_conflict(tmp_path, marker_kind):
    async with _DownloadServer() as server:
        state_dir = tmp_path / ".download.msc-dl"
        state_dir.mkdir()
        existing = state_dir / "user-file"
        existing.write_bytes(b"preserve")
        interrupted = state_dir / "merged.Abc123.tmp"
        interrupted.write_bytes(b"preserve without a valid marker")
        marker = state_dir / "state.json"
        if marker_kind == "unreadable" and os.name == "posix" and os.geteuid() == 0:
            pytest.skip("Requires permission enforcement")
        if marker_kind != "missing":
            marker.write_text("{broken" if marker_kind == "corrupt" else json.dumps({"format": "other"}))
        original_marker = marker.read_bytes() if marker.exists() else None
        if marker_kind == "unreadable":
            marker.chmod(0)
        try:
            if marker_kind == "missing":
                server.fail_at = None
                path = tmp_path / "download"
                assert await server.client().download_multipart_to_file("key", str(path)) == len(server.body)
                assert path.read_bytes() == server.body
            else:
                with pytest.raises(RuntimeError):
                    await server.client().download_multipart_to_file("key", str(tmp_path / "download"))
        finally:
            if marker_kind == "unreadable":
                marker.chmod(0o600)
        assert existing.read_bytes() == b"preserve"
        if marker_kind == "missing":
            assert not interrupted.exists()
            assert json.loads(marker.read_text())["key"] == "key"
        else:
            assert interrupted.read_bytes() == b"preserve without a valid marker"
            assert (marker.read_bytes() if marker.exists() else None) == original_marker
        assert server.requests == ([0, 4, 8] if marker_kind == "missing" else [])


@pytest.mark.parametrize("leftovers", ["empty", "lock", "initializing"])
@pytest.mark.parametrize("published", [False, True])
@pytest.mark.asyncio
async def test_rustclient_download_missing_marker_reinitializes_same_directory(tmp_path, leftovers, published):
    async with _DownloadServer() as server:
        path = tmp_path / "download"
        if published:
            path.write_bytes(b"original destination")
        state_dir = tmp_path / ".download.msc-dl"
        state_dir.mkdir()
        if leftovers != "empty":
            (state_dir / "lock").write_bytes(b"")
        if leftovers == "initializing":
            (state_dir / "state.json.Abc123.tmp").write_bytes(b"{unfinished")
            (state_dir / "000000.chunk").write_bytes(b"WRNG")
        original_inode = state_dir.stat().st_ino

        for attempt in (0, 1):
            server.requests.clear()
            with pytest.raises(RustRetryableError):
                await server.client().download_multipart_to_file("key", str(path))
            assert server.requests == ([0, 4] if attempt == 0 else [4])
            assert path.exists() is published
            if published:
                assert path.read_bytes() == b"original destination"
            assert state_dir.stat().st_ino == original_inode
            assert json.loads((state_dir / "state.json").read_text())["key"] == "key"
            assert (state_dir / "000000.chunk").read_bytes() == server.body[:4]
            assert list(tmp_path.glob(".*.msc-dl")) == [state_dir]

        server.fail_at = None
        server.requests.clear()
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            _DOWNLOAD_SCRIPT,
            json.dumps(server.configs()),
            str(path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await process.communicate()
        assert process.returncode == 0, stderr.decode()
        assert server.requests == [4, 8]
        assert path.read_bytes() == server.body
        assert list(tmp_path.iterdir()) == [path]


@pytest.mark.asyncio
async def test_rustclient_download_resume_cleans_interrupted_temps(tmp_path):
    async with _DownloadServer() as server:
        path = tmp_path / "download"
        with pytest.raises(RustRetryableError):
            await server.client().download_multipart_to_file("key", str(path))
        state_dir = tmp_path / ".download.msc-dl"
        marker = (state_dir / "state.json").read_bytes()
        interrupted = ["merged.Abc123.tmp", "000001.chunk.Def456.tmp", "state.json.Ghi789.tmp"]
        unrelated = [
            "notes.tmp",
            "notes.chunk.Abc123.tmp",
            "merged.notes.tmp",
            "merged.notes.txt",
            "state.json.user.tmp",
            "000001.chunk.notes.txt",
        ]
        for name in interrupted + unrelated:
            (state_dir / name).write_bytes(b"preserve unrelated files")
        unrelated_dir = state_dir / "merged.Jkl012.tmp"
        unrelated_dir.mkdir()
        (unrelated_dir / "file").write_bytes(b"preserve directory")
        external = tmp_path / "external"
        external.write_bytes(b"preserve symlink target")
        link = state_dir / "merged.Mno345.tmp"
        link.symlink_to(external)
        server.requests.clear()
        with pytest.raises(RustRetryableError):
            await server.client().download_multipart_to_file("key", str(path))
        assert all(not (state_dir / name).exists() for name in interrupted)
        assert all((state_dir / name).read_bytes() == b"preserve unrelated files" for name in unrelated)
        assert (unrelated_dir / "file").read_bytes() == b"preserve directory"
        assert link.is_symlink() and external.read_bytes() == b"preserve symlink target"
        assert (state_dir / "state.json").read_bytes() == marker
        assert (state_dir / "000000.chunk").read_bytes() == server.body[:4]
        assert (state_dir / "lock").is_file()
        assert server.requests == [4]
        assert not path.exists()


@pytest.mark.asyncio
async def test_rustclient_download_without_validator(tmp_path):
    async with _DownloadServer() as server:
        server.fail_at = None
        server.etag = ""
        path = tmp_path / "download"
        assert await server.client().download_multipart_to_file("key", str(path)) == len(server.body)
        assert path.read_bytes() == server.body
        assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("second_process", [False, True])
@pytest.mark.asyncio
async def test_rustclient_download_concurrent_destination(tmp_path, second_process):
    async with _DownloadServer() as server:
        server.fail_at = None
        server.pause_zero = True
        configs = server.configs()
        configs["max_concurrency"] = 2
        path = tmp_path / "download"
        state_dir = tmp_path / ".download.msc-dl"
        first = asyncio.ensure_future(
            RustClient(configs=configs, retry=RustRetryConfig(attempts=0)).download_multipart_to_file("key", str(path))
        )
        second = None

        async def wait_for(predicate):
            while not predicate():
                await asyncio.sleep(0.01)

        async def download_again():
            if not second_process:
                return await RustClient(configs=configs, retry=RustRetryConfig(attempts=0)).download_multipart_to_file(
                    "key", str(path)
                )
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                _DOWNLOAD_SCRIPT,
                json.dumps(configs),
                str(path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                _, stderr = await process.communicate()
                assert process.returncode == 0, stderr.decode()
                return len(server.body)
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.communicate()

        try:
            await asyncio.wait_for(wait_for(lambda: (state_dir / "000001.chunk").exists()), timeout=10)
            active_temp = state_dir / "merged.Abc123.tmp"
            active_temp.write_bytes(b"owned by the first download")
            second = asyncio.create_task(download_again())
            await asyncio.wait_for(wait_for(lambda: server.requests.count(0) == 2), timeout=10)
            independent = list(tmp_path.glob(".msc-dl-*.msc-dl"))
            assert len(independent) == 1
            assert independent[0] != state_dir
            assert (state_dir / "000001.chunk").read_bytes() == server.body[4:8]
            assert active_temp.read_bytes() == b"owned by the first download"
            server.release_zero.set()
            assert await asyncio.gather(first, second) == [len(server.body), len(server.body)]
            assert path.read_bytes() == server.body
            assert list(tmp_path.iterdir()) == [path]
        finally:
            server.release_zero.set()
            await asyncio.gather(first, *([second] if second is not None else []), return_exceptions=True)


@pytest.mark.parametrize("with_validator", [False, True])
@pytest.mark.asyncio
async def test_rustclient_download_resume_after_process_killed(tmp_path, with_validator):
    async with _DownloadServer() as server:
        server.fail_at = None
        server.pause_zero = True
        if not with_validator:
            server.etag = ""
        configs = server.configs()
        configs["max_concurrency"] = 2
        path = tmp_path / "download"
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            _DOWNLOAD_SCRIPT,
            json.dumps(configs),
            str(path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        state_dir = tmp_path / ".download.msc-dl" if with_validator else None

        async def wait_for_chunk() -> None:
            nonlocal state_dir
            while True:
                if state_dir is None:
                    state_dir = next(tmp_path.glob(".msc-dl-*.msc-dl"), None)
                if state_dir is not None and (state_dir / "000001.chunk").exists():
                    return
                if process.returncode is not None:
                    _, stderr = await process.communicate()
                    pytest.fail(
                        f"Download subprocess exited with code {process.returncode} before completing a chunk:\n"
                        f"{stderr.decode(errors='replace')}"
                    )
                await asyncio.sleep(0.01)

        try:
            await asyncio.wait_for(wait_for_chunk(), timeout=10)
        finally:
            if process.returncode is None:
                process.kill()
            await process.communicate()
        assert state_dir is not None
        assert state_dir.name.startswith(".") and state_dir.name.endswith(".msc-dl")
        completed = {int(chunk.stem) * 4 for chunk in state_dir.glob("*.chunk")}
        assert (state_dir / "state.json").exists() is with_validator
        assert 4 in completed
        assert 0 not in completed
        server.pause_zero = False
        server.release_zero.set()
        await asyncio.gather(*server.tasks)
        server.requests.clear()
        assert await server.client().download_multipart_to_file("key", str(path)) == len(server.body)
        if with_validator:
            assert not completed.intersection(server.requests)
            assert not state_dir.exists()
        else:
            assert server.requests == [0, 4, 8]
            assert state_dir.exists()
            assert list(tmp_path.glob(".*.msc-dl")) == [state_dir]
        assert path.read_bytes() == server.body


@pytest.mark.parametrize("first_batch", [False, True])
@pytest.mark.asyncio
async def test_download_resume_single_and_batch(tmp_path, first_batch):
    async with _DownloadServer() as server:
        config = StorageClientConfig.from_dict(
            {
                "profiles": {
                    "source": {
                        "storage_provider": {
                            "type": "s3",
                            "options": {
                                "base_path": "test-bucket",
                                "endpoint_url": f"http://127.0.0.1:{server.port}",
                                "region_name": "us-east-1",
                                "multipart_threshold": 1,
                                "rust_client": {
                                    "allow_http": True,
                                    "multipart_chunksize": 4,
                                    "max_concurrency": 1,
                                    "retry": {"attempts": 0},
                                },
                            },
                        },
                        "credentials_provider": {
                            "type": "S3Credentials",
                            "options": {
                                "access_key": "test",
                                "secret_key": "test",
                            },
                        },
                    }
                }
            },
            profile="source",
        )
        client = StorageClient(config)
        path = tmp_path / "download"

        def transfer(batch):
            if batch:
                client.download_files(["key"], [str(path)])
            else:
                client.download_file("key", str(path))

        with pytest.raises(BatchTransferError if first_batch else RetryableError):
            await asyncio.to_thread(transfer, first_batch)
        assert (tmp_path / ".download.msc-dl" / "000000.chunk").read_bytes() == server.body[:4]
        server.fail_at = None
        server.requests.clear()
        await asyncio.to_thread(transfer, not first_batch)
        assert server.requests == [4, 8]
        assert path.read_bytes() == server.body
        assert not (tmp_path / ".download.msc-dl").exists()


@pytest.mark.skipif(os.name != "posix" or os.geteuid() == 0, reason="Requires POSIX permission enforcement")
@pytest.mark.parametrize("reset_source", [False, True])
@pytest.mark.asyncio
async def test_rustclient_download_cleanup_failure_keeps_recognized_state(tmp_path, reset_source):
    async with _DownloadServer() as server:
        path = tmp_path / "download"
        with pytest.raises(RustRetryableError):
            await server.client().download_multipart_to_file("key", str(path))
        state_dir = tmp_path / ".download.msc-dl"
        blocked = state_dir / "blocked"
        if reset_source:
            state_dir.chmod(0o500)
        else:
            blocked.mkdir()
            (blocked / "file").write_bytes(b"preserve unrelated contents")
        server.fail_at = None
        if reset_source:
            server.etag = '"revision-2"'
        try:
            if reset_source:
                with pytest.raises(RuntimeError):
                    await server.client().download_multipart_to_file("key", str(path))
                assert not path.exists()
            else:
                assert await server.client().download_multipart_to_file("key", str(path)) == len(server.body)
                assert path.read_bytes() == server.body
            assert json.loads((state_dir / "state.json").read_text())["format"] == "msc-download-v1"
        finally:
            state_dir.chmod(0o700)
            if not reset_source:
                assert (blocked / "file").read_bytes() == b"preserve unrelated contents"
                (blocked / "file").unlink()
                blocked.rmdir()
        assert await server.client().download_multipart_to_file("key", str(path)) == len(server.body)
        assert path.read_bytes() == server.body
        assert not state_dir.exists()


@pytest.mark.skipif(os.name != "posix" or os.geteuid() == 0, reason="Requires POSIX permission enforcement")
@pytest.mark.asyncio
async def test_rustclient_download_state_initialization_failure_removes_directory(tmp_path):
    async with _DownloadServer() as server:
        path = tmp_path / "download"
        previous_umask = os.umask(0o777)
        try:
            with pytest.raises(RuntimeError):
                await server.client().download_multipart_to_file("key", str(path))
            assert not (tmp_path / ".download.msc-dl").exists()
            assert server.requests == []
        finally:
            os.umask(previous_umask)
            state_dir = tmp_path / ".download.msc-dl"
            if state_dir.exists():
                state_dir.chmod(0o700)
        server.fail_at = None
        assert await server.client().download_multipart_to_file("key", str(path)) == len(server.body)
        assert path.read_bytes() == server.body


@pytest.mark.skipif(os.name != "posix", reason="Requires POSIX permissions")
@pytest.mark.asyncio
async def test_rustclient_download_private_permissions(tmp_path):
    async with _DownloadServer() as server:
        path = tmp_path / "download"
        path.write_bytes(b"old destination")
        path.chmod(0o644)
        previous_umask = os.umask(0)
        try:
            with pytest.raises(RustRetryableError):
                await server.client().download_multipart_to_file("key", str(path))
            state_dir = tmp_path / ".download.msc-dl"
            assert stat.S_IMODE(state_dir.stat().st_mode) == 0o700
            assert stat.S_IMODE((state_dir / "state.json").stat().st_mode) == 0o600
            assert stat.S_IMODE((state_dir / "000000.chunk").stat().st_mode) == 0o600
            server.fail_at = None
            assert await server.client().download_multipart_to_file("key", str(path)) == len(server.body)
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
            assert path.read_bytes() == server.body
        finally:
            os.umask(previous_umask)
