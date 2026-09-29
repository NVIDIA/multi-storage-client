# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, IncompleteReadError, ReadTimeoutError, ResponseStreamingError
from botocore.stub import Stubber

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers.s3 import S3StorageProvider, StaticS3CredentialsProvider
from multistorageclient.types import PreconditionFailedError, RetryableError
from multistorageclient_rust import RustClient, RustClientError, RustRetryableError


class _FailingRustClient:
    async def upload_multipart_from_bytes(self, key, data):
        raise RustRetryableError("Connection reset by peer")


def _create_s3_provider() -> S3StorageProvider:
    credentials_provider = StaticS3CredentialsProvider(
        access_key="test_access_key",
        secret_key="test_secret_key",
    )
    return S3StorageProvider(
        region_name="us-west-2",
        endpoint_url="https://s3.amazonaws.com",
        credentials_provider=credentials_provider,
    )


def _create_client_error(status_code: int, error_code: str, message: str = "Test error message") -> ClientError:
    return ClientError(
        error_response={
            "Error": {
                "Code": error_code,
                "Message": message,
            },
            "ResponseMetadata": {
                "HTTPStatusCode": status_code,
                "RequestId": "test-request-id",
                "HostId": "test-host-id",
            },
        },
        operation_name="TestOperation",
    )


def test_translate_errors_success():
    provider = _create_s3_provider()

    def success_func():
        return "success_result"

    result = provider._translate_errors(
        func=success_func,
        operation="GET",
        bucket="test-bucket",
        key="test-key",
    )

    assert result == "success_result"


def test_translate_errors_client_error_404_no_such_upload():
    provider = _create_s3_provider()
    error = _create_client_error(404, "NoSuchUpload", "The specified multipart upload does not exist.")

    def failing_func():
        raise error

    with pytest.raises(RetryableError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="PUT",
            bucket="test-bucket",
            key="test-key",
        )

    assert "Multipart upload failed" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)


def test_translate_errors_client_error_404_file_not_found():
    provider = _create_s3_provider()
    error = _create_client_error(404, "NoSuchKey")

    def failing_func():
        raise error

    with pytest.raises(FileNotFoundError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="GET",
            bucket="test-bucket",
            key="test-key",
        )

    assert "test-bucket/test-key" in str(exc_info.value)
    assert "does not exist" in str(exc_info.value)
    assert "test-request-id" in str(exc_info.value)


def test_translate_errors_client_error_412_precondition_failed():
    provider = _create_s3_provider()
    error = _create_client_error(412, "PreconditionFailed")

    def failing_func():
        raise error

    with pytest.raises(PreconditionFailedError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="PUT",
            bucket="test-bucket",
            key="test-key",
        )

    assert "ETag mismatch" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)


def test_translate_errors_client_error_403() -> None:
    provider = _create_s3_provider()
    error = _create_client_error(403, "AccessDenied")

    def failing_func() -> None:
        raise error

    with pytest.raises(PermissionError, match="Permission denied to PUT") as exc_info:
        provider._translate_errors(failing_func, operation="PUT", bucket="test-bucket", key="test-key")

    assert "test-bucket/test-key" in str(exc_info.value)
    assert "test-request-id" in str(exc_info.value)
    assert "test-host-id" in str(exc_info.value)
    assert "status_code: 403" in str(exc_info.value)
    assert exc_info.value.__cause__ is error


@pytest.mark.parametrize("use_rust", [False, True], ids=["boto", "rust"])
@pytest.mark.parametrize("operation", ["upload_file", "write", "wb", "ab", "copy"])
def test_storage_client_write_denied(tmp_path: Path, use_rust: bool, operation: str) -> None:
    provider = _create_s3_provider()
    boto_error = _create_client_error(403, "AccessDenied")
    boto_client = MagicMock()
    boto_client.put_object.side_effect = boto_error
    boto_client.copy.side_effect = boto_error
    boto_client.head_object.return_value = {"ContentLength": len(b"existing")}
    boto_client.get_object.return_value = {"Body": io.BytesIO(b"existing")}
    provider._s3_client = boto_client
    provider._rust_client = None
    error = boto_error
    denied_call = boto_client.copy if operation == "copy" else boto_client.put_object

    if use_rust:
        rust_error = RustClientError("Access denied", 403)
        rust_client = MagicMock(spec=RustClient)
        rust_client.put = AsyncMock(side_effect=rust_error)
        rust_client.upload = AsyncMock(side_effect=rust_error)

        async def download(key: str, local_path: str) -> None:
            Path(local_path).write_bytes(b"existing")

        rust_client.download = AsyncMock(side_effect=download)
        provider._rust_client = rust_client
        if operation != "copy":
            error = rust_error
            denied_call = rust_client.upload if operation in ("upload_file", "ab") else rust_client.put

    client = StorageClient(StorageClientConfig(profile="test", storage_provider=provider))
    remote_path = "test-bucket/test-key"
    local_path = tmp_path / "upload.bin"
    local_path.write_bytes(b"new content")

    with pytest.raises(PermissionError, match="Permission denied") as exc_info:
        if operation == "upload_file":
            client.upload_file(remote_path, str(local_path))
        elif operation == "write":
            client.write(remote_path, b"new content")
        elif operation == "copy":
            client.copy("test-bucket/source-key", remote_path)
        else:
            with client.open(remote_path, operation) as file:
                file.write(b"new content")

    assert "test-bucket/test-key" in str(exc_info.value)
    assert exc_info.value.__cause__ is error
    denied_call.assert_called_once()


@pytest.mark.parametrize(
    ("status_code", "error_code", "expected_error"),
    [(403, "AccessDenied", PermissionError), (500, "InternalError", RuntimeError)],
)
def test_multipart_upload_error_translation(
    tmp_path: Path, status_code: int, error_code: str, expected_error: type[Exception]
) -> None:
    provider = _create_s3_provider()
    provider._multipart_threshold = 1
    provider._transfer_config = TransferConfig(
        multipart_threshold=1, use_threads=False, preferred_transfer_client="classic"
    )
    client = StorageClient(StorageClientConfig(profile="test", storage_provider=provider))
    local_path = tmp_path / "upload.bin"
    local_path.write_bytes(b"new content")

    with Stubber(provider._s3_client) as stubber:
        stubber.add_client_error(
            "create_multipart_upload",
            service_error_code=error_code,
            http_status_code=status_code,
        )
        with pytest.raises(expected_error) as exc_info:
            client.upload_file("test-bucket/test-key", str(local_path))
        assert "test-bucket/test-key" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, S3UploadFailedError)
        assert isinstance(exc_info.value.__cause__.__context__, ClientError)
        assert exc_info.value.__cause__.__context__.response["ResponseMetadata"]["HTTPStatusCode"] == status_code
        stubber.assert_no_pending_responses()


def test_translate_errors_client_error_429_too_many_requests():
    provider = _create_s3_provider()
    error = _create_client_error(429, "TooManyRequests")

    def failing_func():
        raise error

    with pytest.raises(RetryableError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="DELETE",
            bucket="test-bucket",
            key="test-key",
        )

    assert "Too many request" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)


def test_translate_errors_client_error_503_service_unavailable():
    provider = _create_s3_provider()
    error = _create_client_error(503, "ServiceUnavailable")

    def failing_func():
        raise error

    with pytest.raises(RetryableError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="GET",
            bucket="test-bucket",
            key="test-key",
        )

    assert "Service unavailable" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)


def test_translate_errors_client_error_501_not_implemented():
    provider = _create_s3_provider()
    error = _create_client_error(501, "NotImplemented")

    def failing_func():
        raise error

    with pytest.raises(NotImplementedError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="CUSTOM",
            bucket="test-bucket",
            key="test-key",
        )

    assert "not implemented" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)


def test_translate_errors_client_error_408_request_timeout():
    provider = _create_s3_provider()
    error = _create_client_error(408, "RequestTimeout")

    def failing_func():
        raise error

    with pytest.raises(RetryableError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="PUT",
            bucket="test-bucket",
            key="test-key",
        )

    assert "Request timeout" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)


def test_translate_errors_client_error_other_status_code():
    provider = _create_s3_provider()
    error = _create_client_error(500, "InternalError")

    def failing_func():
        raise error

    with pytest.raises(RuntimeError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="GET",
            bucket="test-bucket",
            key="test-key",
        )

    assert "Failed to GET" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)
    assert "ClientError" in str(exc_info.value)


def test_translate_errors_rust_client_error_404():
    provider = _create_s3_provider()
    error = RustClientError("Object not found", 404)

    def failing_func():
        raise error

    with pytest.raises(FileNotFoundError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="GET",
            bucket="test-bucket",
            key="test-key",
        )

    assert "test-bucket/test-key" in str(exc_info.value)
    assert "does not exist" in str(exc_info.value)


def test_translate_errors_rust_client_error_403():
    provider = _create_s3_provider()
    error = RustClientError("Access denied", 403)

    def failing_func():
        raise error

    with pytest.raises(PermissionError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="PUT",
            bucket="test-bucket",
            key="test-key",
        )

    assert "Permission denied" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)


def test_translate_errors_rust_client_error_other_status():
    provider = _create_s3_provider()
    error = RustClientError("Network error", 500)

    def failing_func():
        raise error

    with pytest.raises(RetryableError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="GET",
            bucket="test-bucket",
            key="test-key",
        )

    assert "Failed to GET" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)
    assert "status_code: 500" in str(exc_info.value)


def test_translate_errors_read_timeout_error():
    provider = _create_s3_provider()
    error = ReadTimeoutError(endpoint_url="https://s3.amazonaws.com", pool=MagicMock())

    def failing_func():
        raise error

    with pytest.raises(RetryableError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="GET",
            bucket="test-bucket",
            key="test-key",
        )

    assert "network timeout or incomplete read" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)
    assert "ReadTimeoutError" in str(exc_info.value)


def test_translate_errors_incomplete_read_error():
    provider = _create_s3_provider()
    error = IncompleteReadError(actual_bytes=100, expected_bytes=200)

    def failing_func():
        raise error

    with pytest.raises(RetryableError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="GET",
            bucket="test-bucket",
            key="test-key",
        )

    assert "network timeout or incomplete read" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)
    assert "IncompleteReadError" in str(exc_info.value)


def test_translate_errors_response_streaming_error():
    provider = _create_s3_provider()
    error = ResponseStreamingError(error=Exception("Stream failed"))

    def failing_func():
        raise error

    with pytest.raises(RetryableError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="GET",
            bucket="test-bucket",
            key="test-key",
        )

    assert "network timeout or incomplete read" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)
    assert "ResponseStreamingError" in str(exc_info.value)


def test_translate_errors_rust_retryable_error():
    provider = _create_s3_provider()
    error = RustRetryableError("Connection reset by peer")

    def failing_func():
        raise error

    with pytest.raises(RetryableError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="PUT",
            bucket="test-bucket",
            key="test-key",
        )

    assert "retryable error from Rust" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)
    assert "RustRetryableError" in str(exc_info.value)


def test_large_bytesio_upload_releases_buffer_after_rust_retryable_error():
    provider = _create_s3_provider()
    provider._rust_client = cast(RustClient, _FailingRustClient())
    provider._multipart_threshold = 1
    provider._checksum_algorithm = None

    stream = io.BytesIO(b"large enough for multipart")

    with pytest.raises(RetryableError):
        provider._upload_file("test-bucket/test-key", stream)

    stream.close()


def test_translate_errors_generic_exception():
    provider = _create_s3_provider()
    error = ValueError("Unexpected error")

    def failing_func():
        raise error

    with pytest.raises(RuntimeError) as exc_info:
        provider._translate_errors(
            func=failing_func,
            operation="GET",
            bucket="test-bucket",
            key="test-key",
        )

    assert "Failed to GET" in str(exc_info.value)
    assert "test-bucket/test-key" in str(exc_info.value)
    assert "ValueError" in str(exc_info.value)
    assert "Unexpected error" in str(exc_info.value)
