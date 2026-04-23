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
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.providers.gcs_s3 import GoogleS3StorageProvider
from multistorageclient.providers.s3 import (
    SUPPORTED_CHECKSUM_ALGORITHMS,
    S3StorageProvider,
    StaticS3CredentialsProvider,
)
from multistorageclient.providers.s8k import S8KStorageProvider


def _make_provider(
    *,
    checksum_algorithm: Optional[str] = None,
    rust_client: Optional[dict[str, Any]] = None,
    base_path: str = "test-bucket",
) -> S3StorageProvider:
    """Construct an S3StorageProvider for unit tests; callers patch the boto3 / Rust clients as needed."""
    kwargs: dict[str, Any] = {
        "region_name": "us-east-1",
        "endpoint_url": "https://s3.example.com",
        "base_path": base_path,
        "credentials_provider": StaticS3CredentialsProvider(access_key="test", secret_key="test"),
    }
    if checksum_algorithm is not None:
        kwargs["checksum_algorithm"] = checksum_algorithm
    if rust_client is not None:
        kwargs["rust_client"] = rust_client
    return S3StorageProvider(**kwargs)


def test_validate_checksum_algorithm_accepts_all_supported():
    for algo in SUPPORTED_CHECKSUM_ALGORITHMS:
        assert S3StorageProvider._validate_checksum_algorithm(algo) == algo


def test_validate_checksum_algorithm_normalizes_case():
    for variant in ("sha256", "SHA256", "Sha256", "ShA256"):
        assert S3StorageProvider._validate_checksum_algorithm(variant) == "SHA256"


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("", id="empty-string"),
        pytest.param("md5", id="unsupported-algo"),
        pytest.param(123, id="non-string"),
    ],
)
def test_validate_checksum_algorithm_rejects_invalid_inputs(value: Any):
    with pytest.raises(ValueError, match="checksum_algorithm"):
        S3StorageProvider._validate_checksum_algorithm(value)


def test_python_put_object_threads_checksum():
    """When set and Rust client is off, boto3 put_object receives ChecksumAlgorithm."""
    provider = _make_provider(checksum_algorithm="SHA256")
    provider._s3_client = MagicMock()

    provider._put_object(path="test-bucket/key.txt", body=b"hello")

    _, call_kwargs = provider._s3_client.put_object.call_args
    assert call_kwargs.get("ChecksumAlgorithm") == "SHA256"


def test_python_put_object_omits_checksum_when_unset():
    provider = _make_provider(checksum_algorithm=None)
    provider._s3_client = MagicMock()

    provider._put_object(path="test-bucket/key.txt", body=b"hello")

    _, call_kwargs = provider._s3_client.put_object.call_args
    assert "ChecksumAlgorithm" not in call_kwargs


def test_python_upload_file_threads_checksum_in_extra_args(tmp_path):
    """Large-file ``upload_file`` path threads ChecksumAlgorithm into ExtraArgs."""
    provider = _make_provider(checksum_algorithm="SHA256")
    provider._s3_client = MagicMock()
    # Force the multipart branch by lowering the threshold.
    provider._multipart_threshold = 1

    local = tmp_path / "big.bin"
    local.write_bytes(b"x" * 16)

    provider._upload_file(remote_path="test-bucket/key.bin", f=str(local))

    _, call_kwargs = provider._s3_client.upload_file.call_args
    assert call_kwargs["ExtraArgs"].get("ChecksumAlgorithm") == "SHA256"


def test_python_upload_fileobj_threads_checksum_in_extra_args():
    """Large-file BytesIO ``upload_fileobj`` path threads ChecksumAlgorithm into ExtraArgs."""
    provider = _make_provider(checksum_algorithm="SHA256")
    provider._s3_client = MagicMock()
    provider._multipart_threshold = 1

    body = io.BytesIO(b"y" * 16)
    provider._upload_file(remote_path="test-bucket/key.bin", f=body)

    _, call_kwargs = provider._s3_client.upload_fileobj.call_args
    assert call_kwargs["ExtraArgs"].get("ChecksumAlgorithm") == "SHA256"


def test_small_file_upload_routes_through_put_object_with_checksum(tmp_path):
    """Small-file ``upload_file`` with attributes routes through ``_put_object`` and threads ChecksumAlgorithm."""
    provider = _make_provider(checksum_algorithm="SHA256")
    provider._s3_client = MagicMock()

    local = tmp_path / "small.bin"
    local.write_bytes(b"abc")

    provider._upload_file(
        remote_path="test-bucket/small.bin",
        f=str(local),
        attributes={"k": "v"},
    )

    _, call_kwargs = provider._s3_client.put_object.call_args
    assert call_kwargs.get("ChecksumAlgorithm") == "SHA256"


@patch("multistorageclient.providers.s3.RustClient")
def test_rust_receives_lowercase_checksum_in_configs(rust_client_cls: MagicMock):
    rust_client_cls.return_value = MagicMock()
    _make_provider(checksum_algorithm="SHA256", rust_client={})

    _, call_kwargs = rust_client_cls.call_args
    assert call_kwargs["configs"].get("checksum_algorithm") == "sha256"


@patch("multistorageclient.providers.s3.RustClient")
def test_rust_omits_checksum_when_unset(rust_client_cls: MagicMock):
    rust_client_cls.return_value = MagicMock()
    _make_provider(checksum_algorithm=None, rust_client={})

    _, call_kwargs = rust_client_cls.call_args
    assert "checksum_algorithm" not in call_kwargs["configs"]


@patch("multistorageclient.providers.s3.RustClient")
def test_rust_rejects_non_sha256_algorithm(rust_client_cls: MagicMock):
    rust_client_cls.return_value = MagicMock()
    with pytest.raises(ValueError, match="Rust client only supports checksum_algorithm='SHA256'"):
        _make_provider(checksum_algorithm="CRC32", rust_client={})


@patch("multistorageclient.providers.s3.RustClient")
def test_rust_path_still_taken_when_only_checksum_is_set(rust_client_cls: MagicMock, tmp_path):
    """Setting checksum_algorithm does not trip the ``not extra_args`` Rust-gating check."""
    rust_client_cls.return_value = MagicMock()

    provider = _make_provider(checksum_algorithm="SHA256", rust_client={})
    provider._s3_client = MagicMock()
    provider._multipart_threshold = 1

    local = tmp_path / "big.bin"
    local.write_bytes(b"z" * 16)

    with patch("multistorageclient.providers.s3.run_async_rust_client_method") as run_rust:
        provider._upload_file(remote_path="test-bucket/key.bin", f=str(local))
        run_rust.assert_called_once()
        provider._s3_client.upload_file.assert_not_called()


@patch("multistorageclient.providers.s3.RustClient")
def test_python_fallback_with_if_match_threads_checksum(rust_client_cls: MagicMock):
    """``_put_object`` with ``if_match`` falls back to boto3 and still threads ChecksumAlgorithm."""
    rust_client_cls.return_value = MagicMock()

    provider = _make_provider(checksum_algorithm="SHA256", rust_client={})
    provider._s3_client = MagicMock()

    with patch("multistorageclient.providers.s3.run_async_rust_client_method") as run_rust:
        provider._put_object(path="test-bucket/key.txt", body=b"hello", if_match="some-etag")

        run_rust.assert_not_called()
        _, call_kwargs = provider._s3_client.put_object.call_args
        assert call_kwargs.get("ChecksumAlgorithm") == "SHA256"
        assert call_kwargs.get("IfMatch") == "some-etag"


@patch("multistorageclient.providers.s3.RustClient")
def test_top_level_checksum_overrides_nested_value(rust_client_cls: MagicMock):
    """Top-level ``checksum_algorithm`` wins on both paths and overrides any nested value."""
    rust_client_cls.return_value = MagicMock()

    provider = _make_provider(checksum_algorithm="SHA256", rust_client={"checksum_algorithm": "crc32"})

    assert provider._checksum_algorithm == "SHA256"
    _, call_kwargs = rust_client_cls.call_args
    assert call_kwargs["configs"].get("checksum_algorithm") == "sha256"


@patch("multistorageclient.providers.s3.RustClient")
def test_nested_only_checksum_drives_rust_path_only(rust_client_cls: MagicMock):
    """Nested-only ``checksum_algorithm`` reaches the Rust client; the Python path is unaffected."""
    rust_client_cls.return_value = MagicMock()

    provider = _make_provider(rust_client={"checksum_algorithm": "sha256"})

    assert provider._checksum_algorithm is None
    _, call_kwargs = rust_client_cls.call_args
    assert call_kwargs["configs"].get("checksum_algorithm") == "sha256"


@patch("multistorageclient.providers.s3.RustClient")
def test_nested_only_non_sha256_rejected_when_rust_enabled(rust_client_cls: MagicMock):
    """SHA256-only Rust constraint applies to nested-only values too."""
    rust_client_cls.return_value = MagicMock()
    with pytest.raises(ValueError, match="Rust client only supports checksum_algorithm='SHA256'"):
        _make_provider(rust_client={"checksum_algorithm": "crc32"})


@pytest.mark.parametrize(
    "extra_kwargs",
    [
        pytest.param({"checksum_algorithm": "SHA256"}, id="top-level"),
        pytest.param({"rust_client": {"checksum_algorithm": "sha256"}}, id="nested-in-rust-client"),
    ],
)
def test_gcs_s3_rejects_checksum_algorithm(extra_kwargs: dict[str, Any]):
    with pytest.raises(ValueError, match="checksum_algorithm is not supported for gcs_s3"):
        GoogleS3StorageProvider(
            region_name="us-east-1",
            endpoint_url="https://storage.googleapis.com",
            base_path="bucket",
            credentials_provider=StaticS3CredentialsProvider(access_key="a", secret_key="b"),
            **extra_kwargs,
        )


@pytest.mark.parametrize(
    "algo",
    ["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"],
)
def test_s8k_accepts_all_checksum_algorithms(algo: str):
    """SwiftStack accepts all five flexible checksum algorithms."""
    provider = S8KStorageProvider(
        region_name="us-east-1",
        endpoint_url="https://s8k.example.com",
        base_path="bucket",
        credentials_provider=StaticS3CredentialsProvider(access_key="a", secret_key="b"),
        checksum_algorithm=algo,
    )
    assert provider._checksum_algorithm == algo


@patch("multistorageclient.providers.s3.RustClient")
def test_s8k_propagates_sha256_to_rust_client(rust_client_cls: MagicMock):
    rust_client_cls.return_value = MagicMock()
    S8KStorageProvider(
        region_name="us-east-1",
        endpoint_url="https://s8k.example.com",
        base_path="bucket",
        credentials_provider=StaticS3CredentialsProvider(access_key="a", secret_key="b"),
        checksum_algorithm="SHA256",
        rust_client={},
    )

    _, call_kwargs = rust_client_cls.call_args
    assert call_kwargs["configs"].get("checksum_algorithm") == "sha256"
