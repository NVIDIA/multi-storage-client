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

"""
Unit tests for the S3-over-RDMA (cuObject) data plane wiring.

The native cuObject engine is mocked, so these run anywhere -- they verify the
provider plumbing (option parsing, wire-contract config, single-shot routing,
empty-body PUT / sized GET), not the RDMA transfer itself. The transfer is
covered end-to-end against a live RDMA endpoint by ``examples/rdma_roundtrip.py``.
"""

import base64
import io
import struct
from array import array
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.providers._cuobj import CuObjEngine as _RealCuObjEngine
from multistorageclient.providers.s3 import (
    RDMA_SINGLE_SHOT_THRESHOLD,
    S3StorageProvider,
    StaticS3CredentialsProvider,
)
from multistorageclient.types import Range

_FAKE_CHECKSUM = "ZmFrZWNyYzY0"


def _make_rdma_provider(engine_cls: MagicMock, **extra: Any) -> S3StorageProvider:
    """Construct an RDMA-enabled provider with the cuObject engine mocked out."""
    engine_cls.client_config_overrides.return_value = _RealCuObjEngine.client_config_overrides()
    return S3StorageProvider(
        region_name="us-east-1",
        endpoint_url="https://s3.example.com",
        base_path="test-bucket",
        credentials_provider=StaticS3CredentialsProvider(access_key="test", secret_key="test"),
        rdma={},
        **extra,
    )


def test_rdma_and_rust_client_are_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        S3StorageProvider(
            region_name="us-east-1",
            endpoint_url="https://s3.example.com",
            base_path="test-bucket",
            credentials_provider=StaticS3CredentialsProvider(access_key="a", secret_key="b"),
            rdma={},
            rust_client={},
        )


def test_client_config_overrides_enforce_empty_body_contract():
    overrides = _RealCuObjEngine.client_config_overrides()
    assert overrides["request_checksum_calculation"] == "when_required"
    assert overrides["response_checksum_validation"] == "when_required"
    assert overrides["s3"]["payload_signing_enabled"] is False


@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_enables_single_shot_and_installs_hooks(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)

    assert provider._rdma_engine is engine_cls.return_value
    assert provider._rust_client is None
    assert provider._checksum_algorithm is None
    assert provider._multipart_threshold == RDMA_SINGLE_SHOT_THRESHOLD
    engine_cls.return_value.install_hooks.assert_called_once_with(provider._s3_client)


@patch.object(S3StorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_put_sends_empty_body_checksum_and_registers_buffer(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    engine = engine_cls.return_value

    written = provider._put_object(path="test-bucket/key.bin", body=b"hello world")

    assert written == len("hello world")
    assert engine.transfer.call_args.kwargs["is_put"] is True
    _, put_kwargs = provider._s3_client.put_object.call_args
    assert put_kwargs["Body"] == b""
    # Precomputed CRC64NVME sent so a non-RDMA endpoint rejects the empty body
    # instead of storing a 0-byte object.
    assert put_kwargs["ChecksumCRC64NVME"] == _FAKE_CHECKSUM
    engine.check_reply.assert_called_once()


@patch.object(S3StorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_put_reuses_writable_buffer_and_copies_readonly(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    engine = engine_cls.return_value

    # Writable buffers (bytearray, writable memoryview) are registered in place.
    writable = bytearray(b"writable payload")
    provider._put_object(path="test-bucket/k1", body=writable)
    assert engine.transfer.call_args.args[0] is writable

    view = memoryview(bytearray(b"view payload"))
    provider._put_object(path="test-bucket/k2", body=view)
    assert engine.transfer.call_args.args[0] is view

    # Read-only bytes are copied into a writable bytearray (cannot be pinned).
    provider._put_object(path="test-bucket/k3", body=b"immutable payload")
    copied = engine.transfer.call_args.args[0]
    assert isinstance(copied, bytearray)
    assert bytes(copied) == b"immutable payload"


def test_rdma_checksum_matches_awscrt():
    checksums = pytest.importorskip("awscrt.checksums")
    data = b"the quick brown fox" * 1000
    expected = base64.b64encode(struct.pack(">Q", checksums.crc64nvme(data))).decode("ascii")
    assert S3StorageProvider._rdma_checksum(data) == expected


@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_put_empty_payload_skips_rdma(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    engine = engine_cls.return_value

    written = provider._put_object(path="test-bucket/empty", body=b"")

    assert written == 0
    engine.transfer.assert_not_called()
    provider._s3_client.put_object.assert_called_once()


@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_get_byte_range_sizes_buffer_and_passes_range(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    engine = engine_cls.return_value

    result = provider._get_object(path="test-bucket/key.bin", byte_range=Range(offset=10, size=32))

    assert isinstance(result, bytearray)
    assert len(result) == 32
    assert engine.transfer.call_args.kwargs["is_put"] is False
    _, get_kwargs = provider._s3_client.get_object.call_args
    assert get_kwargs["Range"] == "bytes=10-41"


@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_get_full_object_heads_for_size(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    engine = engine_cls.return_value

    metadata = MagicMock()
    metadata.content_length = 128
    with patch.object(provider, "_get_object_metadata", return_value=metadata) as head:
        result = provider._get_object(path="test-bucket/key.bin")

    head.assert_called_once()
    assert len(result) == 128
    assert engine.transfer.call_args.kwargs["is_put"] is False
    _, get_kwargs = provider._s3_client.get_object.call_args
    assert "Range" not in get_kwargs


@patch.object(S3StorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_upload_small_uses_single_shot(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16

    provider._upload_file(remote_path="test-bucket/small.bin", f=io.BytesIO(b"x" * 10))

    provider._s3_client.create_multipart_upload.assert_not_called()
    provider._s3_client.put_object.assert_called_once()


@patch.object(S3StorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_upload_multipart_splits_and_completes(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16
    provider._s3_client.create_multipart_upload.return_value = {"UploadId": "uid"}
    provider._s3_client.upload_part.side_effect = [{"ETag": f"etag{i}"} for i in range(1, 4)]
    engine = engine_cls.return_value

    written = provider._upload_file(remote_path="test-bucket/big.bin", f=io.BytesIO(b"a" * 40))

    assert written == 40
    # 40 bytes / 16 => parts of 16, 16, 8.
    assert provider._s3_client.upload_part.call_count == 3
    assert engine.transfer.call_count == 3
    for call in provider._s3_client.upload_part.call_args_list:
        assert call.kwargs["Body"] == b""
        assert call.kwargs["ChecksumCRC64NVME"] == _FAKE_CHECKSUM
    part_numbers = [c.kwargs["PartNumber"] for c in provider._s3_client.upload_part.call_args_list]
    assert part_numbers == [1, 2, 3]
    _, complete_kwargs = provider._s3_client.complete_multipart_upload.call_args
    assert complete_kwargs["MultipartUpload"]["Parts"] == [
        {"PartNumber": 1, "ETag": "etag1"},
        {"PartNumber": 2, "ETag": "etag2"},
        {"PartNumber": 3, "ETag": "etag3"},
    ]


@patch.object(S3StorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_upload_multipart_aborts_on_failure(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16
    provider._s3_client.create_multipart_upload.return_value = {"UploadId": "uid"}
    provider._s3_client.upload_part.side_effect = [{"ETag": "etag1"}, RuntimeError("part failed")]

    with pytest.raises(RuntimeError):
        provider._upload_file(remote_path="test-bucket/big.bin", f=io.BytesIO(b"a" * 40))

    provider._s3_client.abort_multipart_upload.assert_called_once()
    provider._s3_client.complete_multipart_upload.assert_not_called()


def test_install_hooks_registers_token_for_put_get_and_upload_part():
    engine = object.__new__(_RealCuObjEngine)
    s3_client = MagicMock()

    engine.install_hooks(s3_client)

    registered = {call.args[0] for call in s3_client.meta.events.register.call_args_list}
    assert registered == {
        "before-sign.s3.PutObject",
        "before-sign.s3.GetObject",
        "before-sign.s3.UploadPart",
    }


def test_transfer_registers_full_nbytes_for_multibyte_memoryview():
    import multistorageclient.providers._cuobj as cuobj

    engine = object.__new__(_RealCuObjEngine)
    buffer = memoryview(array("H", [0x1111, 0x2222, 0x3333, 0x4444]))  # 4 items, 8 bytes
    assert len(buffer) == 4 and buffer.nbytes == 8

    with (
        patch.object(cuobj, "register_buffer") as register,
        patch.object(cuobj, "get_rdma_token", return_value="tok") as get_token,
        patch.object(cuobj, "put_rdma_token"),
        patch.object(cuobj, "deregister_buffer"),
    ):
        with engine.transfer(buffer, is_put=False):
            pass

    assert register.call_args.args[1] == 8  # nbytes, not len() == 4
    assert get_token.call_args.args[1] == 8


@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_get_full_object_binds_ifmatch_to_head_version(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()

    metadata = MagicMock()
    metadata.content_length = 64
    metadata.etag = '"abc123"'
    with patch.object(provider, "_get_object_metadata", return_value=metadata):
        provider._get_object(path="test-bucket/key.bin")

    _, get_kwargs = provider._s3_client.get_object.call_args
    assert get_kwargs["IfMatch"] == '"abc123"'


@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_multipart_chunksize_must_be_positive(engine_cls: MagicMock):
    engine_cls.client_config_overrides.return_value = _RealCuObjEngine.client_config_overrides()
    with pytest.raises(ValueError, match="multipart_chunksize"):
        S3StorageProvider(
            region_name="us-east-1",
            endpoint_url="https://s3.example.com",
            base_path="test-bucket",
            credentials_provider=StaticS3CredentialsProvider(access_key="a", secret_key="b"),
            rdma={"multipart_chunksize": 0},
        )
