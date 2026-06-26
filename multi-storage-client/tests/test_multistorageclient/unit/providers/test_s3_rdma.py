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


@patch("multistorageclient.providers.s3.CuObjEngine")
def test_rdma_put_sends_empty_body_and_registers_buffer(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    engine = engine_cls.return_value

    written = provider._put_object(path="test-bucket/key.bin", body=b"hello world")

    assert written == len("hello world")
    assert engine.transfer.call_args.kwargs["is_put"] is True
    _, put_kwargs = provider._s3_client.put_object.call_args
    assert put_kwargs["Body"] == b""
    engine.check_reply.assert_called_once()


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
