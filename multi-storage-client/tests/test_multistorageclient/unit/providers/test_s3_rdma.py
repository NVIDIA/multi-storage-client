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
from typing import Any, cast
from unittest.mock import MagicMock, patch
from unittest.mock import call as mock_call

import pytest
from botocore.exceptions import ClientError

import multistorageclient.providers._cuobj as cuobj
from multistorageclient.providers._cuobj import CuObjEngine as _RealCuObjEngine
from multistorageclient.providers._cuobj import CuObjError, parse_rdma_reply
from multistorageclient.providers.s3 import StaticS3CredentialsProvider
from multistorageclient.providers.s3_cuobject import (
    RDMA_SINGLE_SHOT_THRESHOLD,
    S3CuObjectStorageProvider,
)
from multistorageclient.types import Range

_FAKE_CHECKSUM = "ZmFrZWNyYzY0"


def _response(status: int = 200, etag: str = '"etag"', **headers: str) -> dict:
    return {
        "ETag": etag,
        "Body": io.BytesIO(b""),
        "ResponseMetadata": {"HTTPStatusCode": status, "HTTPHeaders": headers},
    }


def _get_response(status: int, reply: str, transferred: Any = None) -> dict:
    headers = {"x-amz-rdma-reply": reply}
    if transferred is not None:
        headers["x-amz-rdma-bytes-transferred"] = str(transferred)
    return _response(status, **headers)


def _make_rdma_provider(engine_cls: MagicMock, **extra: Any) -> S3CuObjectStorageProvider:
    """Construct an RDMA-enabled provider with the cuObject engine mocked out."""
    engine_cls.client_config_overrides.return_value = _RealCuObjEngine.client_config_overrides()
    return S3CuObjectStorageProvider(
        region_name="us-east-1",
        endpoint_url="https://s3.example.com",
        base_path="test-bucket",
        credentials_provider=StaticS3CredentialsProvider(access_key="test", secret_key="test"),
        rdma={},
        **extra,
    )


def test_rdma_and_rust_client_are_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        S3CuObjectStorageProvider(
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


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_enables_single_shot_and_installs_hooks(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)

    assert provider._rdma_engine is engine_cls.return_value
    assert provider._rust_client is None
    assert provider._checksum_algorithm is None
    assert provider._multipart_threshold == RDMA_SINGLE_SHOT_THRESHOLD
    engine_cls.return_value.install_hooks.assert_called_once_with(provider._s3_client)


@patch.object(S3CuObjectStorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
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
    engine.check_reply.assert_called_once_with(provider._s3_client.put_object.return_value, is_put=True)


@patch.object(S3CuObjectStorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_put_reuses_writable_buffer_and_copies_readonly(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    engine = engine_cls.return_value

    # Writable buffers (bytearray, writable memoryview) are registered in place.
    writable = bytearray(b"writable payload")
    provider._rdma_put({"Bucket": "test-bucket", "Key": "k1"}, writable)
    assert engine.transfer.call_args.args[0] is writable

    view = memoryview(bytearray(b"view payload"))
    provider._rdma_put({"Bucket": "test-bucket", "Key": "k2"}, view)
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
    assert S3CuObjectStorageProvider._rdma_checksum(data) == expected


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_put_empty_payload_skips_rdma(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    engine = engine_cls.return_value

    written = provider._put_object(path="test-bucket/empty", body=b"")

    assert written == 0
    engine.transfer.assert_not_called()
    provider._s3_client.put_object.assert_called_once()


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_get_byte_range_sizes_buffer_and_passes_range(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._s3_client.get_object.return_value = _get_response(206, "206", 32)
    engine = engine_cls.return_value

    result = provider._get_object(path="test-bucket/key.bin", byte_range=Range(offset=10, size=32))

    assert isinstance(result, bytes)
    assert len(result) == 32
    assert engine.transfer.call_args.kwargs["is_put"] is False
    _, get_kwargs = provider._s3_client.get_object.call_args
    assert get_kwargs["Range"] == "bytes=10-41"


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_get_full_object_heads_for_size(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._s3_client.get_object.return_value = _get_response(200, "200", 128)
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


@patch.object(S3CuObjectStorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_upload_small_uses_single_shot(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16

    provider._upload_file(remote_path="test-bucket/small.bin", f=io.BytesIO(b"x" * 10))

    provider._s3_client.create_multipart_upload.assert_not_called()
    provider._s3_client.put_object.assert_called_once()


@patch.object(S3CuObjectStorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
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


@patch.object(S3CuObjectStorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
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
    engine = object.__new__(_RealCuObjEngine)
    buffer = memoryview(array("H", [0x1111, 0x2222, 0x3333, 0x4444]))  # 4 items, 8 bytes
    assert len(buffer) == 4 and buffer.nbytes == 8

    with (
        patch.object(cuobj, "register_buffer") as register,
        patch.object(cuobj, "get_rdma_token", return_value="tok") as get_token,
        patch.object(cuobj, "put_rdma_token"),
        patch.object(cuobj, "deregister_buffer"),
        engine.transfer(buffer, is_put=False),
    ):
        _RealCuObjEngine._inject_token(MagicMock(headers={}))

    assert register.call_args.args[1] == 8  # nbytes, not len() == 4
    assert get_token.call_args.args[1] == 8


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_get_full_object_binds_ifmatch_to_head_version(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._s3_client.get_object.return_value = _get_response(200, "200", 64)

    metadata = MagicMock()
    metadata.content_length = 64
    metadata.etag = '"abc123"'
    with patch.object(provider, "_get_object_metadata", return_value=metadata):
        provider._get_object(path="test-bucket/key.bin")

    _, get_kwargs = provider._s3_client.get_object.call_args
    assert get_kwargs["IfMatch"] == '"abc123"'


@pytest.mark.parametrize("chunksize", [0, cuobj.RDMA_MAX_MEMORY_REG_SIZE + 1])
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_multipart_chunksize_must_fit_token_window(engine_cls: MagicMock, chunksize: int):
    engine_cls.client_config_overrides.return_value = _RealCuObjEngine.client_config_overrides()
    with pytest.raises(ValueError, match="multipart_chunksize"):
        S3CuObjectStorageProvider(
            region_name="us-east-1",
            endpoint_url="https://s3.example.com",
            base_path="test-bucket",
            credentials_provider=StaticS3CredentialsProvider(access_key="a", secret_key="b"),
            rdma={"multipart_chunksize": chunksize},
        )


@patch.object(S3CuObjectStorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_upload_text_stream_uses_single_shot(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16

    # A text-mode stream must not be read by the raw multipart stream reader --
    # its chunks are str and would crash it -- so it is encoded first and then
    # split as an in-memory body.
    text_stream = io.TextIOWrapper(io.BytesIO(b"a" * 40))
    written = provider._upload_file(remote_path="test-bucket/text.bin", f=text_stream)

    assert written == 40
    assert provider._s3_client.upload_part.call_count == 3
    provider._s3_client.put_object.assert_not_called()


@pytest.mark.parametrize(
    ("reply", "expected"),
    [
        ("200", 200),
        ("204", 204),
        ("206", 206),
        ("500", 500),
        ("501", 501),
        ("", 501),
        (None, 501),
        ("not-a-number", None),
        ("200xyz", None),
        ("200 ", None),
        (" 200", None),
        ("+200", None),
        ("2_00", None),
        ("0x200", None),
        ("\u0662\u0660\u0660", None),
        ("-2", None),
        ("99", None),
        ("600", None),
        ("501x", None),
    ],
)
def test_parse_rdma_reply(reply, expected):
    assert parse_rdma_reply(reply) == expected


@pytest.mark.parametrize("reply", [None, "200", "204"])
def test_check_reply_accepts_put(reply):
    headers = {} if reply is None else {"x-amz-rdma-reply": reply}
    _RealCuObjEngine.check_reply(_response(200, **headers), is_put=True)


@pytest.mark.parametrize(
    ("response", "match"),
    [
        (_response(200, **{"x-amz-rdma-reply": "501"}), "declined"),
        (_response(200, **{"x-amz-rdma-reply": "200xyz"}), "failed"),
        (_response(200, **{"x-amz-rdma-reply": "500"}), "failed"),
        (_response(200, **{"x-amz-rdma-reply": "206"}), "failed"),
        (_response(500, **{"x-amz-rdma-reply": "200"}), "failed"),
        (_response(200, etag=""), "failed"),
        (_response(200, etag="", **{"x-amz-rdma-reply": "200"}), "failed"),
    ],
)
def test_check_reply_rejects_put(response, match):
    with pytest.raises(CuObjError, match=match):
        _RealCuObjEngine.check_reply(response, is_put=True)


@pytest.mark.parametrize(("status", "reply"), [(200, "200"), (206, "206")])
def test_check_reply_accepts_matched_get(status, reply):
    _RealCuObjEngine.check_reply(_get_response(status, reply), is_put=False)


@pytest.mark.parametrize(("status", "reply"), [(200, "206"), (206, "200"), (200, "204"), (404, "200")])
def test_check_reply_rejects_mismatched_get(status, reply):
    with pytest.raises(CuObjError, match="failed"):
        _RealCuObjEngine.check_reply(_get_response(status, reply), is_put=False)


def test_check_reply_get_declined_without_header():
    with pytest.raises(CuObjError, match="declined"):
        _RealCuObjEngine.check_reply(_response(200), is_put=False)


@pytest.mark.parametrize(("transferred", "expected"), [(None, 0), (0, 0), (10, 10), (16, 16)])
def test_parse_rdma_bytes_transferred(transferred, expected):
    assert cuobj.parse_rdma_bytes_transferred(_get_response(206, "206", transferred), 16) == expected


@pytest.mark.parametrize("transferred", [17, "1x", "-1", " 5"])
def test_parse_rdma_bytes_transferred_rejects(transferred):
    with pytest.raises(CuObjError):
        cuobj.parse_rdma_bytes_transferred(_get_response(206, "206", transferred), 16)


@pytest.mark.parametrize("transferred", [None, 63])
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_get_full_object_rejects_short_transfer(engine_cls: MagicMock, transferred):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._s3_client.get_object.return_value = _get_response(200, "200", transferred)

    metadata = MagicMock()
    metadata.content_length = 64
    with (
        patch.object(provider, "_get_object_metadata", return_value=metadata),
        pytest.raises(RuntimeError, match="delivered"),
    ):
        provider._get_object(path="test-bucket/key.bin")


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_get_truncates_to_bytes_transferred(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._s3_client.get_object.return_value = _get_response(206, "206", 5)

    result = provider._get_object(path="test-bucket/key.bin", byte_range=Range(offset=95, size=32))

    assert len(result) == 5


def test_transfer_mints_fresh_verbatim_token_per_signing_attempt():
    engine = object.__new__(_RealCuObjEngine)
    first, retry = MagicMock(headers={}), MagicMock(headers={})

    with (
        patch.object(cuobj, "register_buffer"),
        patch.object(cuobj, "get_rdma_token", side_effect=["descriptor-1", "descriptor-2"]) as get_token,
        patch.object(cuobj, "put_rdma_token") as put_token,
        patch.object(cuobj, "deregister_buffer") as deregister,
        engine.transfer(bytearray(8), is_put=True),
    ):
        _RealCuObjEngine._inject_token(first)
        _RealCuObjEngine._inject_token(retry)
        assert put_token.call_args_list == [mock_call("descriptor-1")]

    assert first.headers == {"x-amz-rdma-token": "descriptor-1"}
    assert retry.headers == {"x-amz-rdma-token": "descriptor-2"}
    assert get_token.call_count == 2
    assert put_token.call_args_list == [mock_call("descriptor-1"), mock_call("descriptor-2")]
    deregister.assert_called_once()


def test_inject_token_is_noop_outside_transfer():
    request = MagicMock(headers={})
    with patch.object(cuobj, "get_rdma_token") as get_token:
        _RealCuObjEngine._inject_token(request)
    get_token.assert_not_called()
    assert request.headers == {}


def test_transfer_rejects_buffer_over_token_window():
    engine = object.__new__(_RealCuObjEngine)
    view = MagicMock()
    view.nbytes = cuobj.RDMA_MAX_MEMORY_REG_SIZE + 1

    with (
        patch.object(cuobj, "memoryview", return_value=view, create=True),
        patch.object(cuobj, "register_buffer") as register,
        pytest.raises(CuObjError, match="exceeds"),
        engine.transfer(bytearray(1), is_put=True),
    ):
        pass

    register.assert_not_called()


def _client_error(status: int, reply: Any = None) -> ClientError:
    headers = {} if reply is None else {"x-amz-rdma-reply": reply}
    return ClientError(
        {"Error": {"Code": "Declined"}, "ResponseMetadata": {"HTTPStatusCode": status, "HTTPHeaders": headers}},
        "GetObject",
    )


@pytest.mark.parametrize("status", [501, 503])
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_get_surfaces_decline_from_client_error(engine_cls: MagicMock, status: int):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._s3_client.get_object.side_effect = _client_error(status, "501")

    with pytest.raises(RuntimeError, match="declined RDMA") as excinfo:
        provider._get_object(path="test-bucket/key.bin", byte_range=Range(offset=0, size=8))
    assert isinstance(excinfo.value.__cause__, CuObjError)


@patch.object(S3CuObjectStorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_put_surfaces_decline_from_client_error(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._s3_client.put_object.side_effect = _client_error(501, "501")

    with pytest.raises(RuntimeError, match="declined RDMA"):
        provider._put_object(path="test-bucket/key.bin", body=b"payload")


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_get_client_error_without_decline_is_translated(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._s3_client.get_object.side_effect = _client_error(404)

    with pytest.raises(FileNotFoundError):
        provider._get_object(path="test-bucket/key.bin", byte_range=Range(offset=0, size=8))


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_get_full_object_splits_into_ranged_parts(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16
    provider._s3_client.get_object.side_effect = [
        _get_response(206, "206", 16),
        _get_response(206, "206", 16),
        _get_response(206, "206", 8),
    ]
    registered: list[int] = []
    engine_cls.return_value.transfer.side_effect = lambda view, is_put: registered.append(view.nbytes) or MagicMock()

    metadata = MagicMock()
    metadata.content_length = 40
    metadata.etag = '"v1"'
    with patch.object(provider, "_get_object_metadata", return_value=metadata):
        result = provider._get_object(path="test-bucket/key.bin")

    assert len(result) == 40
    calls = provider._s3_client.get_object.call_args_list
    assert [c.kwargs["Range"] for c in calls] == ["bytes=0-15", "bytes=16-31", "bytes=32-39"]
    assert all(c.kwargs["IfMatch"] == '"v1"' for c in calls)
    assert registered == [16, 16, 8]


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_ranged_get_stops_at_end_of_object(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16
    first = _get_response(206, "206", 16)
    first["ETag"] = '"v1"'
    provider._s3_client.get_object.side_effect = [first, _get_response(206, "206", 4)]

    result = provider._get_object(path="test-bucket/key.bin", byte_range=Range(offset=100, size=48))

    assert len(result) == 20
    calls = provider._s3_client.get_object.call_args_list
    assert len(calls) == 2
    assert "IfMatch" not in calls[0].kwargs
    assert calls[1].kwargs["IfMatch"] == '"v1"'


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_ranged_get_ending_on_part_boundary_stops_at_416(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16
    provider._s3_client.get_object.side_effect = [_get_response(206, "206", 16), _client_error(416)]

    result = provider._get_object(path="test-bucket/key.bin", byte_range=Range(offset=0, size=32))

    assert len(result) == 16


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_ranged_get_first_part_416_is_raised(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16
    provider._s3_client.get_object.side_effect = [_client_error(416)]

    with pytest.raises(RuntimeError, match="status_code: 416"):
        provider._get_object(path="test-bucket/key.bin", byte_range=Range(offset=64, size=32))


@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_get_split_full_object_rejects_short_part(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16
    provider._s3_client.get_object.side_effect = [_get_response(206, "206", 10)]

    metadata = MagicMock()
    metadata.content_length = 40
    with (
        patch.object(provider, "_get_object_metadata", return_value=metadata),
        pytest.raises(RuntimeError, match="delivered 10 of 40"),
    ):
        provider._get_object(path="test-bucket/key.bin")


@patch.object(S3CuObjectStorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_put_object_over_part_size_uses_multipart(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16
    provider._s3_client.create_multipart_upload.return_value = {"UploadId": "uid"}
    engine = engine_cls.return_value
    body = bytearray(b"b" * 40)

    written = provider._put_object(
        path="test-bucket/big.bin",
        body=cast(bytes, body),
        if_none_match="*",
        attributes={"k": "v"},
        content_type="text/plain",
    )

    assert written == 40
    provider._s3_client.put_object.assert_not_called()
    _, create_kwargs = provider._s3_client.create_multipart_upload.call_args
    assert create_kwargs["ContentType"] == "text/plain"
    assert create_kwargs["Metadata"] == {"k": "v"}
    assert provider._s3_client.upload_part.call_count == 3
    # Writable bodies are registered in place as zero-copy slices.
    parts = [c.args[0] for c in engine.transfer.call_args_list]
    assert [memoryview(p).nbytes for p in parts] == [16, 16, 8]
    assert all(isinstance(p, memoryview) for p in parts)
    _, complete_kwargs = provider._s3_client.complete_multipart_upload.call_args
    assert complete_kwargs["IfNoneMatch"] == "*"
    assert "IfMatch" not in complete_kwargs


@patch.object(S3CuObjectStorageProvider, "_rdma_checksum", staticmethod(lambda buffer: _FAKE_CHECKSUM))
@patch("multistorageclient.providers.s3_cuobject.CuObjEngine")
def test_rdma_put_object_multipart_copies_readonly_parts(engine_cls: MagicMock):
    provider = _make_rdma_provider(engine_cls)
    provider._s3_client = MagicMock()
    provider._rdma_multipart_chunksize = 16
    provider._s3_client.create_multipart_upload.return_value = {"UploadId": "uid"}
    engine = engine_cls.return_value

    provider._put_object(path="test-bucket/big.bin", body=b"c" * 40, if_match='"v1"')

    parts = [c.args[0] for c in engine.transfer.call_args_list]
    assert all(isinstance(p, bytearray) for p in parts)
    assert b"".join(bytes(p) for p in parts) == b"c" * 40
    assert provider._s3_client.complete_multipart_upload.call_args.kwargs["IfMatch"] == '"v1"'
