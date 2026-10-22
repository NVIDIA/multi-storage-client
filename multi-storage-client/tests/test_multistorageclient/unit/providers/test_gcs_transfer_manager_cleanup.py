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

import builtins
import io
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import Forbidden

from multistorageclient.providers.gcs import GoogleStorageProvider
from multistorageclient.types import ObjectMetadata, RetryableError
from multistorageclient_rust import RustClientError, RustRetryableError


@pytest.fixture
def gcs_provider(tmp_path, monkeypatch):
    # Route staging files into an isolated directory so leaks are observable.
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "staging"))
    (tmp_path / "staging").mkdir()
    with patch.object(GoogleStorageProvider, "_create_gcs_client", return_value=MagicMock()):
        provider = GoogleStorageProvider(project_id="project", base_path="bucket")
        provider._multipart_threshold = 0  # force the transfer-manager paths
        yield provider


def _staging_files(tmp_path):
    return sorted(p.name for p in (tmp_path / "staging").iterdir())


@pytest.mark.parametrize("operation", ["upload", "download"])
@pytest.mark.parametrize("multipart_threshold", [0, 1024])
def test_stringio_transfers_use_utf8_with_ascii_locale(gcs_provider, tmp_path, operation, multipart_threshold):
    text = "caf\u00e9 \u65e5\u672c\u8a9e\n"
    payload = text.encode("utf-8")
    gcs_provider._multipart_threshold = multipart_threshold
    real_open = builtins.open
    real_tempfile = tempfile.NamedTemporaryFile
    transferred: list[bytes] = []

    def locale_open(file, mode="r", *args, **kwargs):
        if "b" not in mode:
            kwargs.setdefault("encoding", "ascii")
        return real_open(file, mode, *args, **kwargs)

    def locale_tempfile(*args, **kwargs):
        if "b" not in kwargs.get("mode", "w+b"):
            kwargs.setdefault("encoding", "ascii")
        return real_tempfile(*args, **kwargs)

    def upload_chunks(filename, *args, **kwargs):
        with real_open(filename, "rb") as fp:
            transferred.append(fp.read())

    def download_chunks(blob, filename, **kwargs):
        with real_open(filename, "wb") as fp:
            fp.write(payload)

    blob = gcs_provider._gcs_client.bucket.return_value.blob.return_value
    blob.upload_from_string.side_effect = transferred.append
    blob.download_as_bytes.return_value = payload
    with (
        patch("multistorageclient.providers.gcs.open", locale_open, create=True),
        patch("multistorageclient.providers.gcs.tempfile.NamedTemporaryFile", locale_tempfile),
        patch("multistorageclient.providers.gcs.transfer_manager.upload_chunks_concurrently", upload_chunks),
        patch("multistorageclient.providers.gcs.transfer_manager.download_chunks_concurrently", download_chunks),
    ):
        if operation == "upload":
            gcs_provider._upload_file("bucket/key", io.StringIO(text))
            assert transferred == [payload]
        else:
            metadata = ObjectMetadata(
                key="bucket/key", content_length=len(payload), last_modified=datetime(2025, 1, 1, tzinfo=timezone.utc)
            )
            destination = io.StringIO()
            gcs_provider._download_file("bucket/key", destination, metadata)
            assert destination.getvalue() == text

    assert _staging_files(tmp_path) == []


@pytest.mark.parametrize("operation", ["upload", "download"])
@pytest.mark.parametrize(
    ("rust_error", "expected_error"),
    [
        (RustRetryableError("connection reset"), RetryableError),
        (RustClientError("access denied", 403), PermissionError),
        (RustClientError("not found", 404), FileNotFoundError),
    ],
)
def test_small_rust_transfer_errors_are_translated(gcs_provider, tmp_path, operation, rust_error, expected_error):
    gcs_provider._rust_client = MagicMock()
    gcs_provider._multipart_threshold = 8
    local_file = tmp_path / "data.bin"
    local_file.write_bytes(b"payload")
    metadata = ObjectMetadata(
        key="bucket/key", content_length=7, last_modified=datetime(2025, 1, 1, tzinfo=timezone.utc)
    )

    with (
        patch("multistorageclient.providers.gcs.run_async_rust_client_method", side_effect=rust_error),
        pytest.raises(expected_error, match="bucket/key") as error,
    ):
        if operation == "upload":
            gcs_provider._upload_file("bucket/key", str(local_file))
        else:
            gcs_provider._download_file("bucket/key", str(local_file), metadata)

    assert error.value.__cause__ is rust_error
    assert local_file.read_bytes() == b"payload"


@pytest.mark.parametrize(("method_name", "remote_path"), [("download_file", "key"), ("_download_file", "bucket/key")])
def test_download_metadata_permission_errors_are_translated(gcs_provider, tmp_path, method_name, remote_path):
    gcs_provider._rust_client = MagicMock()
    error = Forbidden("access denied")
    gcs_provider._gcs_client.bucket.return_value.get_blob.side_effect = error
    destination = tmp_path / "out.bin"

    with pytest.raises(PermissionError, match="bucket/key") as raised:
        getattr(gcs_provider, method_name)(remote_path, str(destination))

    assert raised.value.__cause__ is error
    assert not destination.exists()
    assert _staging_files(tmp_path) == []


def test_rust_uploads_receive_attributes(gcs_provider, tmp_path):
    rust_client = MagicMock()
    gcs_provider._rust_client = rust_client
    attributes = {"source": "rust"}
    local_file = tmp_path / "data.bin"
    local_file.write_bytes(b"payload")

    with patch("multistorageclient.providers.gcs.run_async_rust_client_method") as run_rust:
        gcs_provider._put_object("bucket/put", b"payload", attributes=attributes)
        run_rust.assert_called_once_with(rust_client, "put", "put", b"payload", attributes=attributes)
        gcs_provider._gcs_client.bucket.return_value.blob.return_value.upload_from_string.assert_not_called()

        gcs_provider._multipart_threshold = 8
        run_rust.reset_mock()
        gcs_provider._upload_file("bucket/small", str(local_file), attributes=attributes)
        run_rust.assert_called_once_with(rust_client, "upload", str(local_file), "small", attributes=attributes)

        gcs_provider._multipart_threshold = 1
        run_rust.reset_mock()
        gcs_provider._upload_file("bucket/large", str(local_file), attributes=attributes)
        run_rust.assert_called_once_with(
            rust_client, "upload_multipart_from_file", str(local_file), "large", attributes=attributes
        )


@pytest.mark.parametrize(
    "attributes",
    [
        {"Owner": "alice"},
        {"my key": "value"},
        {"名字": "value"},
        {"key": "line\nfeed"},
        {"key": " leading"},
        {"key": "trailing "},
        {"key": ""},
    ],
)
def test_gcs_metadata_is_forwarded_to_rust(gcs_provider, tmp_path, attributes):
    rust_client = MagicMock()
    gcs_provider._rust_client = rust_client
    blob = gcs_provider._gcs_client.bucket.return_value.blob.return_value
    local_file = tmp_path / "data.bin"
    local_file.write_bytes(b"payload")

    with (
        patch("multistorageclient.providers.gcs.run_async_rust_client_method") as run_rust,
        patch("multistorageclient.providers.gcs.transfer_manager.upload_chunks_concurrently") as upload_chunks,
    ):
        gcs_provider._put_object("bucket/put", b"payload", attributes=attributes)
        run_rust.assert_called_once_with(rust_client, "put", "put", b"payload", attributes=attributes)
        blob.upload_from_string.assert_not_called()

        gcs_provider._multipart_threshold = 8
        run_rust.reset_mock()
        gcs_provider._upload_file("bucket/small", str(local_file), attributes=attributes)
        run_rust.assert_called_once_with(rust_client, "upload", str(local_file), "small", attributes=attributes)

        gcs_provider._multipart_threshold = 1
        run_rust.reset_mock()
        gcs_provider._upload_file("bucket/large", str(local_file), attributes=attributes)
        run_rust.assert_called_once_with(
            rust_client, "upload_multipart_from_file", str(local_file), "large", attributes=attributes
        )
        upload_chunks.assert_not_called()


def test_gcs_rust_invalid_metadata_reports_context(gcs_provider, tmp_path):
    gcs_provider._rust_client = MagicMock()
    blob = gcs_provider._gcs_client.bucket.return_value.blob.return_value
    local_file = tmp_path / "data.bin"
    local_file.write_bytes(b"payload")

    with patch(
        "multistorageclient.providers.gcs.run_async_rust_client_method",
        side_effect=ValueError("Invalid metadata key: 'my key'"),
    ):
        with pytest.raises(RuntimeError, match="Invalid metadata key"):
            gcs_provider._put_object("bucket/put", b"payload", attributes={"my key": "value"})

        gcs_provider._multipart_threshold = 1
        with pytest.raises(RuntimeError, match="Invalid metadata key"):
            gcs_provider._upload_file("bucket/large", str(local_file), attributes={"my key": "value"})

    blob.upload_from_string.assert_not_called()


def test_upload_file_object_removes_staging_file_on_failure(gcs_provider, tmp_path):
    with (
        patch(
            "multistorageclient.providers.gcs.transfer_manager.upload_chunks_concurrently",
            side_effect=RuntimeError("upload failed"),
        ),
        pytest.raises(RuntimeError, match="upload failed"),
    ):
        gcs_provider._upload_file("bucket/key.bin", io.BytesIO(b"payload"), None)

    assert _staging_files(tmp_path) == []


def test_download_file_object_removes_staging_file_on_failure(gcs_provider, tmp_path):
    metadata = ObjectMetadata(
        key="bucket/key.bin", content_length=7, last_modified=datetime(2025, 1, 1, tzinfo=timezone.utc)
    )
    with (
        patch(
            "multistorageclient.providers.gcs.transfer_manager.download_chunks_concurrently",
            side_effect=RuntimeError("download failed"),
        ),
        pytest.raises(RuntimeError, match="download failed"),
    ):
        gcs_provider._download_file("bucket/key.bin", io.BytesIO(), metadata)

    assert _staging_files(tmp_path) == []


def test_download_file_object_success_removes_staging_file(gcs_provider, tmp_path):
    metadata = ObjectMetadata(
        key="bucket/key.bin", content_length=7, last_modified=datetime(2025, 1, 1, tzinfo=timezone.utc)
    )

    def fake_download(blob, filename, **kwargs):
        with open(filename, "wb") as fp:
            fp.write(b"payload")

    f = io.BytesIO()
    with patch(
        "multistorageclient.providers.gcs.transfer_manager.download_chunks_concurrently", side_effect=fake_download
    ):
        assert gcs_provider._download_file("bucket/key.bin", f, metadata) == 7

    assert f.getvalue() == b"payload"
    assert _staging_files(tmp_path) == []


def test_download_to_path_removes_staging_file_on_failure(gcs_provider, tmp_path):
    metadata = ObjectMetadata(
        key="bucket/key.bin", content_length=7, last_modified=datetime(2025, 1, 1, tzinfo=timezone.utc)
    )
    with (
        patch(
            "multistorageclient.providers.gcs.transfer_manager.download_chunks_concurrently",
            side_effect=RuntimeError("download failed"),
        ),
        pytest.raises(RuntimeError, match="download failed"),
    ):
        gcs_provider._download_file("bucket/key.bin", str(tmp_path / "out.bin"), metadata)

    # The staging file is created next to the destination, so it must not leak there.
    assert sorted(p.name for p in tmp_path.iterdir()) == ["staging"]
