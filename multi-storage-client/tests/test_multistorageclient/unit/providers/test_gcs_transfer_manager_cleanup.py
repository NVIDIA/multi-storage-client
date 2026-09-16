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
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.providers.gcs import GoogleStorageProvider
from multistorageclient.types import ObjectMetadata


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
