# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import pytest

pytest.importorskip("fastapi", reason="FastAPI required for explorer tests")
pytest.importorskip("httpx", reason="httpx required for FastAPI testing")

from fastapi.testclient import TestClient

# Import the server module directly
import multistorageclient.explorer.api.server as app_module

# Tests for the /api/health endpoint


def test_health_returns_ok():
    """Test that health endpoint returns healthy status."""
    with TestClient(app_module.app) as client:
        response = client.get("/api/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "config_loaded" in data
    assert "frontend_available" in data


# Tests for configuration-related endpoints


def test_get_profiles_without_config_returns_error():
    """Test that getting profiles without config returns error."""
    # Ensure no config is loaded
    app_module.msc_config = None

    with TestClient(app_module.app) as client:
        response = client.get("/api/config/profiles")

    assert response.status_code == 400
    assert "No configuration loaded" in response.json()["detail"]


def test_get_profiles_with_config_returns_profiles(test_client, mock_msc_config):
    """Test that getting profiles with config returns the profile list."""
    response = test_client.get("/api/config/profiles")

    assert response.status_code == 200
    data = response.json()
    assert "profiles" in data
    assert "count" in data
    assert data["count"] == 2
    assert "test-s3" in data["profiles"]
    assert "test-local" in data["profiles"]


# Tests for the /api/files/list endpoint


def test_list_files_without_config_returns_error():
    """Test that listing files without config returns error."""
    app_module.msc_config = None

    with TestClient(app_module.app) as client:
        response = client.post("/api/files/list", json={"url": "msc://test-s3/"})

    assert response.status_code == 400
    assert "configuration" in response.json()["detail"].lower()


def test_list_files_invalid_url_returns_error(test_client):
    """Test that listing files with invalid URL returns error."""
    response = test_client.post("/api/files/list", json={"url": "invalid://path"})

    assert response.status_code == 400
    assert "msc://" in response.json()["detail"]


def test_list_files_invalid_profile_returns_error(test_client):
    """Test that listing files with invalid profile returns error."""
    response = test_client.post("/api/files/list", json={"url": "msc://nonexistent/"})

    assert response.status_code == 400
    assert "not found" in response.json()["detail"].lower()


def test_get_msc_client_and_path_normalizes_duplicate_slashes(mock_msc_config):
    """Test that Explorer URL parsing uses normalized MSC paths."""
    app_module.msc_config = mock_msc_config
    cached_client = object()
    app_module._client_cache["test-s3"] = cached_client

    try:
        client, path = app_module.get_msc_client_and_path("msc://test-s3/path//to///object.bin")
    finally:
        app_module.msc_config = None
        app_module._client_cache.clear()

    assert client is cached_client
    assert path == "path/to/object.bin"


# Tests for Pydantic models


def test_list_request_model():
    """Test ListRequest model validation."""
    from multistorageclient.explorer.api.models import ListRequest

    request = ListRequest(url="msc://profile/path")
    assert request.url == "msc://profile/path"
    assert request.include_directories is True
    assert request.limit is None


def test_list_request_with_options():
    """Test ListRequest model with all options."""
    from multistorageclient.explorer.api.models import ListRequest

    request = ListRequest(
        url="msc://profile/path",
        start_after="file1.txt",
        end_at="file99.txt",
        include_directories=False,
        limit=100,
    )
    assert request.start_after == "file1.txt"
    assert request.end_at == "file99.txt"
    assert request.include_directories is False
    assert request.limit == 100


def test_config_upload_response_model():
    """Test ConfigUploadResponse model."""
    from multistorageclient.explorer.api.models import ConfigUploadResponse

    response = ConfigUploadResponse(
        status="success",
        message="Loaded 2 profiles",
        profiles=["s3", "gcs"],
    )
    assert response.status == "success"
    assert len(response.profiles) == 2


def test_preview_request_default_max_bytes():
    """Test PreviewRequest model has default max_bytes."""
    from multistorageclient.explorer.api.models import PreviewRequest

    request = PreviewRequest(url="msc://profile/file.txt")
    assert request.max_bytes == 1048576  # 1 MB default


def test_sync_request_default_options():
    """Test SyncRequest model default options."""
    from multistorageclient.explorer.api.models import SyncRequest

    request = SyncRequest(
        source_url="msc://src/",
        target_url="msc://dest/",
    )
    assert request.delete_unmatched_files is False
    assert request.preserve_source_attributes is False
