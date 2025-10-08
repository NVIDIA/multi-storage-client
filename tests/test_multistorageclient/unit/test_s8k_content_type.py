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
# limitations under the License

"""Unit tests for S8K content type inference."""

import io
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.providers.s8k import S8KStorageProvider


class TestS8KContentTypeInference:
    """Test suite for content type inference in S8K storage provider."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        client = MagicMock()
        client.put_object.return_value = {"ResponseMetadata": {"HTTPHeaders": {}}}
        client.upload_file.return_value = {"ResponseMetadata": {"HTTPHeaders": {}}}
        client.upload_fileobj.return_value = None
        return client

    @pytest.fixture
    def s8k_provider_with_inference(self, mock_s3_client):
        """Create S8K provider with content type inference enabled."""
        with patch("multistorageclient.providers.s3.boto3.client", return_value=mock_s3_client):
            provider = S8KStorageProvider(
                base_path="test-bucket",
                region_name="us-east-1",
                endpoint_url="https://test-endpoint.com",
                infer_content_type=True,
            )
            provider._s3_client = mock_s3_client
            return provider

    @pytest.fixture
    def s8k_provider_without_inference(self, mock_s3_client):
        """Create S8K provider without content type inference."""
        with patch("multistorageclient.providers.s3.boto3.client", return_value=mock_s3_client):
            provider = S8KStorageProvider(
                base_path="test-bucket",
                region_name="us-east-1",
                endpoint_url="https://test-endpoint.com",
                infer_content_type=False,
            )
            provider._s3_client = mock_s3_client
            return provider

    def test_guess_content_type_enabled(self, s8k_provider_with_inference):
        """Test content type inference when enabled."""
        # Test various file types
        assert s8k_provider_with_inference._guess_content_type("file.wav") == "audio/x-wav"
        assert s8k_provider_with_inference._guess_content_type("file.mp3") == "audio/mpeg"
        assert s8k_provider_with_inference._guess_content_type("file.json") == "application/json"
        assert s8k_provider_with_inference._guess_content_type("file.png") == "image/png"
        assert s8k_provider_with_inference._guess_content_type("file.pdf") == "application/pdf"

    def test_guess_content_type_disabled(self, s8k_provider_without_inference):
        """Test content type inference when disabled."""
        # Should return None when disabled
        assert s8k_provider_without_inference._guess_content_type("file.wav") is None
        assert s8k_provider_without_inference._guess_content_type("file.json") is None

    def test_guess_content_type_unknown_extension(self, s8k_provider_with_inference):
        """Test content type inference with unknown extension."""
        # Unknown extensions should return None
        assert s8k_provider_with_inference._guess_content_type("file.unknown123") is None

    def test_put_object_with_content_type_inference(self, s8k_provider_with_inference):
        """Test put_object with content type inference."""
        s8k_provider_with_inference._put_object("test-bucket/file.wav", b"test data")

        # Verify put_object was called with ContentType
        call_args = s8k_provider_with_inference._s3_client.put_object.call_args
        assert call_args[1]["ContentType"] == "audio/x-wav"
        assert call_args[1]["Key"] == "file.wav"

    def test_put_object_without_content_type_inference(self, s8k_provider_without_inference):
        """Test put_object without content type inference."""
        s8k_provider_without_inference._put_object("test-bucket/file.wav", b"test data")

        # Verify put_object was called without ContentType
        call_args = s8k_provider_without_inference._s3_client.put_object.call_args
        assert "ContentType" not in call_args[1]

    def test_upload_file_from_path_with_inference(self, s8k_provider_with_inference):
        """Test upload_file from file path with content type inference."""
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            f.write(b"test audio data")
            temp_path = f.name

        try:
            # Small file test (uses put_object internally)
            s8k_provider_with_inference._upload_file("test-bucket/audio.wav", temp_path)

            # Verify put_object was called with ContentType
            call_args = s8k_provider_with_inference._s3_client.put_object.call_args
            assert call_args[1]["ContentType"] == "audio/x-wav"
        finally:
            import os

            os.unlink(temp_path)

    def test_upload_file_from_fileobj_with_inference(self, s8k_provider_with_inference):
        """Test upload_file from file object with content type inference."""
        file_obj = io.BytesIO(b"test json data")

        # Small file test
        s8k_provider_with_inference._upload_file("test-bucket/data.json", file_obj)

        # Verify put_object was called with ContentType inferred from remote path
        call_args = s8k_provider_with_inference._s3_client.put_object.call_args
        assert call_args[1]["ContentType"] == "application/json"
