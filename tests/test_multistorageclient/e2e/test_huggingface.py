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

import io
import os
import tempfile

import pytest

import multistorageclient as msc
from multistorageclient.types import Range


@pytest.mark.parametrize("profile_name", ["test-hf-public-model"])
def test_huggingface_public_model_download(profile_name):
    """Test downloading files from a public HuggingFace model repository."""
    profile = profile_name
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")

    test_file = "config.json"

    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
        temp_path = temp_file.name

    try:
        client.download_file(test_file, temp_path)

        assert os.path.exists(temp_path), f"Downloaded file {temp_path} should exist"
        file_size = os.path.getsize(temp_path)
        assert file_size > 0, f"Downloaded file should have content, got {file_size} bytes"

        with open(temp_path, "r") as f:
            content = f.read()
            assert '"model_type"' in content or '"architectures"' in content, "Should be a valid model config"

    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


@pytest.mark.parametrize("profile_name", ["test-hf-private-dataset"])
def test_huggingface_download(profile_name):
    """Test downloading from a HuggingFace repository with XET enabled."""
    profile = profile_name
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")

    test_file = "small_file_0.parquet"

    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temp_file:
        temp_path = temp_file.name

    try:
        client.download_file(test_file, temp_path)

        assert os.path.exists(temp_path), "Downloaded file should exist"
        assert os.path.getsize(temp_path) > 0, "Downloaded file should have content"

        binary_buffer = io.BytesIO()
        client.download_file(test_file, binary_buffer)

        data = binary_buffer.getvalue()

        assert len(data) > 0, "The binary buffer should not be empty"
        assert data.startswith(b"PAR1"), "Content should be a valid parquet file"

    except Exception as e:
        pytest.skip(f"XET-enabled repository test skipped: {e}")
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


@pytest.mark.parametrize("profile_name", ["test-hf-private-dataset"])
def test_huggingface_get_object(profile_name):
    """Test getting an object from a private HuggingFace dataset."""
    profile = profile_name
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")

    # Use a small file from the XET-enabled repository
    test_file = "small_file_0.parquet"

    try:
        # Get the object as bytes
        data = client.read(test_file)

        # Verify we got bytes data
        assert isinstance(data, bytes), "get_object should return bytes"
        assert len(data) > 0, f"Object should have content, got {len(data)} bytes"

        # Verify it's a valid parquet file
        assert data.startswith(b"PAR1"), "Content should be a valid parquet file"

        # This should raise ValueError since HuggingFace doesn't support range reads
        with pytest.raises(ValueError, match="HuggingFace provider does not support partial range reads"):
            client.read(test_file, byte_range=Range(0, 100))

    except Exception as e:
        pytest.skip(f"get_object test skipped: {e}")
