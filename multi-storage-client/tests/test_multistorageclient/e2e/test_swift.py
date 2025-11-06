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

import os
import tempfile
import uuid

import pytest

import multistorageclient as msc
import test_multistorageclient.e2e.common as common


@pytest.mark.parametrize("profile_name", ["test-swift-pdx", "test-swift-pdx-base-path-with-prefix"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_swift_shortcuts(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_shortcuts(profile)


@pytest.mark.parametrize("profile_name", ["test-swift-pdx"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_swift_storage_client(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_storage_client(profile)


@pytest.mark.parametrize("profile_name", ["test-swift-pdx"])
def test_swift_open_with_source_version_check(profile_name):
    profile = profile_name
    common.test_open_with_source_version_check(profile)


@pytest.mark.parametrize("profile_name", ["test-swift-pdx-rust"])
def test_swift_shortcuts_rust(profile_name):
    profile = profile_name
    common.test_shortcuts(profile)


@pytest.mark.parametrize("profile_name", ["test-swift-pdx-rust"])
def test_swift_storage_client_rust(profile_name):
    profile = profile_name
    common.test_storage_client(profile)


def test_swift_content_type_inference():
    """
    Test that WAV files are uploaded with correct content-type when infer_content_type is enabled.

    This test verifies the fix for JIRA NGCDP-5511 where audio files were uploaded with
    binary/octet-stream instead of audio/x-wav, preventing browsers from playing them inline.

    Note: This test requires a Swift/S8K profile with infer_content_type: true in the config.
    """
    # This test requires a profile with infer_content_type enabled
    # You may need to create a specific test profile or skip if not configured
    try:
        profile = "test-swift-pdx-content-type"
        client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    except Exception:
        pytest.skip("Profile 'test-swift-pdx-content-type' not configured. Skipping content type inference test.")

    prefix = f"content-type-test-{uuid.uuid4()}"

    try:
        # Test 1: Upload a WAV file using write() and verify content type
        wav_file_path = f"{prefix}/test_audio.wav"
        # Create a minimal valid WAV file header
        wav_data = b"RIFF" + b"\x00" * 4 + b"WAVE" + b"fmt " + b"\x00" * 100

        client.write(wav_file_path, wav_data)

        # Verify the file was uploaded with correct content type
        metadata = client.info(wav_file_path)
        assert metadata is not None, "File metadata should exist"
        assert metadata.content_type is not None, "Content type should be set"
        assert metadata.content_type == "audio/x-wav", (
            f"Expected content type 'audio/x-wav', got '{metadata.content_type}'"
        )

        # Test 2: Upload a WAV file using upload_file() and verify content type
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_file:
            temp_file.write(wav_data)
            temp_file.flush()
            temp_path = temp_file.name

        try:
            uploaded_wav_path = f"{prefix}/uploaded_audio.wav"
            client.upload_file(uploaded_wav_path, temp_path)

            # Verify the uploaded file has correct content type
            upload_metadata = client.info(uploaded_wav_path)
            assert upload_metadata is not None, "Uploaded file metadata should exist"
            assert upload_metadata.content_type is not None, "Uploaded file content type should be set"
            assert upload_metadata.content_type == "audio/x-wav", (
                f"Expected uploaded file content type 'audio/x-wav', got '{upload_metadata.content_type}'"
            )
        finally:
            os.unlink(temp_path)

    finally:
        # Clean up all test files
        common.delete_files(client, prefix)
