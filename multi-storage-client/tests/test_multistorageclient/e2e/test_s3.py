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

import urllib.request
import uuid

import boto3
import pytest

import multistorageclient as msc
import test_multistorageclient.e2e.common as common
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers.s3 import S3StorageProvider
from multistorageclient.types import PreconditionFailedError


@pytest.mark.parametrize("profile_name", ["test-s3-iad", "test-s3-iad-base-path-with-prefix"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_s3_shortcuts(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_shortcuts(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
@pytest.mark.parametrize("config_suffix", ["", "-rclone"])
def test_s3_storage_client(profile_name, config_suffix):
    profile = profile_name + config_suffix
    common.test_storage_client(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_conditional_put(profile_name):
    """Test conditional PUT operations in S3 using if-match and if-none-match conditions."""
    profile = profile_name
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")

    # S3 uses PreconditionFailedError for both if_none_match="*" and if_match failures
    # and NotImplementedError for if_none_match with specific etag
    common.test_conditional_put(
        storage_client=client,
        if_none_match_error_type=PreconditionFailedError,
        if_match_error_type=PreconditionFailedError,
        if_none_match_specific_error_type=NotImplementedError,
    )


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_open_with_source_version_check(profile_name):
    profile = profile_name
    common.test_open_with_source_version_check(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_attributes(profile_name):
    """Test S3 attributes functionality - storing custom metadata with msc_ prefix."""
    profile = profile_name
    common.test_attributes(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad-rust"])
def test_s3_shortcuts_rust(profile_name):
    profile = profile_name
    common.test_shortcuts(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad-rust"])
def test_s3_storage_client_rust(profile_name):
    profile = profile_name
    common.test_storage_client(profile)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_presigned_url(profile_name):
    common.test_presigned_url(profile_name)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_cloudfront_presigned_url(profile_name):
    common.test_cloudfront_presigned_url(profile_name)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_presigned_url_with_sts_credentials(profile_name):
    """Test presigned GET and PUT URLs work when signed with STS temporary credentials.

    Uses ``sts:GetSessionToken`` to convert the profile's long-lived credentials
    into short-lived temporary credentials (access key + secret key + session token),
    then builds a new StorageClient with those credentials and verifies presigned
    URL round-trip.

    The generated URLs are logged so a human can re-test them after the 15-minute
    credential expiry to confirm they stop working.
    """
    original_client, _ = msc.resolve_storage_client(f"msc://{profile_name}/")
    prefix = f"sts-presigned-{uuid.uuid4()}"
    get_key = f"{prefix}/testfile.bin"
    put_key = f"{prefix}/put-testfile.bin"
    get_body = b"sts presigned-url GET test content"
    put_body = b"sts presigned-url PUT test content"

    assert original_client._credentials_provider is not None
    original_creds = original_client._credentials_provider.get_credentials()
    provider = original_client._storage_provider
    assert isinstance(provider, S3StorageProvider)

    sts = boto3.client(
        "sts",
        region_name=provider._region_name,
        aws_access_key_id=original_creds.access_key,
        aws_secret_access_key=original_creds.secret_key,
    )
    temp = sts.get_session_token(DurationSeconds=900)["Credentials"]

    config = StorageClientConfig.from_dict(
        config_dict={
            "profiles": {
                "sts-test": {
                    "storage_provider": {
                        "type": "s3",
                        "options": {
                            "base_path": provider._base_path,
                            "endpoint_url": provider._endpoint_url,
                            "region_name": provider._region_name,
                        },
                    },
                    "credentials_provider": {
                        "type": "S3Credentials",
                        "options": {
                            "access_key": temp["AccessKeyId"],
                            "secret_key": temp["SecretAccessKey"],
                            "session_token": temp["SessionToken"],
                        },
                    },
                }
            }
        },
        profile="sts-test",
    )
    sts_client = StorageClient(config)

    try:
        original_client.write(get_key, get_body)

        get_url = sts_client.generate_presigned_url(get_key, method="GET")
        with urllib.request.urlopen(get_url) as resp:
            assert resp.read() == get_body

        put_url = sts_client.generate_presigned_url(put_key, method="PUT")
        req = urllib.request.Request(put_url, data=put_body, method="PUT")
        with urllib.request.urlopen(req):
            pass

        assert original_client.read(put_key) == put_body
    finally:
        common.delete_files(original_client, prefix)


@pytest.mark.parametrize("profile_name", ["test-s3-iad"])
def test_s3_list_exact_file_no_prefix_match(profile_name):
    """Test that listing exact file path doesn't match files with same prefix."""
    profile = profile_name
    common.test_list_exact_file_no_prefix_match(profile)


@pytest.mark.skip(reason="Temporarily disable due to hangs in CI")
@pytest.mark.parametrize("profile_name", ["test-s3-iad-with-replica"])
def test_s3_replica_read_using_msc_open(profile_name):
    profile = profile_name
    common.test_replica_read_using_msc_open(profile)


@pytest.mark.skip(reason="Temporarily disable due to hangs in CI")
@pytest.mark.parametrize("profile_name", ["test-s3-iad-with-replica"])
def test_s3_replica_read_using_msc_read(profile_name):
    profile = profile_name
    common.test_replica_read_using_msc_read(profile)


@pytest.mark.skip(reason="Temporarily disable due to hangs in CI")
@pytest.mark.parametrize("profile_name", ["test-s3-iad-with-replica"])
def test_s3_on_demand_replica_fetch_with_cache_using_open(profile_name):
    profile = profile_name
    common.test_on_demand_replica_fetch_with_cache_using_open(profile)


@pytest.mark.skip(reason="Temporarily disable due to hangs in CI")
@pytest.mark.parametrize("profile_name", ["test-s3-iad-with-replica"])
def test_s3_on_demand_replica_fetch_with_cache_using_read(profile_name):
    profile = profile_name
    common.test_on_demand_replica_fetch_with_cache_using_read(profile)
