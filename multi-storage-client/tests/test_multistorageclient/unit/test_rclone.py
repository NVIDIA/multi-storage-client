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

import configparser

from multistorageclient.rclone import _parse_from_config_parser


def test_parse_from_config_parser_empty():
    """
    Test parsing a ConfigParser with no sections.
    Expect "profiles" to be empty.
    """
    cfg = configparser.ConfigParser()
    result = _parse_from_config_parser(cfg)
    assert "profiles" in result
    assert result["profiles"] == {}


def test_parse_from_config_parser_single_profile():
    cfg = configparser.ConfigParser()
    cfg.add_section("s3-local")
    cfg.set("s3-local", "type", "s3")
    cfg.set("s3-local", "region", "us-east-1")
    cfg.set("s3-local", "endpoint", "http://localhost:9000")
    cfg.set("s3-local", "access_key_id", "test-access")
    cfg.set("s3-local", "secret_access_key", "test-secret")
    # A key that is not processed, to test leftover
    cfg.set("s3-local", "base_path", "my-bucket")

    result = _parse_from_config_parser(cfg)
    profiles = result["profiles"]
    assert len(profiles) == 1
    assert "s3-local" in profiles

    profile_data = profiles["s3-local"]
    # name should match
    assert profile_data["name"] == "s3-local"

    storage_provider = profile_data["storage_provider"]
    credentials_provider = profile_data["credentials_provider"]

    # Check storage provider type
    assert storage_provider["type"] == "s3"
    # Check merged S3 options
    options = storage_provider["options"]
    assert options["region_name"] == "us-east-1"
    assert options["endpoint_url"] == "http://localhost:9000"
    # leftover key
    assert options["base_path"] == "my-bucket"

    # Check S3 credentials
    assert credentials_provider["type"] == "S3Credentials"
    creds_options = credentials_provider["options"]
    assert creds_options["access_key"] == "test-access"
    assert creds_options["secret_key"] == "test-secret"


def test_parse_from_config_parser_multiple_profiles():
    """
    Test parsing a ConfigParser with multiple sections.
    Verifies each section is processed and stored as a separate profile.
    """
    cfg = configparser.ConfigParser()
    # First profile: S3
    cfg.add_section("s3-local")
    cfg.set("s3-local", "type", "s3")
    cfg.set("s3-local", "region", "us-east-2")

    # Second profile: FTP
    cfg.add_section("ftp")
    cfg.set("ftp", "type", "ftp")
    cfg.set("ftp", "authn_endpoint", "ftp://namespace.com")
    cfg.set("ftp", "username", "myuser")

    result = _parse_from_config_parser(cfg)
    profiles = result["profiles"]
    assert len(profiles) == 1
    assert "s3-local" in profiles
    assert "ftp" not in profiles

    # Check S3
    s3_profile = profiles["s3-local"]
    assert s3_profile["storage_provider"]["type"] == "s3"
    s3_options = s3_profile["storage_provider"]["options"]
    assert s3_options["region_name"] == "us-east-2"
    assert "base_path" in s3_options
