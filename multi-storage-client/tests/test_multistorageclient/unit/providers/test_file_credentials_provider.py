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

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pytest

from multistorageclient.providers.file_credentials import FileBasedCredentialsProvider


def create_credential_file(
    path: Path,
    version: int = 1,
    access_key: str = "test-access-key",
    secret_key: str = "test-secret-key",
    session_token: Optional[str] = None,
    expiration: Optional[str] = None,
) -> None:
    """Helper function to create a credential file with the given parameters."""
    data = {
        "Version": version,
        "AccessKeyId": access_key,
        "SecretAccessKey": secret_key,
    }
    if session_token is not None:
        data["SessionToken"] = session_token
    if expiration is not None:
        data["Expiration"] = expiration

    with open(path, "w") as f:
        json.dump(data, f)


def test_file_based_credentials_provider_basic():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file)

        provider = FileBasedCredentialsProvider(str(cred_file))
        credentials = provider.get_credentials()

        assert credentials.access_key == "test-access-key"
        assert credentials.secret_key == "test-secret-key"
        assert credentials.token is None
        assert credentials.expiration is None


def test_file_based_credentials_provider_with_session_token():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file, session_token="test-session-token")

        provider = FileBasedCredentialsProvider(str(cred_file))
        credentials = provider.get_credentials()

        assert credentials.access_key == "test-access-key"
        assert credentials.secret_key == "test-secret-key"
        assert credentials.token == "test-session-token"
        assert credentials.expiration is None


def test_file_based_credentials_provider_with_expiration():
    expiration_time = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file, expiration=expiration_time)

        provider = FileBasedCredentialsProvider(str(cred_file))
        credentials = provider.get_credentials()

        assert credentials.access_key == "test-access-key"
        assert credentials.secret_key == "test-secret-key"
        assert credentials.expiration == expiration_time
        assert not credentials.is_expired()


def test_file_based_credentials_provider_with_expired_credentials():
    expiration_time = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file, expiration=expiration_time)

        provider = FileBasedCredentialsProvider(str(cred_file))
        credentials = provider.get_credentials()

        assert credentials.is_expired()


def test_file_based_credentials_provider_file_not_found():
    with pytest.raises(FileNotFoundError, match="Credential file not found"):
        FileBasedCredentialsProvider("/nonexistent/path/credentials.json")


def test_file_based_credentials_provider_path_is_directory():
    with tempfile.TemporaryDirectory() as tmpdir:
        with pytest.raises(ValueError, match="Credential path is not a file"):
            FileBasedCredentialsProvider(tmpdir)


def test_file_based_credentials_provider_invalid_json():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        with open(cred_file, "w") as f:
            f.write("not valid json {")

        with pytest.raises(ValueError, match="Credential file is not valid JSON"):
            FileBasedCredentialsProvider(str(cred_file))


def test_file_based_credentials_provider_json_not_object():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        with open(cred_file, "w") as f:
            json.dump(["not", "an", "object"], f)

        with pytest.raises(ValueError, match="Credential file must contain a JSON object"):
            FileBasedCredentialsProvider(str(cred_file))


def test_file_based_credentials_provider_missing_version():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        with open(cred_file, "w") as f:
            json.dump(
                {
                    "AccessKeyId": "test-key",
                    "SecretAccessKey": "test-secret",
                },
                f,
            )

        with pytest.raises(ValueError, match="Credential file missing required field: 'Version'"):
            FileBasedCredentialsProvider(str(cred_file))


def test_file_based_credentials_provider_invalid_version():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file, version=2)

        with pytest.raises(ValueError, match="Unsupported credential version: 2"):
            FileBasedCredentialsProvider(str(cred_file))


def test_file_based_credentials_provider_missing_access_key():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        with open(cred_file, "w") as f:
            json.dump(
                {
                    "Version": 1,
                    "SecretAccessKey": "test-secret",
                },
                f,
            )

        with pytest.raises(ValueError, match="Credential file missing required field: 'AccessKeyId'"):
            FileBasedCredentialsProvider(str(cred_file))


def test_file_based_credentials_provider_missing_secret_key():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        with open(cred_file, "w") as f:
            json.dump(
                {
                    "Version": 1,
                    "AccessKeyId": "test-key",
                },
                f,
            )

        with pytest.raises(ValueError, match="Credential file missing required field: 'SecretAccessKey'"):
            FileBasedCredentialsProvider(str(cred_file))


def test_file_based_credentials_provider_access_key_not_string():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        with open(cred_file, "w") as f:
            json.dump(
                {
                    "Version": 1,
                    "AccessKeyId": 12345,
                    "SecretAccessKey": "test-secret",
                },
                f,
            )

        with pytest.raises(ValueError, match="'AccessKeyId' must be a string"):
            FileBasedCredentialsProvider(str(cred_file))


def test_file_based_credentials_provider_secret_key_not_string():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        with open(cred_file, "w") as f:
            json.dump(
                {
                    "Version": 1,
                    "AccessKeyId": "test-key",
                    "SecretAccessKey": 67890,
                },
                f,
            )

        with pytest.raises(ValueError, match="'SecretAccessKey' must be a string"):
            FileBasedCredentialsProvider(str(cred_file))


def test_file_based_credentials_provider_session_token_not_string():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        with open(cred_file, "w") as f:
            json.dump(
                {
                    "Version": 1,
                    "AccessKeyId": "test-key",
                    "SecretAccessKey": "test-secret",
                    "SessionToken": 12345,
                },
                f,
            )

        with pytest.raises(ValueError, match="'SessionToken' must be a string or null"):
            FileBasedCredentialsProvider(str(cred_file))


def test_file_based_credentials_provider_expiration_not_string():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        with open(cred_file, "w") as f:
            json.dump(
                {
                    "Version": 1,
                    "AccessKeyId": "test-key",
                    "SecretAccessKey": "test-secret",
                    "Expiration": 1234567890,
                },
                f,
            )

        with pytest.raises(ValueError, match="'Expiration' must be a string or null"):
            FileBasedCredentialsProvider(str(cred_file))


def test_file_based_credentials_provider_session_token_null():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        with open(cred_file, "w") as f:
            json.dump(
                {
                    "Version": 1,
                    "AccessKeyId": "test-key",
                    "SecretAccessKey": "test-secret",
                    "SessionToken": None,
                },
                f,
            )

        provider = FileBasedCredentialsProvider(str(cred_file))
        credentials = provider.get_credentials()
        assert credentials.token is None


def test_file_based_credentials_provider_expiration_null():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        with open(cred_file, "w") as f:
            json.dump(
                {
                    "Version": 1,
                    "AccessKeyId": "test-key",
                    "SecretAccessKey": "test-secret",
                    "Expiration": None,
                },
                f,
            )

        provider = FileBasedCredentialsProvider(str(cred_file))
        credentials = provider.get_credentials()
        assert credentials.expiration is None


def test_file_based_credentials_provider_refresh_credentials():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file, access_key="initial-key", secret_key="initial-secret")

        provider = FileBasedCredentialsProvider(str(cred_file))
        initial_credentials = provider.get_credentials()

        assert initial_credentials.access_key == "initial-key"
        assert initial_credentials.secret_key == "initial-secret"

        create_credential_file(
            cred_file,
            access_key="refreshed-key",
            secret_key="refreshed-secret",
            session_token="new-token",
        )

        provider.refresh_credentials()
        refreshed_credentials = provider.get_credentials()

        assert refreshed_credentials.access_key == "refreshed-key"
        assert refreshed_credentials.secret_key == "refreshed-secret"
        assert refreshed_credentials.token == "new-token"


def test_file_based_credentials_provider_refresh_with_invalid_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file)

        provider = FileBasedCredentialsProvider(str(cred_file))
        credentials = provider.get_credentials()
        assert credentials.access_key == "test-access-key"

        with open(cred_file, "w") as f:
            f.write("invalid json {")

        with pytest.raises(ValueError, match="Credential file is not valid JSON"):
            provider.refresh_credentials()


def test_file_based_credentials_provider_refresh_with_deleted_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file)

        provider = FileBasedCredentialsProvider(str(cred_file))
        credentials = provider.get_credentials()
        assert credentials.access_key == "test-access-key"

        cred_file.unlink()

        with pytest.raises(FileNotFoundError, match="Credential file not found"):
            provider.refresh_credentials()


def test_file_based_credentials_provider_caching():
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file)

        provider = FileBasedCredentialsProvider(str(cred_file))

        credentials1 = provider.get_credentials()
        credentials2 = provider.get_credentials()

        assert credentials1 is credentials2


def test_file_based_credentials_provider_full_example():
    expiration_time = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat()

    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(
            cred_file,
            access_key="AKIAIOSFODNN7EXAMPLE",
            secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            session_token="AQoDYXdzEJr...<remainder of session token>",
            expiration=expiration_time,
        )

        provider = FileBasedCredentialsProvider(str(cred_file))
        credentials = provider.get_credentials()

        assert credentials.access_key == "AKIAIOSFODNN7EXAMPLE"
        assert credentials.secret_key == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        assert credentials.token == "AQoDYXdzEJr...<remainder of session token>"
        assert credentials.expiration == expiration_time
        assert not credentials.is_expired()
