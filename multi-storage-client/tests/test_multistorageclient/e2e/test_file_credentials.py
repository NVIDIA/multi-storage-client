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
from pathlib import Path

from multistorageclient.client import StorageClient
from multistorageclient.config import StorageClientConfig
from multistorageclient.providers.file_credentials import FileBasedCredentialsProvider


def create_credential_file(
    path: Path,
    access_key: str = "test-access-key",
    secret_key: str = "test-secret-key",
    session_token: str = "test-session-token",
    expiration: str = "2025-12-31T23:59:59Z",
) -> None:
    """Helper function to create a credential file."""
    data = {
        "Version": 1,
        "AccessKeyId": access_key,
        "SecretAccessKey": secret_key,
        "SessionToken": session_token,
        "Expiration": expiration,
    }
    with open(path, "w") as f:
        json.dump(data, f)


def test_file_credentials_with_dict_config():
    """Test that FileBasedCredentials can be configured via dict configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file)

        config = StorageClientConfig.from_dict(
            {
                "profiles": {
                    "test-profile": {
                        "storage_provider": {
                            "type": "file",
                            "options": {
                                "base_path": tmpdir,
                            },
                        },
                        "credentials_provider": {
                            "type": "FileBasedCredentials",
                            "options": {
                                "credential_file_path": str(cred_file),
                            },
                        },
                    }
                }
            },
            profile="test-profile",
        )

        client = StorageClient(config)
        assert isinstance(client._credentials_provider, FileBasedCredentialsProvider)

        credentials = client._credentials_provider.get_credentials()
        assert credentials.access_key == "test-access-key"
        assert credentials.secret_key == "test-secret-key"
        assert credentials.token == "test-session-token"
        assert credentials.expiration == "2025-12-31T23:59:59Z"

        credentials = client._credentials_provider.get_credentials()
        assert credentials.access_key == "test-access-key"
        assert credentials.secret_key == "test-secret-key"
        assert credentials.token == "test-session-token"
        assert credentials.expiration == "2025-12-31T23:59:59Z"


def test_file_credentials_with_json_config():
    """Test that FileBasedCredentials can be configured via JSON configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file)

        config = StorageClientConfig.from_json(
            f"""{{
            "profiles": {{
                "test-profile": {{
                    "storage_provider": {{
                        "type": "file",
                        "options": {{
                            "base_path": "{tmpdir}"
                        }}
                    }},
                    "credentials_provider": {{
                        "type": "FileBasedCredentials",
                        "options": {{
                            "credential_file_path": "{str(cred_file)}"
                        }}
                    }}
                }}
            }}
        }}""",
            profile="test-profile",
        )

        client = StorageClient(config)
        assert isinstance(client._credentials_provider, FileBasedCredentialsProvider)

        credentials = client._credentials_provider.get_credentials()
        assert credentials.access_key == "test-access-key"
        assert credentials.secret_key == "test-secret-key"
        assert credentials.token == "test-session-token"
        assert credentials.expiration == "2025-12-31T23:59:59Z"


def test_file_credentials_with_yaml_config():
    """Test that FileBasedCredentials can be configured via YAML configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cred_file = Path(tmpdir) / "credentials.json"
        create_credential_file(cred_file)

        config = StorageClientConfig.from_yaml(
            f"""
        profiles:
          test-profile:
            storage_provider:
              type: file
              options:
                base_path: {tmpdir}
            credentials_provider:
              type: FileBasedCredentials
              options:
                credential_file_path: {str(cred_file)}
        """,
            profile="test-profile",
        )

        client = StorageClient(config)
        assert isinstance(client._credentials_provider, FileBasedCredentialsProvider)

        credentials = client._credentials_provider.get_credentials()
        assert credentials.access_key == "test-access-key"
        assert credentials.secret_key == "test-secret-key"
        assert credentials.token == "test-session-token"
        assert credentials.expiration == "2025-12-31T23:59:59Z"
