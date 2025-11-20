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
import logging
import time
from pathlib import Path
from typing import Any, Optional

from multistorageclient.types import Credentials, CredentialsProvider

logger = logging.getLogger(__name__)


class FileBasedCredentialsProvider(CredentialsProvider):
    """
    A concrete implementation of :py:class:`multistorageclient.types.CredentialsProvider` that reads
    credentials from a JSON file following the AWS external process credential provider format.

    The JSON file must contain credentials in the following schema:

    .. code-block:: json

        {
          "Version": 1,
          "AccessKeyId": "your-access-key-id",
          "SecretAccessKey": "your-secret-access-key",
          "SessionToken": "your-session-token",
          "Expiration": "2024-12-31T23:59:59Z"
        }

    Where:
        - ``Version``: Must be 1 (required)
        - ``AccessKeyId``: The access key for authentication (required)
        - ``SecretAccessKey``: The secret key for authentication (required)
        - ``SessionToken``: An optional session token for temporary credentials
        - ``Expiration``: An optional ISO 8601 formatted timestamp indicating when the credentials expire

    The credential file can be periodically updated by an external process, and credentials
    will be re-read from the file when :py:meth:`refresh_credentials` is called.
    """

    _credential_file_path: Path
    _cached_credentials: Optional[Credentials]
    _last_mtime: float

    def __init__(self, credential_file_path: str):
        """
        Initializes the :py:class:`FileBasedCredentialsProvider` with the path to the credential file.

        :param credential_file_path: The path to the JSON file containing credentials.
        :raises FileNotFoundError: If the credential file does not exist.
        :raises ValueError: If the file is not valid JSON or does not match the expected schema.
        """
        self._credential_file_path = Path(credential_file_path)
        self._cached_credentials = None
        self._last_mtime = 0.0

        self._validate_credential_file()

        self._cached_credentials = self._load_credentials()

    def _validate_credential_file(self) -> None:
        """
        Validates that the credential file exists and contains valid JSON with the expected schema.

        :raises FileNotFoundError: If the credential file does not exist.
        :raises ValueError: If the file is not valid JSON or does not match the expected schema.
        """
        if not self._credential_file_path.exists():
            raise FileNotFoundError(f"Credential file not found: {self._credential_file_path}")

        if not self._credential_file_path.is_file():
            raise ValueError(f"Credential path is not a file: {self._credential_file_path}")

        try:
            with open(self._credential_file_path, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Credential file is not valid JSON: {self._credential_file_path}") from e

        self._validate_credential_schema(data)

    def _validate_credential_schema(self, data: dict[str, Any]) -> None:
        """
        Validates that the credential data matches the expected AWS external process schema.

        :param data: The parsed JSON data from the credential file.
        :raises ValueError: If the schema is invalid.
        """
        if not isinstance(data, dict):
            raise ValueError("Credential file must contain a JSON object")

        if "Version" not in data:
            raise ValueError("Credential file missing required field: 'Version'")

        if data["Version"] != 1:
            raise ValueError(f"Unsupported credential version: {data['Version']}. Expected version 1")

        if "AccessKeyId" not in data:
            raise ValueError("Credential file missing required field: 'AccessKeyId'")

        if "SecretAccessKey" not in data:
            raise ValueError("Credential file missing required field: 'SecretAccessKey'")

        if not isinstance(data["AccessKeyId"], str):
            raise ValueError("'AccessKeyId' must be a string")

        if not isinstance(data["SecretAccessKey"], str):
            raise ValueError("'SecretAccessKey' must be a string")

        if "SessionToken" in data and data["SessionToken"] is not None and not isinstance(data["SessionToken"], str):
            raise ValueError("'SessionToken' must be a string or null")

        if "Expiration" in data and data["Expiration"] is not None and not isinstance(data["Expiration"], str):
            raise ValueError("'Expiration' must be a string or null")

    def _load_credentials(self) -> Credentials:
        """
        Loads credentials from the credential file.

        :return: The credentials loaded from the file.
        :raises FileNotFoundError: If the credential file does not exist.
        :raises ValueError: If the file is not valid JSON or does not match the expected schema.
        """
        try:
            # Ensure file modification time is at least 1 second ago
            stat = self._credential_file_path.stat()
            current_mtime = stat.st_mtime
            if time.time() - current_mtime < 1.0:
                time.sleep(1.0)
                # Refresh stat after sleep to get latest mtime if it changed during sleep
                stat = self._credential_file_path.stat()
                current_mtime = stat.st_mtime

            with open(self._credential_file_path, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Credential file is not valid JSON: {self._credential_file_path}") from e
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Credential file not found: {self._credential_file_path}") from e

        self._validate_credential_schema(data)
        self._last_mtime = current_mtime

        return Credentials(
            access_key=data["AccessKeyId"],
            secret_key=data["SecretAccessKey"],
            token=data.get("SessionToken"),
            expiration=data.get("Expiration"),
        )

    def get_credentials(self) -> Credentials:
        """
        Retrieves the current credentials from the cache.

        If credentials have expired, they should be refreshed using :py:meth:`refresh_credentials`.

        :return: The current credentials.
        """
        if self._cached_credentials is None:
            self._cached_credentials = self._load_credentials()

        return self._cached_credentials

    def refresh_credentials(self) -> None:
        """
        Refreshes the credentials by re-reading them from the credential file.

        This method should be called when credentials are expired or when an external
        process has updated the credential file with new credentials.
        """
        logger.debug(f"Refreshing credentials from file: {self._credential_file_path}")
        if self._credential_file_path.exists():
            current_mtime = self._credential_file_path.stat().st_mtime
            if self._last_mtime and current_mtime == self._last_mtime:
                logger.debug("Credential file has not changed, skipping refresh")
                return

        self._cached_credentials = self._load_credentials()
