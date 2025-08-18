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

from datetime import datetime, timezone
from typing import Union

from multistorageclient.types import Credentials, CredentialsProvider


class RefreshableTestCredentialsProvider(CredentialsProvider):
    """
    A test credentials provider that tracks refresh calls and simulates refreshable credentials.
    The credentials are valid until the refresh_credentials method is called.
    When the refresh_credentials method is called, it sets the credentials to invalid.
    """

    def __init__(self, access_key: str, secret_key: str, expiration: Union[str, None] = None):
        self._access_key = access_key
        self._secret_key = secret_key
        self._expiration = expiration
        self._refresh_count = 0

    def get_credentials(self) -> Credentials:
        return Credentials(
            access_key=self._access_key,
            secret_key=self._secret_key,
            token=None,
            expiration=self._expiration,
        )

    def refresh_credentials(self) -> None:
        self._refresh_count += 1
        self._access_key = "invalid_access_key"
        self._secret_key = "invalid_secret_key"
        self._expiration = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def refresh_count(self) -> int:
        return self._refresh_count
