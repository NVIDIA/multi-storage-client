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

import logging
import time
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
BACKOFF_FACTOR = 0.5


class AccessTokenProvider:
    auth_options: dict[str, Any]

    def __init__(self, auth_options: dict[str, Any]):
        self.auth_options = auth_options

    def _require_refresh(self) -> bool:
        return False

    def _refresh_token(self) -> Any:
        return None

    def get_token(self) -> Any:
        return None


class AzureAccessTokenProvider(AccessTokenProvider):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.azure_scopes = self.auth_options["scopes"]
        except KeyError as e:
            logger.error("Error: 'scopes' key is missing in auth options")
            raise e

        # Auth options that shouldn't be passed to :py:class:`msal.ConfidentialClientApplication`.
        nonpassthrough_auth_options_keys: set[str] = {"scopes"}
        passthrough_auth_options: dict[str, Any] = {
            key: value for key, value in self.auth_options.items() if key not in nonpassthrough_auth_options_keys
        }

        import msal
        from requests.adapters import HTTPAdapter, Retry

        msal_session = requests.Session()
        retries = Retry(
            total=MAX_RETRIES,
            backoff_factor=BACKOFF_FACTOR,
            connect=MAX_RETRIES,
            read=MAX_RETRIES,
            status_forcelist=[408, 429, 500, 501, 502, 503, 504],
        )
        msal_session.mount("https://", HTTPAdapter(max_retries=retries))
        self.msal_client = msal.ConfidentialClientApplication(http_client=msal_session, **passthrough_auth_options)

    def get_token(self):
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                # since msal 1.23, acquire_token_for_client stores tokens in cache and handles expired token automatically
                result = self.msal_client.acquire_token_for_client(scopes=self.azure_scopes)
                if result:
                    if "access_token" in result:
                        return result["access_token"]
                    else:
                        logger.warning(
                            f"no access token available in response: {result.get('error')}, description: {result.get('error_description')}"
                        )
                else:
                    logger.warning("authn response from msal client is empty")
                return None
            except requests.exceptions.ConnectionError as e:
                # This is a special case where we need to retry because the server closed the connection
                # MSAL http client's retry mechanism doesn't handle this case properly
                logger.debug(f"Getting token attempt {retry_count + 1} failed with error: {str(e)}")
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    sleep_time = min(BACKOFF_FACTOR * (2**retry_count), 60)
                    time.sleep(sleep_time)
            except Exception as e:
                logger.error(f"Unexpected error during getting token attempt {retry_count + 1}: {str(e)}")
                return None

        logger.debug(f"All {MAX_RETRIES} token fetch attempts failed")
        return None


class AccessTokenProviderFactory:
    @staticmethod
    def create_access_token_provider(auth_config: dict[str, Any]) -> Optional[AccessTokenProvider]:
        if not auth_config:
            return None
        auth_type = auth_config.get("type", None)
        auth_options = auth_config.get("options", {})

        if auth_type == "azure":
            return AzureAccessTokenProvider(auth_options)
        else:
            raise ValueError(f"auth_type: {auth_type} is not supported")
