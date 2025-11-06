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

from typing import Any, Callable, Optional, Union

# Import botocore patch to handle AIStore redirects.
# See https://github.com/NVIDIA/aistore/tree/main/python/aistore/botocore_patch
from aistore.botocore_patch import botocore  # noqa: F401

from ..telemetry import Telemetry
from ..types import AWARE_DATETIME_MIN, CredentialsProvider, ObjectMetadata
from .s3 import S3StorageProvider, StaticS3CredentialsProvider

PROVIDER = "ais_s3"

# Default dummy credentials for AIStore S3 when auth is disabled
DEFAULT_ACCESS_KEY = "FAKEKEY"
DEFAULT_SECRET_KEY = "FAKESECRET"


class AIStoreS3StorageProvider(S3StorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with
    AIStore via its S3-compatible interface.
    """

    def __init__(
        self,
        region_name: str = "",
        endpoint_url: str = "",
        base_path: str = "",
        credentials_provider: Optional[CredentialsProvider] = None,
        config_dict: Optional[dict[str, Any]] = None,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
        verify: Optional[Union[bool, str]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes the :py:class:`AIStoreS3StorageProvider` with AIStore S3 endpoint and optional JWT authentication.

        :param region_name: The AWS region (can be any valid region, AIStore ignores this).
        :param endpoint_url: The AIStore S3 endpoint (e.g., ``http://localhost:8080/s3`` or ``https://aistore.example.com/s3``).
        :param base_path: The root prefix path within the bucket where all operations will be scoped.
        :param credentials_provider: The provider to retrieve AIStore credentials (AISCredentials).
            If not provided, uses dummy credentials for unauthenticated access.
        :param config_dict: Resolved MSC config.
        :param telemetry_provider: A function that provides a telemetry instance.
        :param verify: Controls SSL certificate verification. Can be ``True`` (verify using system CA bundle, default),
            ``False`` (skip verification for self-signed certificates), or a string path to a custom CA certificate bundle.
        :param kwargs: Additional keyword arguments. See :py:class:`S3StorageProvider` for all available options.
        """
        self._ais_credentials_provider = credentials_provider

        dummy_s3_credentials = StaticS3CredentialsProvider(access_key=DEFAULT_ACCESS_KEY, secret_key=DEFAULT_SECRET_KEY)

        super().__init__(
            region_name=region_name or "us-east-1",
            endpoint_url=endpoint_url,
            base_path=base_path,
            credentials_provider=dummy_s3_credentials,
            config_dict=config_dict,
            telemetry_provider=telemetry_provider,
            verify=verify,
            **kwargs,
        )

        self._provider_name = PROVIDER

        # Register event handler to inject JWT token if credentials are provided
        # Use 'before-send' instead of 'before-sign' to inject the header AFTER boto3 signs the request
        # This prevents boto3 from overwriting our Authorization header with AWS signatures
        if self._ais_credentials_provider:
            self._s3_client.meta.events.register("before-send.s3.*", self._inject_auth_header)

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        """
        Override to handle AIStore S3 API quirk where HEAD requests on directory-like paths return 400.
        """
        try:
            return super()._get_object_metadata(path, strict=strict)
        except RuntimeError as error:
            # AIStore returns 400 for invalid HEAD requests (e.g., directory-like paths)
            # Treat this the same as FileNotFoundError and check if it's a directory
            if strict and "status_code: 400" in str(error):
                path = self._append_delimiter(path)
                if self._is_dir(path):
                    return ObjectMetadata(
                        key=path,
                        type="directory",
                        content_length=0,
                        last_modified=AWARE_DATETIME_MIN,
                    )
            raise

    def _inject_auth_header(self, request, **kwargs):
        """
        Event handler that injects the JWT Bearer token into the Authorization header.

        This is called after boto3 signs the request but before it's sent over the network.
        It replaces the AWS signature with the AIStore JWT token.
        More details: https://github.com/NVIDIA/aistore/tree/main/python/aistore/botocore_patch#boto3-with-aistore-authentication

        :param request: The request object from botocore containing headers, URL, etc.
        :param kwargs: Additional keyword arguments from the event system.
        """
        if self._ais_credentials_provider:
            credentials = self._ais_credentials_provider.get_credentials()
            if credentials.token:
                # Replace the Authorization header with the JWT Bearer token
                request.headers["Authorization"] = f"Bearer {credentials.token}"
