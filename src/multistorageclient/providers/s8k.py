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

import mimetypes
from typing import IO, Optional, Union

from .s3 import S3StorageProvider

PROVIDER = "s8k"


class S8KStorageProvider(S3StorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with SwiftStack.

    This provider extends S3StorageProvider with SwiftStack-specific features:

    - Legacy retry mode for handling HTTP 429 errors
    - Optional content type inference from file extensions

    :param infer_content_type: When True, automatically infers MIME types from file extensions during upload operations.
        For example, ``.wav`` files are uploaded with ``Content-Type: audio/x-wav``, enabling browsers to play
        media files inline rather than downloading them. Uses Python's built-in ``mimetypes`` module for inference.
        Default is False. Only affects write operations (``upload_file``, ``write``, ``put_object``).

    .. note::
        This provider inherits all parameters from :py:class:`multistorageclient.providers.s3.S3StorageProvider`.
        See the S3 provider documentation for additional configuration options like ``region_name``, ``endpoint_url``,
        ``base_path``, ``credentials_provider``, ``multipart_threshold``, ``max_concurrency``, etc.

    Example:
        .. code-block:: yaml

            profiles:
              my-s8k-profile:
                storage_provider:
                  type: s8k
                  options:
                    base_path: my-bucket
                    region_name: us-east-1
                    endpoint_url: https://s8k.example.com
                    infer_content_type: true  # Enable MIME type inference
    """

    def __init__(self, *args, **kwargs):
        # Extract the infer_content_type option before passing to parent
        self._infer_content_type = kwargs.pop("infer_content_type", False)

        kwargs["request_checksum_calculation"] = "when_required"
        kwargs["response_checksum_validation"] = "when_required"

        # "legacy" retry mode is required for SwiftStack (retry on HTTP 429 errors)
        kwargs["retries"] = kwargs.get("retries", {}) | {"mode": "legacy"}

        super().__init__(*args, **kwargs)

        # override the provider name from "s3"
        self._provider_name = PROVIDER

    def _guess_content_type(self, file_path: str) -> Optional[str]:
        """
        Guess the content type based on the file extension using Python's mimetypes module.

        This method mimics the behavior of python-swiftclient, which automatically infers
        MIME types from file extensions (e.g., .wav → audio/x-wav).

        :param file_path: The path or key of the file (can be local path or remote key).
        :return: The guessed MIME type, or None if inference is disabled or type cannot be determined.
        """
        if not self._infer_content_type:
            return None

        # Use mimetypes to guess the content type based on file extension
        content_type, _ = mimetypes.guess_type(file_path)
        return content_type

    def _put_object(
        self,
        path: str,
        body: bytes,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
        content_type: Optional[str] = None,
    ) -> int:
        """
        Uploads an object with optional content type inference.

        Infers the content type from the file extension if enabled and not explicitly provided.

        :param path: The S3 path where the object will be uploaded.
        :param body: The content of the object as bytes.
        :param if_match: Optional If-Match header value.
        :param if_none_match: Optional If-None-Match header value.
        :param attributes: Optional attributes to attach to the object.
        :param content_type: Optional explicit Content-Type. If not provided, will be inferred if enabled.
        """
        # Infer content type from the path if not explicitly provided
        if content_type is None:
            content_type = self._guess_content_type(path)

        # Delegate to parent with inferred or explicit content_type
        return super()._put_object(path, body, if_match, if_none_match, attributes, content_type)

    def _upload_file(
        self,
        remote_path: str,
        f: Union[str, IO],
        attributes: Optional[dict[str, str]] = None,
        content_type: Optional[str] = None,
    ) -> int:
        """
        Uploads a file with optional content type inference.

        Infers the content type from the file extension if enabled and not explicitly provided.

        :param remote_path: The remote path where the file will be uploaded.
        :param f: The source file to upload (file path or file object).
        :param attributes: Optional attributes to attach to the file.
        :param content_type: Optional explicit Content-Type. If not provided, will be inferred if enabled.
        """
        # Infer content type if not explicitly provided
        if content_type is None:
            # For file paths, infer from the local file path
            # For file objects, infer from the remote path (destination key)
            if isinstance(f, str):
                content_type = self._guess_content_type(f)
            else:
                content_type = self._guess_content_type(remote_path)

        # Delegate to parent with inferred or explicit content_type
        return super()._upload_file(remote_path, f, attributes, content_type)
