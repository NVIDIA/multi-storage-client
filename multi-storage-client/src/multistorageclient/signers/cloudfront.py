# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from cryptography.hazmat.primitives.asymmetric.padding import PKCS1v15
from cryptography.hazmat.primitives.hashes import SHA1
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from .base import URLSigner

DEFAULT_CLOUDFRONT_EXPIRES_IN = 3600


class CloudFrontURLSigner(URLSigner):
    """
    Generates CloudFront signed URLs using an RSA key pair.

    Implements the CloudFront canned-policy signing spec directly so that it
    has no dependency on ``botocore`` — only the ``cryptography`` package is
    required (RSA-SHA1 / PKCS1v15).
    """

    _key_pair_id: str
    _private_key_path: str
    _domain: str
    _expires_in: int

    def __init__(
        self,
        *,
        key_pair_id: str,
        private_key_path: str,
        domain: str,
        expires_in: int = DEFAULT_CLOUDFRONT_EXPIRES_IN,
        **_kwargs: Any,
    ) -> None:
        self._key_pair_id = key_pair_id
        self._private_key_path = private_key_path
        self._domain = domain.rstrip("/")
        self._expires_in = expires_in
        self._private_key: Any = None

    def _get_private_key(self) -> Any:
        if self._private_key is None:
            with open(self._private_key_path, "rb") as f:
                self._private_key = load_pem_private_key(f.read(), password=None)
        return self._private_key

    def generate_presigned_url(self, path: str, *, method: str = "GET") -> str:
        private_key = self._get_private_key()

        url = f"https://{self._domain}/{path.lstrip('/')}"
        expiry = datetime.now(timezone.utc) + timedelta(seconds=self._expires_in)
        epoch = int(expiry.timestamp())

        policy = json.dumps(
            {"Statement": [{"Resource": url, "Condition": {"DateLessThan": {"AWS:EpochTime": epoch}}}]},
            separators=(",", ":"),
        )

        signature = private_key.sign(policy.encode("utf-8"), PKCS1v15(), SHA1())  # type: ignore[union-attr]

        encoded_sig = _cf_b64encode(signature)
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}Expires={epoch}&Signature={encoded_sig}&Key-Pair-Id={self._key_pair_id}"


def _cf_b64encode(data: bytes) -> str:
    """CloudFront URL-safe base64: ``+`` → ``-``, ``=`` → ``_``, ``/`` → ``~``.

    Standard base64 characters ``+``, ``=``, and ``/`` are reserved in URLs;
    CloudFront requires this substitution for signed URL query parameters.
    See https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-creating-signed-url-canned-policy.html
    """
    return base64.b64encode(data).decode("ascii").replace("+", "-").replace("=", "_").replace("/", "~")
