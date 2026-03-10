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

from unittest.mock import MagicMock, patch

import pytest

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers.s3 import S3StorageProvider, S3URLSigner
from multistorageclient.signers import CloudFrontURLSigner, URLSigner
from multistorageclient.types import SignerType

# ---------------------------------------------------------------------------
# S3URLSigner
# ---------------------------------------------------------------------------


class TestS3URLSigner:
    def test_generate_presigned_url_get(self):
        mock_client = MagicMock()
        mock_client.generate_presigned_url.return_value = "https://bucket.s3.amazonaws.com/key?signature"
        signer = S3URLSigner(mock_client, "my-bucket", expires_in=900)

        url = signer.generate_presigned_url("data/file.bin", method="GET")

        assert url == "https://bucket.s3.amazonaws.com/key?signature"
        mock_client.generate_presigned_url.assert_called_once_with(
            ClientMethod="get_object",
            Params={"Bucket": "my-bucket", "Key": "data/file.bin"},
            ExpiresIn=900,
        )

    def test_generate_presigned_url_put(self):
        mock_client = MagicMock()
        mock_client.generate_presigned_url.return_value = "https://bucket.s3.amazonaws.com/key?sig"
        signer = S3URLSigner(mock_client, "my-bucket")

        signer.generate_presigned_url("data/file.bin", method="PUT")

        mock_client.generate_presigned_url.assert_called_once_with(
            ClientMethod="put_object",
            Params={"Bucket": "my-bucket", "Key": "data/file.bin"},
            ExpiresIn=3600,
        )

    def test_unsupported_method_raises(self):
        signer = S3URLSigner(MagicMock(), "bucket")
        with pytest.raises(ValueError, match="Unsupported method"):
            signer.generate_presigned_url("key", method="DELETE")

    def test_is_url_signer(self):
        assert issubclass(S3URLSigner, URLSigner)


# ---------------------------------------------------------------------------
# CloudFrontURLSigner
# ---------------------------------------------------------------------------


class TestCloudFrontURLSigner:
    """Exercises the real RSA signing pipeline (no mocks) with a temp key pair."""

    @pytest.fixture
    def rsa_key_pair(self, tmp_path):
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat

        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        key_path = tmp_path / "cf_test.pem"
        key_path.write_bytes(private_key.private_bytes(Encoding.PEM, PrivateFormat.TraditionalOpenSSL, NoEncryption()))
        return str(key_path), private_key

    def test_url_structure(self, rsa_key_pair):
        from urllib.parse import parse_qs, urlparse

        key_path, _ = rsa_key_pair
        signer = CloudFrontURLSigner(
            key_pair_id="K2JCJMDEHXQW5F",
            private_key_path=key_path,
            domain="d111111abcdef8.cloudfront.net",
            expires_in=7200,
        )

        url = signer.generate_presigned_url("data/file.bin")

        parsed = urlparse(url)
        assert parsed.scheme == "https"
        assert parsed.netloc == "d111111abcdef8.cloudfront.net"
        assert parsed.path == "/data/file.bin"

        params = parse_qs(parsed.query)
        assert "Expires" in params
        assert "Signature" in params
        assert params["Key-Pair-Id"] == ["K2JCJMDEHXQW5F"]

    def test_signature_is_cryptographically_valid(self, rsa_key_pair):
        import base64
        import json
        from urllib.parse import parse_qs, urlparse

        from cryptography.hazmat.primitives.asymmetric.padding import PKCS1v15
        from cryptography.hazmat.primitives.hashes import SHA1

        key_path, private_key = rsa_key_pair
        signer = CloudFrontURLSigner(
            key_pair_id="KTEST123",
            private_key_path=key_path,
            domain="cdn.example.com",
            expires_in=3600,
        )

        url = signer.generate_presigned_url("path/to/object.bin")

        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        epoch = int(params["Expires"][0])

        policy = json.dumps(
            {
                "Statement": [
                    {
                        "Resource": "https://cdn.example.com/path/to/object.bin",
                        "Condition": {"DateLessThan": {"AWS:EpochTime": epoch}},
                    }
                ]
            },
            separators=(",", ":"),
        )

        encoded_sig = params["Signature"][0]
        raw_sig = base64.b64decode(encoded_sig.replace("-", "+").replace("_", "=").replace("~", "/"))

        public_key = private_key.public_key()
        public_key.verify(raw_sig, policy.encode("utf-8"), PKCS1v15(), SHA1())

    def test_expiry_is_in_future(self, rsa_key_pair):
        import time
        from urllib.parse import parse_qs, urlparse

        key_path, _ = rsa_key_pair
        expires_in = 1800
        before = int(time.time())

        signer = CloudFrontURLSigner(
            key_pair_id="KTEST",
            private_key_path=key_path,
            domain="cdn.example.com",
            expires_in=expires_in,
        )
        url = signer.generate_presigned_url("file.bin")
        after = int(time.time())

        epoch = int(parse_qs(urlparse(url).query)["Expires"][0])
        assert before + expires_in <= epoch <= after + expires_in

    def test_leading_slash_normalised(self, rsa_key_pair):
        from urllib.parse import urlparse

        key_path, _ = rsa_key_pair
        signer = CloudFrontURLSigner(
            key_pair_id="K1",
            private_key_path=key_path,
            domain="cdn.example.com",
        )

        url = signer.generate_presigned_url("/leading/slash/file.bin")
        assert urlparse(url).path == "/leading/slash/file.bin"

    def test_is_url_signer(self):
        assert issubclass(CloudFrontURLSigner, URLSigner)


# ---------------------------------------------------------------------------
# S3StorageProvider._generate_presigned_url dispatch
# ---------------------------------------------------------------------------


class TestS3StorageProviderPresign:
    @pytest.fixture
    def s3_provider(self):
        with (
            patch("multistorageclient.providers.s3.S3StorageProvider._create_s3_client") as mock_create,
            patch("multistorageclient.providers.s3.S3StorageProvider._create_rust_client"),
        ):
            mock_s3 = MagicMock()
            mock_create.return_value = mock_s3
            provider = S3StorageProvider(
                region_name="us-east-1",
                base_path="my-bucket/prefix",
            )
            provider._s3_client = mock_s3
            yield provider, mock_s3

    def test_native_presign_default(self, s3_provider):
        provider, mock_s3 = s3_provider
        mock_s3.generate_presigned_url.return_value = "https://presigned.url"

        url = provider.generate_presigned_url("data/file.bin")

        assert url == "https://presigned.url"
        mock_s3.generate_presigned_url.assert_called_once()
        call_kwargs = mock_s3.generate_presigned_url.call_args
        assert call_kwargs.kwargs.get("ClientMethod") or call_kwargs[1].get("ClientMethod") == "get_object"

    def test_native_presign_explicit_s3_type(self, s3_provider):
        provider, mock_s3 = s3_provider
        mock_s3.generate_presigned_url.return_value = "https://presigned.url"

        url = provider.generate_presigned_url("data/file.bin", signer_type=SignerType.S3)

        assert url == "https://presigned.url"
        mock_s3.generate_presigned_url.assert_called_once()

    def test_cloudfront_dispatch(self, s3_provider):
        provider, _ = s3_provider

        with patch.object(CloudFrontURLSigner, "generate_presigned_url", return_value="https://cf.url") as mock_cf:
            url = provider.generate_presigned_url(
                "data/file.bin",
                signer_type=SignerType.CLOUDFRONT,
                signer_options={
                    "key_pair_id": "KEYPAIR",
                    "private_key_path": "/tmp/key.pem",
                    "domain": "d123.cloudfront.net",
                },
            )

        assert url == "https://cf.url"
        mock_cf.assert_called_once()

    def test_unsupported_signer_type_raises(self, s3_provider):
        provider, _ = s3_provider
        with pytest.raises(ValueError, match="Unsupported signer type"):
            provider.generate_presigned_url("data/file.bin", signer_type="unknown")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Unsupported provider
# ---------------------------------------------------------------------------


class TestUnsupportedProvider:
    def test_posix_provider_raises(self):
        config = StorageClientConfig.from_yaml(
            """
            profiles:
              test-posix:
                storage_provider:
                  type: file
                  options:
                    base_path: /tmp/test
            """,
            profile="test-posix",
        )
        client = StorageClient(config)
        with pytest.raises(NotImplementedError, match="does not support presigned URL"):
            client.generate_presigned_url("some/path")


# ---------------------------------------------------------------------------
# Client delegation (SingleStorageClient)
# ---------------------------------------------------------------------------


class TestClientDelegation:
    def test_single_client_delegates_to_provider(self):
        config = StorageClientConfig.from_yaml(
            """
            profiles:
              test-single:
                storage_provider:
                  type: file
                  options:
                    base_path: /tmp/test
            """,
            profile="test-single",
        )
        client = StorageClient(config)

        mock_provider = MagicMock()
        mock_provider.generate_presigned_url.return_value = "https://presigned"
        client._storage_provider = mock_provider

        url = client.generate_presigned_url("path/to/obj", method="PUT", signer_type=SignerType.S3)

        assert url == "https://presigned"
        mock_provider.generate_presigned_url.assert_called_once_with(
            "path/to/obj", method="PUT", signer_type=SignerType.S3, signer_options=None
        )


# ---------------------------------------------------------------------------
# Shortcut generate_presigned_url()
# ---------------------------------------------------------------------------


class TestGeneratePresignedUrlShortcut:
    @patch("multistorageclient.shortcuts.resolve_storage_client")
    def test_generate_presigned_url_shortcut(self, mock_resolve):
        mock_client = MagicMock()
        mock_client.generate_presigned_url.return_value = "https://presigned"
        mock_resolve.return_value = (mock_client, "prefix/file.bin")

        from multistorageclient import generate_presigned_url

        url = generate_presigned_url("msc://profile/prefix/file.bin", method="GET")

        assert url == "https://presigned"
        mock_resolve.assert_called_once_with("msc://profile/prefix/file.bin")
        mock_client.generate_presigned_url.assert_called_once_with(
            "prefix/file.bin", method="GET", signer_type=None, signer_options=None
        )
