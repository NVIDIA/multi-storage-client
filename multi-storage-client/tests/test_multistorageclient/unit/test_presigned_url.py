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

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers.azure import (
    AzureBlobStorageProvider,
    AzureURLSigner,
    DefaultAzureCredentialsProvider,
    StaticAzureCredentialsProvider,
    _parse_account_name_from_url,
    _parse_connection_string,
)
from multistorageclient.providers.s3 import S3StorageProvider, S3URLSigner
from multistorageclient.signers import CloudFrontURLSigner, URLSigner
from multistorageclient.types import CredentialsProvider, SignerType

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

    def test_origin_path_stripped_from_url(self, rsa_key_pair):
        from urllib.parse import urlparse

        key_path, _ = rsa_key_pair
        signer = CloudFrontURLSigner(
            key_pair_id="K1",
            private_key_path=key_path,
            domain="d111111abcdef8.cloudfront.net",
            origin_path="/cloudfront",
        )

        url = signer.generate_presigned_url("cloudfront/a.bin")
        assert urlparse(url).path == "/a.bin"

    def test_origin_path_stripped_with_leading_slash_on_path(self, rsa_key_pair):
        from urllib.parse import urlparse

        key_path, _ = rsa_key_pair
        signer = CloudFrontURLSigner(
            key_pair_id="K1",
            private_key_path=key_path,
            domain="d111111abcdef8.cloudfront.net",
            origin_path="cloudfront",  # no leading slash — should still work
        )

        url = signer.generate_presigned_url("/cloudfront/subdir/a.bin")
        assert urlparse(url).path == "/subdir/a.bin"

    def test_origin_path_mismatch_raises(self, rsa_key_pair):
        key_path, _ = rsa_key_pair
        signer = CloudFrontURLSigner(
            key_pair_id="K1",
            private_key_path=key_path,
            domain="d111111abcdef8.cloudfront.net",
            origin_path="/cloudfront",
        )

        with pytest.raises(ValueError, match="origin path"):
            signer.generate_presigned_url("other/a.bin")

    def test_no_origin_path_unchanged(self, rsa_key_pair):
        from urllib.parse import urlparse

        key_path, _ = rsa_key_pair
        signer = CloudFrontURLSigner(
            key_pair_id="K1",
            private_key_path=key_path,
            domain="d111111abcdef8.cloudfront.net",
            # no origin
        )

        url = signer.generate_presigned_url("cloudfront/a.bin")
        assert urlparse(url).path == "/cloudfront/a.bin"


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


# ---------------------------------------------------------------------------
# Azure helpers
# ---------------------------------------------------------------------------


class TestAzureHelpers:
    def test_parse_account_name_from_url(self):
        assert _parse_account_name_from_url("https://myaccount.blob.core.windows.net") == "myaccount"

    def test_parse_account_name_from_invalid_url_raises(self):
        with pytest.raises(ValueError, match="Invalid Azure account URL"):
            _parse_account_name_from_url("not-a-valid-url")

    def test_parse_connection_string(self):
        conn_str = "AccountName=myaccount;AccountKey=mykey==;DefaultEndpointsProtocol=https"
        parsed = _parse_connection_string(conn_str)
        assert parsed["AccountName"] == "myaccount"
        assert parsed["AccountKey"] == "mykey=="
        assert parsed["DefaultEndpointsProtocol"] == "https"


# ---------------------------------------------------------------------------
# AzureURLSigner
# ---------------------------------------------------------------------------


class TestAzureURLSigner:
    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="sv=2021&sig=abc")
    def test_get_url(self, mock_sas):
        signer = AzureURLSigner(
            account_name="myaccount",
            account_url="https://myaccount.blob.core.windows.net",
            account_key="mykey==",
            expires_in=3600,
        )
        url = signer.generate_presigned_url("mycontainer/path/to/blob.bin", method="GET")

        assert url == "https://myaccount.blob.core.windows.net/mycontainer/path/to/blob.bin?sv=2021&sig=abc"
        call_kwargs = mock_sas.call_args.kwargs
        assert call_kwargs["account_name"] == "myaccount"
        assert call_kwargs["container_name"] == "mycontainer"
        assert call_kwargs["blob_name"] == "path/to/blob.bin"
        assert call_kwargs["account_key"] == "mykey=="
        perm = call_kwargs["permission"]
        assert perm.read is True
        assert perm.write is False
        assert perm.delete is False

    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="sv=2021&sig=xyz")
    def test_put_url(self, mock_sas):
        signer = AzureURLSigner(
            account_name="myaccount",
            account_url="https://myaccount.blob.core.windows.net",
            account_key="mykey==",
        )
        signer.generate_presigned_url("mycontainer/blob.bin", method="PUT")

        perm = mock_sas.call_args.kwargs["permission"]
        assert perm.write is True
        assert perm.create is True
        assert perm.read is False
        assert perm.delete is False

    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="token")
    def test_delete_url(self, mock_sas):
        signer = AzureURLSigner(
            account_name="a",
            account_url="https://a.blob.core.windows.net",
            account_key="key",
        )
        signer.generate_presigned_url("container/blob", method="DELETE")
        perm = mock_sas.call_args.kwargs["permission"]
        assert perm.delete is True
        assert perm.read is False
        assert perm.write is False

    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="token")
    def test_unknown_method_defaults_to_read(self, mock_sas):
        signer = AzureURLSigner(
            account_name="a",
            account_url="https://a.blob.core.windows.net",
            account_key="key",
        )
        signer.generate_presigned_url("container/blob", method="HEAD")
        perm = mock_sas.call_args.kwargs["permission"]
        assert perm.read is True
        assert perm.write is False

    def test_no_credentials_raises(self):
        with pytest.raises(ValueError, match="account_key or user_delegation_key"):
            AzureURLSigner(account_name="a", account_url="https://a.blob.core.windows.net")

    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="token")
    def test_user_delegation_key_path(self, mock_sas):
        udk = MagicMock()
        signer = AzureURLSigner(
            account_name="myaccount",
            account_url="https://myaccount.blob.core.windows.net",
            user_delegation_key=udk,
        )
        signer.generate_presigned_url("container/blob.bin")

        call_kwargs = mock_sas.call_args.kwargs
        assert call_kwargs["user_delegation_key"] is udk
        assert "account_key" not in call_kwargs

    def test_is_url_signer(self):
        assert issubclass(AzureURLSigner, URLSigner)


# ---------------------------------------------------------------------------
# AzureBlobStorageProvider._generate_presigned_url
# ---------------------------------------------------------------------------


class TestAzureStorageProviderPresign:
    @pytest.fixture
    def static_provider(self):
        conn_str = "AccountName=myaccount;AccountKey=mykey=="
        with patch("multistorageclient.providers.azure.AzureBlobStorageProvider._create_blob_service_client"):
            provider = AzureBlobStorageProvider(
                endpoint_url="https://myaccount.blob.core.windows.net",
                credentials_provider=StaticAzureCredentialsProvider(conn_str),
            )
        return provider

    @pytest.fixture
    def default_cred_provider(self):
        # Patch DefaultAzureCredential so no real auth is attempted and
        # _create_blob_service_client so no real SDK client is created.
        with (
            patch("multistorageclient.providers.azure.DefaultAzureCredential"),
            patch("multistorageclient.providers.azure.AzureBlobStorageProvider._create_blob_service_client"),
        ):
            creds_provider = DefaultAzureCredentialsProvider()
            provider = AzureBlobStorageProvider(
                endpoint_url="https://myaccount.blob.core.windows.net",
                credentials_provider=creds_provider,
            )
            # Replace the blob service client with a plain MagicMock for test control.
            provider._blob_service_client = MagicMock()
        return provider

    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="token=abc")
    def test_account_key_path(self, mock_sas, static_provider):
        url = static_provider._generate_presigned_url("mycontainer/blob.bin")
        assert "mycontainer/blob.bin" in url
        assert "token=abc" in url

    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="token=abc")
    def test_account_key_signing_material_is_cached(self, mock_sas, static_provider):
        static_provider._generate_presigned_url("mycontainer/blob.bin")
        static_provider._generate_presigned_url("mycontainer/blob2.bin")
        # Connection string should be parsed once and reused as static signing material.
        assert mock_sas.call_count == 2
        assert static_provider._account_key_signing_material == ("myaccount", "mykey==")

    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="token=udk")
    def test_delegation_key_path(self, mock_sas, default_cred_provider):
        udk = MagicMock()
        default_cred_provider._blob_service_client.get_user_delegation_key.return_value = udk

        url = default_cred_provider._generate_presigned_url("mycontainer/blob.bin")
        assert "token=udk" in url
        default_cred_provider._blob_service_client.get_user_delegation_key.assert_called_once()
        # Delegation key should be requested for the maximum Azure-allowed lifetime (7 days).
        now = datetime.now(timezone.utc)
        key_expiry = default_cred_provider._blob_service_client.get_user_delegation_key.call_args.kwargs[
            "key_expiry_time"
        ]
        assert timedelta(days=6, hours=23) <= (key_expiry - now) <= timedelta(days=7, hours=1)

    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="token=udk")
    def test_delegation_key_cached_until_near_expiry(self, mock_sas, default_cred_provider):
        udk = MagicMock()
        default_cred_provider._blob_service_client.get_user_delegation_key.return_value = udk

        # First call creates the delegation key.
        default_cred_provider._generate_presigned_url("mycontainer/blob.bin")
        # Second call within the expiry window reuses it.
        default_cred_provider._generate_presigned_url("mycontainer/blob2.bin")

        default_cred_provider._blob_service_client.get_user_delegation_key.assert_called_once()

    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="token=udk")
    def test_delegation_key_refreshed_when_near_expiry(self, mock_sas, default_cred_provider):
        udk = MagicMock()
        default_cred_provider._blob_service_client.get_user_delegation_key.return_value = udk

        # Simulate a cached delegation key that is about to expire (within refresh buffer).
        default_cred_provider._delegation_user_key = MagicMock()
        default_cred_provider._delegation_signer_expiry = datetime.now(timezone.utc) + timedelta(minutes=2)

        default_cred_provider._generate_presigned_url("mycontainer/blob.bin")

        # A fresh delegation key must have been fetched.
        default_cred_provider._blob_service_client.get_user_delegation_key.assert_called_once()

    def test_unsupported_signer_type_raises(self, static_provider):
        with pytest.raises(ValueError, match="Unsupported signer type"):
            static_provider._generate_presigned_url("c/b", signer_type=SignerType.CLOUDFRONT)

    def test_unsupported_credentials_raises(self):
        with patch("multistorageclient.providers.azure.AzureBlobStorageProvider._create_blob_service_client"):
            provider = AzureBlobStorageProvider(
                endpoint_url="https://myaccount.blob.core.windows.net",
                credentials_provider=MagicMock(spec=CredentialsProvider),
            )
        # Patch refresh so it doesn't try to rebuild the blob service client.
        with (
            patch.object(provider, "_refresh_blob_service_client_if_needed"),
            pytest.raises(ValueError, match="StaticAzureCredentialsProvider"),
        ):
            provider._generate_presigned_url("c/b")

    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="tok")
    def test_explicit_azure_signer_type_accepted(self, mock_sas, static_provider):
        url = static_provider._generate_presigned_url("c/b", signer_type=SignerType.AZURE)
        assert "tok" in url

    @patch("multistorageclient.providers.azure.generate_blob_sas", return_value="tok")
    def test_custom_expires_in(self, mock_sas, static_provider):
        static_provider._generate_presigned_url("c/b", signer_options={"expires_in": 900})
        expiry = mock_sas.call_args.kwargs["expiry"]
        now = datetime.now(timezone.utc)
        assert timedelta(seconds=850) <= (expiry - now) <= timedelta(seconds=950)
