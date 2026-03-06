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

import datetime
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from multistorageclient.instrumentation.auth import CertificatePaths, VaultCertificateProvider


def _generate_pem_cert(
    not_valid_before: datetime.datetime,
    not_valid_after: datetime.datetime,
) -> tuple[str, str]:
    """Generate a self-signed PEM certificate and private key with the given validity window."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_valid_before)
        .not_valid_after(not_valid_after)
        .sign(key, hashes.SHA256())
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM).decode()
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    return cert_pem, key_pem


def _make_valid_vault_response(cert_pem: str, key_pem: str, ca_pem: str) -> dict:
    return {
        "data": {
            "data": {
                "cert": cert_pem,
                "key": key_pem,
                "ca": ca_pem,
            }
        }
    }


@pytest.fixture
def vault_config():
    """Fixture providing basic Vault configuration."""
    return {
        "vault_endpoint": "https://vault.example.com",
        "vault_namespace": "test-namespace",
        "approle_id": "test-role-id",
        "approle_secret": "test-secret-id",
    }


@pytest.fixture
def mock_hvac():
    """Fixture to mock hvac library."""
    with patch("multistorageclient.instrumentation.auth.hvac") as mock:
        yield mock


@pytest.fixture
def valid_cert_data():
    """Fixture providing a valid (non-expired) self-signed certificate."""
    now = datetime.datetime.now(datetime.timezone.utc)
    cert_pem, key_pem = _generate_pem_cert(
        not_valid_before=now - datetime.timedelta(days=30),
        not_valid_after=now + datetime.timedelta(days=60),
    )
    ca_pem = cert_pem
    return {"cert": cert_pem, "key": key_pem, "ca": ca_pem}


@pytest.fixture
def expired_cert_data():
    """Fixture providing an expired self-signed certificate."""
    now = datetime.datetime.now(datetime.timezone.utc)
    cert_pem, key_pem = _generate_pem_cert(
        not_valid_before=now - datetime.timedelta(days=120),
        not_valid_after=now - datetime.timedelta(days=1),
    )
    ca_pem = cert_pem
    return {"cert": cert_pem, "key": key_pem, "ca": ca_pem}


@pytest.fixture
def provider_with_mocked_vault(vault_config, mock_hvac, valid_cert_data):
    """Fixture to create a VaultCertificateProvider with mocked Vault client returning valid certs."""
    mock_client = MagicMock()
    mock_hvac.Client.return_value = mock_client

    mock_client.auth.approle.login.return_value = {"auth": {"client_token": "test-client-token"}}
    mock_client.secrets.kv.v2.read_secret_version.return_value = _make_valid_vault_response(
        valid_cert_data["cert"], valid_cert_data["key"], valid_cert_data["ca"]
    )

    provider = VaultCertificateProvider(**vault_config)
    yield provider, mock_client, mock_hvac


@pytest.fixture(autouse=True)
def isolated_cert_cache(tmp_path, monkeypatch):
    """Isolate each test's cert cache to its own temp directory."""
    monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))


class TestVaultCertificateProvider:
    def test_init_with_defaults(self, vault_config):
        """Test initialization with default values."""
        provider = VaultCertificateProvider(**vault_config)

        assert provider._vault_endpoint == vault_config["vault_endpoint"]
        assert provider._vault_namespace == vault_config["vault_namespace"]
        assert provider._approle_id == vault_config["approle_id"]
        assert provider._approle_secret == vault_config["approle_secret"]
        assert provider._secret_path == "certificates"
        assert provider._mount_point == "secret"
        assert provider._cert_key == "cert"
        assert provider._key_key == "key"
        assert provider._ca_key == "ca"

    def test_init_with_custom_values(self, vault_config):
        """Test initialization with custom values."""
        provider = VaultCertificateProvider(
            **vault_config,
            secret_path="custom/path",
            mount_point="custom/mount",
            cert_key="custom_cert",
            key_key="custom_key",
            ca_key="custom_ca",
        )

        assert provider._secret_path == "custom/path"
        assert provider._mount_point == "custom/mount"
        assert provider._cert_key == "custom_cert"
        assert provider._key_key == "custom_key"
        assert provider._ca_key == "custom_ca"

    def test_get_cert_cache_dir(self, vault_config):
        """Test certificate cache directory path generation."""
        provider = VaultCertificateProvider(**vault_config)
        cache_dir = provider._get_cert_cache_dir()
        expected_dir = os.path.join(tempfile.gettempdir(), "msc", "observability", "mtls")
        assert cache_dir == expected_dir

    def test_authenticate_to_vault_success(self, provider_with_mocked_vault):
        """Test successful authentication to Vault."""
        provider, mock_client, _ = provider_with_mocked_vault

        token = provider._authenticate_to_vault()

        assert token == "test-client-token"
        mock_client.auth.approle.login.assert_called_once_with(
            role_id="test-role-id",
            secret_id="test-secret-id",
        )

    def test_authenticate_to_vault_failure(self, vault_config, mock_hvac):
        """Test authentication failure handling."""
        mock_client = MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.auth.approle.login.side_effect = Exception("Auth failed")

        provider = VaultCertificateProvider(**vault_config)

        with patch("multistorageclient.instrumentation.auth.time.sleep"):
            with pytest.raises(RuntimeError, match="Failed to authenticate to Vault"):
                provider._authenticate_to_vault()

    def test_fetch_certificates_from_vault_success(self, provider_with_mocked_vault):
        """Test successful certificate fetching."""
        provider, mock_client, _ = provider_with_mocked_vault

        certs = provider._fetch_certificates_from_vault("test-token")

        assert "cert" in certs
        assert "key" in certs
        assert "ca" in certs
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="certificates",
            mount_point="secret",
            raise_on_deleted_version=True,
        )

    def test_fetch_certificates_missing_key(self, vault_config, mock_hvac):
        """Test handling of missing certificate key."""
        mock_client = MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {
                "data": {
                    "cert": "test-cert",
                    # Missing "key" and "ca"
                }
            }
        }

        provider = VaultCertificateProvider(**vault_config)

        with patch("multistorageclient.instrumentation.auth.time.sleep"):
            with pytest.raises(RuntimeError, match="Failed to fetch certificates"):
                provider._fetch_certificates_from_vault("test-token")

    def test_write_certificates_to_disk(self, vault_config, valid_cert_data):
        """Test writing certificates to disk with correct permissions."""
        provider = VaultCertificateProvider(**vault_config)

        paths = provider._write_certificates_to_disk(valid_cert_data)

        assert isinstance(paths, CertificatePaths)
        assert os.path.exists(paths.client_certificate_file)
        assert os.path.exists(paths.client_key_file)
        assert os.path.exists(paths.certificate_file)

        with open(paths.client_certificate_file) as f:
            assert f.read() == valid_cert_data["cert"]
        with open(paths.client_key_file) as f:
            assert f.read() == valid_cert_data["key"]
        with open(paths.certificate_file) as f:
            assert f.read() == valid_cert_data["ca"]

        cert_mode = os.stat(paths.client_certificate_file).st_mode & 0o777
        key_mode = os.stat(paths.client_key_file).st_mode & 0o777
        ca_mode = os.stat(paths.certificate_file).st_mode & 0o777

        assert cert_mode == 0o644
        assert key_mode == 0o600
        assert ca_mode == 0o644

    def test_get_certificates_full_flow(self, provider_with_mocked_vault):
        """Test full certificate fetching flow."""
        provider, _, _ = provider_with_mocked_vault

        paths = provider.get_certificates()

        assert isinstance(paths, CertificatePaths)
        assert os.path.exists(paths.client_certificate_file)
        assert os.path.exists(paths.client_key_file)
        assert os.path.exists(paths.certificate_file)
        assert os.path.exists(provider._get_cache_metadata_path())

    def test_get_certificates_uses_in_memory_cache(self, provider_with_mocked_vault):
        """Test that in-memory cached certificates are returned on subsequent calls."""
        provider, mock_client, _ = provider_with_mocked_vault

        paths1 = provider.get_certificates()
        paths2 = provider.get_certificates()

        assert paths1 == paths2
        assert mock_client.auth.approle.login.call_count == 1

    def test_get_certificates_uses_disk_cache_cross_instance(self, vault_config, mock_hvac, valid_cert_data):
        """Test that a new provider instance uses certificates cached on disk by another instance."""
        mock_client = MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.auth.approle.login.return_value = {"auth": {"client_token": "test-client-token"}}
        mock_client.secrets.kv.v2.read_secret_version.return_value = _make_valid_vault_response(
            valid_cert_data["cert"], valid_cert_data["key"], valid_cert_data["ca"]
        )

        provider1 = VaultCertificateProvider(**vault_config)
        paths1 = provider1.get_certificates()
        assert mock_client.auth.approle.login.call_count == 1

        provider2 = VaultCertificateProvider(**vault_config)
        paths2 = provider2.get_certificates()

        assert mock_client.auth.approle.login.call_count == 1
        assert paths1 == paths2

    def test_get_certificates_invalidates_cache_on_config_change(self, vault_config, mock_hvac, valid_cert_data):
        """Test that cached certificates are invalidated when auth config changes."""
        mock_client = MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.auth.approle.login.return_value = {"auth": {"client_token": "test-client-token"}}
        mock_client.secrets.kv.v2.read_secret_version.return_value = _make_valid_vault_response(
            valid_cert_data["cert"], valid_cert_data["key"], valid_cert_data["ca"]
        )

        provider1 = VaultCertificateProvider(**vault_config)
        provider1.get_certificates()
        assert mock_client.auth.approle.login.call_count == 1

        changed_config = vault_config.copy()
        changed_config["vault_endpoint"] = "https://vault-new.example.com"
        provider2 = VaultCertificateProvider(**changed_config)
        provider2.get_certificates()

        assert mock_client.auth.approle.login.call_count == 2

    def test_get_certificates_invalidates_cache_on_expired_cert(
        self, vault_config, mock_hvac, expired_cert_data, valid_cert_data
    ):
        """Test that expired cached certificates trigger a re-fetch from Vault."""
        mock_client = MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.auth.approle.login.return_value = {"auth": {"client_token": "test-client-token"}}

        mock_client.secrets.kv.v2.read_secret_version.return_value = _make_valid_vault_response(
            expired_cert_data["cert"], expired_cert_data["key"], expired_cert_data["ca"]
        )

        provider1 = VaultCertificateProvider(**vault_config)
        provider1.get_certificates()
        assert mock_client.auth.approle.login.call_count == 1

        mock_client.secrets.kv.v2.read_secret_version.return_value = _make_valid_vault_response(
            valid_cert_data["cert"], valid_cert_data["key"], valid_cert_data["ca"]
        )

        provider2 = VaultCertificateProvider(**vault_config)
        provider2.get_certificates()

        assert mock_client.auth.approle.login.call_count == 2


class TestConfigFingerprint:
    def test_deterministic(self, vault_config):
        """Test that config fingerprint is deterministic for the same config."""
        provider1 = VaultCertificateProvider(**vault_config)
        provider2 = VaultCertificateProvider(**vault_config)
        assert provider1._compute_config_fingerprint() == provider2._compute_config_fingerprint()

    def test_changes_with_config(self, vault_config):
        """Test that config fingerprint changes when any auth parameter changes."""
        provider1 = VaultCertificateProvider(**vault_config)
        fp1 = provider1._compute_config_fingerprint()

        for key, new_value in [
            ("vault_endpoint", "https://other-vault.example.com"),
            ("vault_namespace", "other-namespace"),
            ("approle_id", "other-role-id"),
            ("approle_secret", "other-secret-id"),
        ]:
            changed_config = vault_config.copy()
            changed_config[key] = new_value
            provider2 = VaultCertificateProvider(**changed_config)
            assert provider2._compute_config_fingerprint() != fp1, f"Fingerprint should change when {key} changes"


class TestCacheMetadata:
    def test_write_and_read_roundtrip(self, vault_config):
        """Test that cache metadata can be written and read back."""
        provider = VaultCertificateProvider(**vault_config)
        cert_dir = provider._get_cert_cache_dir()
        os.makedirs(cert_dir, mode=0o700, exist_ok=True)
        provider._write_cache_metadata()

        metadata = provider._read_cache_metadata()
        assert metadata is not None
        assert metadata["config_fingerprint"] == provider._compute_config_fingerprint()

    def test_read_returns_none_when_missing(self, vault_config):
        """Test that reading non-existent metadata returns None."""
        provider = VaultCertificateProvider(**vault_config)
        assert provider._read_cache_metadata() is None

    def test_read_returns_none_on_corrupt_file(self, vault_config):
        """Test that reading corrupt metadata returns None."""
        provider = VaultCertificateProvider(**vault_config)
        cert_dir = provider._get_cert_cache_dir()
        os.makedirs(cert_dir, mode=0o700, exist_ok=True)
        with open(provider._get_cache_metadata_path(), "w") as f:
            f.write("not-valid-json{{{")

        assert provider._read_cache_metadata() is None


class TestCertExpiry:
    def test_valid_cert_not_expired(self, vault_config, valid_cert_data):
        """Test that a valid (non-expired) certificate is not flagged as expired."""
        provider = VaultCertificateProvider(**vault_config)
        provider._write_certificates_to_disk(valid_cert_data)

        assert not provider._is_client_cert_expired()

    def test_expired_cert_detected(self, vault_config, expired_cert_data):
        """Test that an expired certificate is correctly detected."""
        provider = VaultCertificateProvider(**vault_config)
        provider._write_certificates_to_disk(expired_cert_data)

        assert provider._is_client_cert_expired()

    def test_missing_cert_treated_as_expired(self, vault_config):
        """Test that a missing certificate file is treated as expired."""
        provider = VaultCertificateProvider(**vault_config)
        assert provider._is_client_cert_expired()

    def test_unparseable_cert_treated_as_expired(self, vault_config):
        """Test that an unparseable certificate file is treated as expired."""
        provider = VaultCertificateProvider(**vault_config)
        cert_dir = provider._get_cert_cache_dir()
        os.makedirs(cert_dir, mode=0o700, exist_ok=True)
        cert_path = provider._get_cert_paths().client_certificate_file
        with open(cert_path, "w") as f:
            f.write("not a valid PEM certificate")

        assert provider._is_client_cert_expired()


class TestCacheValidation:
    def test_valid_cache(self, vault_config, valid_cert_data):
        """Test that cache is valid when files exist, fingerprint matches, and cert is not expired."""
        provider = VaultCertificateProvider(**vault_config)
        provider._write_certificates_to_disk(valid_cert_data)
        provider._write_cache_metadata()

        assert provider._is_cache_valid()

    def test_invalid_without_metadata(self, vault_config, valid_cert_data):
        """Test that cache is invalid when metadata file is missing."""
        provider = VaultCertificateProvider(**vault_config)
        provider._write_certificates_to_disk(valid_cert_data)

        assert not provider._is_cache_valid()

    def test_invalid_with_different_config(self, vault_config, valid_cert_data):
        """Test that cache is invalid when config fingerprint doesn't match."""
        provider1 = VaultCertificateProvider(**vault_config)
        provider1._write_certificates_to_disk(valid_cert_data)
        provider1._write_cache_metadata()

        changed_config = vault_config.copy()
        changed_config["vault_endpoint"] = "https://vault-new.example.com"
        provider2 = VaultCertificateProvider(**changed_config)

        assert not provider2._is_cache_valid()

    def test_invalid_with_expired_cert(self, vault_config, expired_cert_data):
        """Test that cache is invalid when the certificate has expired."""
        provider = VaultCertificateProvider(**vault_config)
        provider._write_certificates_to_disk(expired_cert_data)
        provider._write_cache_metadata()

        assert not provider._is_cache_valid()

    def test_invalid_without_cert_files(self, vault_config):
        """Test that cache is invalid when cert files don't exist."""
        provider = VaultCertificateProvider(**vault_config)
        assert not provider._is_cache_valid()


class TestInvalidateDiskCache:
    def test_removes_all_files(self, provider_with_mocked_vault):
        """Test that _invalidate_disk_cache removes cert files and metadata."""
        provider, _, _ = provider_with_mocked_vault
        provider.get_certificates()

        paths = provider._get_cert_paths()
        assert os.path.exists(paths.client_certificate_file)
        assert os.path.exists(paths.client_key_file)
        assert os.path.exists(paths.certificate_file)
        assert os.path.exists(provider._get_cache_metadata_path())

        provider._invalidate_disk_cache()

        assert not os.path.exists(paths.client_certificate_file)
        assert not os.path.exists(paths.client_key_file)
        assert not os.path.exists(paths.certificate_file)
        assert not os.path.exists(provider._get_cache_metadata_path())

    def test_noop_when_no_cache(self, vault_config):
        """Test that _invalidate_disk_cache doesn't fail when there's nothing to remove."""
        provider = VaultCertificateProvider(**vault_config)
        provider._invalidate_disk_cache()


class TestCertificatePaths:
    def test_certificate_paths_dataclass(self):
        """Test CertificatePaths dataclass."""
        paths = CertificatePaths(
            client_certificate_file="/path/to/cert.crt",
            client_key_file="/path/to/key.key",
            certificate_file="/path/to/ca.crt",
        )

        assert paths.client_certificate_file == "/path/to/cert.crt"
        assert paths.client_key_file == "/path/to/key.key"
        assert paths.certificate_file == "/path/to/ca.crt"


class TestOptionalDependencyImport:
    def test_auth_module_importable_without_hvac(self):
        """Test that the auth module can be imported without hvac installed."""
        import importlib

        from multistorageclient.instrumentation import auth as auth_module

        with patch.dict(sys.modules, {"hvac": None}):
            importlib.reload(auth_module)

        importlib.reload(auth_module)

    def test_constructor_raises_import_error_without_hvac(self, vault_config):
        """Test that VaultCertificateProvider raises ImportError when hvac is not installed."""
        with patch("multistorageclient.instrumentation.auth.hvac", None):
            with pytest.raises(ImportError, match="pip install 'multi-storage-client\\[vault\\]'"):
                VaultCertificateProvider(**vault_config)
