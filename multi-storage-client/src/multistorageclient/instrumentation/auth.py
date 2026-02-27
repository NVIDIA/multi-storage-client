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
import fcntl
import hashlib
import json
import logging
import os
import tempfile
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

import hvac
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


@dataclass
class CertificatePaths:
    """Paths to mTLS certificate files."""

    client_certificate_file: str
    client_key_file: str
    certificate_file: str


class CertificateProvider(ABC):
    """Base class for certificate providers used in mTLS authentication."""

    @abstractmethod
    def get_certificates(self) -> CertificatePaths:
        """
        Fetch and return paths to mTLS certificate files.

        :return: CertificatePaths containing paths to client cert, key, and CA cert.
        :raises RuntimeError: If certificate fetching fails.
        """
        pass


class VaultCertificateProvider(CertificateProvider):
    """
    Certificate provider that fetches mTLS certificates from HashiCorp Vault using AppRole authentication.

    This provider authenticates to Vault using AppRole credentials, fetches mTLS
    certificates, and stores them in a temporary location for use with OTLP exporters.
    """

    def __init__(
        self,
        vault_endpoint: str,
        vault_namespace: str,
        approle_id: str,
        approle_secret: str,
        mount_point: str = "secret",
        secret_path: str = "certificates",
        cert_key: str = "cert",
        key_key: str = "key",
        ca_key: str = "ca",
    ):
        """
        Initialize the VaultCertificateProvider.

        :param vault_endpoint: URL of the Vault server (e.g., "https://vault.example.com")
        :param vault_namespace: Vault namespace (e.g., "my-namespace")
        :param approle_id: AppRole role ID for authentication
        :param approle_secret: AppRole secret ID for authentication
        :param mount_point: KV secrets engine mount point (e.g., "secret")
        :param secret_path: Path to the secret in Vault containing certificates.
            Note: hvac internally concatenates this as "<mount_point>/data/<secret_path>"
            when reading from KV v2 secrets engine.
        :param cert_key: Key name for the client certificate in the secret
        :param key_key: Key name for the client key in the secret
        :param ca_key: Key name for the CA certificate in the secret
        """
        self._vault_endpoint = vault_endpoint
        self._vault_namespace = vault_namespace
        self._approle_id = approle_id
        self._approle_secret = approle_secret
        self._secret_path = secret_path
        self._mount_point = mount_point
        self._cert_key = cert_key
        self._key_key = key_key
        self._ca_key = ca_key
        self._cached_paths: Optional[CertificatePaths] = None

    def _get_cert_cache_dir(self) -> str:
        """Get the directory path for caching certificates."""
        return os.path.join(tempfile.gettempdir(), "msc", "observability", "mtls")

    def _compute_config_fingerprint(self) -> str:
        """Compute a SHA-256 fingerprint of the auth config that determines which certs to fetch."""
        config_data = {
            "vault_endpoint": self._vault_endpoint,
            "vault_namespace": self._vault_namespace,
            "approle_id": self._approle_id,
            "approle_secret": self._approle_secret,
            "mount_point": self._mount_point,
            "secret_path": self._secret_path,
            "cert_key": self._cert_key,
            "key_key": self._key_key,
            "ca_key": self._ca_key,
        }
        config_json = json.dumps(config_data, sort_keys=True)
        return hashlib.sha256(config_json.encode()).hexdigest()

    def _get_cache_metadata_path(self) -> str:
        """Get the path to the cache metadata file."""
        return os.path.join(self._get_cert_cache_dir(), ".cache_metadata")

    def _read_cache_metadata(self) -> Optional[dict[str, Any]]:
        """Read and return cached metadata, or None if missing/corrupt."""
        metadata_path = self._get_cache_metadata_path()
        if not os.path.exists(metadata_path):
            return None
        try:
            with open(metadata_path) as f:
                return json.load(f)
        except (IOError, OSError, json.JSONDecodeError):
            return None

    def _write_cache_metadata(self) -> None:
        """Write cache metadata (config fingerprint) to disk."""
        metadata = {
            "config_fingerprint": self._compute_config_fingerprint(),
        }
        metadata_path = self._get_cache_metadata_path()
        with open(metadata_path, "w") as f:
            json.dump(metadata, f)
        os.chmod(metadata_path, 0o600)

    def _is_client_cert_expired(self) -> bool:
        """
        Check if the cached client certificate has expired by parsing its X.509 notAfter field.

        Returns True if the cert is expired or unreadable, False if still valid.
        """
        cert_path = self._get_cert_paths().client_certificate_file
        try:
            from cryptography import x509

            with open(cert_path, "rb") as f:
                cert = x509.load_pem_x509_certificate(f.read())
            now = datetime.datetime.now(datetime.timezone.utc)
            return now >= cert.not_valid_after_utc
        except Exception:
            logger.debug("Failed to parse certificate expiry, treating as expired", exc_info=True)
            return True

    def _is_cache_valid(self) -> bool:
        """
        Check if the disk cache is valid: cert files exist, config fingerprint matches,
        and the client certificate has not expired.
        """
        if not self._certificates_exist_on_disk():
            return False

        metadata = self._read_cache_metadata()
        if metadata is None:
            return False

        if metadata.get("config_fingerprint") != self._compute_config_fingerprint():
            return False

        if self._is_client_cert_expired():
            return False

        return True

    def _invalidate_disk_cache(self) -> None:
        """Remove cached certificate files and metadata from disk."""
        paths = self._get_cert_paths()
        for path in [paths.client_certificate_file, paths.client_key_file, paths.certificate_file]:
            if os.path.exists(path):
                os.remove(path)
        metadata_path = self._get_cache_metadata_path()
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
        logger.debug("Invalidated cached certificates from disk")

    def _authenticate_to_vault(self) -> str:
        """
        Authenticate to Vault using AppRole and return the client token.

        :return: Vault client token
        :raises RuntimeError: If authentication fails
        """
        client = hvac.Client(url=self._vault_endpoint, namespace=self._vault_namespace)

        retry_count = 0
        last_error = None

        while retry_count < MAX_RETRIES:
            try:
                login_response = client.auth.approle.login(
                    role_id=self._approle_id,
                    secret_id=self._approle_secret,
                )

                if login_response and "auth" in login_response and "client_token" in login_response["auth"]:
                    return login_response["auth"]["client_token"]
                else:
                    raise RuntimeError("Invalid response from Vault AppRole login")

            except Exception as e:
                last_error = e
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    sleep_time = min(BACKOFF_FACTOR * (2**retry_count), 60)
                    logger.debug(f"Vault auth attempt {retry_count} failed: {e}. Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)

        raise RuntimeError(f"Failed to authenticate to Vault after {MAX_RETRIES} attempts: {last_error}")

    def _fetch_certificates_from_vault(self, client_token: str) -> dict[str, str]:
        """
        Fetch certificates from Vault using the client token.

        :param client_token: Vault client token from authentication
        :return: Dictionary containing certificate data
        :raises RuntimeError: If fetching certificates fails
        """
        client = hvac.Client(url=self._vault_endpoint, namespace=self._vault_namespace, token=client_token)

        retry_count = 0
        last_error = None

        while retry_count < MAX_RETRIES:
            try:
                secret_response = client.secrets.kv.v2.read_secret_version(
                    path=self._secret_path, mount_point=self._mount_point, raise_on_deleted_version=True
                )

                if secret_response and "data" in secret_response and "data" in secret_response["data"]:
                    secret_data = secret_response["data"]["data"]
                    required_keys = [self._cert_key, self._key_key, self._ca_key]

                    for key in required_keys:
                        if key not in secret_data:
                            raise RuntimeError(f"Certificate key '{key}' not found in Vault secret")

                    return {
                        "cert": secret_data[self._cert_key],
                        "key": secret_data[self._key_key],
                        "ca": secret_data[self._ca_key],
                    }
                else:
                    raise RuntimeError("Invalid response from Vault when reading secret")

            except Exception as e:
                last_error = e
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    sleep_time = min(BACKOFF_FACTOR * (2**retry_count), 60)
                    logger.debug(f"Vault secret read attempt {retry_count} failed: {e}. Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)

        raise RuntimeError(f"Failed to fetch certificates from Vault after {MAX_RETRIES} attempts: {last_error}")

    def _get_cert_paths(self) -> CertificatePaths:
        """Get the expected certificate file paths."""
        cert_dir = self._get_cert_cache_dir()
        return CertificatePaths(
            client_certificate_file=os.path.join(cert_dir, "cert.crt"),
            client_key_file=os.path.join(cert_dir, "key.key"),
            certificate_file=os.path.join(cert_dir, "ca.crt"),
        )

    def _certificates_exist_on_disk(self) -> bool:
        """Check if all certificate files exist on disk."""
        paths = self._get_cert_paths()
        return (
            os.path.exists(paths.client_certificate_file)
            and os.path.exists(paths.client_key_file)
            and os.path.exists(paths.certificate_file)
        )

    def _write_certificates_to_disk(self, cert_data: dict[str, str]) -> CertificatePaths:
        """
        Write certificates to disk with appropriate permissions.

        :param cert_data: Dictionary containing certificate data
        :return: CertificatePaths with paths to written files
        """
        cert_dir = self._get_cert_cache_dir()
        os.makedirs(cert_dir, mode=0o700, exist_ok=True)

        paths = self._get_cert_paths()

        with open(paths.client_certificate_file, "w") as f:
            f.write(cert_data["cert"])
        os.chmod(paths.client_certificate_file, 0o644)

        with open(paths.client_key_file, "w") as f:
            f.write(cert_data["key"])
        os.chmod(paths.client_key_file, 0o600)

        with open(paths.certificate_file, "w") as f:
            f.write(cert_data["ca"])
        os.chmod(paths.certificate_file, 0o644)

        logger.debug(f"Certificates written to {cert_dir}")

        return paths

    def get_certificates(self) -> CertificatePaths:
        """
        Fetch and return paths to mTLS certificate files.

        Uses file locking to ensure only one process fetches from Vault while others wait.
        Certificates are cached on disk and reused across processes when:

        - The auth config fingerprint matches the current config.
        - The cached client certificate has not expired (checked via X.509 ``notAfter``).

        If either condition fails, the disk cache is invalidated and certificates
        are re-fetched from Vault.

        :return: CertificatePaths containing paths to client cert, key, and CA cert.
        :raises RuntimeError: If certificate fetching or writing fails.
        """
        if self._cached_paths is not None:
            return self._cached_paths

        if self._is_cache_valid():
            logger.debug("Using cached certificates from disk")
            self._cached_paths = self._get_cert_paths()
            return self._cached_paths

        cert_dir = self._get_cert_cache_dir()
        os.makedirs(cert_dir, mode=0o700, exist_ok=True)
        lock_file = os.path.join(cert_dir, ".lock")

        with open(lock_file, "w") as lock_fd:
            logger.debug("Acquiring lock for certificate fetching")
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            try:
                if self._is_cache_valid():
                    logger.debug("Certificates already fetched by another process")
                    self._cached_paths = self._get_cert_paths()
                    return self._cached_paths

                if self._certificates_exist_on_disk():
                    logger.info("Cached certificates are stale (config changed or cert expired), invalidating")
                    self._invalidate_disk_cache()

                logger.info("Fetching certificates from Vault")
                client_token = self._authenticate_to_vault()
                cert_data = self._fetch_certificates_from_vault(client_token)
                self._cached_paths = self._write_certificates_to_disk(cert_data)
                self._write_cache_metadata()
                return self._cached_paths
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)


class AzureAccessTokenProvider(AccessTokenProvider):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            # scopes should be a list to avoid assertion error in msal
            # Ref: https://github.com/AzureAD/microsoft-authentication-library-for-python/blob/1.31.1/msal/application.py#L1429
            self.azure_scopes = list(self.auth_options["scopes"])
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
