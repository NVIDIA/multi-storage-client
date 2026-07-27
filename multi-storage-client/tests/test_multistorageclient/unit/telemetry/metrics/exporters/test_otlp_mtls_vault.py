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

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.instrumentation.auth import CertificatePaths
from multistorageclient.telemetry import _METRICS_EXPORTER_MAPPING


@pytest.fixture
def temp_cert_files():
    """Create temporary certificate files for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_path = os.path.join(tmpdir, "cert.crt")
        key_path = os.path.join(tmpdir, "key.key")
        ca_path = os.path.join(tmpdir, "ca.crt")

        for path in [cert_path, key_path, ca_path]:
            with open(path, "w") as f:
                f.write("test-content")

        yield {
            "cert_path": cert_path,
            "key_path": key_path,
            "ca_path": ca_path,
            "tmpdir": tmpdir,
        }


@pytest.fixture
def mock_vault_provider(temp_cert_files):
    """Mock VaultCertificateProvider to return test certificate paths."""
    mock_provider = MagicMock()
    mock_provider.get_certificates.return_value = CertificatePaths(
        client_certificate_file=temp_cert_files["cert_path"],
        client_key_file=temp_cert_files["key_path"],
        certificate_file=temp_cert_files["ca_path"],
    )
    return mock_provider


class TestOTLPmTLSVaultMetricExporter:
    def test_exporter_mapping_exists(self):
        """Test that _otlp_mtls_vault is in the metrics exporter mapping."""
        assert "_otlp_mtls_vault" in _METRICS_EXPORTER_MAPPING
        assert (
            _METRICS_EXPORTER_MAPPING["_otlp_mtls_vault"]
            == "multistorageclient.telemetry.metrics.exporters.otlp_mtls_vault._OTLPmTLSVaultMetricExporter"
        )

    def test_exporter_initialization(self, mock_vault_provider, temp_cert_files):
        """Test that exporter initializes correctly with Vault certificates."""
        with patch(
            "multistorageclient.telemetry.metrics.exporters.otlp_mtls_vault.VaultCertificateProvider",
            return_value=mock_vault_provider,
        ):
            with patch(
                "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter.__init__",
                return_value=None,
            ) as mock_init:
                from multistorageclient.telemetry.metrics.exporters.otlp_mtls_vault import (
                    _OTLPmTLSVaultMetricExporter,
                )

                auth_config = {
                    "vault_endpoint": "https://vault.example.com",
                    "vault_namespace": "test-namespace",
                    "approle_id": "test-role-id",
                    "approle_secret": "test-secret-id",
                    "mount_point": "secret",
                    "secret_path": "path/to/certs",
                }

                exporter_config = {
                    "endpoint": "https://otlp.example.com/v1/metrics",
                }

                _OTLPmTLSVaultMetricExporter(auth=auth_config, exporter=exporter_config)

                mock_init.assert_called_once()
                call_kwargs = mock_init.call_args[1]

                assert call_kwargs["endpoint"] == "https://otlp.example.com/v1/metrics"
                assert call_kwargs["client_certificate_file"] == temp_cert_files["cert_path"]
                assert call_kwargs["client_key_file"] == temp_cert_files["key_path"]
                assert call_kwargs["certificate_file"] == temp_cert_files["ca_path"]

    def test_exporter_preserves_additional_options(self, mock_vault_provider, temp_cert_files):
        """Test that additional exporter options are preserved."""
        with patch(
            "multistorageclient.telemetry.metrics.exporters.otlp_mtls_vault.VaultCertificateProvider",
            return_value=mock_vault_provider,
        ):
            with patch(
                "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter.__init__",
                return_value=None,
            ) as mock_init:
                from multistorageclient.telemetry.metrics.exporters.otlp_mtls_vault import (
                    _OTLPmTLSVaultMetricExporter,
                )

                auth_config = {
                    "vault_endpoint": "https://vault.example.com",
                    "vault_namespace": "test-namespace",
                    "approle_id": "test-role-id",
                    "approle_secret": "test-secret-id",
                }

                exporter_config = {
                    "endpoint": "https://otlp.example.com/v1/metrics",
                    "timeout": 30,
                    "compression": "gzip",
                }

                _OTLPmTLSVaultMetricExporter(auth=auth_config, exporter=exporter_config)

                call_kwargs = mock_init.call_args[1]

                assert call_kwargs["timeout"] == 30
                assert call_kwargs["compression"] == "gzip"

    def test_exporter_does_not_modify_original_config(self, mock_vault_provider):
        """Test that original exporter config dict is not modified."""
        with patch(
            "multistorageclient.telemetry.metrics.exporters.otlp_mtls_vault.VaultCertificateProvider",
            return_value=mock_vault_provider,
        ):
            with patch(
                "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter.__init__",
                return_value=None,
            ):
                from multistorageclient.telemetry.metrics.exporters.otlp_mtls_vault import (
                    _OTLPmTLSVaultMetricExporter,
                )

                auth_config = {
                    "vault_endpoint": "https://vault.example.com",
                    "vault_namespace": "test-namespace",
                    "approle_id": "test-role-id",
                    "approle_secret": "test-secret-id",
                }

                exporter_config = {
                    "endpoint": "https://otlp.example.com/v1/metrics",
                }

                _OTLPmTLSVaultMetricExporter(auth=auth_config, exporter=exporter_config)

                assert "client_certificate_file" not in exporter_config
                assert "client_key_file" not in exporter_config
                assert "certificate_file" not in exporter_config

    def test_exporter_raises_clear_error_when_client_certificate_unsupported(self, mock_vault_provider):
        """An OTLP exporter lacking client_certificate_file support must raise a clear
        RuntimeError, not an opaque TypeError, into the caller."""

        # Models opentelemetry-exporter-otlp-proto-http < 1.32.1: no mTLS client cert options.
        def _legacy_init(self, endpoint=None, certificate_file=None, headers=None, timeout=None, compression=None):
            pass

        with patch(
            "multistorageclient.telemetry.metrics.exporters.otlp_mtls_vault.VaultCertificateProvider",
            return_value=mock_vault_provider,
        ):
            with patch(
                "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter.__init__",
                _legacy_init,
            ):
                from multistorageclient.telemetry.metrics.exporters.otlp_mtls_vault import (
                    _OTLPmTLSVaultMetricExporter,
                )

                auth_config = {
                    "vault_endpoint": "https://vault.example.com",
                    "vault_namespace": "test-namespace",
                    "approle_id": "test-role-id",
                    "approle_secret": "test-secret-id",
                }
                exporter_config = {"endpoint": "https://otlp.example.com/v1/metrics"}

                with pytest.raises(RuntimeError, match="client_certificate_file"):
                    _OTLPmTLSVaultMetricExporter(auth=auth_config, exporter=exporter_config)

    def test_vault_provider_receives_auth_config(self, mock_vault_provider):
        """Test that VaultCertificateProvider is initialized with auth config."""
        with patch(
            "multistorageclient.telemetry.metrics.exporters.otlp_mtls_vault.VaultCertificateProvider",
            return_value=mock_vault_provider,
        ) as mock_provider_class:
            with patch(
                "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter.__init__",
                return_value=None,
            ):
                from multistorageclient.telemetry.metrics.exporters.otlp_mtls_vault import (
                    _OTLPmTLSVaultMetricExporter,
                )

                auth_config = {
                    "vault_endpoint": "https://vault.example.com",
                    "vault_namespace": "test-namespace",
                    "approle_id": "test-role-id",
                    "approle_secret": "test-secret-id",
                    "mount_point": "custom/mount",
                    "secret_path": "custom/path",
                }

                exporter_config = {
                    "endpoint": "https://otlp.example.com/v1/metrics",
                }

                _OTLPmTLSVaultMetricExporter(auth=auth_config, exporter=exporter_config)

                mock_provider_class.assert_called_once_with(**auth_config)
