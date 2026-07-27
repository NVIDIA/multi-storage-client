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

import inspect
import logging
from typing import Any

from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

from multistorageclient.instrumentation.auth import VaultCertificateProvider

logger = logging.getLogger(__name__)


class _OTLPmTLSVaultMetricExporter(OTLPMetricExporter):
    """
    OTLP metric exporter with mTLS using certificates from HashiCorp Vault.

    This exporter fetches mTLS certificates from Vault using AppRole authentication
    and configures the OTLP exporter to use them for secure communication.

    Example YAML configuration::

        opentelemetry:
          metrics:
            exporter:
              type: _otlp_mtls_vault
              options:
                exporter:
                  endpoint: "https://otlp.example.com/v1/metrics"
                auth:
                  vault_endpoint: https://vault.example.com
                  vault_namespace: my-namespace
                  approle_id: <approle-role-id>
                  approle_secret: <approle-secret-id>
                  mount_point: secret
                  secret_path: path/to/certs
    """

    def __init__(
        self,
        auth: dict[str, Any],
        exporter: dict[str, Any],
    ):
        """
        Initialize the mTLS Vault metric exporter.

        :param auth: Vault authentication config dictionary containing:
            - vault_endpoint: URL of the Vault server
            - vault_namespace: Vault namespace
            - approle_id: AppRole role ID
            - approle_secret: AppRole secret ID
            - mount_point: KV secrets engine mount point (default: "secret")
            - secret_path: Path to the secret containing certificates
            - cert_key: Key name for client certificate (default: "cert")
            - key_key: Key name for client key (default: "key")
            - ca_key: Key name for CA certificate (default: "ca")
        :param exporter: OTLP metric exporter config dictionary (passed through to OTLPMetricExporter).
        :raises RuntimeError: If the installed OTLP exporter does not support mTLS client certificate options.
        """
        # mTLS client certificate options were added in opentelemetry-exporter-otlp-proto-http 1.32.1.
        # Fail fast with a clear error (instead of an opaque TypeError from the parent) on older versions.
        parameters = inspect.signature(OTLPMetricExporter.__init__).parameters
        supports_client_certificate = "client_certificate_file" in parameters or any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
        )
        if not supports_client_certificate:
            raise RuntimeError(
                "The installed opentelemetry-exporter-otlp-proto-http does not support the mTLS client "
                "certificate options (client_certificate_file / client_key_file). Upgrade to "
                "opentelemetry-exporter-otlp-proto-http >= 1.32.1 to use the _otlp_mtls_vault exporter."
            )

        provider = VaultCertificateProvider(**auth)
        cert_paths = provider.get_certificates()

        exporter_with_certs = exporter.copy()
        exporter_with_certs["client_certificate_file"] = cert_paths.client_certificate_file
        exporter_with_certs["client_key_file"] = cert_paths.client_key_file
        exporter_with_certs["certificate_file"] = cert_paths.certificate_file

        super().__init__(**exporter_with_certs)
