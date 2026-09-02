# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
from typing import Any

import requests
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

from multistorageclient.instrumentation.auth import AzureAccessTokenProvider
from multistorageclient.telemetry.metrics.exporters.otlp_msal import _OTLPMSALMetricExporter

logger = logging.getLogger(__name__)


class _OTLPMSALSpanExporter(OTLPSpanExporter):
    """
    OTLP span exporter with MSAL for auth.
    """

    _MAX_RETRIES = 5

    # Shared with the metric exporter so both get the same retry policy (POST retries, status forcelist).
    AccessTokenHTTPAdapter = _OTLPMSALMetricExporter.AccessTokenHTTPAdapter

    def __init__(
        self,
        auth: dict[str, Any],
        exporter: dict[str, Any],
    ):
        """
        :param auth: MSAL auth config dictionary.
        :param exporter: OTLP span exporter config dictionary.
        """

        session = requests.Session()
        # Disable keep-alive.
        session.headers.update({"Connection": "close"})
        adapter = _OTLPMSALSpanExporter.AccessTokenHTTPAdapter(
            access_token_provider=AzureAccessTokenProvider(auth),
            max_retries=_OTLPMSALSpanExporter._MAX_RETRIES,
        )
        session.mount(prefix="https://", adapter=adapter)
        session.mount(prefix="http://", adapter=adapter)

        super().__init__(**exporter, session=session)
