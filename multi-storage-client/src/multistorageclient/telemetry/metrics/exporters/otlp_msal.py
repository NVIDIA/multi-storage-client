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

import logging
from typing import Any

import requests
import requests.adapters as requests_adapters
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

from multistorageclient.instrumentation.auth import AccessTokenProvider, AzureAccessTokenProvider

logger = logging.getLogger(__name__)


class _OTLPMSALMetricExporter(OTLPMetricExporter):
    """
    OTLP metric exporter with MSAL for auth.
    """

    _MAX_RETRIES = 5
    _BACKOFF_FACTOR = 0.5

    class AccessTokenHTTPAdapter(requests_adapters.HTTPAdapter):
        """
        HTTP adapter for retry and auth.
        """

        _access_token_provider: AccessTokenProvider

        def __init__(self, access_token_provider: AccessTokenProvider, *args, **kwargs):
            max_retries = kwargs.get("max_retries", _OTLPMSALMetricExporter._MAX_RETRIES)
            kwargs["max_retries"] = requests_adapters.Retry(
                total=max_retries,
                backoff_factor=_OTLPMSALMetricExporter._BACKOFF_FACTOR,
                connect=max_retries,
                read=max_retries,
            )
            super().__init__(*args, **kwargs)
            self._access_token_provider = access_token_provider

        def send(self, request: requests.PreparedRequest, *args, **kwargs):
            if self._access_token_provider:
                token = self._access_token_provider.get_token()
                if token:
                    request.headers["Authorization"] = f"Bearer {token}"
                else:
                    logger.warning("Failed to retrieve authentication token! Request might fail.")
            return super().send(request, *args, **kwargs)

    def __init__(
        self,
        auth: dict[str, Any],
        exporter: dict[str, Any],
    ):
        """
        :param auth: MSAL auth config dictionary.
        :param exporter: OTLP metric exporter config dictionary.
        """

        session = requests.Session()
        # Disable keep-alive.
        session.headers.update({"Connection": "close"})
        adapter = _OTLPMSALMetricExporter.AccessTokenHTTPAdapter(
            access_token_provider=AzureAccessTokenProvider(auth),
            max_retries=_OTLPMSALMetricExporter._MAX_RETRIES,
        )
        session.mount(prefix="https://", adapter=adapter)
        session.mount(prefix="http://", adapter=adapter)

        super().__init__(**exporter, session=session)
