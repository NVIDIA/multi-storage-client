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

from unittest.mock import MagicMock, patch

from multistorageclient.telemetry.metrics.exporters.otlp_msal import _OTLPMSALMetricExporter
from multistorageclient.telemetry.traces.exporters.otlp_msal import _OTLPMSALSpanExporter


def test_span_exporter_retries_posts_like_metric_exporter():
    """The OTLP span exporter sends POSTs; its retry policy must allow them, like the metric exporter's."""
    with patch("multistorageclient.telemetry.traces.exporters.otlp_msal.AzureAccessTokenProvider", MagicMock()):
        exporter = _OTLPMSALSpanExporter(auth={}, exporter={"endpoint": "https://otlp.example.com/v1/traces"})

    adapter = exporter._session.get_adapter("https://otlp.example.com/v1/traces")
    assert isinstance(adapter, _OTLPMSALMetricExporter.AccessTokenHTTPAdapter)
    retry = adapter.max_retries
    allowed_methods = getattr(retry, "allowed_methods", None) or getattr(retry, "method_whitelist", None)
    assert allowed_methods is not None
    assert "POST" in allowed_methods
    assert retry.status_forcelist == [429, 500, 502, 503, 504]
    assert retry.total == _OTLPMSALSpanExporter._MAX_RETRIES
