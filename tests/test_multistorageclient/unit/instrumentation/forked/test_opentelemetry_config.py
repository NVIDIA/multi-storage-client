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

import pytest
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, DEFAULT_ON
from opentelemetry.trace import ProxyTracerProvider

import multistorageclient.instrumentation
from multistorageclient import StorageClientConfig
from multistorageclient.instrumentation.auth import AzureAccessTokenProvider

"""
`opentelemetry.trace` and `opentelemetry.metrics` have global vars that can only be set once per process.
To bypass it we use `@pytest.mark.forked` to create forked processes for each test cases.
"""


@pytest.mark.forked
def test_default_config() -> None:
    multistorageclient.instrumentation._IS_SETUP_DONE = False

    _ = StorageClientConfig.from_dict(
        {
            "profiles": {"default": {"storage_provider": {"type": "file", "options": {"base_path": "/"}}}},
            "opentelemetry": {"traces": {}},
        }
    )

    # trace
    tracer_provider: TracerProvider = trace.get_tracer_provider()  # pyright: ignore [reportAssignmentType]
    assert tracer_provider.sampler == DEFAULT_ON
    assert len(tracer_provider._active_span_processor._span_processors) == 1

    span_processor = tracer_provider._active_span_processor._span_processors[0]
    assert isinstance(span_processor, BatchSpanProcessor)
    assert isinstance(span_processor.span_exporter, ConsoleSpanExporter)


@pytest.mark.forked
def test_invalid_config() -> None:
    multistorageclient.instrumentation._IS_SETUP_DONE = False

    _ = StorageClientConfig.from_dict(
        {
            "profiles": {"default": {"storage_provider": {"type": "file", "options": {"base_path": "/"}}}},
            "opentelemetry": {},
        }
    )

    # trace
    tracer_provider: TracerProvider = trace.get_tracer_provider()  # pyright: ignore [reportAssignmentType]
    assert isinstance(tracer_provider, ProxyTracerProvider)


@pytest.mark.forked
def test_otlp_config() -> None:
    multistorageclient.instrumentation._IS_SETUP_DONE = False

    trace_endpoint = "localhost:4718/v1/traces"

    config_dict = {
        "profiles": {"default": {"storage_provider": {"type": "file", "options": {"base_path": "/"}}}},
        "opentelemetry": {
            "traces": {
                "exporter": {
                    "type": "otlp",
                    "options": {"endpoint": f"{trace_endpoint}"},
                    "auth": {
                        "type": "azure",
                        "options": {
                            "client_id": "your-client-id",
                            "client_credential": "your-client-secret",
                            "scopes": ["scope1"],
                            "validate_authority": False,
                        },
                    },
                },
                "sampler": {"type": "ALWAYS_OFF", "options": {}},
            },
        },
    }

    _ = StorageClientConfig.from_dict(config_dict)

    # trace
    tracer_provider: TracerProvider = trace.get_tracer_provider()  # pyright: ignore [reportAssignmentType]
    assert tracer_provider.sampler == ALWAYS_OFF
    assert len(tracer_provider._active_span_processor._span_processors) == 1

    span_processor = tracer_provider._active_span_processor._span_processors[0]
    assert isinstance(span_processor, BatchSpanProcessor)

    exporter = span_processor.span_exporter
    assert isinstance(exporter, OTLPSpanExporter)
    assert exporter._endpoint == trace_endpoint
    adapter = exporter._session.adapters["https://"]
    assert isinstance(adapter, multistorageclient.instrumentation.CustomHTTPAdapter)
    assert isinstance(adapter.auth_provider, AzureAccessTokenProvider)
