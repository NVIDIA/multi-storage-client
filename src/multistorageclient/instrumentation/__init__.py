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

# pyright: reportPossiblyUnboundVariable=false

import logging
import threading
from typing import Any, Optional, Union

from opentelemetry import trace

from ..utils import import_class

# Try importing optional observability dependencies
try:
    import requests
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.trace.sampling import DEFAULT_ON, ParentBased, StaticSampler
    from requests.adapters import HTTPAdapter, Retry

    from .auth import AccessTokenProvider, AccessTokenProviderFactory
    from .error_aware_processor import ErrorAwareBatchSpanProcessor

    _RESOURCE = Resource.create(
        {
            "service.name": "multistorageclient",
            "service.namespace": "client",
            "service.version": "1.0",
        }
    )

    class CustomHTTPAdapter(HTTPAdapter):
        """
        Custom HTTP adapter for retry and auth
        """

        def __init__(self, auth_provider, *args, **kwargs):
            max_retries = kwargs.get("max_retries", MAX_RETRIES)
            retry = Retry(total=max_retries, backoff_factor=BACKOFF_FACTOR, connect=max_retries, read=max_retries)
            kwargs["max_retries"] = retry
            super().__init__(*args, **kwargs)
            self.auth_provider = auth_provider

        def send(self, request, *args, **kwargs):
            if self.auth_provider:
                token = self.auth_provider.get_token()
                if token:
                    request.headers["Authorization"] = f"Bearer {token}"
                else:
                    logger.warning("Warning: Failed to retrieve authentication token. Request might fail.")
            return super().send(request, *args, **kwargs)

    def create_session(auth_provider: Optional[AccessTokenProvider] = None) -> requests.Session:
        session = requests.Session()

        # Disable keep-alive
        session.headers.update({"Connection": "close"})

        # use adaptor for retry & auth
        adapter = CustomHTTPAdapter(auth_provider, max_retries=MAX_RETRIES)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    HAS_OBSERVABILITY_DEPS = True
except ImportError:
    HAS_OBSERVABILITY_DEPS = False

_TRACE_SAMPLER_MODULE_NAME = "opentelemetry.sdk.trace.sampling"

_OTEL_TRACE_EXPORTER_MAPPING = {
    "console": "opentelemetry.sdk.trace.export.ConsoleSpanExporter",
    "otlp": "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter",
    # "Private" until it's decided whether this will be official.
    "_otlp_msal": "multistorageclient.telemetry.traces.exporters.otlp_msal._OTLPMSALSpanExporter",
}

_IS_SETUP_DONE = False

MAX_RETRIES = 5
BACKOFF_FACTOR = 0.5

_setup_lock = threading.Lock()

logger = logging.getLogger(__name__)


def _setup_opentelemetry_impl(config: dict[str, Any]) -> None:
    """
    Setup global OpenTelemetry providers for trace/metrics
    """

    with _setup_lock:
        global _IS_SETUP_DONE

        # This function should only be called once even multiple storage clients
        # with different profiles have been created
        if _IS_SETUP_DONE:
            return

        if config:
            trace_config_dict = config.get("traces", None)

            if trace_config_dict is not None:
                # exporter
                trace_exporter_dict = trace_config_dict.get("exporter", None)
                if trace_exporter_dict:
                    exporter_type: str = trace_exporter_dict["type"]
                    module_name, class_name = _OTEL_TRACE_EXPORTER_MAPPING.get(exporter_type, exporter_type).rsplit(
                        ".", 1
                    )
                    options = trace_exporter_dict.get("options", {})
                    cls = import_class(class_name, module_name)
                    auth_dict = trace_exporter_dict.get("auth", {})
                    auth_provider = AccessTokenProviderFactory.create_access_token_provider(auth_dict)
                    if exporter_type == "otlp":
                        options["session"] = create_session(auth_provider)
                    exporter = cls(**options)
                else:
                    # provide default console exporter if dict is not provided
                    exporter = ConsoleSpanExporter()

                # sampler
                trace_sampler_dict = trace_config_dict.get("sampler", None)
                sampler: Union[StaticSampler, ParentBased, None]
                if trace_sampler_dict:
                    class_name = trace_sampler_dict["type"]
                    options = trace_sampler_dict.get("options", {})
                    cls_or_obj = import_class(class_name, _TRACE_SAMPLER_MODULE_NAME)
                    if isinstance(cls_or_obj, StaticSampler):
                        sampler = cls_or_obj
                    else:
                        sampler = cls_or_obj(**options)
                    # if sampler is specified by users, then we use BatchSpanProcssor; otherwise, use the custom error aware processor for tail sampling
                    span_processor = BatchSpanProcessor(exporter)
                else:
                    sampler = DEFAULT_ON
                    span_processor = ErrorAwareBatchSpanProcessor(exporter)

                # set up trace provider for current process
                tracer_provider = TracerProvider(resource=_RESOURCE, sampler=sampler)
                tracer_provider.add_span_processor(span_processor)
                trace.set_tracer_provider(tracer_provider)

            # Only set the _IS_SETUP_DONE = true if the providers are successfully set once
            _IS_SETUP_DONE = True


def setup_opentelemetry(config: dict[str, Any]) -> None:
    """
    Setup global OpenTelemetry providers for trace/metrics.
    When dependencies are not available, this becomes a no-op function.
    """
    if not HAS_OBSERVABILITY_DEPS:
        logger.warning(
            "Instrumentation dependencies not available. Skipping OpenTelemetry setup. "
            "To enable OpenTelemetry features, install the optional dependency: "
            "pip install multi-storage-client[observability-otel]"
        )
        return

    _setup_opentelemetry_impl(config)
