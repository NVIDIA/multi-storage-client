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

from multiprocessing import get_context
from multiprocessing.managers import BaseProxy
from multiprocessing.pool import Pool
from typing import Any, Optional

import psutil
import pytest
from opentelemetry.context import get_current
from opentelemetry.metrics import Counter, Meter, MeterProvider, _Gauge
from opentelemetry.trace import Span, Tracer, TracerProvider, use_span

import multistorageclient.telemetry as telemetry
from test_multistorageclient.unit.utils.telemetry.metrics.export import InMemoryMetricExporter
from test_multistorageclient.unit.utils.telemetry.trace.export import InMemorySpanExporter


def test_telemetry_local_objects():
    opentelemetry_config = {
        "metrics": {"exporter": {"type": telemetry._fully_qualified_name(InMemoryMetricExporter)}},
        "traces": {"exporter": {"type": telemetry._fully_qualified_name(InMemorySpanExporter)}},
    }

    # Make sure caching works.
    telemetry_resources_str: Optional[str] = None
    meter_provider_str: Optional[str] = None
    meter_str: Optional[str] = None
    gauge_str: Optional[str] = None
    counter_str: Optional[str] = None
    tracer_provider_str: Optional[str] = None
    tracer_str: Optional[str] = None

    for _ in range(2):
        telemetry_resources: telemetry.Telemetry = telemetry.init(mode=telemetry.TelemetryMode.LOCAL)
        assert not isinstance(telemetry_resources, BaseProxy)

        if telemetry_resources_str is None:
            telemetry_resources_str = str(telemetry_resources)
        else:
            assert telemetry_resources_str == str(telemetry_resources)

        meter_provider: Optional[MeterProvider] = telemetry_resources.meter_provider(
            config=opentelemetry_config["metrics"]
        )
        assert meter_provider is not None
        assert not isinstance(meter_provider, BaseProxy)

        if meter_provider_str is None:
            meter_provider_str = str(meter_provider)
        else:
            assert meter_provider_str == str(meter_provider)

        meter: Optional[Meter] = telemetry_resources.meter(config=opentelemetry_config["metrics"])
        assert meter is not None
        assert not isinstance(meter, BaseProxy)

        if meter_str is None:
            meter_str = str(meter)
        else:
            assert meter_str == str(meter)

        gauge: Optional[_Gauge] = telemetry_resources.gauge(
            opentelemetry_config["metrics"], name=telemetry.Telemetry.GaugeName.DATA_SIZE
        )
        assert gauge is not None
        assert not isinstance(gauge, BaseProxy)

        if gauge_str is None:
            gauge_str = str(gauge)
        else:
            assert gauge_str == str(gauge)

        gauge.set(1)

        counter: Optional[Counter] = telemetry_resources.counter(
            config=opentelemetry_config["metrics"], name=telemetry.Telemetry.CounterName.DATA_SIZE_SUM
        )
        assert counter is not None
        assert not isinstance(counter, BaseProxy)

        if counter_str is None:
            counter_str = str(counter)
        else:
            assert counter_str == str(counter)

        counter.add(1)

        tracer_provider: Optional[TracerProvider] = telemetry_resources.tracer_provider(opentelemetry_config["traces"])
        assert tracer_provider is not None
        assert not isinstance(tracer_provider, BaseProxy)

        if tracer_provider_str is None:
            tracer_provider_str = str(tracer_provider)
        else:
            assert tracer_provider_str == str(tracer_provider)

        tracer: Optional[Tracer] = telemetry_resources.tracer(opentelemetry_config["traces"])
        assert tracer is not None
        assert not isinstance(tracer, BaseProxy)

        if tracer_str is None:
            tracer_str = str(tracer)
        else:
            assert tracer_str == str(tracer)

        span: Span = tracer.start_span("span", context=get_current())
        assert not isinstance(span, BaseProxy)

        with use_span(span, end_on_exit=True) as active_span:  # pyright: ignore [reportCallIssue]
            active_span.add_event("event")


def test_telemetry_manager_server_port():
    # Make sure the port is in the dynamic/private/ephemeral port range.
    port = telemetry._telemetry_manager_server_port(process_id=psutil.Process().pid)
    assert (2**15 + 2**14) <= port
    assert port < 2**16


# Invoke in a separate process.
def _test_telemetry_proxy_objects_client(
    opentelemetry_config: dict[str, Any],
    # Make sure caching works across processes.
    #
    # BaseProxy.__str__ returns the __str__ of the referent.
    # BaseProxy.__repr__ returns the __repr__ of the proxy object.
    #
    # https://docs.python.org/3/library/multiprocessing.html#proxy-objects
    telemetry_resources_referent_str: str,
    telemetry_resources_proxy_repr: str,
    meter_provider_referent_str: str,
    meter_provider_proxy_repr: str,
    meter_referent_str: str,
    meter_proxy_repr: str,
    gauge_referent_str: str,
    gauge_proxy_repr: str,
    counter_referent_str: str,
    counter_proxy_repr: str,
    tracer_provider_referent_str: str,
    tracer_provider_proxy_repr: str,
    tracer_referent_str: str,
    tracer_proxy_repr: str,
):
    telemetry_resources: telemetry.Telemetry = telemetry.init(mode=telemetry.TelemetryMode.CLIENT)
    assert isinstance(telemetry_resources, BaseProxy)

    assert telemetry_resources_referent_str == str(telemetry_resources)
    assert telemetry_resources_proxy_repr != repr(telemetry_resources)

    meter_provider: Optional[MeterProvider] = telemetry_resources.meter_provider(opentelemetry_config["metrics"])
    assert meter_provider is not None
    assert isinstance(meter_provider, BaseProxy)

    assert meter_provider_referent_str == str(meter_provider)
    assert meter_provider_proxy_repr != repr(meter_provider)

    meter: Optional[Meter] = telemetry_resources.meter(config=opentelemetry_config["metrics"])
    assert meter is not None
    assert isinstance(meter, BaseProxy)

    assert meter_referent_str == str(meter)
    assert meter_proxy_repr != repr(meter)

    gauge: Optional[_Gauge] = telemetry_resources.gauge(
        opentelemetry_config["metrics"], name=telemetry.Telemetry.GaugeName.DATA_SIZE
    )
    assert gauge is not None
    assert isinstance(gauge, BaseProxy)

    assert gauge_referent_str == str(gauge)
    assert gauge_proxy_repr != repr(gauge)

    gauge.set(1)

    counter: Optional[Counter] = telemetry_resources.counter(
        config=opentelemetry_config["metrics"], name=telemetry.Telemetry.CounterName.DATA_SIZE_SUM
    )
    assert counter is not None
    assert isinstance(counter, BaseProxy)

    assert counter_referent_str == str(counter)
    assert counter_proxy_repr != repr(counter)

    counter.add(1)

    tracer_provider: Optional[TracerProvider] = telemetry_resources.tracer_provider(opentelemetry_config["traces"])
    assert tracer_provider is not None
    assert isinstance(tracer_provider, BaseProxy)

    assert tracer_provider_referent_str == str(tracer_provider)
    assert tracer_provider_proxy_repr != repr(tracer_provider)

    tracer: Optional[Tracer] = telemetry_resources.tracer(opentelemetry_config["traces"])
    assert tracer is not None
    assert isinstance(tracer, BaseProxy)

    assert tracer_referent_str == str(tracer)
    assert tracer_proxy_repr != repr(tracer)

    # Passes the current process' span context to the remote constructor.
    span: Span = tracer.start_span("span", context=get_current())
    assert isinstance(span, BaseProxy)

    with use_span(span, end_on_exit=True) as active_span:  # pyright: ignore [reportCallIssue]
        active_span.add_event("event")


@pytest.mark.parametrize(argnames=["process_start_method"], argvalues=[["fork"], ["spawn"]])
def test_telemetry_proxy_objects(process_start_method: str):
    opentelemetry_config = {
        "metrics": {"exporter": {"type": telemetry._fully_qualified_name(InMemoryMetricExporter)}},
        "traces": {"exporter": {"type": telemetry._fully_qualified_name(InMemorySpanExporter)}},
    }

    # --------------------------------------------------------------------------------
    #
    # Server mode.
    #
    # We need to keep a reference to the resources so the server stays alive during
    # the client mode portion.
    #
    # --------------------------------------------------------------------------------

    telemetry_resources: telemetry.Telemetry = telemetry.init(mode=telemetry.TelemetryMode.SERVER)
    assert isinstance(telemetry_resources, BaseProxy)

    meter_provider: Optional[MeterProvider] = telemetry_resources.meter_provider(opentelemetry_config["metrics"])
    assert meter_provider is not None
    assert isinstance(meter_provider, BaseProxy)

    meter: Optional[Meter] = telemetry_resources.meter(config=opentelemetry_config["metrics"])
    assert meter is not None
    assert isinstance(meter, BaseProxy)

    gauge: Optional[_Gauge] = telemetry_resources.gauge(
        opentelemetry_config["metrics"], name=telemetry.Telemetry.GaugeName.DATA_SIZE
    )
    assert gauge is not None
    assert isinstance(gauge, BaseProxy)

    gauge.set(1)

    counter: Optional[Counter] = telemetry_resources.counter(
        config=opentelemetry_config["metrics"], name=telemetry.Telemetry.CounterName.DATA_SIZE_SUM
    )
    assert counter is not None
    assert isinstance(counter, BaseProxy)

    counter.add(1)

    tracer_provider: Optional[TracerProvider] = telemetry_resources.tracer_provider(opentelemetry_config["traces"])
    assert tracer_provider is not None
    assert isinstance(tracer_provider, BaseProxy)

    tracer: Optional[Tracer] = telemetry_resources.tracer(opentelemetry_config["traces"])
    assert tracer is not None
    assert isinstance(tracer, BaseProxy)

    # Passes the current process' span context to the remote constructor.
    span: Span = tracer.start_span("span", context=get_current())
    assert isinstance(span, BaseProxy)

    with use_span(span, end_on_exit=True) as active_span:  # pyright: ignore [reportCallIssue]
        active_span.add_event("event")

    # --------------------------------------------------------------------------------
    #
    # Client mode.
    #
    # --------------------------------------------------------------------------------

    pool = Pool(context=get_context(method=process_start_method))

    pool.apply(
        _test_telemetry_proxy_objects_client,
        kwds={
            "opentelemetry_config": opentelemetry_config,
            "telemetry_resources_referent_str": str(telemetry_resources),
            "telemetry_resources_proxy_repr": repr(telemetry_resources),
            "meter_provider_referent_str": str(meter_provider),
            "meter_provider_proxy_repr": repr(meter_provider),
            "meter_referent_str": str(meter),
            "meter_proxy_repr": repr(meter),
            "gauge_referent_str": str(gauge),
            "gauge_proxy_repr": repr(gauge),
            "counter_referent_str": str(counter),
            "counter_proxy_repr": repr(counter),
            "tracer_provider_referent_str": str(tracer_provider),
            "tracer_provider_proxy_repr": repr(tracer_provider),
            "tracer_referent_str": str(tracer),
            "tracer_proxy_repr": repr(tracer),
        },
    )
