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

import atexit
import enum
import inspect
import json
import logging
import multiprocessing
import multiprocessing.managers
import threading
from typing import Any, Optional, Union

import opentelemetry.metrics as api_metrics
import opentelemetry.trace as api_trace
import psutil

from .. import utils

# MSC telemetry prefers publishing raw samples when possible to support arbitrary post-hoc aggregations.
#
# Some setups, however, may need resampling to reduce sample volume. The resampling methods we use
# sacrifice temporal resolution to preserve other information. Which method is used depends on if
# the expected post-hoc aggregate function is decomposable:
#
# * Decomposable aggregate functions (e.g. count, sum, min, max).
#   * Use client-side aggregation.
#     * E.g. preserve request + response counts.
# * Non-decomposable aggregate functions (e.g. average, percentile).
#   * Use decimation by an integer factor or last value.
#     * E.g. preserve the shape of the latency distribution (unlike tail sampling).

_METRICS_EXPORTER_MAPPING = {
    "console": "opentelemetry.sdk.metrics.export.ConsoleMetricExporter",
    "otlp": "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter",
    # "Private" until it's decided whether this will be official.
    "_otlp_msal": "multistorageclient.telemetry.metrics.exporters.otlp_msal._OTLPMSALMetricExporter",
}

_TRACE_EXPORTER_MAPPING = {
    "console": "opentelemetry.sdk.trace.export.ConsoleSpanExporter",
    "otlp": "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter",
}

logger = logging.getLogger(__name__)


class Telemetry:
    """
    Provides telemetry resources.

    Instances shouldn't be copied between processes. Not fork-safe or pickleable.

    Instances can be shared between processes by registering with a :py:class:`multiprocessing.managers.BaseManager` and using proxy objects.
    """

    # Metrics are named `multistorageclient.{property}(.{aggregation})?`.
    #
    # For example:
    #
    # - multistorageclient.data_size
    #   - Gauge for data size per individual operation.
    #   - For distributions (e.g. post-hoc histograms + heatmaps).
    # - multistorageclient.data_size.sum
    #   - Counter (sum) for data size across all operations.
    #   - For aggregates (e.g. post-hoc data rate calculations).

    # https://opentelemetry.io/docs/specs/semconv/general/naming#metrics
    class GaugeName(enum.Enum):
        LATENCY = "multistorageclient.latency"
        DATA_SIZE = "multistorageclient.data_size"
        DATA_RATE = "multistorageclient.data_rate"

    # https://opentelemetry.io/docs/specs/semconv/general/metrics#units
    _GAUGE_UNIT_MAPPING: dict[GaugeName, str] = {
        # Seconds.
        GaugeName.LATENCY: "s",
        # Bytes.
        GaugeName.DATA_SIZE: "By",
        # Bytes/second.
        GaugeName.DATA_RATE: "By/s",
    }

    # https://opentelemetry.io/docs/specs/semconv/general/naming#metrics
    class CounterName(enum.Enum):
        REQUEST_SUM = "multistorageclient.request.sum"
        RESPONSE_SUM = "multistorageclient.response.sum"
        DATA_SIZE_SUM = "multistorageclient.data_size.sum"

    # https://opentelemetry.io/docs/specs/semconv/general/metrics#units
    _COUNTER_UNIT_MAPPING: dict[CounterName, str] = {
        # Unitless.
        CounterName.REQUEST_SUM: "{request}",
        # Unitless.
        CounterName.RESPONSE_SUM: "{response}",
        # Bytes.
        CounterName.DATA_SIZE_SUM: "By",
    }

    # Map of config as a sorted JSON string (since dictionaries can't be hashed) to meter provider.
    _meter_provider_cache: dict[str, api_metrics.MeterProvider]
    _meter_provider_cache_lock: threading.Lock
    # Map of config as a sorted JSON string (since dictionaries can't be hashed) to meter.
    _meter_cache: dict[str, api_metrics.Meter]
    _meter_cache_lock: threading.Lock
    # Map of config as a sorted JSON string (since dictionaries can't be hashed) to gauge name to gauge.
    _gauge_cache: dict[str, dict[GaugeName, api_metrics._Gauge]]
    _gauge_cache_lock: threading.Lock
    # Map of config as a sorted JSON string (since dictionaries can't be hashed) to counter name to counter.
    _counter_cache: dict[str, dict[CounterName, api_metrics.Counter]]
    _counter_cache_lock: threading.Lock
    # Map of config as a sorted JSON string (since dictionaries can't be hashed) to tracer provider.
    _tracer_provider_cache: dict[str, api_trace.TracerProvider]
    _tracer_provider_cache_lock: threading.Lock
    # Map of config as a sorted JSON string (since dictionaries can't be hashed) to tracer.
    _tracer_cache: dict[str, api_trace.Tracer]
    _tracer_cache_lock: threading.Lock

    def __init__(self):
        self._meter_provider_cache = {}
        self._meter_provider_cache_lock = threading.Lock()
        self._meter_cache = {}
        self._meter_cache_lock = threading.Lock()
        self._gauge_cache = {}
        self._gauge_cache_lock = threading.Lock()
        self._counter_cache = {}
        self._counter_cache_lock = threading.Lock()
        self._tracer_provider_cache = {}
        self._tracer_provider_cache_lock = threading.Lock()
        self._tracer_cache = {}
        self._tracer_cache_lock = threading.Lock()

    def meter_provider(self, config: dict[str, Any]) -> Optional[api_metrics.MeterProvider]:
        """
        Create or return an existing :py:class:`api_metrics.MeterProvider` for a config.

        :param config: ``.opentelemetry.metrics`` config dict.
        :return: A :py:class:`api_metrics.MeterProvider` or ``None`` if no valid exporter is configured.
        """
        config_json = json.dumps(config, sort_keys=True)
        with self._meter_provider_cache_lock:
            if config_json in self._meter_provider_cache:
                return self._meter_provider_cache[config_json]
            else:
                if "exporter" in config:
                    try:
                        import opentelemetry.sdk.metrics as sdk_metrics
                        import opentelemetry.sdk.metrics.export as sdk_metrics_export

                        from .metrics.readers.diperiodic_exporting import DiperiodicExportingMetricReader

                        exporter_type: str = config["exporter"]["type"]
                        exporter_fully_qualified_name = _METRICS_EXPORTER_MAPPING.get(exporter_type, exporter_type)
                        exporter_module_name, exporter_class_name = exporter_fully_qualified_name.rsplit(".", 1)
                        cls = utils.import_class(exporter_class_name, exporter_module_name)
                        exporter_options = config["exporter"].get("options", {})
                        exporter: sdk_metrics_export.MetricExporter = cls(**exporter_options)

                        reader_options = config.get("reader", {}).get("options", {})
                        reader: sdk_metrics_export.MetricReader = DiperiodicExportingMetricReader(
                            **reader_options, exporter=exporter
                        )

                        return self._meter_provider_cache.setdefault(
                            config_json, sdk_metrics.MeterProvider(metric_readers=[reader])
                        )
                    except (AttributeError, ImportError):
                        logger.error(
                            "Failed to import OpenTelemetry Python SDK or exporter! Disabling metrics.", exc_info=True
                        )
                        return None
                else:
                    # Don't return a no-op meter provider to avoid unnecessary overhead.
                    logger.error("No exporter configured! Disabling metrics.")
                    return None

    def meter(self, config: dict[str, Any]) -> Optional[api_metrics.Meter]:
        """
        Create or return an existing :py:class:`api_metrics.Meter` for a config.

        :param config: ``.opentelemetry.metrics`` config dict.
        :return: A :py:class:`api_metrics.Meter` or ``None`` if no valid exporter is configured.
        """
        config_json = json.dumps(config, sort_keys=True)
        with self._meter_cache_lock:
            if config_json in self._meter_cache:
                return self._meter_cache[config_json]
            else:
                meter_provider = self.meter_provider(config=config)
                if meter_provider is None:
                    return None
                else:
                    return self._meter_cache.setdefault(
                        config_json, meter_provider.get_meter(name="multistorageclient")
                    )

    def gauge(self, config: dict[str, Any], name: GaugeName) -> Optional[api_metrics._Gauge]:
        """
        Create or return an existing :py:class:`api_metrics.Gauge` for a config and gauge name.

        :param config: ``.opentelemetry.metrics`` config dict.
        :return: A :py:class:`api_metrics.Gauge` or ``None`` if no valid exporter is configured.
        """
        config_json = json.dumps(config, sort_keys=True)
        with self._gauge_cache_lock:
            if config_json in self._gauge_cache and name in self._gauge_cache[config_json]:
                return self._gauge_cache[config_json][name]
            else:
                meter = self.meter(config=config)
                if meter is None:
                    return None
                else:
                    return self._gauge_cache.setdefault(config_json, {}).setdefault(
                        name,
                        meter.create_gauge(name=name.value, unit=Telemetry._GAUGE_UNIT_MAPPING.get(name, "")),
                    )

    def counter(self, config: dict[str, Any], name: CounterName) -> Optional[api_metrics.Counter]:
        """
        Create or return an existing :py:class:`api_metrics.Counter` for a config and counter name.

        :param config: ``.opentelemetry.metrics`` config dict.
        :return: A :py:class:`api_metrics.Counter` or ``None`` if no valid exporter is configured.
        """
        config_json = json.dumps(config, sort_keys=True)
        with self._counter_cache_lock:
            if config_json in self._counter_cache and name in self._counter_cache[config_json]:
                return self._counter_cache[config_json][name]
            else:
                meter = self.meter(config=config)
                if meter is None:
                    return None
                else:
                    return self._counter_cache.setdefault(config_json, {}).setdefault(
                        name,
                        meter.create_counter(name=name.value, unit=Telemetry._COUNTER_UNIT_MAPPING.get(name, "")),
                    )

    def tracer_provider(self, config: dict[str, Any]) -> Optional[api_trace.TracerProvider]:
        """
        Create or return an existing :py:class:`api_trace.TracerProvider` for a config.

        :param config: ``.opentelemetry.traces`` config dict.
        :return: A :py:class:`api_trace.TracerProvider` or ``None`` if no valid exporter is configured.
        """
        config_json = json.dumps(config, sort_keys=True)
        with self._tracer_provider_cache_lock:
            if config_json in self._tracer_provider_cache:
                return self._tracer_provider_cache[config_json]
            else:
                if "exporter" in config:
                    try:
                        import opentelemetry.sdk.trace as sdk_trace
                        import opentelemetry.sdk.trace.export as sdk_trace_export
                        import opentelemetry.sdk.trace.sampling as sdk_trace_sampling

                        exporter_type: str = config["exporter"]["type"]
                        exporter_fully_qualified_name = _TRACE_EXPORTER_MAPPING.get(exporter_type, exporter_type)
                        exporter_module_name, exporter_class_name = exporter_fully_qualified_name.rsplit(".", 1)
                        cls = utils.import_class(exporter_class_name, exporter_module_name)
                        exporter_options = config["exporter"].get("options", {})
                        exporter: sdk_trace_export.SpanExporter = cls(**exporter_options)

                        processor: sdk_trace.SpanProcessor = sdk_trace.SynchronousMultiSpanProcessor()
                        processor.add_span_processor(sdk_trace_export.BatchSpanProcessor(span_exporter=exporter))

                        # TODO: Add sampler to configuration schema.
                        sampler: sdk_trace_sampling.Sampler = sdk_trace_sampling.ALWAYS_ON

                        return self._tracer_provider_cache.setdefault(
                            config_json,
                            sdk_trace.TracerProvider(active_span_processor=processor, sampler=sampler),
                        )
                    except (AttributeError, ImportError):
                        logger.error(
                            "Failed to import OpenTelemetry Python SDK or exporter! Disabling traces.", exc_info=True
                        )
                        return None
                else:
                    logger.error("No exporter configured! Disabling traces.")
                    return None

    def tracer(self, config: dict[str, Any]) -> Optional[api_trace.Tracer]:
        """
        Create or return an existing :py:class:`api_trace.Tracer` for a config.

        :param config: ``.opentelemetry.traces`` config dict.
        :return: A :py:class:`api_trace.Tracer` or ``None`` if no valid exporter is configured.
        """
        config_json = json.dumps(config, sort_keys=True)
        with self._tracer_cache_lock:
            if config_json in self._tracer_cache:
                return self._tracer_cache[config_json]
            else:
                tracer_provider = self.tracer_provider(config=config)
                if tracer_provider is None:
                    return None
                else:
                    return self._tracer_cache.setdefault(
                        config_json, tracer_provider.get_tracer(instrumenting_module_name="multistorageclient")
                    )


# To share a single :py:class:`Telemetry` within a process (e.g. local, manager).
#
# A manager's server processes shouldn't be forked, so this should be safe.
_TELEMETRY: Optional[Telemetry] = None
_TELEMETRY_LOCK = threading.Lock()


def _init() -> Telemetry:
    """
    Create or return an existing :py:class:`Telemetry`.

    :return: A telemetry instance.
    """
    global _TELEMETRY
    global _TELEMETRY_LOCK

    with _TELEMETRY_LOCK:
        if _TELEMETRY is None:
            _TELEMETRY = Telemetry()
    return _TELEMETRY


class TelemetryManager(multiprocessing.managers.BaseManager):
    """
    A :py:class:`multiprocessing.managers.BaseManager` for telemetry resources.

    The OpenTelemetry Python SDK isn't fork-safe since telemetry sample buffers can be duplicated.

    In addition, Python ≤3.12 doesn't call exit handlers for forked processes.
    This causes the OpenTelemetry Python SDK to not flush telemetry before exiting.

    * https://github.com/open-telemetry/opentelemetry-python/issues/4215
    * https://github.com/open-telemetry/opentelemetry-python/issues/3307

    Forking is multiprocessing's default start method for non-macOS POSIX systems until Python 3.14.

    * https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods

    To fully support multiprocessing, resampling + publishing is handled by
    a single process that's (ideally) a child of (i.e. directly under) the main process. This:

    * Relieves other processes of this work.

      * Avoids issues with duplicate samples when forking and unpublished samples when exiting forks.

    * Allows cross-process resampling.
    * Reuses a single connection pool to telemetry backends.

    The downside is it essentially re-introduces global interpreter lock (GIL) with
    additional IPC overhead. Telemetry operations, however, should be lightweight so
    this isn't expected to be a problem. Remote data store latency should still be
    the primary throughput limiter for storage clients.

    :py:class:`multiprocessing.managers.BaseManager` is used for this since it creates
    a separate server process for shared objects.

    Telemetry resources are provided as
    `proxy objects <https://docs.python.org/3/library/multiprocessing.html#proxy-objects>`_
    for location transparency.

    The documentation isn't particularly detailed, but others have written comprehensively on this:

    * https://zpz.github.io/blog/python-mp-manager-1
    * https://zpz.github.io/blog/python-mp-manager-2
    * https://zpz.github.io/blog/python-mp-manager-3

    By specification, metric and tracer providers must call shutdown on any
    underlying metric readers + span processors + exporters.

    * https://opentelemetry.io/docs/specs/otel/metrics/sdk#shutdown
    * https://opentelemetry.io/docs/specs/otel/trace/sdk#shutdown

    In the OpenTelemetry Python SDK, provider shutdown is called automatically
    by exit handlers (when they work at least). Consequently, clients should:

    * Only receive proxy objects.

      * Enables metric reader + span processor + exporter re-use across processes.

    * Never call shutdown on the proxy objects.

      * The shutdown exit handler is registered on the manager's server process.
      * ⚠️ We expect a finite number of providers (i.e. no dynamic configs) so we don't leak them.
    """

    pass


def _fully_qualified_name(c: type[Any]) -> str:
    """
    Return the fully qualified name for a class (e.g. ``module.Class``).

    For :py:class:`multiprocessing.Manager` type IDs.
    """
    return ".".join([c.__module__, c.__qualname__])


# Metrics proxy object setup.
TelemetryManager.register(typeid=_fully_qualified_name(api_metrics._Gauge))
TelemetryManager.register(typeid=_fully_qualified_name(api_metrics.Counter))
TelemetryManager.register(
    typeid=_fully_qualified_name(api_metrics.Meter),
    method_to_typeid={
        api_metrics.Meter.create_gauge.__name__: _fully_qualified_name(api_metrics._Gauge),
        api_metrics.Meter.create_counter.__name__: _fully_qualified_name(api_metrics.Counter),
    },
)
TelemetryManager.register(
    typeid=_fully_qualified_name(api_metrics.MeterProvider),
    method_to_typeid={api_metrics.MeterProvider.get_meter.__name__: _fully_qualified_name(api_metrics.Meter)},
)

# Traces proxy object setup.
TelemetryManager.register(
    typeid=_fully_qualified_name(api_trace.Span),
    # Non-public methods (i.e. ones starting with a ``_``) are omitted by default.
    #
    # We need ``__enter__`` and ``__exit__`` so the ``Span`` can be used as a ``ContextManager``.
    exposed=[name for name, _ in inspect.getmembers(api_trace.Span, predicate=inspect.isfunction)],
    method_to_typeid={api_trace.Span.__enter__.__name__: _fully_qualified_name(api_trace.Span)},
)
TelemetryManager.register(
    typeid=_fully_qualified_name(api_trace.Tracer),
    # Can't proxy ``Tracer.start_as_current_span`` since it returns a generator (not pickleable)
    # and tries to use the process-local global context (in this case, the manager's server process).
    #
    # Instead, spans should be constructed by:
    #
    # 1. Calling ``opentelemetry.context.get_current()`` to get the process-local global context (pickleable).
    # 2. Creating a new span with the process-local global context.
    # 3. Calling ``opentelemetry.trace.use_span()`` with the span and ``end_on_exit=True``.
    method_to_typeid={api_trace.Tracer.start_span.__name__: _fully_qualified_name(api_trace.Span)},
)
TelemetryManager.register(
    typeid=_fully_qualified_name(api_trace.TracerProvider),
    method_to_typeid={api_trace.TracerProvider.get_tracer.__name__: _fully_qualified_name(api_trace.Tracer)},
)

# Telemetry proxy object setup.
#
# This should be the only registered type with a ``callable``.
# It's the only type we create directly with a ``TelemetryManager``.
TelemetryManager.register(
    typeid=Telemetry.__name__,
    callable=_init,
    method_to_typeid={
        Telemetry.meter_provider.__name__: _fully_qualified_name(api_metrics.MeterProvider),
        Telemetry.meter.__name__: _fully_qualified_name(api_metrics.Meter),
        Telemetry.gauge.__name__: _fully_qualified_name(api_metrics._Gauge),
        Telemetry.counter.__name__: _fully_qualified_name(api_metrics.Counter),
        Telemetry.tracer_provider.__name__: _fully_qualified_name(api_trace.TracerProvider),
        Telemetry.tracer.__name__: _fully_qualified_name(api_trace.Tracer),
    },
)


# Map of init options as a sorted JSON string (since dictionaries can't be hashed) to telemetry proxy.
_TELEMETRY_PROXIES: dict[str, Telemetry] = {}
# To share :py:class:`Telemetry` proxy objects within a process (e.g. client, server).
#
# Forking isn't expected to happen while this is held (may lead to a deadlock).
_TELEMETRY_PROXIES_LOCK = threading.Lock()


class TelemetryMode(enum.Enum):
    """
    How to create a :py:class:`Telemetry` object.
    """

    #: Keep everything local to the process (not fork-safe).
    LOCAL = "local"
    #: Start + connect to a telemetry IPC server.
    SERVER = "server"
    #: Connect to a telemetry IPC server.
    CLIENT = "client"


def _telemetry_manager_server_port(process_id: int) -> int:
    """
    Get the default telemetry manager server port.

    This is PID-based to:

    * Avoid collisions between multiple independent Python interpreters running on the same machine.
    * Let child processes deterministically find their parent's telemetry manager server.

    :param process_id: Process ID used to calculate the port.
    """

    # Use the dynamic/private/ephemeral port range.
    #
    # https://www.rfc-editor.org/rfc/rfc6335.html#section-6
    # https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers#Dynamic,_private_or_ephemeral_ports
    #
    # Modulo the parent/child process PID by the port range length, then add the initial offset.
    return (2**15 + 2**14) + (process_id % ((2**16) - (2**15 + 2**14)))


def init(
    mode: TelemetryMode = TelemetryMode.SERVER if multiprocessing.parent_process() is None else TelemetryMode.CLIENT,
    address: Optional[Union[str, tuple[str, int]]] = None,
) -> Telemetry:
    """
    Create or return an existing :py:class:`Telemetry` instance or :py:class:`Telemetry` proxy object.

    :param mode: How to create a :py:class:`Telemetry` object. Defaults to :py:const:`TelemetryMode.SERVER`/:py:const:`TelemetryMode.CLIENT` if the current process is a main/child Python process.
    :param address: Telemetry IPC server address. Passed directly to a :py:class:`multiprocessing.managers.BaseManager`. Ignored if the mode is :py:const:`TelemetryMode.LOCAL`.
    :return: A telemetry instance.
    """

    global _TELEMETRY_PROXIES
    global _TELEMETRY_PROXIES_LOCK

    if mode == TelemetryMode.LOCAL:
        return _init()
    elif mode == TelemetryMode.SERVER:
        address = address or ("127.0.0.1", _telemetry_manager_server_port(process_id=psutil.Process().pid))

        init_options = {"mode": mode.value, "address": address}
        init_options_json = json.dumps(init_options, sort_keys=True)

        with _TELEMETRY_PROXIES_LOCK:
            if init_options_json in _TELEMETRY_PROXIES:
                return _TELEMETRY_PROXIES[init_options_json]
            else:
                telemetry_manager = TelemetryManager(
                    address=address,
                    # Use spawn instead of the platform-specific default (may be fork) to avoid aforementioned issues with fork.
                    ctx=multiprocessing.get_context(method="spawn"),
                )

                logger.debug(f"Creating telemetry manager server at {telemetry_manager.address}.")
                try:
                    telemetry_manager.start()
                    atexit.register(telemetry_manager.shutdown)
                    logger.debug(f"Started telemetry manager server at {telemetry_manager.address}.")
                except Exception as e:
                    logger.error(
                        f"Failed to create telemetry manager server at {telemetry_manager.address}!", exc_info=True
                    )
                    raise e

                logger.debug(f"Connecting to telemetry manager server at {telemetry_manager.address}.")
                try:
                    telemetry_manager.connect()
                    logger.debug(f"Connected to telemetry manager server at {telemetry_manager.address}.")
                    return _TELEMETRY_PROXIES.setdefault(init_options_json, telemetry_manager.Telemetry())  # pyright: ignore [reportAttributeAccessIssue]
                except Exception as e:
                    logger.error(
                        f"Failed to connect to telemetry manager server at {telemetry_manager.address}!", exc_info=True
                    )
                    raise e
    elif mode == TelemetryMode.CLIENT:
        candidate_addresses: list[Union[str, tuple[str, int]]] = []

        if address is not None:
            candidate_addresses = [address]
        else:
            current_process = psutil.Process()
            # Python processes from leaf to root.
            python_process_ancestry: list[psutil.Process] = [
                current_process,
                # Try the default telemetry manager server port for all ancestor process IDs.
                #
                # psutil is used since multiprocessing only exposes the parent process.
                #
                # We can't use `itertools.takewhile(lambda ancestor_process: ancestor_process.name() == current_process.name(), ...)`
                # to limit ourselves to ancestor Python processes by process name since some may not be named
                # `python{optional version}` in some cases (e.g. may be named `pytest`).
                *current_process.parents(),
            ]
            # Try to connect from leaf to root.
            candidate_addresses = [
                ("127.0.0.1", _telemetry_manager_server_port(process_id=process.pid))
                for process in python_process_ancestry
            ]

        for candidate_address in candidate_addresses:
            init_options = {"mode": mode.value, "address": candidate_address}
            init_options_json = json.dumps(init_options, sort_keys=True)

            with _TELEMETRY_PROXIES_LOCK:
                if init_options_json in _TELEMETRY_PROXIES:
                    return _TELEMETRY_PROXIES[init_options_json]
                else:
                    telemetry_manager = TelemetryManager(address=candidate_address)

                    logger.debug(f"Connecting to telemetry manager server at {telemetry_manager.address}.")
                    try:
                        telemetry_manager.connect()
                        logger.debug(f"Connected to telemetry manager server at {telemetry_manager.address}.")
                        return _TELEMETRY_PROXIES.setdefault(init_options_json, telemetry_manager.Telemetry())  # pyright: ignore [reportAttributeAccessIssue]
                    except Exception:
                        logger.debug(
                            f"Failed to connect to telemetry manager server at {telemetry_manager.address}!",
                            exc_info=True,
                        )

        raise ConnectionError(f"Failed to connect to telemetry manager server at any of {candidate_addresses}!")
    else:
        raise ValueError(f"Unsupported telemetry mode: {mode}")
