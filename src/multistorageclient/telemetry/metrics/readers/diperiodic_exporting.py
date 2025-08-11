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
import math
import os
import threading
import time
import weakref
from typing import Optional

import opentelemetry.sdk.environment_variables as sdk_environment_variables
import opentelemetry.sdk.metrics as sdk_metrics
import opentelemetry.sdk.metrics.export as sdk_metrics_export

# Not OTel spec. Use 1 second to keep the data volume per export interval reasonably small.
DEFAULT_COLLECT_INTERVAL_MILLIS: float = 1000
# Not OTel spec. Use the default on :py:meth:`sdk_metrics_export.MetricReader.collect`.
DEFAULT_COLLECT_TIMEOUT_MILLIS: float = 10000
# OTel spec.
DEFAULT_EXPORT_INTERVAL_MILLIS: float = 60000
# OTel spec.
DEFAULT_EXPORT_TIMEOUT_MILLIS: float = 30000

logger = logging.getLogger(__name__)


class DiperiodicExportingMetricReader(sdk_metrics_export.MetricReader):
    """
    :py:class:`opentelemetry.sdk.metrics.export.MetricReader` that collects + exports metrics on separate user-configurable time intervals.
    This is in contrast with :py:class:`opentelemetry.sdk.metrics.export.PeriodicExportingMetricReader` which couples them with a 1 minute default.

    The metrics collection interval limits the temporal resolution. Most metric backends have 1 millisecond or finer temporal resolution.
    """

    #: Collect buffer.
    _collect_metrics_data: Optional[sdk_metrics_export.MetricsData]
    _collect_metrics_data_lock: threading.Lock
    #: Export buffer.
    _export_metrics_data: Optional[sdk_metrics_export.MetricsData]
    _export_metrics_data_lock: threading.Lock

    _exporter: sdk_metrics_export.MetricExporter
    _collect_interval_millis: float
    _collect_timeout_millis: float
    _export_interval_millis: float
    _export_timeout_millis: float

    _shutdown_event: threading.Event
    _shutdown_event_lock: threading.Lock
    _collect_daemon: Optional[threading.Thread]
    _export_daemon: Optional[threading.Thread]

    def __init__(
        self,
        exporter: sdk_metrics_export.MetricExporter,
        collect_interval_millis: Optional[float] = None,
        collect_timeout_millis: Optional[float] = None,
        export_interval_millis: Optional[float] = None,
        export_timeout_millis: Optional[float] = None,
    ):
        """
        :param exporter: Metrics exporter.
        :param collect_interval_millis: Collect interval in milliseconds.
        :param collect_timeout_millis: Collect timeout in milliseconds.
        :param export_interval_millis: Export interval in milliseconds.
        :param export_timeout_millis: Export timeout in milliseconds.
        """

        # Defer to the exporter for aggregation and temporality configurations.
        super().__init__(
            preferred_aggregation=exporter._preferred_aggregation, preferred_temporality=exporter._preferred_temporality
        )

        self._collect_metrics_data = None
        self._collect_metrics_data_lock = threading.Lock()
        self._export_metrics_data = None
        self._export_metrics_data_lock = threading.Lock()

        self._exporter = exporter
        if collect_interval_millis is None:
            # OTEL_METRIC_COLLECT_INTERVAL isn't an official OTel SDK environment variable (yet).
            collect_interval_millis = DEFAULT_COLLECT_INTERVAL_MILLIS
        if collect_timeout_millis is None:
            # OTEL_METRIC_COLLECT_TIMEOUT isn't an official OTel SDK environment variable (yet).
            collect_timeout_millis = DEFAULT_COLLECT_TIMEOUT_MILLIS
        if export_interval_millis is None:
            try:
                export_interval_millis = float(
                    os.environ.get(
                        sdk_environment_variables.OTEL_METRIC_EXPORT_INTERVAL, DEFAULT_EXPORT_INTERVAL_MILLIS
                    )
                )
            except ValueError:
                logger.warning(
                    f"Found invalid value for export interval. Using default of {DEFAULT_EXPORT_INTERVAL_MILLIS}."
                )
                export_interval_millis = DEFAULT_EXPORT_INTERVAL_MILLIS
        if export_timeout_millis is None:
            try:
                export_timeout_millis = float(
                    os.environ.get(sdk_environment_variables.OTEL_METRIC_EXPORT_TIMEOUT, DEFAULT_EXPORT_TIMEOUT_MILLIS)
                )
            except ValueError:
                logger.warning(
                    f"Found invalid value for export timeout. Using default of {DEFAULT_EXPORT_TIMEOUT_MILLIS}."
                )
                export_timeout_millis = DEFAULT_EXPORT_TIMEOUT_MILLIS
        self._collect_interval_millis = collect_interval_millis
        self._collect_timeout_millis = collect_timeout_millis
        self._export_interval_millis = export_interval_millis
        self._export_timeout_millis = export_timeout_millis

        self._shutdown_event = threading.Event()
        self._shutdown_event_lock = threading.Lock()
        self._collect_daemon = None
        self._export_daemon = None
        if (
            self._collect_interval_millis > 0
            and self._collect_interval_millis < math.inf
            and self._export_interval_millis > 0
            and self._export_interval_millis < math.inf
        ):
            self._init_daemons()
            if hasattr(os, "register_at_fork"):
                os.register_at_fork(after_in_child=weakref.WeakMethod(self._init_daemons)())
        else:
            raise ValueError("Collect and export intervals must be in (0, infinity).")

    def _init_daemons(self) -> None:
        # Empty the buffers. Prevents duplicate metrics when forking.
        with self._collect_metrics_data_lock, self._export_metrics_data_lock:
            self._collect_metrics_data, self._export_metrics_data = None, None

        # Create the collect daemon.
        self._collect_daemon = threading.Thread(
            name="OtelDiperiodicExportingMetricReader._collect_daemon", target=self._collect_daemon_target, daemon=True
        )
        self._collect_daemon.start()

        # Create the export daemon.
        self._export_daemon = threading.Thread(
            name="OtelDiperiodicExportingMetricReader._export_daemon", target=self._export_daemon_target, daemon=True
        )
        self._export_daemon.start()

    def _collect_daemon_target(self) -> None:
        while not self._shutdown_event.wait(timeout=self._collect_interval_millis / 10**3):
            self._collect_iteration()

    def _export_daemon_target(self) -> None:
        while not self._shutdown_event.wait(timeout=self._export_interval_millis / 10**3):
            self._export_iteration()
        # Final collect + export.
        self._collect_iteration()
        self._export_iteration()

    # :py:meth:`sdk_metrics_export.MetricReader._collect` is reserved. Using another name.
    def _collect_iteration(self, timeout_millis: Optional[float] = None) -> None:
        try:
            # Only set when registered on a :py:class:`sdk_metrics.MeterProvider` which calls
            # :py:meth:`sdk_metrics_export.MetricReader._set_collect_callback`.
            if self._collect is not None:
                # Inherited from :py:class:``sdk_metrics_export.MetricReader``.
                self.collect(timeout_millis=timeout_millis or self._collect_timeout_millis)
        except sdk_metrics.MetricsTimeoutError:
            logger.warning("Metrics collection timed out.", exc_info=True)
        except Exception:
            logger.exception("Exception while collecting metrics.")

    # Called by :py:meth:`sdk_metrics_export.MetricReader.collect`.
    def _receive_metrics(
        self, metrics_data: sdk_metrics_export.MetricsData, timeout_millis: float = 0, **kwargs
    ) -> None:
        with self._collect_metrics_data_lock:
            self._collect_metrics_data = sdk_metrics_export.MetricsData(
                resource_metrics=(
                    *(() if self._collect_metrics_data is None else self._collect_metrics_data.resource_metrics),
                    *metrics_data.resource_metrics,
                )
            )

    def _export_iteration(self, timeout_millis: Optional[float] = None) -> None:
        with self._export_metrics_data_lock:
            with self._collect_metrics_data_lock:
                # Rotate the collect + export buffers.
                #
                # We don't merge the collect buffer into the export buffer to prevent infinite accumulation.
                self._collect_metrics_data, self._export_metrics_data = None, self._collect_metrics_data

            if self._export_metrics_data is not None:
                try:
                    # Export.
                    self._exporter.export(
                        metrics_data=self._export_metrics_data,
                        timeout_millis=timeout_millis or self._export_timeout_millis,
                    )
                except sdk_metrics.MetricsTimeoutError:
                    logger.warning("Metrics export timed out.", exc_info=True)
                except Exception:
                    logger.exception("Exception while exporting metrics.")
                finally:
                    # Immediately empty the export buffer for garbage collection.
                    self._export_metrics_data = None

    def force_flush(
        self, timeout_millis: float = DEFAULT_COLLECT_TIMEOUT_MILLIS + DEFAULT_EXPORT_TIMEOUT_MILLIS
    ) -> bool:
        deadline_ns = time.time_ns() + (timeout_millis * 10**6)

        # Calls :py:meth:`sdk_metrics_export.MetricReader.collect`.
        super().force_flush(timeout_millis=(deadline_ns - time.time_ns()) / 10**6)
        self._export_iteration(timeout_millis=(deadline_ns - time.time_ns()) / 10**6)
        self._exporter.force_flush(timeout_millis=(deadline_ns - time.time_ns()) / 10**6)
        return True

    def shutdown(
        self, timeout_millis: float = DEFAULT_COLLECT_TIMEOUT_MILLIS + DEFAULT_EXPORT_TIMEOUT_MILLIS, **kwargs
    ) -> None:
        deadline_ns = time.time_ns() + (timeout_millis * 10**6)

        with self._shutdown_event_lock:
            if not self._shutdown_event.is_set():
                if self._collect_daemon is not None:
                    self._collect_daemon.join(timeout=(deadline_ns - time.time_ns()) / 10**9)
                if self._export_daemon is not None:
                    self._export_daemon.join(timeout=(deadline_ns - time.time_ns()) / 10**9)
                self._exporter.shutdown(timeout_millis=(deadline_ns - time.time_ns()) / 10**6)
                self._shutdown_event.set()
