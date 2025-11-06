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

import time

import opentelemetry.sdk.metrics as sdk_metrics
import opentelemetry.sdk.metrics.export as sdk_metrics_export

from multistorageclient.telemetry.metrics.readers.diperiodic_exporting import DiperiodicExportingMetricReader
from test_multistorageclient.unit.utils.telemetry.metrics.export import InMemoryMetricExporter


def test_diperiodic_exporting_metric_reader():
    collect_interval_millis = 1
    export_interval_millis = 1000
    # 2 periods of each to avoid race conditions.
    shutdown_timeout_millis = 2 * (collect_interval_millis + export_interval_millis)

    exporter = InMemoryMetricExporter()
    reader = DiperiodicExportingMetricReader(
        exporter=exporter,
        collect_interval_millis=collect_interval_millis,
        export_interval_millis=export_interval_millis,
    )

    # Skip collection before registering on a meter provider.
    reader._collect_iteration()

    meter_provider = sdk_metrics.MeterProvider(metric_readers=[reader])
    meter = meter_provider.get_meter("meter")
    gauge = meter.create_gauge("gauge")

    # Periodic export.
    gauge.set(1)
    time.sleep(2)
    metrics_data = exporter.metrics_data()
    assert metrics_data is not None
    assert len(metrics_data.resource_metrics) == 1
    assert len(metrics_data.resource_metrics[0].scope_metrics) == 1
    assert len(metrics_data.resource_metrics[0].scope_metrics[0].metrics) == 1
    assert isinstance(metrics_data.resource_metrics[0].scope_metrics[0].metrics[0].data, sdk_metrics_export.Gauge)
    assert len(metrics_data.resource_metrics[0].scope_metrics[0].metrics[0].data.data_points) == 1
    assert metrics_data.resource_metrics[0].scope_metrics[0].metrics[0].data.data_points[0].value == 1

    # Force flush.
    gauge.set(2)
    reader.force_flush(timeout_millis=shutdown_timeout_millis)
    metrics_data = exporter.metrics_data()
    assert metrics_data is not None
    assert len(metrics_data.resource_metrics) == 1
    assert len(metrics_data.resource_metrics[0].scope_metrics) == 1
    assert len(metrics_data.resource_metrics[0].scope_metrics[0].metrics) == 1
    assert isinstance(metrics_data.resource_metrics[0].scope_metrics[0].metrics[0].data, sdk_metrics_export.Gauge)
    assert len(metrics_data.resource_metrics[0].scope_metrics[0].metrics[0].data.data_points) == 1
    assert metrics_data.resource_metrics[0].scope_metrics[0].metrics[0].data.data_points[0].value == 2

    # Shutdown.
    gauge.set(3)
    reader.shutdown(timeout_millis=shutdown_timeout_millis)
    metrics_data = exporter.metrics_data()
    assert metrics_data is not None
    assert len(metrics_data.resource_metrics) == 1
    assert len(metrics_data.resource_metrics[0].scope_metrics) == 1
    assert len(metrics_data.resource_metrics[0].scope_metrics[0].metrics) == 1
    assert isinstance(metrics_data.resource_metrics[0].scope_metrics[0].metrics[0].data, sdk_metrics_export.Gauge)
    assert len(metrics_data.resource_metrics[0].scope_metrics[0].metrics[0].data.data_points) == 1
    assert metrics_data.resource_metrics[0].scope_metrics[0].metrics[0].data.data_points[0].value == 3

    # Shutdown is idempotent.
    reader.shutdown(timeout_millis=shutdown_timeout_millis)
