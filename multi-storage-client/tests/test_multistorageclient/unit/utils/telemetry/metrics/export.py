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

import threading
from typing import Optional

import opentelemetry.sdk.metrics.export as sdk_metrics_export
import opentelemetry.sdk.metrics.view as sdk_metrics_view


class InMemoryMetricExporter(sdk_metrics_export.MetricExporter):
    """
    Implementation of :py:class:``sdk_metrics_export.MetricExporter`` that saves the last metrics data export in memory.
    """

    _metrics_data: Optional[sdk_metrics_export.MetricsData]
    _metrics_data_lock: threading.Lock

    def __init__(
        self,
        preferred_temporality: dict[type, sdk_metrics_export.AggregationTemporality] = {},
        preferred_aggregation: dict[type, sdk_metrics_view.Aggregation] = {},
    ):
        super().__init__(preferred_aggregation=preferred_aggregation, preferred_temporality=preferred_temporality)
        self._metrics_data = None
        self._metrics_data_lock = threading.Lock()

    def export(
        self,
        metrics_data: sdk_metrics_export.MetricsData,
        timeout_millis: float = 0,
        **kwargs,
    ) -> sdk_metrics_export.MetricExportResult:
        with self._metrics_data_lock:
            self._metrics_data = metrics_data
        return sdk_metrics_export.MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 0) -> bool:
        return True

    def shutdown(self, timeout_millis: float = 0, **kwargs) -> None:
        pass

    def metrics_data(self) -> Optional[sdk_metrics_export.MetricsData]:
        return self._metrics_data
