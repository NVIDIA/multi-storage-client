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
from collections.abc import Sequence
from typing import Optional

import opentelemetry.sdk.trace as sdk_trace
import opentelemetry.sdk.trace.export as sdk_trace_export


class InMemorySpanExporter(sdk_trace_export.SpanExporter):
    """
    Implementation of :py:class:``sdk_trace_export.SpanExporter`` that saves the last spans export in memory.
    """

    _spans: Optional[Sequence[sdk_trace.ReadableSpan]]
    _spans_lock: threading.Lock

    def __init__(self):
        self._spans = None
        self._spans_lock = threading.Lock()

    def export(self, spans: Sequence[sdk_trace.ReadableSpan]) -> sdk_trace_export.SpanExportResult:
        with self._spans_lock:
            self._spans = spans
        return sdk_trace_export.SpanExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 0) -> bool:
        return True

    def shutdown(self) -> None:
        pass

    def spans(self) -> Optional[Sequence[sdk_trace.ReadableSpan]]:
        return self._spans
