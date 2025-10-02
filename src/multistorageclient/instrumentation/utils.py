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

import os
import time
from collections.abc import Callable, Mapping, MutableMapping
from functools import wraps
from typing import Any, Optional, Union

from opentelemetry import metrics, trace
from opentelemetry.metrics import get_meter_provider
from opentelemetry.trace import StatusCode, Tracer, set_span_in_context

from . import HAS_OBSERVABILITY_DEPS

# import optional dependencies if dependencies are available
if HAS_OBSERVABILITY_DEPS:
    import datasketches
    from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider

METER = metrics.get_meter("opentelemetry.instrumentation.multistorageclient")
TRACER: Tracer = trace.get_tracer("opentelemetry.instrumentation.multistorageclient")

MB = 1024 * 1024
TRACE_INACTIVITY_TIMEOUT_IN_SECONDS = 0.1

DURATION_HISTOGRAM = METER.create_histogram(
    name="storageclient_api_duration",
    unit="ms",
    description="Measures the duration of storage operations (e.g., GET, PUT, LIST) in milliseconds.",
)

OBJECT_SIZE_HISTOGRAM = METER.create_histogram(
    name="storageclient_object_size",
    unit="mb",
    description="Tracks the size of objects involved in storage operations (e.g., GET, PUT) in megabytes.",
)

DURATION_P50_GAUGE = METER.create_gauge(
    name="storageclient_api_duration_p50",
    unit="ms",
    description="Measures the P50 duration of storage operations (e.g., GET, PUT, LIST) in milliseconds.",
)

DURATION_P99_GAUGE = METER.create_gauge(
    name="storageclient_api_duration_p99",
    unit="ms",
    description="Measures the P99 duration of storage operations (e.g., GET, PUT, LIST) in milliseconds.",
)

DURATION_P999_GAUGE = METER.create_gauge(
    name="storageclient_api_duration_p999",
    unit="ms",
    description="Measures the P99.9 duration of storage operations (e.g., GET, PUT, LIST) in milliseconds.",
)

OBJECT_SIZE_P50_GAUGE = METER.create_gauge(
    name="storageclient_object_size_p50",
    unit="mb",
    description="Tracks the P50 size of objects involved in storage operations (e.g., GET, PUT) in megabytes.",
)

OBJECT_SIZE_P99_GAUGE = METER.create_gauge(
    name="storageclient_object_size_p99",
    unit="mb",
    description="Tracks the P99 size of objects involved in storage operations (e.g., GET, PUT) in megabytes.",
)

OBJECT_SIZE_P999_GAUGE = METER.create_gauge(
    name="storageclient_object_size_p999",
    unit="mb",
    description="Tracks the P99.9 size of objects involved in storage operations (e.g., GET, PUT) in megabytes.",
)

CACHE_MANAGER_COUNTER = METER.create_counter(
    name="storageclient_cache_manager_count",
    description="Counts the number of operations (e.g., SET, READ, OPEN, DELETE) in cache manager.",
)


class AttributeProvider:
    def detect(self, env: Mapping[str, Any]) -> bool:
        """Detect if the current environment matches this provider."""
        raise NotImplementedError

    def collect_attributes(self, env: Mapping[str, Any]) -> dict[str, Any]:
        """Collect attributes specific to this provider."""
        raise NotImplementedError


class K8SAttributeProvider(AttributeProvider):
    def detect(self, env: Mapping[str, Any]) -> bool:
        # Check if running inside a Kubernetes cluster using default environment variables
        return "KUBERNETES_SERVICE_HOST" in env

    def collect_attributes(self, env: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "job_id": None,
            "job_name": None,
            "job_user": None,
            "node_id": env.get("HOSTNAME"),
            "cluster": None,
        }


class SlurmAttributeProvider(AttributeProvider):
    def detect(self, env: Mapping[str, Any]) -> bool:
        return "SLURM_JOB_ID" in env

    def collect_attributes(self, env: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "job_id": env.get("SLURM_JOB_ID"),
            "job_name": env.get("SLURM_JOB_NAME"),
            "job_user": env.get("SLURM_JOB_USER"),
            "node_id": env.get("SLURM_NODEID"),
            "cluster": env.get("SLURM_CLUSTER_NAME"),
        }


class MSCAttributeProvider(AttributeProvider):
    def detect(self, env: Mapping[str, Any]) -> bool:
        # Always checks for MSC env vars as they act as base values.
        return True

    def collect_attributes(self, env: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "job_id": env.get("MSC_JOB_ID"),
            "job_name": env.get("MSC_JOB_NAME"),
            "job_user": env.get("MSC_JOB_USER"),
            "node_id": env.get("MSC_NODEID"),
            "cluster": env.get("MSC_CLUSTER_NAME"),
        }


providers: list[AttributeProvider] = [
    K8SAttributeProvider(),
    SlurmAttributeProvider(),
]

msc_base_provider = MSCAttributeProvider()


def collect_default_attributes(  # pylint: disable=dangerous-default-value
    env: Mapping[str, Any] = os.environ,
) -> Mapping[str, Any]:
    collected_attributes: dict[str, Any] = {}

    for provider in providers:
        if provider.detect(env):
            collected_attributes = provider.collect_attributes(env)
            break

    # Fill in missing attributes from base MSC provider.
    msc_attributes = msc_base_provider.collect_attributes(env)
    for key, value in msc_attributes.items():
        if key not in collected_attributes or collected_attributes[key] is None:
            collected_attributes[key] = value

    collected_attributes = {k: v for k, v in collected_attributes.items() if v is not None}

    return collected_attributes


DEFAULT_ATTRIBUTES = collect_default_attributes()


class TDigestPercentiles:
    """
    A class that tracks percentiles using `datasketches.tdigest_float` and updates corresponding
    OpenTelemetry Gauges for the 50th, 99th, and 99.9th percentiles.
    """

    def __init__(self, p50_gauge: metrics._Gauge, p99_gauge: metrics._Gauge, p999_gauge: metrics._Gauge):
        self._tdigests: MutableMapping[tuple, Any] = {}
        self._p50_gauge = p50_gauge
        self._p99_gauge = p99_gauge
        self._p999_gauge = p999_gauge

    def record(self, amount: Union[int, float], attributes: Optional[Mapping[str, Any]] = None) -> None:
        """
        Records an amount into the T-digest for the specified attribute combination and updates
        the P50, P99, and P99.9 gauges accordingly.
        """
        if not HAS_OBSERVABILITY_DEPS:
            return

        if not attributes:
            attributes = {}
        # Record the amount into the specific tdigest for this attribute combination
        tdigest = self._get_tdigest(attributes)
        tdigest.update(amount)

        # Update gauges
        self._p50_gauge.set(tdigest.get_quantile(0.50), attributes)
        self._p99_gauge.set(tdigest.get_quantile(0.99), attributes)
        self._p999_gauge.set(tdigest.get_quantile(0.999), attributes)

    def _get_tdigest(self, attributes: Optional[Mapping[str, Any]] = None) -> Any:
        # Create a key based on the attributes (sorted for consistent keys)
        if not attributes:
            attributes = {}
        attributes_key = tuple(sorted(attributes.items())) if attributes else tuple()

        # Get or create a tdigest for this specific attribute combination
        if attributes_key not in self._tdigests:
            self._tdigests[attributes_key] = datasketches.tdigest_float()  # pyright: ignore [reportAttributeAccessIssue]

        return self._tdigests[attributes_key]

    def _serialize_tdigests(self) -> Mapping[tuple, bytes]:
        """Serialize tdigests objects into bytes."""
        m = {}
        for k, v in self._tdigests.items():
            m[k] = v.serialize()
        return m

    def _deserialize_tdigests(self, tdigests: Mapping[tuple, bytes]) -> MutableMapping[tuple, Any]:
        """Deserialize tdigests objects from bytes."""

        m = {}
        for k, v in tdigests.items():
            m[k] = datasketches.tdigest_float.deserialize(v)  # pyright: ignore [reportAttributeAccessIssue]
        return m

    def __setstate__(self, state: dict[str, Any]) -> None:
        self._tdigests = self._deserialize_tdigests(state["_tdigests"])
        self._p50_gauge = state["_p50_gauge"]
        self._p99_gauge = state["_p99_gauge"]
        self._p999_gauge = state["_p999_gauge"]

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        state["_tdigests"] = self._serialize_tdigests()
        return state


class StorageProviderMetricsHelper:
    """
    A helper class to record metrics for duration and object size using histograms.
    """

    def __init__(self, attributes: Mapping[str, Any] = DEFAULT_ATTRIBUTES):
        """
        Initializes the MetricHelper with optional default attributes for metrics.

        :param attributes: A dictionary of default attributes to be used when recording metrics.
        """
        self._duration_histogram = DURATION_HISTOGRAM
        self._duration_percentiles = TDigestPercentiles(DURATION_P50_GAUGE, DURATION_P99_GAUGE, DURATION_P999_GAUGE)

        self._object_size_histogram = OBJECT_SIZE_HISTOGRAM
        self._object_size_percentiles = TDigestPercentiles(
            OBJECT_SIZE_P50_GAUGE, OBJECT_SIZE_P99_GAUGE, OBJECT_SIZE_P999_GAUGE
        )

        self._attributes = attributes

    def _merge_attributes(self, attributes: Optional[Mapping[str, Any]] = None) -> dict[str, Any]:
        """
        Merges default attributes with provided attributes.
        """
        if not attributes:
            attributes = {}
        return {**self._attributes, **attributes}

    def record_duration(
        self, duration: Union[int, float], provider: str, operation: str, bucket: str, status_code: int
    ) -> None:
        """
        Records the duration for a given operation with specified attributes.

        :param duration: The duration value in seconds.
        :param provider: The storage provider name (e.g., s3, gcs).
        :param operation: The operation being performed (e.g., GET, PUT, LIST).
        :param bucket: The name of the storage bucket involved.
        :param status_code: The HTTP status code of the operation.
        """
        if not self._is_metrics_enabled():
            return

        attributes = {
            "provider": provider,
            "operation": operation,
            "bucket": bucket,
            "status_code": status_code,
            "proc_id": os.getpid(),
        }
        duration_ms = duration * 1000
        self._duration_histogram.record(duration_ms, attributes=self._merge_attributes(attributes))
        self._duration_percentiles.record(duration_ms, attributes=self._merge_attributes(attributes))

    def record_object_size(
        self, object_size: Union[int, float], provider: str, operation: str, bucket: str, status_code: int
    ) -> None:
        """
        Records the object size for a given operation with specified attributes.

        :param object_size: The size of the object in bytes.
        :param provider: The storage provider name (e.g., s3, gcs).
        :param operation: The operation being performed (e.g., GET, PUT, LIST).
        :param bucket: The name of the storage bucket involved.
        :param status_code: The HTTP status code of the operation.
        """
        if not self._is_metrics_enabled():
            return

        attributes = {
            "provider": provider,
            "operation": operation,
            "bucket": bucket,
            "status_code": status_code,
            "proc_id": os.getpid(),
        }
        object_size_mb = object_size / MB
        self._object_size_histogram.record(object_size_mb, attributes=self._merge_attributes(attributes))
        self._object_size_percentiles.record(object_size_mb, attributes=self._merge_attributes(attributes))

    def _is_metrics_enabled(self) -> bool:
        """
        Checks if metrics are enabled.

        Returns:
            bool: True if metrics are enabled, False otherwise.
        """
        return HAS_OBSERVABILITY_DEPS and isinstance(get_meter_provider(), SdkMeterProvider)


def _get_span_attribute(span: Any, key: str, default: Any = 0) -> Any:
    """Safely get attribute from span, handling both recording and non-recording spans."""
    if hasattr(span, "attributes") and hasattr(span.attributes, "get"):
        return span.attributes.get(key, default)
    return default


def file_tracer(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        managed_file_instance = args[0]
        parent_trace_span = getattr(managed_file_instance, "_trace_span", None)
        function_name = func.__name__

        # Use the parent span's context if it exists
        if parent_trace_span:
            context = set_span_in_context(parent_trace_span)
        else:
            context = None

        current_op_span = getattr(managed_file_instance, "_current_op_span", None)
        current_op_type = getattr(managed_file_instance, "_current_op_type", None)
        current_op_count = getattr(managed_file_instance, "_current_op_count", 0)
        last_op_time = getattr(managed_file_instance, "_last_op_time", 0)

        current_time = time.time()

        # Decide whether to create new span
        create_new_span = (
            current_op_span is None
            or current_op_type != function_name
            or (current_time - last_op_time) > TRACE_INACTIVITY_TIMEOUT_IN_SECONDS
        )

        if create_new_span:
            if current_op_span is not None:
                # Set final operation count before ending the span
                current_op_span.set_attribute("operation_count", current_op_count)  # pyright: ignore[reportOptionalMemberAccess]
                current_op_span.end()

            current_op_span = TRACER.start_span(function_name, context=context)
            setattr(managed_file_instance, "_current_op_span", current_op_span)
            setattr(managed_file_instance, "_current_op_type", function_name)
            current_op_count = 1
        else:
            # Increment operation count for existing span
            current_op_count += 1
        setattr(managed_file_instance, "_current_op_count", current_op_count)
        current_op_span.set_attribute("operation_count", current_op_count)  # pyright: ignore[reportOptionalMemberAccess]

        try:
            # Update span attributes
            if function_name in ["read", "readline", "truncate"]:
                current_size = _get_span_attribute(current_op_span, "size", 0)
                size = args[1] if len(args) > 1 else kwargs.get("size", -1)
                current_op_span.set_attribute("size", current_size + size)  # pyright: ignore[reportOptionalMemberAccess]
            elif function_name == "readlines":
                hint = args[1] if len(args) > 1 else kwargs.get("hint", -1)
                current_op_span.set_attribute("hint", hint)  # pyright: ignore[reportOptionalMemberAccess]
            elif function_name == "write":
                bytes_written = len(args[1]) if len(args) > 1 else len(kwargs.get("b", b""))
                current_bytes = _get_span_attribute(current_op_span, "bytes_written", 0)
                current_op_span.set_attribute("bytes_written", current_bytes + bytes_written)  # pyright: ignore[reportOptionalMemberAccess]
            elif function_name == "writelines":
                lines_written = len(args[1]) if len(args) > 1 else len(kwargs.get("lines", []))
                current_lines = _get_span_attribute(current_op_span, "lines_written", 0)
                current_op_span.set_attribute("lines_written", current_lines + lines_written)  # pyright: ignore[reportOptionalMemberAccess]

            with trace.use_span(current_op_span):  # pyright: ignore[reportArgumentType, reportCallIssue]
                result = func(*args, **kwargs)

                if function_name in ["read", "readline"]:
                    current_bytes = _get_span_attribute(current_op_span, "bytes_read", 0)
                    current_op_span.set_attribute("bytes_read", current_bytes + len(result))  # pyright: ignore[reportOptionalMemberAccess]
                elif function_name == "readlines":
                    current_bytes = _get_span_attribute(current_op_span, "bytes_read", 0)
                    current_op_span.set_attribute("bytes_read", current_bytes + sum(map(len, result)))  # pyright: ignore[reportOptionalMemberAccess]

                setattr(managed_file_instance, "_last_op_time", current_time)
                current_op_span.set_status(StatusCode.OK)  # pyright: ignore[reportOptionalMemberAccess]
                return result
        except Exception as e:
            current_op_span.set_status(StatusCode.ERROR, f"Exception: {str(e)}")  # pyright: ignore[reportOptionalMemberAccess]
            current_op_span.end()  # pyright: ignore[reportOptionalMemberAccess]
            setattr(managed_file_instance, "_current_op_span", None)
            setattr(managed_file_instance, "_current_op_type", None)
            setattr(managed_file_instance, "_current_op_count", 0)
            raise e
        finally:
            # Close spans when file is closed
            if function_name == "close":
                # Set final operation count before closing
                current_op_span.set_attribute("operation_count", current_op_count)  # pyright: ignore[reportOptionalMemberAccess]
                current_op_span.end()  # pyright: ignore[reportOptionalMemberAccess]
                setattr(managed_file_instance, "_current_op_span", None)
                setattr(managed_file_instance, "_current_op_type", None)
                setattr(managed_file_instance, "_current_op_count", 0)
                if parent_trace_span:
                    parent_trace_span.end()
                    managed_file_instance._trace_span = None

    return wrapper


def file_metrics(operation):
    """
    Decorator to emit metrics for PosixFile I/O operations.

    This decorator wraps file I/O methods to emit metrics through the storage provider's
    _emit_metrics infrastructure. It tracks latency, data size, data rate, and request/response
    counts for file operations.

    :param operation: The operation type (BaseStorageProvider._Operation.READ or WRITE)
    :return: Decorated function that emits metrics

    Usage:
        @file_metrics(operation=BaseStorageProvider._Operation.READ)
        def read(self, size: int = -1) -> Any:
            return self._file.read(size)
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args: Any, **kwargs: Any) -> Any:
            storage_provider = self._storage_client._storage_provider
            return storage_provider._emit_metrics(operation=operation, f=lambda: func(self, *args, **kwargs))

        return wrapper

    return decorator


def _generic_tracer(func: Callable, class_name: str) -> Callable:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Use the class_name captured at decoration time
        full_function_name = f"{class_name}.{func.__name__}"
        with TRACER.start_as_current_span(full_function_name) as span:  # pyright: ignore[reportCallIssue,reportAttributeAccessIssue]
            span.set_attribute("function_name", full_function_name)

            for k, v in DEFAULT_ATTRIBUTES.items():
                span.set_attribute(k, v)
            try:
                result = func(*args, **kwargs)
                span.set_status(StatusCode.OK)
                return result
            except Exception as e:
                span.set_status(StatusCode.ERROR, f"Exception: {str(e)}")
                span.record_exception(e)
                raise e

    return wrapper


def instrumented(cls: Any) -> Any:
    """
    A class decorator that automatically instruments all callable attributes
    of the class with the generic tracer.

    This will wrap all methods (including static and class methods) in the class,
    ensuring that every call to those methods creates a new span for tracing.

    :param cls: The class to be instrumented.
    :return: The class with all of its callable attributes wrapped by the generic tracer.
    """
    class_name = cls.__name__
    for attr_name, attr_value in list(cls.__dict__.items()):
        if callable(attr_value) and not attr_name.startswith("_"):
            decorated = _generic_tracer(attr_value, class_name)
            setattr(cls, attr_name, decorated)
    return cls


class CacheManagerMetricsHelper:
    """
    A helper class to record metrics for cache manager.
    """

    def __init__(self, attributes: Mapping[str, Any] = DEFAULT_ATTRIBUTES) -> None:
        self._attributes = attributes
        self._counter = CACHE_MANAGER_COUNTER

    def _merge_attributes(self, attributes: Optional[Mapping[str, Any]] = None) -> dict[str, Any]:
        """
        Merges default attributes with provided attributes.
        """
        if not attributes:
            attributes = {}
        return {**self._attributes, **attributes}

    def increase(self, operation: str, success: bool) -> None:
        """
        Increases the counter of a given operation with specified attributes.

        :param operation: The operation being performed (e.g., SET, READ).
        :param success: True if the operation succeeds.
        """
        attributes = {
            "operation": operation,
            "success": success,
            "proc_id": os.getpid(),
        }
        self._counter.add(1, attributes=self._merge_attributes(attributes))


def set_span_attribute(attribute_name: str, attribute_value: Any) -> None:
    """
    Safely sets an attribute on the current span, if both span and attribute value exist.

    :param attribute_name: The name of the attribute to set
    :param attribute_value: The value of the attribute to set
    """
    if attribute_value is not None:
        span = trace.get_current_span()
        if span is not None:
            span.set_attribute(attribute_name, attribute_value)
