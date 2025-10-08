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
from collections.abc import Callable, Mapping
from functools import wraps
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import StatusCode, Tracer, set_span_in_context

TRACER: Tracer = trace.get_tracer("opentelemetry.instrumentation.multistorageclient")

MB = 1024 * 1024
TRACE_INACTIVITY_TIMEOUT_IN_SECONDS = 0.1


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
