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

import asyncio
import heapq
import importlib.metadata as importlib_metadata
import logging
import os
import queue
import threading
import time
from abc import abstractmethod
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from enum import Enum
from typing import IO, Any, Optional, TypeVar, Union, cast

import opentelemetry.metrics as api_metrics
import opentelemetry.util.types as api_types

from ..rust_utils import run_coroutine_sync
from ..telemetry import Telemetry
from ..telemetry.attributes.base import AttributesProvider, collect_attributes
from ..types import ObjectMetadata, Range, SignerType, StorageProvider
from ..utils import (
    create_attribute_filter_evaluator,
    extract_prefix_from_glob,
    glob,
    import_class,
    insert_directories,
    matches_attribute_filter_expression,
    safe_makedirs,
    split_path,
)

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


_ShallowListResult = tuple[list[str], list[ObjectMetadata]]


class _ListingHeapItem:
    """Heap item for parallel listing: either a prefix to expand or an object to yield."""

    __slots__ = ("key", "is_prefix", "data")

    def __init__(self, key: str, is_prefix: bool, data: Union[str, ObjectMetadata]):
        self.key = key
        self.is_prefix = is_prefix
        self.data = data

    def __lt__(self, other: "_ListingHeapItem") -> bool:
        return self.key < other.key


class _PrefixExpander:
    """
    Expands storage prefixes into children via a bounded thread pool.

    Prefixes are scheduled in sorted order to maximize prefetch hits
    for the heap-based consumer. Only two methods matter to callers:

        enqueue()  — register prefixes for background expansion
        get()      — retrieve a result, blocking if still in-flight
    """

    __slots__ = ("_fn", "_max_inflight", "_executor", "_pending", "_inflight", "_ready")

    def __init__(
        self,
        shallow_list_fn: Callable[[str], _ShallowListResult],
        max_workers: int,
        look_ahead: int,
    ):
        self._fn = shallow_list_fn
        self._max_inflight = max_workers * look_ahead
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._pending: list[str] = []
        self._inflight: dict[str, Future[_ShallowListResult]] = {}
        self._ready: dict[str, _ShallowListResult] = {}

    def enqueue(self, prefixes: list[str]) -> None:
        """Register prefixes for background expansion."""
        if not self._pending:
            # O(N) batch insert via heapify.
            self._pending = list(prefixes)
            heapq.heapify(self._pending)
        else:
            for p in prefixes:
                heapq.heappush(self._pending, p)
        self._fill()

    def get(self, prefix: str) -> _ShallowListResult:
        """Retrieve expansion result, blocking if still in-flight."""
        self._collect()
        if prefix in self._ready:
            return self._ready.pop(prefix)
        if prefix not in self._inflight:
            self._inflight[prefix] = self._executor.submit(self._fn, prefix)
        return self._inflight.pop(prefix).result()

    def _fill(self) -> None:
        """Submit pending prefixes up to the inflight capacity."""
        while self._pending and len(self._inflight) < self._max_inflight:
            p = heapq.heappop(self._pending)
            if p not in self._inflight and p not in self._ready:
                self._inflight[p] = self._executor.submit(self._fn, p)

    def _collect(self) -> None:
        """Move completed futures to the ready dict and refill freed slots."""
        for p in list(self._inflight):
            if self._inflight[p].done():
                self._ready[p] = self._inflight.pop(p).result()
        self._fill()

    def __enter__(self) -> "_PrefixExpander":
        return self

    def __exit__(self, *exc: object) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)


_TELEMETRY_ATTRIBUTES_PROVIDER_MAPPING = {
    "environment_variables": "multistorageclient.telemetry.attributes.environment_variables.EnvironmentVariablesAttributesProvider",
    "host": "multistorageclient.telemetry.attributes.host.HostAttributesProvider",
    "msc_config": "multistorageclient.telemetry.attributes.msc_config.MSCConfigAttributesProvider",
    "process": "multistorageclient.telemetry.attributes.process.ProcessAttributesProvider",
    "static": "multistorageclient.telemetry.attributes.static.StaticAttributesProvider",
    "thread": "multistorageclient.telemetry.attributes.thread.ThreadAttributesProvider",
}

MAX_ASYNC_QUEUE_SIZE = 100_000

MiB = 1024 * 1024
DEFAULT_MULTIPART_THRESHOLD = 64 * MiB


class BaseStorageProvider(StorageProvider):
    """
    Base class for implementing a storage provider that manages object storage paths.

    This class abstracts the translation of paths so that private methods (_put_object, _get_object, etc.)
    always operate on full paths, not relative paths. This is achieved using a `base_path`, which is automatically
    prepended to all provided paths, making the code simpler and more consistent.
    """

    # Reserved attributes.
    class _AttributeName(Enum):
        VERSION = "multistorageclient.version"
        PROVIDER = "multistorageclient.provider"
        OPERATION = "multistorageclient.operation"
        STATUS = "multistorageclient.status"

    class _Operation(Enum):
        READ = "read"
        WRITE = "write"
        COPY = "copy"
        DELETE = "delete"
        INFO = "info"
        LIST = "list"
        DELETE_MANY = "delete_many"

    # Use as the namespace (i.e. prefix) for operation status types.
    class _Status(Enum):
        SUCCESS = "success"
        ERROR = "error"

    # Multi-Storage Client version.
    _VERSION = importlib_metadata.version("multi-storage-client")

    # Operations to emit data size metrics for on success.
    _DATA_IO_OPERATIONS = {_Operation.READ, _Operation.WRITE, _Operation.COPY}

    _base_path: str
    _provider_name: str

    _config_dict: Optional[dict[str, Any]]
    _telemetry_provider: Optional[Callable[[], Telemetry]]

    _metric_init_event: threading.Event
    _metric_gauges: dict[Telemetry.GaugeName, Optional[api_metrics._Gauge]]
    _metric_counters: dict[Telemetry.CounterName, Optional[api_metrics.Counter]]
    _metric_attributes_providers: Sequence[AttributesProvider]
    _metric_init_lock: threading.Lock

    def __init__(
        self,
        base_path: str,
        provider_name: str,
        config_dict: Optional[dict[str, Any]] = None,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
    ):
        self._base_path = base_path
        self._provider_name = provider_name
        self._multipart_threshold = DEFAULT_MULTIPART_THRESHOLD

        self._config_dict = config_dict
        self._telemetry_provider = telemetry_provider

        self._metric_init_event = threading.Event()
        self._metric_gauges = {}
        self._metric_counters = {}
        self._metric_attributes_providers = ()
        self._metric_init_lock = threading.Lock()

        # Async telemetry support
        self._async_metrics_enabled = False
        self._metrics_queue: Optional["queue.Queue[Optional[dict]]"] = None
        self._metrics_worker: Optional[threading.Thread] = None
        self._metrics_worker_shutdown = threading.Event()
        self._metrics_dropped_count = 0
        self._metrics_dropped_count_lock = threading.Lock()

    def __str__(self) -> str:
        return self._provider_name

    def __del__(self) -> None:
        """Destructor to ensure async telemetry is shutdown."""
        if getattr(self, "_async_metrics_enabled", False) and getattr(self, "_metrics_worker", None) is not None:
            try:
                self._shutdown_async_telemetry()
            except Exception as e:
                logger.warning(f"Failed to shutdown async telemetry: {e}", exc_info=True)

    def _init_metrics(self) -> None:
        """
        Initialize metrics.

        Multiprocessing unpickles during the Python interpreter's bootstrap phase for new processes.
        New processes (e.g. multiprocessing manager server) can't be created during this phase.

        The telemetry provider is a thunk to defer telemetry initialization. Evaluate the thunk
        and cache the instruments to avoid IPC and lock contention.
        """
        with self._metric_init_lock:
            if not self._metric_init_event.is_set():
                if self._config_dict is not None and self._telemetry_provider is not None:
                    opentelemetry_config: Optional[dict[str, Any]] = self._config_dict.get("opentelemetry")
                    if opentelemetry_config is not None:
                        try:
                            telemetry = self._telemetry_provider()

                            metrics_config: Optional[dict[str, Any]] = opentelemetry_config.get("metrics")

                            if metrics_config is not None:
                                for name in Telemetry.GaugeName:
                                    self._metric_gauges[name] = telemetry.gauge(config=metrics_config, name=name)
                                for name in Telemetry.CounterName:
                                    self._metric_counters[name] = telemetry.counter(config=metrics_config, name=name)

                                attributes_provider_configs: Optional[list[dict[str, Any]]] = metrics_config.get(
                                    "attributes"
                                )
                                if attributes_provider_configs is not None:
                                    attributes_providers: list[AttributesProvider] = []
                                    for config in attributes_provider_configs:
                                        attributes_provider_type: str = config["type"]
                                        attributes_provider_fully_qualified_name = (
                                            _TELEMETRY_ATTRIBUTES_PROVIDER_MAPPING.get(
                                                attributes_provider_type, attributes_provider_type
                                            )
                                        )
                                        attributes_provider_module_name, attributes_provider_class_name = (
                                            attributes_provider_fully_qualified_name.rsplit(".", 1)
                                        )
                                        cls = import_class(
                                            attributes_provider_class_name, attributes_provider_module_name
                                        )
                                        attributes_provider_options = config.get("options", {})
                                        if (
                                            attributes_provider_fully_qualified_name
                                            == _TELEMETRY_ATTRIBUTES_PROVIDER_MAPPING["msc_config"]
                                        ):
                                            attributes_provider_options["config_dict"] = self._config_dict
                                        attributes_provider: AttributesProvider = cls(**attributes_provider_options)
                                        attributes_providers.append(attributes_provider)
                                    self._metric_attributes_providers = tuple(attributes_providers)

                                # Initialize async telemetry if enabled
                                reader_config: Optional[dict[str, Any]] = metrics_config.get("reader")

                                if reader_config is not None:
                                    self._async_metrics_enabled = reader_config.get("async", False)

                                if self._async_metrics_enabled:
                                    self._init_async_telemetry()
                        except Exception as e:
                            logger.warning(f"Failed to setup metrics: {e}", exc_info=True)
                self._metric_init_event.set()

    def _init_async_telemetry(self) -> None:
        """Initialize async telemetry queue and worker thread."""
        self._metrics_queue = queue.Queue(maxsize=MAX_ASYNC_QUEUE_SIZE)

        self._metrics_worker = threading.Thread(
            target=self._metrics_worker_loop, name="MSC-Metrics-Worker", daemon=True
        )
        self._metrics_worker.start()

    def _shutdown_async_telemetry(self) -> None:
        """Shutdown async telemetry gracefully."""
        if self._metrics_queue is not None and self._metrics_worker is not None:
            self._metrics_worker_shutdown.set()

            try:
                self._metrics_queue.put(None, timeout=1.0)
            except queue.Full:
                logger.warning("Metrics queue full, skipping shutdown")

            self._metrics_worker.join(timeout=10.0)

            # Try to flush remaining metrics, but don't fail if telemetry is gone
            remaining_count = self._metrics_queue.qsize()
            if remaining_count > 0:
                while not self._metrics_queue.empty():
                    try:
                        metric = self._metrics_queue.get_nowait()
                        if metric is not None:
                            self._record_metric_sync(metric)
                    except queue.Empty:
                        break
                    except (EOFError, BrokenPipeError):
                        # Telemetry connection closed - stop trying to flush
                        logger.warning("Telemetry connection closed during shutdown, skipping remaining metrics")
                        break
                    except Exception as e:
                        logger.warning(f"Error flushing metric during shutdown: {e}")

            if self._metrics_dropped_count > 0:
                logger.warning(f"Dropped {self._metrics_dropped_count} metrics due to queue full")

    def _calculate_data_size(self, result: Any, operation: _Operation, error_type: Optional[str]) -> Optional[int]:
        """Calculate data size from operation result.

        :param result: The result from the operation
        :param operation: The operation type
        :param error_type: The type of error that occurred
        :return: Data size in bytes, or None if not applicable
        """
        if operation not in BaseStorageProvider._DATA_IO_OPERATIONS or error_type is not None:
            return None

        data_size: Optional[int] = None

        if isinstance(result, bytes):
            data_size = len(result)
        elif isinstance(result, str):
            data_size = len(result) if result.isascii() else len(result.encode())
        elif isinstance(result, int):
            data_size = result
        elif isinstance(result, list) and len(result) > 0:
            if isinstance(result[0], bytes):
                data_size = sum(len(item) for item in result)
            elif isinstance(result[0], str):
                data_size = sum(len(item) if item.isascii() else len(item.encode()) for item in result)

        return data_size

    def _build_base_attributes(self, operation: _Operation) -> api_types.Attributes:
        """Build base attributes for metrics.

        :param operation: The operation type
        :return: Attributes dictionary
        """
        return {
            **(collect_attributes(attributes_providers=self._metric_attributes_providers) or {}),
            BaseStorageProvider._AttributeName.VERSION.value: self._VERSION,
            BaseStorageProvider._AttributeName.PROVIDER.value: self._provider_name,
            BaseStorageProvider._AttributeName.OPERATION.value: operation.value,
        }

    def _build_status_attributes(
        self, base_attributes: api_types.Attributes, error_type: Optional[str]
    ) -> api_types.Attributes:
        """Build attributes with status information.

        :param base_attributes: Base attributes dictionary
        :param error_type: The type of error that occurred
        :return: Attributes dictionary with status
        """
        assert base_attributes is not None, "Base attributes must not be None"
        return {
            **base_attributes,
            BaseStorageProvider._AttributeName.STATUS.value: (
                BaseStorageProvider._Status.SUCCESS.value
                if error_type is None
                else f"{BaseStorageProvider._Status.ERROR.value}.{error_type}"
            ),
        }

    def _metrics_worker_loop(self) -> None:
        """Background thread that processes queued metrics."""
        assert self._metrics_queue is not None, "Metrics queue must be initialized"
        while not self._metrics_worker_shutdown.is_set():
            try:
                metric_data = self._metrics_queue.get(timeout=1.0)

                if metric_data is None:
                    break

                self._record_metric_sync(metric_data)
            except queue.Empty:
                continue
            except (EOFError, BrokenPipeError):
                # Telemetry manager connection closed - stop processing
                logger.warning("Telemetry connection closed, stopping metrics worker")
                break
            except Exception as e:
                logger.error(f"Error in metrics worker thread: {e}", exc_info=True)

    def _record_metrics(
        self,
        operation: _Operation,
        latency: float,
        data_size: Optional[int],
        error_type: Optional[str],
    ) -> None:
        """Record metrics for an operation.

        :param operation: The operation type
        :param latency: Operation latency in seconds
        :param data_size: Data size in bytes (if applicable)
        :param error_type: The type of error that occurred
        """
        metric_latency = self._metric_gauges.get(Telemetry.GaugeName.LATENCY)
        metric_data_size = self._metric_gauges.get(Telemetry.GaugeName.DATA_SIZE)
        metric_data_rate = self._metric_gauges.get(Telemetry.GaugeName.DATA_RATE)
        metric_request_sum = self._metric_counters.get(Telemetry.CounterName.REQUEST_SUM)
        metric_response_sum = self._metric_counters.get(Telemetry.CounterName.RESPONSE_SUM)
        metric_data_size_sum = self._metric_counters.get(Telemetry.CounterName.DATA_SIZE_SUM)

        attributes = self._build_base_attributes(operation)

        if metric_request_sum is not None:
            metric_request_sum.add(1, attributes=attributes)

        attributes_with_status = self._build_status_attributes(attributes, error_type)

        if metric_latency is not None:
            metric_latency.set(latency, attributes=attributes_with_status)
        if metric_response_sum is not None:
            metric_response_sum.add(1, attributes=attributes_with_status)

        if data_size is not None:
            if metric_data_size is not None:
                metric_data_size.set(data_size, attributes=attributes_with_status)
            if metric_data_rate is not None:
                metric_data_rate.set(data_size / latency, attributes=attributes_with_status)
            if metric_data_size_sum is not None:
                metric_data_size_sum.add(data_size, attributes=attributes_with_status)

    def _record_metric_sync(self, metric_data: dict) -> None:
        """Record a metric synchronously (called from worker thread)."""
        try:
            self._record_metrics(
                operation=metric_data["operation"],
                latency=metric_data["latency"],
                data_size=metric_data.get("data_size"),
                error_type=metric_data.get("error_type"),
            )
        except (EOFError, BrokenPipeError):
            # Telemetry manager connection closed - re-raise to stop worker
            raise
        except Exception as e:
            logger.warning(f"Failed to record metric: {e}")

    def _emit_metrics(self, operation: _Operation, f: Callable[[], _T]) -> _T:
        """
        Metric emission function wrapper.

        :param f: Function performing the operation. This should exclude result post-processing.
        :param operation: Operation being performed.
        :return: Function result.
        """
        # Check if metrics were initialized without locking to avoid lock contention post-initialization.
        if not self._metric_init_event.is_set():
            self._init_metrics()

        # Async path: queue metrics for background recording
        if self._async_metrics_enabled and self._metrics_queue is not None:
            return self._emit_metrics_async(operation, f)

        # Synchronous path: existing implementation
        return self._emit_metrics_sync(operation, f)

    def _emit_metrics_async(self, operation: _Operation, f: Callable[[], _T]) -> _T:
        """Async metric emission - queue for background processing."""
        error: Optional[Exception] = None
        result: _T = cast(_T, None)

        start_time = time.perf_counter()
        try:
            result = f()
            return result
        except Exception as e:
            error = e
            raise e
        finally:
            error_type = type(error).__name__ if error else None
            latency = time.perf_counter() - start_time
            data_size = self._calculate_data_size(result, operation, error_type)
            self._dispatch_metrics(operation, latency, data_size, error_type)

    def _emit_metrics_sync(self, operation: _Operation, f: Callable[[], _T]) -> _T:
        """Synchronous metric emission - original implementation."""
        error: Optional[Exception] = None
        result: _T = cast(_T, None)
        start_time = time.perf_counter()
        try:
            result = f()
            return result
        except Exception as e:
            error = e
            raise e
        finally:
            error_type = type(error).__name__ if error else None
            latency = time.perf_counter() - start_time
            data_size = self._calculate_data_size(result, operation, error_type)
            self._dispatch_metrics(operation, latency, data_size, error_type)

    def _dispatch_metrics(
        self,
        operation: _Operation,
        latency: float,
        data_size: Optional[int],
        error_type: Optional[str],
    ) -> None:
        """
        Dispatch pre-computed metrics via the appropriate path (sync or async queue).

        Unlike :meth:`_emit_metrics` which wraps a callable, this method accepts
        pre-computed metric values. Used by :meth:`_emit_metrics_sync`, :meth:`_emit_metrics_async`,
        and directly when the operation is performed outside the standard wrapper (e.g. async Rust downloads).
        """
        if self._async_metrics_enabled and self._metrics_queue is not None:
            metric_data = {
                "operation": operation,
                "latency": latency,
                "data_size": data_size,
                "error_type": error_type,
            }
            try:
                self._metrics_queue.put_nowait(metric_data)
            except queue.Full:
                with self._metrics_dropped_count_lock:
                    self._metrics_dropped_count += 1
        else:
            self._record_metrics(operation, latency, data_size, error_type)

    def _append_delimiter(self, s: str, delimiter: str = "/") -> str:
        if not s.endswith(delimiter):
            s += delimiter
        return s

    def _prepend_base_path(self, path: str) -> str:
        return os.path.join(self._base_path, path.lstrip("/"))

    def put_object(
        self,
        path: str,
        body: bytes,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
    ) -> None:
        path = self._prepend_base_path(path)
        self._emit_metrics(
            operation=BaseStorageProvider._Operation.WRITE,
            f=lambda: self._put_object(path, body, if_match, if_none_match, attributes),
        )

    def get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        path = self._prepend_base_path(path)
        return self._emit_metrics(
            operation=BaseStorageProvider._Operation.READ,
            f=lambda: self._get_object(path, byte_range),
        )

    def copy_object(self, src_path: str, dest_path: str) -> None:
        src_path = self._prepend_base_path(src_path)
        dest_path = self._prepend_base_path(dest_path)
        self._emit_metrics(
            operation=BaseStorageProvider._Operation.COPY,
            f=lambda: self._copy_object(src_path, dest_path),
        )

    def delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        """
        Deletes an object from the storage provider.

        :param path: The path of the object to delete.
        :param if_match: Optional if-match value to use for conditional deletion.
        :raises FileNotFoundError: If the object does not exist.
        :raises RuntimeError: If deletion fails.
        :raises PreconditionFailedError: If the if_match condition is not met.
        """
        path = self._prepend_base_path(path)
        self._emit_metrics(
            operation=BaseStorageProvider._Operation.DELETE,
            f=lambda: self._delete_object(path, if_match),
        )

    def delete_objects(self, paths: list[str]) -> None:
        """
        Deletes multiple objects from the storage provider.

        Default implementation iterates through paths and deletes each object individually.
        Subclasses may override this to use bulk delete APIs for better performance.

        :param paths: A list of paths of objects to delete.
        """
        paths = [self._prepend_base_path(path) for path in paths]
        self._emit_metrics(
            operation=BaseStorageProvider._Operation.DELETE_MANY,
            f=lambda: self._delete_objects(paths),
        )

    def make_symlink(self, path: str, target: str) -> None:
        """
        Creates a symbolic link at ``path`` pointing to ``target``.

        Prepends :attr:`base_path` to both arguments, emits write metrics,
        and delegates to :meth:`_make_symlink`.

        :param path: Logical path where the symlink will be created.
        :param target: Logical path that the symlink points to.
        """
        path = self._prepend_base_path(path)
        target = self._prepend_base_path(target)
        self._emit_metrics(
            operation=BaseStorageProvider._Operation.WRITE,
            f=lambda: self._make_symlink(path, target),
        )

    def generate_presigned_url(
        self,
        path: str,
        *,
        method: str = "GET",
        signer_type: Optional[SignerType] = None,
        signer_options: Optional[dict[str, Any]] = None,
    ) -> str:
        path = self._prepend_base_path(path)
        return self._generate_presigned_url(path, method=method, signer_type=signer_type, signer_options=signer_options)

    def _generate_presigned_url(
        self,
        path: str,
        *,
        method: str = "GET",
        signer_type: Optional[SignerType] = None,
        signer_options: Optional[dict[str, Any]] = None,
    ) -> str:
        raise NotImplementedError(f"{type(self).__name__} does not support presigned URL generation.")

    def _delete_objects(self, paths: list[str]) -> None:
        """
        Deletes multiple objects from the storage provider.

        :param paths: A list of paths of objects to delete.
        """
        for path in paths:
            self._delete_object(path)

    def get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        path = self._prepend_base_path(path)
        metadata = self._emit_metrics(
            operation=BaseStorageProvider._Operation.INFO,
            f=lambda: self._get_object_metadata(path, strict=strict),
        )
        # Remove base_path from key
        metadata.key = metadata.key.removeprefix(self._base_path).lstrip("/")
        return metadata

    def list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        attribute_filter_expression: Optional[str] = None,
        show_attributes: bool = False,
        follow_symlinks: bool = True,
    ) -> Iterator[ObjectMetadata]:
        """
        Lists objects in the storage provider under the specified path.

        :param path: The path to list objects under. The path must be a valid file or subdirectory path, cannot be partial or just "prefix".
        :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
        :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.
        :param include_directories: Whether to include directories in the result. When ``True``, directories are returned alongside objects.
        :param attribute_filter_expression: The attribute filter expression to apply to the result.
        :param show_attributes: Whether to return attributes in the result. There will be a performance impact if this is set to ``True`` as object metadata is fetched for each object.
        :param follow_symlinks: Whether to follow symbolic links. Only applicable for POSIX file storage providers. When ``False``, symlinks are skipped during listing.

        :return: An iterator over object metadata under the specified path.
        """
        if (start_after is not None) and (end_at is not None) and not (start_after < end_at):
            raise ValueError(f"start_after ({start_after}) must be before end_at ({end_at})!")

        # Prepend the base path to all the paths, the _list_objects method operates on full paths.
        path = self._prepend_base_path(path)

        # Cannot list objects from an empty base path.
        if path.strip() == "":
            raise ValueError(
                "The base_path cannot be empty when calling list_objects. Please provide a valid base_path in your configuration file."
            )

        start_after = self._prepend_base_path(start_after) if start_after else None
        end_at = self._prepend_base_path(end_at) if end_at else None

        # In version 0.33.0, we added the follow_symlinks parameter to the _list_objects method.
        # Fallback for custom storage providers that haven't been updated yet
        try:
            objects = self._emit_metrics(
                operation=BaseStorageProvider._Operation.LIST,
                f=lambda: self._list_objects(
                    path, start_after, end_at, include_directories, follow_symlinks=follow_symlinks
                ),
            )
        except TypeError as exc:
            if "follow_symlinks" in str(exc):
                logger.debug(
                    "We added the follow_symlinks parameter to the _list_objects method in version 0.33.0, please update your provider's interface to support it."
                )
                objects = self._emit_metrics(
                    operation=BaseStorageProvider._Operation.LIST,
                    f=lambda: self._list_objects(path, start_after, end_at, include_directories),
                )
            else:
                raise

        # When attribute_filter_expression or show_attributes is set, get full metadata per object and apply attribute filter
        evaluator = (
            create_attribute_filter_evaluator(attribute_filter_expression) if attribute_filter_expression else None
        )
        for obj in objects:
            if self._base_path:
                obj.key = obj.key.removeprefix(self._base_path).lstrip("/")

            if attribute_filter_expression or show_attributes:
                obj_metadata = None
                try:
                    obj_metadata = self.get_object_metadata(obj.key)
                except Exception as e:
                    logger.debug(
                        f"While listing objects, failed to get object metadata for {obj.key}: {e}, skipping the object"
                    )
                if obj_metadata and matches_attribute_filter_expression(
                    obj_metadata, evaluator
                ):  # if evaluator is None then it will always return True
                    yield obj_metadata
                else:
                    continue
            else:
                yield obj

    def list_objects_recursive(
        self,
        path: str = "",
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        max_workers: int = 32,
        look_ahead: int = 2,
        follow_symlinks: bool = True,
    ) -> Iterator[ObjectMetadata]:
        """
        List all objects recursively with parallel prefix discovery.

        Returns files only (no directories) in lexicographic order.
        Falls back to sequential listing if provider does not support parallel listing.

        :param path: The path to list objects under.
        :param start_after: Start listing after this key (exclusive).
        :param end_at: Stop listing at this key (inclusive).
        :param max_workers: Maximum concurrent listing threads.
        :param look_ahead: Prefixes to buffer ahead per worker (bounds memory, heap algorithm only).
        :param follow_symlinks: Whether to follow symbolic links (POSIX providers only).
        :return: Iterator of ObjectMetadata for files under the path.
        """
        if (start_after is not None) and (end_at is not None) and not (start_after < end_at):
            raise ValueError(f"start_after ({start_after}) must be before end_at ({end_at})!")

        path = self._prepend_base_path(path)

        if path.strip() == "":
            raise ValueError(
                "The base_path cannot be empty when calling list_objects_recursive. "
                "Please provide a valid base_path in your configuration file."
            )

        start_after = self._prepend_base_path(start_after) if start_after else None
        end_at = self._prepend_base_path(end_at) if end_at else None

        objects = self._emit_metrics(
            operation=BaseStorageProvider._Operation.LIST,
            f=lambda: self._list_objects_recursive_impl(
                path, max_workers, look_ahead, start_after, end_at, follow_symlinks
            ),
        )

        for obj in objects:
            if self._base_path:
                obj.key = obj.key.removeprefix(self._base_path).lstrip("/")
            yield obj

    @property
    def supports_parallel_listing(self) -> bool:
        """
        Whether this provider supports parallel recursive listing.

        Providers with delimiter-based shallow listing (S3, GCS) should override
        this to return True. Objects returned by _list_objects within a single
        shallow listing (one prefix level) must be in lexicographic order by key.
        The heap algorithm handles global ordering across prefixes.
        When ``False``, list_objects_recursive falls back to sequential listing.

        :return: ``True`` if parallel listing is supported.
        """
        return False

    def _shallow_list(self, path: str, follow_symlinks: bool) -> tuple[list[str], list[ObjectMetadata]]:
        """
        Perform shallow listing and separate into prefixes and objects.

        :param path: Full path (including base_path) to list.
        :return: Tuple of (child_prefixes, objects_at_this_level).
        """
        prefixes: list[str] = []
        objects: list[ObjectMetadata] = []

        for item in self._list_objects(path, include_directories=True, follow_symlinks=follow_symlinks):
            if item.type == "directory":
                child_prefix = item.key + "/"
                if child_prefix != path:
                    prefixes.append(child_prefix)
            else:
                objects.append(item)

        return prefixes, objects

    def _list_objects_recursive_impl(
        self,
        path: str,
        max_workers: int,
        look_ahead: int,
        start_after: Optional[str],
        end_at: Optional[str],
        follow_symlinks: bool,
    ) -> Iterator[ObjectMetadata]:
        """
        Internal implementation of recursive listing with bounded parallelism.

        Uses a min-heap to yield objects in lexicographic order while a
        _PrefixExpander discovers child prefixes in background threads.
        """
        if not self.supports_parallel_listing:
            yield from self._list_objects(
                path, start_after, end_at, include_directories=False, follow_symlinks=follow_symlinks
            )
            return

        prefixes, objects = self._shallow_list(path, follow_symlinks)

        # Prune prefixes entirely beyond end_at to avoid wasted API calls.
        if end_at:
            prefixes = [p for p in prefixes if p <= end_at]

        if not prefixes:
            for obj in objects:
                if start_after and obj.key <= start_after:
                    continue
                if end_at and obj.key > end_at:
                    break
                yield obj
            return

        # Seed the merge heap with initial prefixes and objects. O(N) via heapify.
        heap: list[_ListingHeapItem] = [_ListingHeapItem(p, True, p) for p in prefixes]
        heap.extend(_ListingHeapItem(o.key, False, o) for o in objects)
        heapq.heapify(heap)

        with _PrefixExpander(lambda p: self._shallow_list(p, follow_symlinks), max_workers, look_ahead) as expander:
            expander.enqueue(prefixes)

            while heap:
                entry = heap[0]

                # Heap is sorted: if the smallest key exceeds end_at, we're done.
                if end_at and entry.key > end_at:
                    break

                if not entry.is_prefix:
                    heapq.heappop(heap)
                    obj = cast(ObjectMetadata, entry.data)
                    if start_after and obj.key <= start_after:
                        continue
                    yield obj
                else:
                    child_prefixes, child_objects = expander.get(entry.key)
                    heapq.heappop(heap)

                    # Prune child prefixes beyond end_at.
                    if end_at:
                        child_prefixes = [cp for cp in child_prefixes if cp <= end_at]

                    for cp in child_prefixes:
                        heapq.heappush(heap, _ListingHeapItem(cp, True, cp))

                    if child_prefixes:
                        # Non-leaf: push objects to heap to interleave with sub-prefixes.
                        for co in child_objects:
                            heapq.heappush(heap, _ListingHeapItem(co.key, False, co))
                        expander.enqueue(child_prefixes)
                    else:
                        # Leaf prefix: yield sorted objects directly, skip heap overhead.
                        for obj in child_objects:
                            if start_after and obj.key <= start_after:
                                continue
                            if end_at and obj.key > end_at:
                                break
                            yield obj

    def upload_file(self, remote_path: str, f: Union[str, IO], attributes: Optional[dict[str, str]] = None) -> None:
        remote_path = self._prepend_base_path(remote_path)
        self._emit_metrics(
            operation=BaseStorageProvider._Operation.WRITE,
            f=lambda: self._upload_file(remote_path, f, attributes),
        )

    def download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        remote_path = self._prepend_base_path(remote_path)
        self._emit_metrics(
            operation=BaseStorageProvider._Operation.READ,
            f=lambda: self._download_file(remote_path, f, metadata),
        )

    def download_files(
        self,
        remote_paths: list[str],
        local_paths: list[str],
        metadata: Optional[Sequence[Optional[ObjectMetadata]]] = None,
        max_workers: int = 16,
    ) -> None:
        if len(remote_paths) != len(local_paths):
            raise ValueError("remote_paths and local_paths must have the same length")

        if metadata is not None and len(metadata) != len(remote_paths):
            raise ValueError("metadata must have the same length as remote_paths and local_paths")

        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")

        if not remote_paths:
            return

        resolved_metadata = self._resolve_download_metadata(remote_paths, metadata, max_workers)

        rust_client = getattr(self, "_rust_client", None)
        if rust_client is not None:
            self._download_files_async(rust_client, remote_paths, local_paths, resolved_metadata, max_workers)
        else:
            self._download_files_threaded(remote_paths, local_paths, resolved_metadata, max_workers)

    def _resolve_download_metadata(
        self,
        remote_paths: list[str],
        metadata: Optional[Sequence[Optional[ObjectMetadata]]],
        max_workers: int,
    ) -> list[ObjectMetadata]:
        """Ensure every entry has metadata, fetching missing ones concurrently."""
        resolved: list[ObjectMetadata] = [None] * len(remote_paths)  # type: ignore[list-item]
        missing_indices: list[int] = []

        for i, remote_path in enumerate(remote_paths):
            meta = metadata[i] if metadata is not None else None
            if meta is not None:
                resolved[i] = meta
            else:
                missing_indices.append(i)

        if missing_indices:

            def _fetch(idx: int) -> tuple[int, ObjectMetadata]:
                full_path = self._prepend_base_path(remote_paths[idx])
                return idx, self._get_object_metadata(full_path)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for idx, fetched in executor.map(lambda i: _fetch(i), missing_indices):
                    resolved[idx] = fetched

        return resolved

    def _download_files_threaded(
        self,
        remote_paths: list[str],
        local_paths: list[str],
        metadata: Sequence[ObjectMetadata],
        max_workers: int,
    ) -> None:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.download_file, rp, lp, metadata[i])
                for i, (rp, lp) in enumerate(zip(remote_paths, local_paths))
            ]
            for future in as_completed(futures):
                future.result()

    def _download_files_async(
        self,
        rust_client: Any,
        remote_paths: list[str],
        local_paths: list[str],
        metadata: Sequence[ObjectMetadata],
        max_workers: int,
    ) -> None:
        if not self._metric_init_event.is_set():
            self._init_metrics()

        multipart_threshold = self._multipart_threshold

        items: list[tuple[str, str, bool]] = []
        for i, (remote_path, local_path) in enumerate(zip(remote_paths, local_paths)):
            full_path = self._prepend_base_path(remote_path)
            _, key = split_path(full_path)
            if os.path.dirname(local_path):
                safe_makedirs(os.path.dirname(local_path))
            use_multipart = metadata[i].content_length > multipart_threshold
            items.append((key, local_path, use_multipart))

        semaphore = asyncio.Semaphore(max_workers)

        async def _download_one(key: str, local_path: str, use_multipart: bool) -> None:
            error_type: Optional[str] = None
            data_size: Optional[int] = None
            async with semaphore:
                start_time = time.perf_counter()
                try:
                    if use_multipart:
                        data_size = await rust_client.download_multipart_to_file(key, local_path)
                    else:
                        data_size = await rust_client.download(key, local_path)
                except Exception as e:
                    error_type = type(e).__name__
                    raise
                finally:
                    latency = time.perf_counter() - start_time
                    self._dispatch_metrics(
                        operation=BaseStorageProvider._Operation.READ,
                        latency=latency,
                        data_size=data_size,
                        error_type=error_type,
                    )

        async def _download_all() -> None:
            results = await asyncio.gather(
                *[_download_one(key, lp, mp) for key, lp, mp in items],
                return_exceptions=True,
            )
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                logger.error("download_files: %d/%d downloads failed", len(errors), len(items))
                raise errors[0]

        run_coroutine_sync(_download_all)

    def upload_files(
        self,
        local_paths: list[str],
        remote_paths: list[str],
        attributes: Optional[Sequence[Optional[dict[str, str]]]] = None,
        max_workers: int = 16,
    ) -> None:
        if len(local_paths) != len(remote_paths):
            raise ValueError("local_paths and remote_paths must have the same length")

        if attributes is not None and len(attributes) != len(local_paths):
            raise ValueError("attributes must have the same length as local_paths and remote_paths")

        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")

        if not local_paths:
            return

        has_any_attributes = attributes is not None and any(a is not None for a in attributes)
        rust_client = getattr(self, "_rust_client", None)
        if rust_client is not None and not has_any_attributes:
            self._upload_files_async(rust_client, local_paths, remote_paths, max_workers)
        else:
            self._upload_files_threaded(local_paths, remote_paths, attributes, max_workers)

    def _upload_files_threaded(
        self,
        local_paths: list[str],
        remote_paths: list[str],
        attributes: Optional[Sequence[Optional[dict[str, str]]]] = None,
        max_workers: int = 16,
    ) -> None:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.upload_file, rp, lp, attributes[i] if attributes is not None else None)
                for i, (lp, rp) in enumerate(zip(local_paths, remote_paths))
            ]
            for future in as_completed(futures):
                future.result()

    def _upload_files_async(
        self, rust_client: Any, local_paths: list[str], remote_paths: list[str], max_workers: int
    ) -> None:
        if not self._metric_init_event.is_set():
            self._init_metrics()

        multipart_threshold = self._multipart_threshold

        # Build (local_path, object_key, use_multipart) tuples so each upload
        # knows its resolved key and whether it exceeds the multipart threshold.
        items: list[tuple[str, str, bool]] = []
        for local_path, remote_path in zip(local_paths, remote_paths):
            full_path = self._prepend_base_path(remote_path)
            _, key = split_path(full_path)
            file_size = os.path.getsize(local_path)
            items.append((local_path, key, file_size > multipart_threshold))

        semaphore = asyncio.Semaphore(max_workers)

        async def _upload_one(local_path: str, key: str, use_multipart: bool) -> None:
            error_type: Optional[str] = None
            data_size: Optional[int] = None
            async with semaphore:
                start_time = time.perf_counter()
                try:
                    if use_multipart:
                        data_size = await rust_client.upload_multipart_from_file(local_path, key)
                    else:
                        data_size = await rust_client.upload(local_path, key)
                except Exception as e:
                    error_type = type(e).__name__
                    raise
                finally:
                    latency = time.perf_counter() - start_time
                    self._dispatch_metrics(
                        operation=BaseStorageProvider._Operation.WRITE,
                        latency=latency,
                        data_size=data_size,
                        error_type=error_type,
                    )

        async def _upload_all() -> None:
            results = await asyncio.gather(
                *[_upload_one(lp, key, mp) for lp, key, mp in items],
                return_exceptions=True,
            )
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                logger.error("upload_files: %d/%d uploads failed", len(errors), len(items))
                raise errors[0]

        run_coroutine_sync(_upload_all)

    def glob(self, pattern: str, attribute_filter_expression: Optional[str] = None) -> list[str]:
        parent_dir = extract_prefix_from_glob(pattern)
        keys = [object.key for object in self.list_objects(path=parent_dir)]
        keys = insert_directories(keys)

        matched_keys = [key for key in glob(keys, pattern)]
        if attribute_filter_expression:
            evaluator = create_attribute_filter_evaluator(attribute_filter_expression)
            filtered_keys = []
            for key in matched_keys:
                obj_metadata = self.get_object_metadata(key)
                if matches_attribute_filter_expression(obj_metadata, evaluator):
                    filtered_keys.append(key)
            return filtered_keys
        else:
            return matched_keys

    def is_file(self, path: str) -> bool:
        try:
            metadata = self.get_object_metadata(path)
            return metadata.type == "file"
        except FileNotFoundError:
            return False

    @abstractmethod
    def _put_object(
        self,
        path: str,
        body: bytes,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
    ) -> int:
        """
        :return: Data size in bytes.
        """
        pass

    @abstractmethod
    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        pass

    @abstractmethod
    def _copy_object(self, src_path: str, dest_path: str) -> int:
        """
        :return: Data size in bytes.
        """
        pass

    @abstractmethod
    def _delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        """
        Deletes an object from the storage provider.

        :param path: The path of the object to delete.
        :param if_match: Optional if-match value to use for conditional deletion.
        :raises FileNotFoundError: If the object does not exist.
        :raises RuntimeError: If deletion fails.
        :raises PreconditionFailedError: If the if_match condition is not met.
        """
        pass

    @abstractmethod
    def _make_symlink(self, path: str, target: str) -> None:
        """
        Creates a symbolic link at ``path`` pointing to ``target``.

        :param path: Full physical path for the symlink.
        :param target: Full physical path that the symlink points to.
        """
        pass

    @abstractmethod
    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        pass

    @abstractmethod
    def _list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        follow_symlinks: bool = True,
    ) -> Iterator[ObjectMetadata]:
        """
        Lists objects in the storage provider under the specified path.

        :param path: The path to list objects under.
        :param start_after: The key to start after.
        :param end_at: The key to end at.
        :param include_directories: Whether to include directories in the result.
        :param follow_symlinks: Whether to follow symbolic links. Only applicable for POSIX file storage providers.
        """
        pass

    @abstractmethod
    def _upload_file(self, remote_path: str, f: Union[str, IO], attributes: Optional[dict[str, str]] = None) -> int:
        """
        :return: Data size in bytes.
        """
        pass

    @abstractmethod
    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> int:
        """
        :return: Data size in bytes.
        """
        pass
