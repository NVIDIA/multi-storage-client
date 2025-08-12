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

import importlib.metadata as importlib_metadata
import logging
import os
import time
from abc import abstractmethod
from collections.abc import Callable, Iterator, Sequence
from enum import Enum
from typing import IO, Optional, TypeVar, Union, cast

import opentelemetry.metrics as api_metrics
import opentelemetry.util.types as api_types

from ..instrumentation.utils import StorageProviderMetricsHelper, instrumented
from ..telemetry import Telemetry
from ..telemetry.attributes.base import AttributesProvider, collect_attributes
from ..types import ObjectMetadata, Range, StorageProvider
from ..utils import (
    create_attribute_filter_evaluator,
    extract_prefix_from_glob,
    glob,
    insert_directories,
    matches_attribute_filter_expression,
)

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


@instrumented
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
    _metric_gauges: dict[Telemetry.GaugeName, api_metrics._Gauge]
    _metric_counters: dict[Telemetry.CounterName, api_metrics.Counter]
    _metric_attributes_providers: Sequence[AttributesProvider]

    def __init__(
        self,
        base_path: str,
        provider_name: str,
        metric_gauges: dict[Telemetry.GaugeName, api_metrics._Gauge] = {},
        metric_counters: dict[Telemetry.CounterName, api_metrics.Counter] = {},
        metric_attributes_providers: Sequence[AttributesProvider] = (),
    ):
        self._base_path = base_path
        self._provider_name = provider_name

        self._metric_gauges = metric_gauges
        self._metric_counters = metric_counters
        self._metric_attributes_providers = metric_attributes_providers

        self._metric_helper = StorageProviderMetricsHelper()

    def __str__(self) -> str:
        return self._provider_name

    def _emit_metrics(self, operation: _Operation, f: Callable[[], _T]) -> _T:
        """
        Metric emission function wrapper.

        :param f: Function performing the operation. This should exclude result post-processing.
        :param operation: Operation being performed.
        :return: Function result.
        """
        metric_latency = self._metric_gauges.get(Telemetry.GaugeName.LATENCY)
        metric_data_size = self._metric_gauges.get(Telemetry.GaugeName.DATA_SIZE)
        metric_data_rate = self._metric_gauges.get(Telemetry.GaugeName.DATA_RATE)
        metric_request_sum = self._metric_counters.get(Telemetry.CounterName.REQUEST_SUM)
        metric_response_sum = self._metric_counters.get(Telemetry.CounterName.RESPONSE_SUM)
        metric_data_size_sum = self._metric_counters.get(Telemetry.CounterName.DATA_SIZE_SUM)

        attributes: api_types.Attributes = {
            **(collect_attributes(attributes_providers=self._metric_attributes_providers) or {}),
            BaseStorageProvider._AttributeName.VERSION.value: self._VERSION,
            BaseStorageProvider._AttributeName.PROVIDER.value: self._provider_name,
            BaseStorageProvider._AttributeName.OPERATION.value: operation.value,
        }

        if metric_request_sum is not None:
            metric_request_sum.add(1, attributes=attributes)

        error: Optional[Exception] = None
        # Make the type checker happy.
        result: _T = cast(_T, None)
        # Use a monotonic clock.
        start_time: float = time.perf_counter()
        try:
            result = f()
            return result
        except Exception as e:
            error = e
            raise e
        finally:
            latency: float = time.perf_counter() - start_time

            attributes_with_status: api_types.Attributes = {
                **(attributes or {}),
                BaseStorageProvider._AttributeName.STATUS.value: (
                    BaseStorageProvider._Status.SUCCESS.value
                    if error is None
                    else f"{BaseStorageProvider._Status.ERROR.value}.{type(error).__name__}"
                ),
            }

            if metric_latency is not None:
                metric_latency.set(latency, attributes=attributes_with_status)
            if metric_response_sum is not None:
                metric_response_sum.add(1, attributes=attributes_with_status)

            # Don't emit data size + rate metrics on failure.
            #
            # We don't know how much data was read/written before failure, so the resulting metrics may be misleading.
            if operation in BaseStorageProvider._DATA_IO_OPERATIONS and error is None:
                # Placeholder in case an unhandled data I/O operation is added.
                data_size: Optional[int] = None

                # For _get_object.
                if isinstance(result, bytes):
                    data_size = len(result)
                # For _put_object + _copy_object + _upload_file + _download_file (return the data size).
                elif isinstance(result, int):
                    data_size = result

                if data_size is not None:
                    if metric_data_size is not None:
                        metric_data_size.set(data_size, attributes=attributes_with_status)
                    if metric_data_rate is not None:
                        metric_data_rate.set(data_size / latency, attributes=attributes_with_status)
                    if metric_data_size_sum is not None:
                        metric_data_size_sum.add(data_size, attributes=attributes_with_status)

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
    ) -> Iterator[ObjectMetadata]:
        """
        Lists objects in the storage provider under the specified path.

        :param path: The path to list objects under. The path must be a valid file or subdirectory path, cannot be partial or just "prefix".
        :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
        :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.
        :param include_directories: Whether to include directories in the result. When True, directories are returned alongside objects.
        :param attribute_filter_expression: The attribute filter expression to apply to the result.
        :param show_attributes: Whether to return attributes in the result.  There will be performance impact if this is True as now we need to get object metadata for each object.

        :return: An iterator over objects metadata under the specified path.
        """
        if (start_after is not None) and (end_at is not None) and not (start_after < end_at):
            raise ValueError(f"start_after ({start_after}) must be before end_at ({end_at})!")

        path = self._prepend_base_path(path)
        objects = self._emit_metrics(
            operation=BaseStorageProvider._Operation.LIST,
            f=lambda: self._list_objects(path, start_after, end_at, include_directories),
        )

        # Filter objects based on attribute filter expression
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
    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        pass

    @abstractmethod
    def _list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
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
