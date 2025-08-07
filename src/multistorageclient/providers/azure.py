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

import io
import os
import tempfile
import time
from collections.abc import Callable, Iterator, Sequence, Sized
from typing import IO, Any, Optional, TypeVar, Union

import opentelemetry.metrics as api_metrics
from azure.core import MatchConditions
from azure.core.exceptions import AzureError, HttpResponseError
from azure.storage.blob import BlobPrefix, BlobServiceClient

from ..telemetry import Telemetry
from ..telemetry.attributes.base import AttributesProvider
from ..types import (
    AWARE_DATETIME_MIN,
    Credentials,
    CredentialsProvider,
    ObjectMetadata,
    PreconditionFailedError,
    Range,
)
from ..utils import split_path, validate_attributes
from .base import BaseStorageProvider

_T = TypeVar("_T")

PROVIDER = "azure"


class StaticAzureCredentialsProvider(CredentialsProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.CredentialsProvider` that provides static Azure credentials.
    """

    _connection: str

    def __init__(self, connection: str):
        """
        Initializes the :py:class:`StaticAzureCredentialsProvider` with the provided connection string.

        :param connection: The connection string for Azure Blob Storage authentication.
        """
        self._connection = connection

    def get_credentials(self) -> Credentials:
        return Credentials(
            access_key=self._connection,
            secret_key="",
            token=None,
            expiration=None,
        )

    def refresh_credentials(self) -> None:
        pass


class AzureBlobStorageProvider(BaseStorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with Azure Blob Storage.
    """

    def __init__(
        self,
        endpoint_url: str,
        base_path: str = "",
        credentials_provider: Optional[CredentialsProvider] = None,
        metric_counters: dict[Telemetry.CounterName, api_metrics.Counter] = {},
        metric_gauges: dict[Telemetry.GaugeName, api_metrics._Gauge] = {},
        metric_attributes_providers: Sequence[AttributesProvider] = (),
        **kwargs: dict[str, Any],
    ):
        """
        Initializes the :py:class:`AzureBlobStorageProvider` with the endpoint URL and optional credentials provider.

        :param endpoint_url: The Azure storage account URL.
        :param base_path: The root prefix path within the container where all operations will be scoped.
        :param credentials_provider: The provider to retrieve Azure credentials.
        :param metric_counters: Metric counters.
        :param metric_gauges: Metric gauges.
        :param metric_attributes_providers: Metric attributes providers.
        """
        super().__init__(
            base_path=base_path,
            provider_name=PROVIDER,
            metric_counters=metric_counters,
            metric_gauges=metric_gauges,
            metric_attributes_providers=metric_attributes_providers,
        )

        self._account_url = endpoint_url
        self._credentials_provider = credentials_provider
        # https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob#optional-configuration
        client_optional_configuration_keys = {
            "retry_total",
            "retry_connect",
            "retry_read",
            "retry_status",
            "connection_timeout",
            "read_timeout",
        }
        self._client_optional_configuration = {
            key: value for key, value in kwargs.items() if key in client_optional_configuration_keys
        }
        self._blob_service_client = self._create_blob_service_client()

    def _create_blob_service_client(self) -> BlobServiceClient:
        """
        Creates and configures the Azure BlobServiceClient using the current credentials.

        :return: The configured BlobServiceClient.
        """
        if self._credentials_provider:
            credentials = self._credentials_provider.get_credentials()
            return BlobServiceClient.from_connection_string(
                credentials.access_key, **self._client_optional_configuration
            )
        else:
            return BlobServiceClient(account_url=self._account_url, **self._client_optional_configuration)

    def _refresh_blob_service_client_if_needed(self) -> None:
        """
        Refreshes the BlobServiceClient if the current credentials are expired.
        """
        if self._credentials_provider:
            credentials = self._credentials_provider.get_credentials()
            if credentials.is_expired():
                self._credentials_provider.refresh_credentials()
                self._blob_service_client = self._create_blob_service_client()

    def _collect_metrics(
        self,
        func: Callable[[], _T],
        operation: str,
        container: str,
        blob: str,
        put_object_size: Optional[int] = None,
        get_object_size: Optional[int] = None,
    ) -> _T:
        """
        Collects and records performance metrics around Azure operations such as PUT, GET, DELETE, etc.

        This method wraps an Azure operation and measures the time it takes to complete, along with recording
        the size of the object if applicable. It handles errors like timeouts and client errors and ensures
        proper logging of duration and object size.

        :param func: The function that performs the actual GCS operation.
        :param operation: The type of operation being performed (e.g., "PUT", "GET", "DELETE").
        :param container: The name of the Azure container involved in the operation.
        :param blob: The name of the blob within the Azure container.
        :param put_object_size: The size of the object being uploaded, if applicable (for PUT operations).
        :param get_object_size: The size of the object being downloaded, if applicable (for GET operations).

        :return The result of the GCS operation, typically the return value of the `func` callable.
        """
        start_time = time.time()
        status_code = 200

        object_size = None
        if operation == "PUT":
            object_size = put_object_size
        elif operation == "GET" and get_object_size:
            object_size = get_object_size

        try:
            result = func()
            if operation == "GET" and object_size is None and isinstance(result, Sized):
                object_size = len(result)
            return result
        except HttpResponseError as error:
            status_code = error.status_code if error.status_code else -1
            error_info = f"status_code: {error.status_code}, reason: {error.reason}"
            if status_code == 404:
                raise FileNotFoundError(f"Object {container}/{blob} does not exist.")  # pylint: disable=raise-missing-from
            elif status_code == 412:
                # raised when If-Match or If-Modified fails
                raise PreconditionFailedError(
                    f"Failed to {operation} object(s) at {container}/{blob}. {error_info}"
                ) from error
            else:
                raise RuntimeError(f"Failed to {operation} object(s) at {container}/{blob}. {error_info}") from error
        except AzureError as error:
            status_code = -1
            error_info = f"message: {error.message}"
            raise RuntimeError(f"Failed to {operation} object(s) at {container}/{blob}. {error_info}") from error
        except Exception as error:
            status_code = -1
            raise RuntimeError(
                f"Failed to {operation} object(s) at {container}/{blob}. error_type: {type(error).__name__}, error: {error}"
            ) from error
        finally:
            elapsed_time = time.time() - start_time
            self._metric_helper.record_duration(
                elapsed_time,
                provider=self._provider_name,
                operation=operation,
                bucket=container,
                status_code=status_code,
            )
            if object_size:
                self._metric_helper.record_object_size(
                    object_size,
                    provider=self._provider_name,
                    operation=operation,
                    bucket=container,
                    status_code=status_code,
                )

    def _put_object(
        self,
        path: str,
        body: bytes,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
    ) -> int:
        """
        Uploads an object to Azure Blob Storage.

        :param path: The path to the object to upload.
        :param body: The content of the object to upload.
        :param if_match: Optional ETag to match against the object.
        :param if_none_match: Optional ETag to match against the object.
        :param attributes: Optional attributes to attach to the object.
        """
        container_name, blob_name = split_path(path)
        self._refresh_blob_service_client_if_needed()

        def _invoke_api() -> int:
            blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)

            kwargs = {
                "data": body,
                "overwrite": True,
            }

            validated_attributes = validate_attributes(attributes)
            if validated_attributes:
                kwargs["metadata"] = validated_attributes

            if if_match:
                kwargs["match_condition"] = MatchConditions.IfNotModified
                kwargs["etag"] = if_match

            if if_none_match:
                if if_none_match == "*":
                    raise NotImplementedError("if_none_match='*' is not supported for Azure")
                kwargs["match_condition"] = MatchConditions.IfModified
                kwargs["etag"] = if_none_match

            blob_client.upload_blob(**kwargs)

            return len(body)

        return self._collect_metrics(
            _invoke_api, operation="PUT", container=container_name, blob=blob_name, put_object_size=len(body)
        )

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        container_name, blob_name = split_path(path)
        self._refresh_blob_service_client_if_needed()

        def _invoke_api() -> bytes:
            blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            if byte_range:
                stream = blob_client.download_blob(offset=byte_range.offset, length=byte_range.size)
            else:
                stream = blob_client.download_blob()
            return stream.readall()

        return self._collect_metrics(_invoke_api, operation="GET", container=container_name, blob=blob_name)

    def _copy_object(self, src_path: str, dest_path: str) -> int:
        src_container, src_blob = split_path(src_path)
        dest_container, dest_blob = split_path(dest_path)
        self._refresh_blob_service_client_if_needed()

        src_object = self._get_object_metadata(src_path)

        def _invoke_api() -> int:
            src_blob_client = self._blob_service_client.get_blob_client(container=src_container, blob=src_blob)
            dest_blob_client = self._blob_service_client.get_blob_client(container=dest_container, blob=dest_blob)
            dest_blob_client.start_copy_from_url(src_blob_client.url)

            return src_object.content_length

        return self._collect_metrics(
            _invoke_api,
            operation="COPY",
            container=src_container,
            blob=src_blob,
            put_object_size=src_object.content_length,
        )

    def _delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        container_name, blob_name = split_path(path)
        self._refresh_blob_service_client_if_needed()

        def _invoke_api() -> None:
            blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            # If if_match is provided, use if_match for conditional deletion
            if if_match:
                blob_client.delete_blob(etag=if_match, match_condition=MatchConditions.IfNotModified)
            else:
                # No if_match provided, perform unconditional deletion
                blob_client.delete_blob()

        return self._collect_metrics(_invoke_api, operation="DELETE", container=container_name, blob=blob_name)

    def _is_dir(self, path: str) -> bool:
        # Ensure the path ends with '/' to mimic a directory
        path = self._append_delimiter(path)

        container_name, prefix = split_path(path)
        self._refresh_blob_service_client_if_needed()

        def _invoke_api() -> bool:
            # List objects with the given prefix
            container_client = self._blob_service_client.get_container_client(container=container_name)
            blobs = container_client.walk_blobs(name_starts_with=prefix, delimiter="/")
            # Check if there are any contents or common prefixes
            return any(True for _ in blobs)

        return self._collect_metrics(_invoke_api, operation="LIST", container=container_name, blob=prefix)

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        container_name, blob_name = split_path(path)
        if path.endswith("/") or (container_name and not blob_name):
            # If path ends with "/" or empty blob name is provided, then assume it's a "directory",
            # which metadata is not guaranteed to exist for cases such as
            # "virtual prefix" that was never explicitly created.
            if self._is_dir(path):
                return ObjectMetadata(
                    key=self._append_delimiter(path),
                    type="directory",
                    content_length=0,
                    last_modified=AWARE_DATETIME_MIN,
                )
            else:
                raise FileNotFoundError(f"Directory {path} does not exist.")
        else:
            self._refresh_blob_service_client_if_needed()

            def _invoke_api() -> ObjectMetadata:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                properties = blob_client.get_blob_properties()
                return ObjectMetadata(
                    key=path,
                    content_length=properties.size,
                    content_type=properties.content_settings.content_type,
                    last_modified=properties.last_modified,
                    etag=properties.etag.strip('"') if properties.etag else "",
                    metadata=dict(properties.metadata) if properties.metadata else None,
                )

            try:
                return self._collect_metrics(_invoke_api, operation="HEAD", container=container_name, blob=blob_name)
            except FileNotFoundError as error:
                if strict:
                    # If the object does not exist on the given path, we will append a trailing slash and
                    # check if the path is a directory.
                    path = self._append_delimiter(path)
                    if self._is_dir(path):
                        return ObjectMetadata(
                            key=path,
                            type="directory",
                            content_length=0,
                            last_modified=AWARE_DATETIME_MIN,
                        )
                raise error

    def _list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
        container_name, prefix = split_path(path)
        self._refresh_blob_service_client_if_needed()

        def _invoke_api() -> Iterator[ObjectMetadata]:
            container_client = self._blob_service_client.get_container_client(container=container_name)
            # Azure has no start key option like other object stores.
            if include_directories:
                blobs = container_client.walk_blobs(name_starts_with=prefix, delimiter="/")
            else:
                blobs = container_client.list_blobs(name_starts_with=prefix)
            # Azure guarantees lexicographical order.
            for blob in blobs:
                if isinstance(blob, BlobPrefix):
                    yield ObjectMetadata(
                        key=os.path.join(container_name, blob.name.rstrip("/")),
                        type="directory",
                        content_length=0,
                        last_modified=AWARE_DATETIME_MIN,
                    )
                else:
                    key = blob.name
                    if (start_after is None or start_after < key) and (end_at is None or key <= end_at):
                        if key.endswith("/"):
                            if include_directories:
                                yield ObjectMetadata(
                                    key=os.path.join(container_name, key.rstrip("/")),
                                    type="directory",
                                    content_length=0,
                                    last_modified=blob.last_modified,
                                )
                        else:
                            yield ObjectMetadata(
                                key=os.path.join(container_name, key),
                                content_length=blob.size,
                                content_type=blob.content_settings.content_type,
                                last_modified=blob.last_modified,
                                etag=blob.etag.strip('"') if blob.etag else "",
                            )
                    elif end_at is not None and end_at < key:
                        return

        return self._collect_metrics(_invoke_api, operation="LIST", container=container_name, blob=prefix)

    def _upload_file(self, remote_path: str, f: Union[str, IO], attributes: Optional[dict[str, str]] = None) -> int:
        container_name, blob_name = split_path(remote_path)
        file_size: int = 0
        self._refresh_blob_service_client_if_needed()

        validated_attributes = validate_attributes(attributes)
        if isinstance(f, str):
            file_size = os.path.getsize(f)

            def _invoke_api() -> int:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                with open(f, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True, metadata=validated_attributes or {})

                return file_size

            return self._collect_metrics(
                _invoke_api, operation="PUT", container=container_name, blob=blob_name, put_object_size=file_size
            )
        else:
            # Convert StringIO to BytesIO before upload
            if isinstance(f, io.StringIO):
                fp: IO = io.BytesIO(f.getvalue().encode("utf-8"))  # type: ignore
            else:
                fp = f

            fp.seek(0, io.SEEK_END)
            file_size = fp.tell()
            fp.seek(0)

            def _invoke_api() -> int:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                blob_client.upload_blob(fp, overwrite=True, metadata=validated_attributes or {})

                return file_size

            return self._collect_metrics(
                _invoke_api, operation="PUT", container=container_name, blob=blob_name, put_object_size=file_size
            )

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> int:
        if metadata is None:
            metadata = self._get_object_metadata(remote_path)

        container_name, blob_name = split_path(remote_path)
        self._refresh_blob_service_client_if_needed()

        if isinstance(f, str):
            if os.path.dirname(f):
                os.makedirs(os.path.dirname(f), exist_ok=True)

            def _invoke_api() -> int:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                    temp_file_path = fp.name
                    stream = blob_client.download_blob()
                    fp.write(stream.readall())
                os.rename(src=temp_file_path, dst=f)

                return metadata.content_length

            return self._collect_metrics(
                _invoke_api,
                operation="GET",
                container=container_name,
                blob=blob_name,
                get_object_size=metadata.content_length,
            )
        else:

            def _invoke_api() -> int:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                stream = blob_client.download_blob()
                if isinstance(f, io.StringIO):
                    f.write(stream.readall().decode("utf-8"))
                else:
                    f.write(stream.readall())

                return metadata.content_length

            return self._collect_metrics(
                _invoke_api,
                operation="GET",
                container=container_name,
                blob=blob_name,
                get_object_size=metadata.content_length,
            )
