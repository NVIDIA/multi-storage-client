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
from typing import IO, Any, Callable, Iterator, Optional, Union

import oci
from dateutil.parser import parse as dateutil_parser
from oci._vendor.requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    ContentDecodingError,
)
from oci.exceptions import ServiceError
from oci.object_storage import ObjectStorageClient, UploadManager
from oci.retry import DEFAULT_RETRY_STRATEGY

from ..types import (
    CredentialsProvider,
    ObjectMetadata,
    Range,
    RetryableError,
)
from ..utils import split_path
from .base import BaseStorageProvider

MB = 1024 * 1024

MULTIPART_THRESHOLD = 512 * MB
MULTIPART_CHUNK_SIZE = 256 * MB

PROVIDER = "oci"


class OracleStorageProvider(BaseStorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with
    Oracle Cloud Infrastructure (OCI) Object Storage.
    """

    def __init__(
        self,
        namespace: str,
        base_path: str = "",
        credentials_provider: Optional[CredentialsProvider] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes the :py:class:`OracleStorageProvider` with the region, compartment ID, and optional credentials provider.

        :param region_name: The OCI region where the Object Storage is located.
        :param compartment_id: The OCI compartment ID for the Object Storage.
        :param base_path: The root prefix path within the bucket where all operations will be scoped.
        :param credentials_provider: The provider to retrieve OCI credentials.
        """
        super().__init__(base_path=base_path, provider_name=PROVIDER)

        self._namespace = namespace
        self._credentials_provider = credentials_provider
        self._oci_client = self._create_oci_client()
        self._upload_manager = UploadManager(self._oci_client)
        self._multipart_threshold = int(kwargs.get("multipart_threshold", MULTIPART_THRESHOLD))
        self._multipart_chunk_size = int(kwargs.get("multipart_chunksize", MULTIPART_CHUNK_SIZE))

    def _create_oci_client(self) -> ObjectStorageClient:
        config = oci.config.from_file()
        return ObjectStorageClient(config, retry_strategy=DEFAULT_RETRY_STRATEGY)

    def _refresh_oci_client_if_needed(self) -> None:
        """
        Refreshes the OCI client if the current credentials are expired.
        """
        if self._credentials_provider:
            credentials = self._credentials_provider.get_credentials()
            if credentials.is_expired():
                self._credentials_provider.refresh_credentials()
                self._oci_client = self._create_oci_client()
                self._upload_manager = UploadManager(
                    self._oci_client, allow_parallel_uploads=True, parallel_process_count=4
                )

    def _collect_metrics(
        self,
        func: Callable,
        operation: str,
        bucket: str,
        key: str,
        put_object_size: Optional[int] = None,
        get_object_size: Optional[int] = None,
    ) -> Any:
        """
        Collects and records performance metrics around object storage operations such as PUT, GET, DELETE, etc.

        This method wraps an object storage operation and measures the time it takes to complete, along with recording
        the size of the object if applicable. It handles errors like timeouts and client errors and ensures
        proper logging of duration and object size.

        :param func: The function that performs the actual object storage operation.
        :param operation: The type of operation being performed (e.g., "PUT", "GET", "DELETE").
        :param bucket: The name of the object storage bucket involved in the operation.
        :param key: The key of the object within the object storage bucket.
        :param put_object_size: The size of the object being uploaded, if applicable (for PUT operations).
        :param get_object_size: The size of the object being downloaded, if applicable (for GET operations).

        :return: The result of the object storage operation, typically the return value of the `func` callable.
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
            if operation == "GET" and object_size is None:
                object_size = len(result)
            return result
        except ServiceError as error:
            status_code = error.status
            if status_code == 404:
                raise FileNotFoundError(f"Object {bucket}/{key} does not exist.")  # pylint: disable=raise-missing-from
            elif status_code == 429:
                raise RetryableError(f"Too many request to {operation} object(s) at {bucket}/{key}.") from error
            else:
                raise RuntimeError("Failed to {operation} object(s) at {bucket}/{key}") from error
        except (ConnectionError, ChunkedEncodingError, ContentDecodingError) as error:
            status_code = -1
            raise RetryableError(f"Failed to {operation} object(s) at {bucket}/{key}") from error
        except Exception as error:
            status_code = -1
            raise RuntimeError(f"Failed to {operation} object(s) at {bucket}/{key}") from error
        finally:
            elapsed_time = time.time() - start_time
            self._metric_helper.record_duration(
                elapsed_time, provider=PROVIDER, operation=operation, bucket=bucket, status_code=status_code
            )
            if object_size:
                self._metric_helper.record_object_size(
                    object_size, provider=PROVIDER, operation=operation, bucket=bucket, status_code=status_code
                )

    def _put_object(self, path: str, body: bytes) -> None:
        bucket, key = split_path(path)
        self._refresh_oci_client_if_needed()

        def _invoke_api() -> None:
            self._oci_client.put_object(
                namespace_name=self._namespace, bucket_name=bucket, object_name=key, put_object_body=body
            )

        return self._collect_metrics(_invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=len(body))

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        bucket, key = split_path(path)
        self._refresh_oci_client_if_needed()

        def _invoke_api() -> bytes:
            if byte_range:
                bytes_range = f"bytes={byte_range.offset}-{byte_range.offset + byte_range.size - 1}"
            else:
                bytes_range = None
            response = self._oci_client.get_object(
                namespace_name=self._namespace, bucket_name=bucket, object_name=key, range=bytes_range
            )
            return response.data.content  # pyright: ignore [reportOptionalMemberAccess]

        return self._collect_metrics(_invoke_api, operation="GET", bucket=bucket, key=key)

    def _delete_object(self, path: str) -> None:
        bucket, key = split_path(path)
        self._refresh_oci_client_if_needed()

        def _invoke_api() -> None:
            self._oci_client.delete_object(namespace_name=self._namespace, bucket_name=bucket, object_name=key)

        return self._collect_metrics(_invoke_api, operation="DELETE", bucket=bucket, key=key)

    def _get_object_metadata(self, path: str) -> ObjectMetadata:
        bucket, key = split_path(path)
        self._refresh_oci_client_if_needed()

        def _invoke_api() -> ObjectMetadata:
            response = self._oci_client.head_object(namespace_name=self._namespace, bucket_name=bucket, object_name=key)
            return ObjectMetadata(
                key=path,
                content_length=int(response.headers["Content-Length"]),  # pyright: ignore [reportOptionalMemberAccess]
                content_type=response.headers.get("Content-Type", None),  # pyright: ignore [reportOptionalMemberAccess]
                last_modified=dateutil_parser(response.headers["last-modified"]),  # pyright: ignore [reportOptionalMemberAccess]
                etag=response.headers.get("etag", None),  # pyright: ignore [reportOptionalMemberAccess]
            )

        return self._collect_metrics(_invoke_api, operation="HEAD", bucket=bucket, key=key)

    def _list_objects(
        self, prefix: str, start_after: Optional[str] = None, end_at: Optional[str] = None
    ) -> Iterator[ObjectMetadata]:
        bucket, prefix = split_path(prefix)
        self._refresh_oci_client_if_needed()

        def _invoke_api() -> Iterator[ObjectMetadata]:
            next_start_with: Optional[str] = start_after
            while True:
                response = self._oci_client.list_objects(
                    namespace_name=self._namespace,
                    bucket_name=bucket,
                    prefix=prefix,
                    # This is ≥ instead of >.
                    start=next_start_with,
                )
                # OCI guarantees lexicographical order.
                for response_object in response.data.objects:  # pyright: ignore [reportOptionalMemberAccess]
                    key = response_object.name
                    if (start_after is None or start_after < key) and (end_at is None or key <= end_at):
                        yield ObjectMetadata(
                            key=key,
                            content_length=response_object.size,
                            last_modified=response_object.time_modified,
                            etag=response_object.etag,
                        )
                    elif start_after != key:
                        return
                next_start_with = response.data.next_start_with  # pyright: ignore [reportOptionalMemberAccess]
                if next_start_with is None or (end_at is not None and end_at < next_start_with):
                    return

        return self._collect_metrics(_invoke_api, operation="LIST", bucket=bucket, key=prefix)

    def _upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        bucket, key = split_path(remote_path)
        self._refresh_oci_client_if_needed()

        if isinstance(f, str):
            filesize = os.path.getsize(f)

            def _invoke_api() -> None:
                if filesize > self._multipart_threshold:
                    self._upload_manager.upload_file(
                        namespace_name=self._namespace,
                        bucket_name=bucket,
                        object_name=key,
                        file_path=f,
                        part_size=self._multipart_chunk_size,
                        allow_parallel_uploads=True,
                    )
                else:
                    self._upload_manager.upload_file(
                        namespace_name=self._namespace, bucket_name=bucket, object_name=key, file_path=f
                    )

            return self._collect_metrics(_invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=filesize)
        else:
            # Convert file-like object to BytesIO because stream_ref cannot work with StringIO.
            if isinstance(f, io.StringIO):
                f = io.BytesIO(f.getvalue().encode("utf-8"))

            f.seek(0, io.SEEK_END)
            filesize = f.tell()
            f.seek(0)

            def _invoke_api() -> None:
                if filesize > self._multipart_threshold:
                    self._upload_manager.upload_stream(
                        namespace_name=self._namespace,
                        bucket_name=bucket,
                        object_name=key,
                        stream_ref=f,
                        part_size=self._multipart_chunk_size,
                        allow_parallel_uploads=True,
                    )
                else:
                    self._upload_manager.upload_stream(
                        namespace_name=self._namespace, bucket_name=bucket, object_name=key, stream_ref=f
                    )

            return self._collect_metrics(_invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=filesize)

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        self._refresh_oci_client_if_needed()

        if not metadata:
            metadata = self._get_object_metadata(remote_path)

        bucket, key = split_path(remote_path)

        if isinstance(f, str):
            os.makedirs(os.path.dirname(f), exist_ok=True)

            def _invoke_api() -> None:
                response = self._oci_client.get_object(
                    namespace_name=self._namespace, bucket_name=bucket, object_name=key
                )
                with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                    temp_file_path = fp.name
                    for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):  # pyright: ignore [reportOptionalMemberAccess]
                        fp.write(chunk)
                os.rename(src=temp_file_path, dst=f)

            return self._collect_metrics(
                _invoke_api, operation="GET", bucket=bucket, key=key, get_object_size=metadata.content_length
            )
        else:

            def _invoke_api() -> None:
                response = self._oci_client.get_object(
                    namespace_name=self._namespace, bucket_name=bucket, object_name=key
                )
                # Convert file-like object to BytesIO because stream_ref cannot work with StringIO.
                if isinstance(f, io.StringIO):
                    bytes_fileobj = io.BytesIO()
                    for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):  # pyright: ignore [reportOptionalMemberAccess]
                        bytes_fileobj.write(chunk)
                    f.write(bytes_fileobj.getvalue().decode("utf-8"))
                else:
                    for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):  # pyright: ignore [reportOptionalMemberAccess]
                        f.write(chunk)

            return self._collect_metrics(
                _invoke_api, operation="GET", bucket=bucket, key=key, get_object_size=metadata.content_length
            )
