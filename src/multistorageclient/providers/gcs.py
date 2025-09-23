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

import codecs
import io
import logging
import os
import tempfile
import time
from collections.abc import Callable, Iterator, Sequence, Sized
from typing import IO, Any, Optional, TypeVar, Union

import opentelemetry.metrics as api_metrics
from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.auth import identity_pool
from google.cloud import storage
from google.cloud.storage import transfer_manager
from google.cloud.storage.exceptions import InvalidResponse
from google.oauth2.credentials import Credentials as OAuth2Credentials

from multistorageclient_rust import RustClient, RustRetryableError

from ..rust_utils import run_async_rust_client_method
from ..telemetry import Telemetry
from ..telemetry.attributes.base import AttributesProvider
from ..types import (
    AWARE_DATETIME_MIN,
    Credentials,
    CredentialsProvider,
    NotModifiedError,
    ObjectMetadata,
    PreconditionFailedError,
    Range,
    RetryableError,
)
from ..utils import (
    split_path,
    validate_attributes,
)
from .base import BaseStorageProvider

_T = TypeVar("_T")

PROVIDER = "gcs"

MiB = 1024 * 1024

DEFAULT_MULTIPART_THRESHOLD = 64 * MiB
DEFAULT_MULTIPART_CHUNKSIZE = 32 * MiB
DEFAULT_IO_CHUNKSIZE = 32 * MiB
PYTHON_MAX_CONCURRENCY = 8

logger = logging.getLogger(__name__)


class StringTokenSupplier(identity_pool.SubjectTokenSupplier):
    """
    Supply a string token to the Google Identity Pool.
    """

    def __init__(self, token: str):
        self._token = token

    def get_subject_token(self, context, request):
        return self._token


class GoogleIdentityPoolCredentialsProvider(CredentialsProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.CredentialsProvider` that provides Google's identity pool credentials.
    """

    def __init__(self, audience: str, token_supplier: str):
        """
        Initializes the :py:class:`GoogleIdentityPoolCredentials` with the audience and token supplier.

        :param audience: The audience for the Google Identity Pool.
        :param token_supplier: The token supplier for the Google Identity Pool.
        """
        self._audience = audience
        self._token_supplier = token_supplier

    def get_credentials(self) -> Credentials:
        return Credentials(
            access_key="",
            secret_key="",
            token="",
            expiration=None,
            custom_fields={"audience": self._audience, "token": self._token_supplier},
        )

    def refresh_credentials(self) -> None:
        pass


class GoogleStorageProvider(BaseStorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with Google Cloud Storage.
    """

    def __init__(
        self,
        project_id: str = os.getenv("GOOGLE_CLOUD_PROJECT_ID", ""),
        endpoint_url: str = "",
        base_path: str = "",
        credentials_provider: Optional[CredentialsProvider] = None,
        metric_counters: dict[Telemetry.CounterName, api_metrics.Counter] = {},
        metric_gauges: dict[Telemetry.GaugeName, api_metrics._Gauge] = {},
        metric_attributes_providers: Sequence[AttributesProvider] = (),
        **kwargs: Any,
    ):
        """
        Initializes the :py:class:`GoogleStorageProvider` with the project ID and optional credentials provider.

        :param project_id: The Google Cloud project ID.
        :param endpoint_url: The custom endpoint URL for the GCS service.
        :param base_path: The root prefix path within the bucket where all operations will be scoped.
        :param credentials_provider: The provider to retrieve GCS credentials.
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

        self._project_id = project_id
        self._endpoint_url = endpoint_url
        self._credentials_provider = credentials_provider
        self._gcs_client = self._create_gcs_client()
        self._multipart_threshold = kwargs.get("multipart_threshold", DEFAULT_MULTIPART_THRESHOLD)
        self._multipart_chunksize = kwargs.get("multipart_chunksize", DEFAULT_MULTIPART_CHUNKSIZE)
        self._io_chunksize = kwargs.get("io_chunksize", DEFAULT_IO_CHUNKSIZE)
        self._max_concurrency = kwargs.get("max_concurrency", PYTHON_MAX_CONCURRENCY)
        self._rust_client = None
        if "rust_client" in kwargs:
            # Inherit the rust client options from the kwargs
            rust_client_options = kwargs["rust_client"]
            if "max_concurrency" in kwargs:
                rust_client_options["max_concurrency"] = kwargs["max_concurrency"]
            if "multipart_chunksize" in kwargs:
                rust_client_options["multipart_chunksize"] = kwargs["multipart_chunksize"]
            if "read_timeout" in kwargs:
                rust_client_options["read_timeout"] = kwargs["read_timeout"]
            if "connect_timeout" in kwargs:
                rust_client_options["connect_timeout"] = kwargs["connect_timeout"]
            self._rust_client = self._create_rust_client(rust_client_options)

    def _create_gcs_client(self) -> storage.Client:
        client_options = {}
        if self._endpoint_url:
            client_options["api_endpoint"] = self._endpoint_url

        if self._credentials_provider:
            if isinstance(self._credentials_provider, GoogleIdentityPoolCredentialsProvider):
                audience = self._credentials_provider.get_credentials().get_custom_field("audience")
                token = self._credentials_provider.get_credentials().get_custom_field("token")

                # Use Workload Identity Federation (WIF)
                identity_pool_credentials = identity_pool.Credentials(
                    audience=audience,
                    subject_token_type="urn:ietf:params:oauth:token-type:id_token",
                    subject_token_supplier=StringTokenSupplier(token),
                )
                return storage.Client(
                    project=self._project_id, credentials=identity_pool_credentials, client_options=client_options
                )
            else:
                # Use OAuth 2.0 token
                token = self._credentials_provider.get_credentials().token
                creds = OAuth2Credentials(token=token)
                return storage.Client(project=self._project_id, credentials=creds, client_options=client_options)
        else:
            return storage.Client(project=self._project_id, client_options=client_options)

    def _create_rust_client(self, rust_client_options: Optional[dict[str, Any]] = None):
        configs = {}

        if self._credentials_provider or self._endpoint_url:
            # Workload Identity Federation (WIF) is not supported by the rust client:
            # https://github.com/apache/arrow-rs-object-store/issues/258
            logger.warning(
                "Rust client for GCS only supports Application Default Credentials or Service Account Key, skipping rust client"
            )
            return None

        # Use environment variables if available, could be overridden by rust_client_options
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            configs["application_credentials"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY"):
            configs["service_account_key"] = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
        if os.getenv("GOOGLE_SERVICE_ACCOUNT"):
            configs["service_account_path"] = os.getenv("GOOGLE_SERVICE_ACCOUNT")
        if os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH"):
            configs["service_account_path"] = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH")

        if rust_client_options:
            if "application_credentials" in rust_client_options:
                configs["application_credentials"] = rust_client_options["application_credentials"]
            if "service_account_key" in rust_client_options:
                configs["service_account_key"] = rust_client_options["service_account_key"]
            if "service_account_path" in rust_client_options:
                configs["service_account_path"] = rust_client_options["service_account_path"]
            if "skip_signature" in rust_client_options:
                configs["skip_signature"] = rust_client_options["skip_signature"]
            if "max_concurrency" in rust_client_options:
                configs["max_concurrency"] = rust_client_options["max_concurrency"]
            if "multipart_chunksize" in rust_client_options:
                configs["multipart_chunksize"] = rust_client_options["multipart_chunksize"]
            if "read_timeout" in rust_client_options:
                configs["read_timeout"] = rust_client_options["read_timeout"]
            if "connect_timeout" in rust_client_options:
                configs["connect_timeout"] = rust_client_options["connect_timeout"]

        if rust_client_options and "bucket" in rust_client_options:
            configs["bucket"] = rust_client_options["bucket"]
        else:
            bucket, _ = split_path(self._base_path)
            configs["bucket"] = bucket

        return RustClient(
            provider=PROVIDER,
            configs=configs,
        )

    def _refresh_gcs_client_if_needed(self) -> None:
        """
        Refreshes the GCS client if the current credentials are expired.
        """
        if self._credentials_provider:
            credentials = self._credentials_provider.get_credentials()
            if credentials.is_expired():
                self._credentials_provider.refresh_credentials()
                self._gcs_client = self._create_gcs_client()

    def _collect_metrics(
        self,
        func: Callable[[], _T],
        operation: str,
        bucket: str,
        key: str,
        put_object_size: Optional[int] = None,
        get_object_size: Optional[int] = None,
    ) -> _T:
        """
        Collects and records performance metrics around GCS operations such as PUT, GET, DELETE, etc.

        This method wraps an GCS operation and measures the time it takes to complete, along with recording
        the size of the object if applicable. It handles errors like timeouts and client errors and ensures
        proper logging of duration and object size.

        :param func: The function that performs the actual GCS operation.
        :param operation: The type of operation being performed (e.g., "PUT", "GET", "DELETE").
        :param bucket: The name of the GCS bucket involved in the operation.
        :param key: The key of the object within the GCS bucket.
        :param put_object_size: The size of the object being uploaded, if applicable (for PUT operations).
        :param get_object_size: The size of the object being downloaded, if applicable (for GET operations).

        :return: The result of the GCS operation, typically the return value of the `func` callable.
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
        except GoogleAPICallError as error:
            status_code = error.code if error.code else -1
            error_info = f"status_code: {status_code}, message: {error.message}"
            if status_code == 404:
                raise FileNotFoundError(f"Object {bucket}/{key} does not exist.")  # pylint: disable=raise-missing-from
            elif status_code == 412:
                raise PreconditionFailedError(
                    f"Failed to {operation} object(s) at {bucket}/{key}. {error_info}"
                ) from error
            elif status_code == 304:
                # for if_none_match with a specific etag condition.
                raise NotModifiedError(f"Object {bucket}/{key} has not been modified.") from error
            else:
                raise RuntimeError(f"Failed to {operation} object(s) at {bucket}/{key}. {error_info}") from error
        except InvalidResponse as error:
            status_code = error.response.status_code
            response_text = error.response.text
            error_details = f"error: {error}, error_response_text: {response_text}"
            # Check for NoSuchUpload within the response text
            if "NoSuchUpload" in response_text:
                raise RetryableError(f"Multipart upload failed for {bucket}/{key}, {error_details}") from error
            else:
                raise RuntimeError(f"Failed to {operation} object(s) at {bucket}/{key}. {error_details}") from error
        except RustRetryableError as error:
            status_code = -1
            raise RetryableError(
                f"Failed to {operation} object(s) at {bucket}/{key} due to exhausted retries from Rust. "
                f"error_type: {type(error).__name__}"
            ) from error
        except Exception as error:
            status_code = -1
            error_details = str(error)
            raise RuntimeError(
                f"Failed to {operation} object(s) at {bucket}/{key}. error_type: {type(error).__name__}, {error_details}"
            ) from error
        finally:
            elapsed_time = time.time() - start_time
            self._metric_helper.record_duration(
                elapsed_time, provider=self._provider_name, operation=operation, bucket=bucket, status_code=status_code
            )
            if object_size:
                self._metric_helper.record_object_size(
                    object_size,
                    provider=self._provider_name,
                    operation=operation,
                    bucket=bucket,
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
        Uploads an object to Google Cloud Storage.

        :param path: The path to the object to upload.
        :param body: The content of the object to upload.
        :param if_match: Optional ETag to match against the object.
        :param if_none_match: Optional ETag to match against the object.
        :param attributes: Optional attributes to attach to the object.
        """
        bucket, key = split_path(path)
        self._refresh_gcs_client_if_needed()

        def _invoke_api() -> int:
            bucket_obj = self._gcs_client.bucket(bucket)
            blob = bucket_obj.blob(key)

            kwargs = {}

            if if_match:
                kwargs["if_generation_match"] = int(if_match)  # 412 error code
            if if_none_match:
                if if_none_match == "*":
                    raise NotImplementedError("if_none_match='*' is not supported for GCS")
                else:
                    kwargs["if_generation_not_match"] = int(if_none_match)  # 304 error code

            validated_attributes = validate_attributes(attributes)
            if validated_attributes:
                blob.metadata = validated_attributes

            if (
                self._rust_client
                # Rust client doesn't support creating objects with trailing /, see https://github.com/apache/arrow-rs/issues/7026
                and not path.endswith("/")
                and not kwargs
                and not validated_attributes
            ):
                run_async_rust_client_method(self._rust_client, "put", key, body)
            else:
                blob.upload_from_string(body, **kwargs)

            return len(body)

        return self._collect_metrics(_invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=len(body))

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        bucket, key = split_path(path)
        self._refresh_gcs_client_if_needed()

        def _invoke_api() -> bytes:
            bucket_obj = self._gcs_client.bucket(bucket)
            blob = bucket_obj.blob(key)
            if byte_range:
                if self._rust_client:
                    return run_async_rust_client_method(
                        self._rust_client, "get", key, byte_range.offset, byte_range.offset + byte_range.size - 1
                    )
                else:
                    return blob.download_as_bytes(
                        start=byte_range.offset, end=byte_range.offset + byte_range.size - 1, single_shot_download=True
                    )
            else:
                if self._rust_client:
                    return run_async_rust_client_method(self._rust_client, "get", key)
                else:
                    return blob.download_as_bytes(single_shot_download=True)

        return self._collect_metrics(_invoke_api, operation="GET", bucket=bucket, key=key)

    def _copy_object(self, src_path: str, dest_path: str) -> int:
        src_bucket, src_key = split_path(src_path)
        dest_bucket, dest_key = split_path(dest_path)
        self._refresh_gcs_client_if_needed()

        src_object = self._get_object_metadata(src_path)

        def _invoke_api() -> int:
            source_bucket_obj = self._gcs_client.bucket(src_bucket)
            source_blob = source_bucket_obj.blob(src_key)

            destination_bucket_obj = self._gcs_client.bucket(dest_bucket)
            destination_blob = destination_bucket_obj.blob(dest_key)

            rewrite_tokens = [None]
            while len(rewrite_tokens) > 0:
                rewrite_token = rewrite_tokens.pop()
                next_rewrite_token, _, _ = destination_blob.rewrite(source=source_blob, token=rewrite_token)
                if next_rewrite_token is not None:
                    rewrite_tokens.append(next_rewrite_token)

            return src_object.content_length

        return self._collect_metrics(
            _invoke_api,
            operation="COPY",
            bucket=src_bucket,
            key=src_key,
            put_object_size=src_object.content_length,
        )

    def _delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        bucket, key = split_path(path)
        self._refresh_gcs_client_if_needed()

        def _invoke_api() -> None:
            bucket_obj = self._gcs_client.bucket(bucket)
            blob = bucket_obj.blob(key)

            # If if_match is provided, use it as a precondition
            if if_match:
                generation = int(if_match)
                blob.delete(if_generation_match=generation)
            else:
                # No if_match check needed, just delete
                blob.delete()

        return self._collect_metrics(_invoke_api, operation="DELETE", bucket=bucket, key=key)

    def _is_dir(self, path: str) -> bool:
        # Ensure the path ends with '/' to mimic a directory
        path = self._append_delimiter(path)

        bucket, key = split_path(path)
        self._refresh_gcs_client_if_needed()

        def _invoke_api() -> bool:
            bucket_obj = self._gcs_client.bucket(bucket)
            # List objects with the given prefix
            blobs = bucket_obj.list_blobs(
                prefix=key,
                delimiter="/",
            )
            # Check if there are any contents or common prefixes
            return any(True for _ in blobs) or any(True for _ in blobs.prefixes)

        return self._collect_metrics(_invoke_api, operation="LIST", bucket=bucket, key=key)

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        bucket, key = split_path(path)
        if path.endswith("/") or (bucket and not key):
            # If path ends with "/" or empty key name is provided, then assume it's a "directory",
            # which metadata is not guaranteed to exist for cases such as
            # "virtual prefix" that was never explicitly created.
            if self._is_dir(path):
                return ObjectMetadata(
                    key=path, type="directory", content_length=0, last_modified=AWARE_DATETIME_MIN, etag=None
                )
            else:
                raise FileNotFoundError(f"Directory {path} does not exist.")
        else:
            self._refresh_gcs_client_if_needed()

            def _invoke_api() -> ObjectMetadata:
                bucket_obj = self._gcs_client.bucket(bucket)
                blob = bucket_obj.get_blob(key)
                if not blob:
                    raise NotFound(f"Blob {key} not found in bucket {bucket}")
                return ObjectMetadata(
                    key=path,
                    content_length=blob.size or 0,
                    content_type=blob.content_type,
                    last_modified=blob.updated or AWARE_DATETIME_MIN,
                    etag=str(blob.generation),
                    metadata=dict(blob.metadata) if blob.metadata else None,
                )

            try:
                return self._collect_metrics(_invoke_api, operation="HEAD", bucket=bucket, key=key)
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
        bucket, prefix = split_path(path)
        self._refresh_gcs_client_if_needed()

        def _invoke_api() -> Iterator[ObjectMetadata]:
            bucket_obj = self._gcs_client.bucket(bucket)
            if include_directories:
                blobs = bucket_obj.list_blobs(
                    prefix=prefix,
                    # This is ≥ instead of >.
                    start_offset=start_after,
                    delimiter="/",
                )
            else:
                blobs = bucket_obj.list_blobs(
                    prefix=prefix,
                    # This is ≥ instead of >.
                    start_offset=start_after,
                )

            # GCS guarantees lexicographical order.
            for blob in blobs:
                key = blob.name
                if (start_after is None or start_after < key) and (end_at is None or key <= end_at):
                    if key.endswith("/"):
                        if include_directories:
                            yield ObjectMetadata(
                                key=os.path.join(bucket, key.rstrip("/")),
                                type="directory",
                                content_length=0,
                                last_modified=blob.updated,
                            )
                    else:
                        yield ObjectMetadata(
                            key=os.path.join(bucket, key),
                            content_length=blob.size,
                            content_type=blob.content_type,
                            last_modified=blob.updated,
                            etag=blob.etag,
                        )
                elif start_after != key:
                    return

            # The directories must be accessed last.
            if include_directories:
                for directory in blobs.prefixes:
                    yield ObjectMetadata(
                        key=os.path.join(bucket, directory.rstrip("/")),
                        type="directory",
                        content_length=0,
                        last_modified=AWARE_DATETIME_MIN,
                    )

        return self._collect_metrics(_invoke_api, operation="LIST", bucket=bucket, key=prefix)

    def _upload_file(self, remote_path: str, f: Union[str, IO], attributes: Optional[dict[str, str]] = None) -> int:
        bucket, key = split_path(remote_path)
        file_size: int = 0
        self._refresh_gcs_client_if_needed()

        if isinstance(f, str):
            file_size = os.path.getsize(f)

            # Upload small files
            if file_size <= self._multipart_threshold:
                if self._rust_client and not attributes:
                    run_async_rust_client_method(self._rust_client, "upload", f, key)
                else:
                    with open(f, "rb") as fp:
                        self._put_object(remote_path, fp.read(), attributes=attributes)
                return file_size

            # Upload large files using transfer manager
            def _invoke_api() -> int:
                if self._rust_client and not attributes:
                    run_async_rust_client_method(self._rust_client, "upload_multipart_from_file", f, key)
                else:
                    bucket_obj = self._gcs_client.bucket(bucket)
                    blob = bucket_obj.blob(key)
                    # GCS will raise an error if blob.metadata is None
                    validated_attributes = validate_attributes(attributes)
                    if validated_attributes is not None:
                        blob.metadata = validated_attributes
                    transfer_manager.upload_chunks_concurrently(
                        f,
                        blob,
                        chunk_size=self._multipart_chunksize,
                        max_workers=self._max_concurrency,
                        worker_type=transfer_manager.THREAD,
                    )

                return file_size

            return self._collect_metrics(
                _invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=file_size
            )
        else:
            f.seek(0, io.SEEK_END)
            file_size = f.tell()
            f.seek(0)

            # Upload small files
            if file_size <= self._multipart_threshold:
                if isinstance(f, io.StringIO):
                    self._put_object(remote_path, f.read().encode("utf-8"), attributes=attributes)
                else:
                    self._put_object(remote_path, f.read(), attributes=attributes)
                return file_size

            # Upload large files using transfer manager
            def _invoke_api() -> int:
                bucket_obj = self._gcs_client.bucket(bucket)
                blob = bucket_obj.blob(key)
                validated_attributes = validate_attributes(attributes)
                if validated_attributes:
                    blob.metadata = validated_attributes
                if isinstance(f, io.StringIO):
                    mode = "w"
                else:
                    mode = "wb"

                # transfer manager does not support uploading a file object
                with tempfile.NamedTemporaryFile(mode=mode, delete=False, prefix=".") as fp:
                    temp_file_path = fp.name
                    fp.write(f.read())

                transfer_manager.upload_chunks_concurrently(
                    temp_file_path,
                    blob,
                    chunk_size=self._multipart_chunksize,
                    max_workers=self._max_concurrency,
                    worker_type=transfer_manager.THREAD,
                )

                os.unlink(temp_file_path)

                return file_size

            return self._collect_metrics(
                _invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=file_size
            )

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> int:
        self._refresh_gcs_client_if_needed()

        if metadata is None:
            metadata = self._get_object_metadata(remote_path)

        bucket, key = split_path(remote_path)

        if isinstance(f, str):
            if os.path.dirname(f):
                os.makedirs(os.path.dirname(f), exist_ok=True)
            # Download small files
            if metadata.content_length <= self._multipart_threshold:
                if self._rust_client:
                    run_async_rust_client_method(self._rust_client, "download", key, f)
                else:
                    with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                        temp_file_path = fp.name
                        fp.write(self._get_object(remote_path))
                    os.rename(src=temp_file_path, dst=f)
                return metadata.content_length

            # Download large files using transfer manager
            def _invoke_api() -> int:
                bucket_obj = self._gcs_client.bucket(bucket)
                blob = bucket_obj.blob(key)
                if self._rust_client:
                    run_async_rust_client_method(self._rust_client, "download_multipart_to_file", key, f)
                else:
                    with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                        temp_file_path = fp.name
                        transfer_manager.download_chunks_concurrently(
                            blob,
                            temp_file_path,
                            chunk_size=self._io_chunksize,
                            max_workers=self._max_concurrency,
                            worker_type=transfer_manager.THREAD,
                        )
                    os.rename(src=temp_file_path, dst=f)

                return metadata.content_length

            return self._collect_metrics(
                _invoke_api, operation="GET", bucket=bucket, key=key, get_object_size=metadata.content_length
            )
        else:
            # Download small files
            if metadata.content_length <= self._multipart_threshold:
                response = self._get_object(remote_path)
                # Python client returns `bytes`, but Rust client returns a object implements buffer protocol,
                # so we need to check whether `.decode()` is available.
                if isinstance(f, io.StringIO):
                    if hasattr(response, "decode"):
                        f.write(response.decode("utf-8"))
                    else:
                        f.write(codecs.decode(memoryview(response), "utf-8"))
                else:
                    f.write(response)
                return metadata.content_length

            # Download large files using transfer manager
            def _invoke_api() -> int:
                bucket_obj = self._gcs_client.bucket(bucket)
                blob = bucket_obj.blob(key)

                # transfer manager does not support downloading to a file object
                with tempfile.NamedTemporaryFile(mode="wb", delete=False, prefix=".") as fp:
                    temp_file_path = fp.name
                    transfer_manager.download_chunks_concurrently(
                        blob,
                        temp_file_path,
                        chunk_size=self._io_chunksize,
                        max_workers=self._max_concurrency,
                        worker_type=transfer_manager.THREAD,
                    )

                if isinstance(f, io.StringIO):
                    with open(temp_file_path, "r") as fp:
                        f.write(fp.read())
                else:
                    with open(temp_file_path, "rb") as fp:
                        f.write(fp.read())

                os.unlink(temp_file_path)

                return metadata.content_length

            return self._collect_metrics(
                _invoke_api, operation="GET", bucket=bucket, key=key, get_object_size=metadata.content_length
            )
