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
import os
import tempfile
from collections.abc import Callable, Iterator
from typing import IO, Any, Optional, TypeVar, Union

import boto3
import botocore
from boto3.s3.transfer import TransferConfig
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import ClientError, IncompleteReadError, ReadTimeoutError, ResponseStreamingError
from botocore.session import get_session

from multistorageclient_rust import RustClient, RustClientError, RustRetryableError

from ..rust_utils import run_async_rust_client_method
from ..telemetry import Telemetry
from ..types import (
    AWARE_DATETIME_MIN,
    Credentials,
    CredentialsProvider,
    ObjectMetadata,
    PreconditionFailedError,
    Range,
    RetryableError,
)
from ..utils import (
    get_available_cpu_count,
    split_path,
    validate_attributes,
)
from .base import BaseStorageProvider

_T = TypeVar("_T")

# Default connection pool size scales with CPU count (minimum 32 for backward compatibility)
MAX_POOL_CONNECTIONS = max(32, get_available_cpu_count())

MiB = 1024 * 1024

# Python and Rust share the same multipart_threshold to keep the code simple.
MULTIPART_THRESHOLD = 64 * MiB
MULTIPART_CHUNKSIZE = 32 * MiB
IO_CHUNKSIZE = 32 * MiB
PYTHON_MAX_CONCURRENCY = 8

PROVIDER = "s3"

EXPRESS_ONEZONE_STORAGE_CLASS = "EXPRESS_ONEZONE"


class StaticS3CredentialsProvider(CredentialsProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.CredentialsProvider` that provides static S3 credentials.
    """

    _access_key: str
    _secret_key: str
    _session_token: Optional[str]

    def __init__(self, access_key: str, secret_key: str, session_token: Optional[str] = None):
        """
        Initializes the :py:class:`StaticS3CredentialsProvider` with the provided access key, secret key, and optional
        session token.

        :param access_key: The access key for S3 authentication.
        :param secret_key: The secret key for S3 authentication.
        :param session_token: An optional session token for temporary credentials.
        """
        self._access_key = access_key
        self._secret_key = secret_key
        self._session_token = session_token

    def get_credentials(self) -> Credentials:
        return Credentials(
            access_key=self._access_key,
            secret_key=self._secret_key,
            token=self._session_token,
            expiration=None,
        )

    def refresh_credentials(self) -> None:
        pass


class S3StorageProvider(BaseStorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with Amazon S3 or S3-compatible object stores.
    """

    def __init__(
        self,
        region_name: str = "",
        endpoint_url: str = "",
        base_path: str = "",
        credentials_provider: Optional[CredentialsProvider] = None,
        config_dict: Optional[dict[str, Any]] = None,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
        verify: Optional[Union[bool, str]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes the :py:class:`S3StorageProvider` with the region, endpoint URL, and optional credentials provider.

        :param region_name: The AWS region where the S3 bucket is located.
        :param endpoint_url: The custom endpoint URL for the S3 service.
        :param base_path: The root prefix path within the S3 bucket where all operations will be scoped.
        :param credentials_provider: The provider to retrieve S3 credentials.
        :param config_dict: Resolved MSC config.
        :param telemetry_provider: A function that provides a telemetry instance.
        :param verify: Controls SSL certificate verification. Can be ``True`` (verify using system CA bundle, default),
            ``False`` (skip verification), or a string path to a custom CA certificate bundle.
        """
        super().__init__(
            base_path=base_path,
            provider_name=PROVIDER,
            config_dict=config_dict,
            telemetry_provider=telemetry_provider,
        )

        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._credentials_provider = credentials_provider
        self._verify = verify

        self._signature_version = kwargs.get("signature_version", "")
        self._s3_client = self._create_s3_client(
            request_checksum_calculation=kwargs.get("request_checksum_calculation"),
            response_checksum_validation=kwargs.get("response_checksum_validation"),
            max_pool_connections=kwargs.get("max_pool_connections", MAX_POOL_CONNECTIONS),
            connect_timeout=kwargs.get("connect_timeout"),
            read_timeout=kwargs.get("read_timeout"),
            retries=kwargs.get("retries"),
        )
        self._transfer_config = TransferConfig(
            multipart_threshold=int(kwargs.get("multipart_threshold", MULTIPART_THRESHOLD)),
            max_concurrency=int(kwargs.get("max_concurrency", PYTHON_MAX_CONCURRENCY)),
            multipart_chunksize=int(kwargs.get("multipart_chunksize", MULTIPART_CHUNKSIZE)),
            io_chunksize=int(kwargs.get("io_chunksize", IO_CHUNKSIZE)),
            use_threads=True,
        )

        self._rust_client = None
        if "rust_client" in kwargs:
            # Inherit the rust client options from the kwargs
            rust_client_options = kwargs["rust_client"]
            if "max_pool_connections" in kwargs:
                rust_client_options["max_pool_connections"] = kwargs["max_pool_connections"]
            if "max_concurrency" in kwargs:
                rust_client_options["max_concurrency"] = kwargs["max_concurrency"]
            if "multipart_chunksize" in kwargs:
                rust_client_options["multipart_chunksize"] = kwargs["multipart_chunksize"]
            if "read_timeout" in kwargs:
                rust_client_options["read_timeout"] = kwargs["read_timeout"]
            if "connect_timeout" in kwargs:
                rust_client_options["connect_timeout"] = kwargs["connect_timeout"]
            if self._signature_version == "UNSIGNED":
                rust_client_options["skip_signature"] = True
            self._rust_client = self._create_rust_client(rust_client_options)

    def _is_directory_bucket(self, bucket: str) -> bool:
        """
        Determines if the bucket is a directory bucket based on bucket name.
        """
        # S3 Express buckets have a specific naming convention
        return "--x-s3" in bucket

    def _create_s3_client(
        self,
        request_checksum_calculation: Optional[str] = None,
        response_checksum_validation: Optional[str] = None,
        max_pool_connections: int = MAX_POOL_CONNECTIONS,
        connect_timeout: Union[float, int, None] = None,
        read_timeout: Union[float, int, None] = None,
        retries: Optional[dict[str, Any]] = None,
    ):
        """
        Creates and configures the boto3 S3 client, using refreshable credentials if possible.

        :param request_checksum_calculation: When the underlying S3 client should calculate request checksums. See the equivalent option in the `AWS configuration file <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file>`_.
        :param response_checksum_validation: When the underlying S3 client should validate response checksums. See the equivalent option in the `AWS configuration file <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file>`_.
        :param max_pool_connections: The maximum number of connections to keep in a connection pool.
        :param connect_timeout: The time in seconds till a timeout exception is thrown when attempting to make a connection.
        :param read_timeout: The time in seconds till a timeout exception is thrown when attempting to read from a connection.
        :param retries: A dictionary for configuration related to retry behavior.

        :return: The configured S3 client.
        """
        options = {
            # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
            "config": boto3.session.Config(  # pyright: ignore [reportAttributeAccessIssue]
                max_pool_connections=max_pool_connections,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                retries=retries or {"mode": "standard"},
                request_checksum_calculation=request_checksum_calculation,
                response_checksum_validation=response_checksum_validation,
            ),
        }

        if self._region_name:
            options["region_name"] = self._region_name

        if self._endpoint_url:
            options["endpoint_url"] = self._endpoint_url

        if self._verify is not None:
            options["verify"] = self._verify

        if self._credentials_provider:
            creds = self._fetch_credentials()
            if "expiry_time" in creds and creds["expiry_time"]:
                # Use RefreshableCredentials if expiry_time provided.
                refreshable_credentials = RefreshableCredentials.create_from_metadata(
                    metadata=creds, refresh_using=self._fetch_credentials, method="custom-refresh"
                )

                botocore_session = get_session()
                botocore_session._credentials = refreshable_credentials

                boto3_session = boto3.Session(botocore_session=botocore_session)

                return boto3_session.client("s3", **options)
            else:
                # Add static credentials to the options dictionary
                options["aws_access_key_id"] = creds["access_key"]
                options["aws_secret_access_key"] = creds["secret_key"]
                if creds["token"]:
                    options["aws_session_token"] = creds["token"]

        if self._signature_version:
            signature_config = botocore.config.Config(  # pyright: ignore[reportAttributeAccessIssue]
                signature_version=botocore.UNSIGNED
                if self._signature_version == "UNSIGNED"
                else self._signature_version
            )
            options["config"] = options["config"].merge(signature_config)

        # Fallback to standard credential chain.
        return boto3.client("s3", **options)

    def _create_rust_client(self, rust_client_options: Optional[dict[str, Any]] = None):
        """
        Creates and configures the rust client, using refreshable credentials if possible.
        """
        configs = dict(rust_client_options) if rust_client_options else {}

        if self._region_name and "region_name" not in configs:
            configs["region_name"] = self._region_name

        if self._endpoint_url and "endpoint_url" not in configs:
            configs["endpoint_url"] = self._endpoint_url

        # If the user specifies a bucket, use it. Otherwise, use the base path.
        if "bucket" not in configs:
            bucket, _ = split_path(self._base_path)
            configs["bucket"] = bucket

        if "max_pool_connections" not in configs:
            configs["max_pool_connections"] = MAX_POOL_CONNECTIONS

        return RustClient(
            provider=PROVIDER,
            configs=configs,
            credentials_provider=self._credentials_provider,
        )

    def _fetch_credentials(self) -> dict:
        """
        Refreshes the S3 client if the current credentials are expired.
        """
        if not self._credentials_provider:
            raise RuntimeError("Cannot fetch credentials if no credential provider configured.")
        self._credentials_provider.refresh_credentials()
        credentials = self._credentials_provider.get_credentials()
        return {
            "access_key": credentials.access_key,
            "secret_key": credentials.secret_key,
            "token": credentials.token,
            "expiry_time": credentials.expiration,
        }

    def _translate_errors(
        self,
        func: Callable[[], _T],
        operation: str,
        bucket: str,
        key: str,
    ) -> _T:
        """
        Translates errors like timeouts and client errors.

        :param func: The function that performs the actual S3 operation.
        :param operation: The type of operation being performed (e.g., "PUT", "GET", "DELETE").
        :param bucket: The name of the S3 bucket involved in the operation.
        :param key: The key of the object within the S3 bucket.

        :return: The result of the S3 operation, typically the return value of the `func` callable.
        """
        try:
            return func()
        except ClientError as error:
            status_code = error.response["ResponseMetadata"]["HTTPStatusCode"]
            request_id = error.response["ResponseMetadata"].get("RequestId")
            host_id = error.response["ResponseMetadata"].get("HostId")
            error_code = error.response["Error"]["Code"]
            error_info = f"request_id: {request_id}, host_id: {host_id}, status_code: {status_code}"

            if status_code == 404:
                if error_code == "NoSuchUpload":
                    error_message = error.response["Error"]["Message"]
                    raise RetryableError(f"Multipart upload failed for {bucket}/{key}: {error_message}") from error
                raise FileNotFoundError(f"Object {bucket}/{key} does not exist. {error_info}")  # pylint: disable=raise-missing-from
            elif status_code == 412:  # Precondition Failed
                raise PreconditionFailedError(
                    f"ETag mismatch for {operation} operation on {bucket}/{key}. {error_info}"
                ) from error
            elif status_code == 429:
                raise RetryableError(
                    f"Too many request to {operation} object(s) at {bucket}/{key}. {error_info}"
                ) from error
            elif status_code == 503:
                raise RetryableError(
                    f"Service unavailable when {operation} object(s) at {bucket}/{key}. {error_info}"
                ) from error
            elif status_code == 501:
                raise NotImplementedError(
                    f"Operation {operation} not implemented for object(s) at {bucket}/{key}. {error_info}"
                ) from error
            else:
                raise RuntimeError(
                    f"Failed to {operation} object(s) at {bucket}/{key}. {error_info}, "
                    f"error_type: {type(error).__name__}"
                ) from error
        except RustClientError as error:
            message = error.args[0]
            status_code = error.args[1]
            if status_code == 404:
                raise FileNotFoundError(f"Object {bucket}/{key} does not exist. {message}") from error
            elif status_code == 403:
                raise PermissionError(
                    f"Permission denied to {operation} object(s) at {bucket}/{key}. {message}"
                ) from error
            else:
                raise RetryableError(
                    f"Failed to {operation} object(s) at {bucket}/{key}. {message}. status_code: {status_code}"
                ) from error
        except (ReadTimeoutError, IncompleteReadError, ResponseStreamingError) as error:
            raise RetryableError(
                f"Failed to {operation} object(s) at {bucket}/{key} due to network timeout or incomplete read. "
                f"error_type: {type(error).__name__}"
            ) from error
        except RustRetryableError as error:
            raise RetryableError(
                f"Failed to {operation} object(s) at {bucket}/{key} due to retryable error from Rust. "
                f"error_type: {type(error).__name__}"
            ) from error
        except Exception as error:
            raise RuntimeError(
                f"Failed to {operation} object(s) at {bucket}/{key}, error type: {type(error).__name__}, error: {error}"
            ) from error

    def _put_object(
        self,
        path: str,
        body: bytes,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
        content_type: Optional[str] = None,
    ) -> int:
        """
        Uploads an object to the specified S3 path.

        :param path: The S3 path where the object will be uploaded.
        :param body: The content of the object as bytes.
        :param if_match: Optional If-Match header value. Use "*" to only upload if the object doesn't exist.
        :param if_none_match: Optional If-None-Match header value. Use "*" to only upload if the object doesn't exist.
        :param attributes: Optional attributes to attach to the object.
        :param content_type: Optional Content-Type header value.
        """
        bucket, key = split_path(path)

        def _invoke_api() -> int:
            kwargs = {"Bucket": bucket, "Key": key, "Body": body}
            if content_type:
                kwargs["ContentType"] = content_type
            if self._is_directory_bucket(bucket):
                kwargs["StorageClass"] = EXPRESS_ONEZONE_STORAGE_CLASS
            if if_match:
                kwargs["IfMatch"] = if_match
            if if_none_match:
                kwargs["IfNoneMatch"] = if_none_match
            validated_attributes = validate_attributes(attributes)
            if validated_attributes:
                kwargs["Metadata"] = validated_attributes

            # TODO(NGCDP-5804): Add support to update ContentType header in Rust client
            rust_unsupported_feature_keys = {"Metadata", "StorageClass", "IfMatch", "IfNoneMatch", "ContentType"}
            if (
                self._rust_client
                # Rust client doesn't support creating objects with trailing /, see https://github.com/apache/arrow-rs/issues/7026
                and not path.endswith("/")
                and all(key not in kwargs for key in rust_unsupported_feature_keys)
            ):
                run_async_rust_client_method(self._rust_client, "put", key, body)
            else:
                self._s3_client.put_object(**kwargs)

            return len(body)

        return self._translate_errors(_invoke_api, operation="PUT", bucket=bucket, key=key)

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        bucket, key = split_path(path)

        def _invoke_api() -> bytes:
            if byte_range:
                bytes_range = f"bytes={byte_range.offset}-{byte_range.offset + byte_range.size - 1}"
                if self._rust_client:
                    response = run_async_rust_client_method(
                        self._rust_client,
                        "get",
                        key,
                        byte_range,
                    )
                    return response
                else:
                    response = self._s3_client.get_object(Bucket=bucket, Key=key, Range=bytes_range)
            else:
                if self._rust_client:
                    response = run_async_rust_client_method(self._rust_client, "get", key)
                    return response
                else:
                    response = self._s3_client.get_object(Bucket=bucket, Key=key)

            return response["Body"].read()

        return self._translate_errors(_invoke_api, operation="GET", bucket=bucket, key=key)

    def _copy_object(self, src_path: str, dest_path: str) -> int:
        src_bucket, src_key = split_path(src_path)
        dest_bucket, dest_key = split_path(dest_path)

        src_object = self._get_object_metadata(src_path)

        def _invoke_api() -> int:
            self._s3_client.copy(
                CopySource={"Bucket": src_bucket, "Key": src_key},
                Bucket=dest_bucket,
                Key=dest_key,
                Config=self._transfer_config,
            )

            return src_object.content_length

        return self._translate_errors(_invoke_api, operation="COPY", bucket=dest_bucket, key=dest_key)

    def _delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        bucket, key = split_path(path)

        def _invoke_api() -> None:
            # conditionally delete the object if if_match(etag) is provided, if not, delete the object unconditionally
            if if_match:
                self._s3_client.delete_object(Bucket=bucket, Key=key, IfMatch=if_match)
            else:
                self._s3_client.delete_object(Bucket=bucket, Key=key)

        return self._translate_errors(_invoke_api, operation="DELETE", bucket=bucket, key=key)

    def _is_dir(self, path: str) -> bool:
        # Ensure the path ends with '/' to mimic a directory
        path = self._append_delimiter(path)

        bucket, key = split_path(path)

        def _invoke_api() -> bool:
            # List objects with the given prefix
            response = self._s3_client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1, Delimiter="/")

            # Check if there are any contents or common prefixes
            return bool(response.get("Contents", []) or response.get("CommonPrefixes", []))

        return self._translate_errors(_invoke_api, operation="LIST", bucket=bucket, key=key)

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        bucket, key = split_path(path)
        if path.endswith("/") or (bucket and not key):
            # If path ends with "/" or empty key name is provided, then assume it's a "directory",
            # which metadata is not guaranteed to exist for cases such as
            # "virtual prefix" that was never explicitly created.
            if self._is_dir(path):
                return ObjectMetadata(
                    key=path,
                    type="directory",
                    content_length=0,
                    last_modified=AWARE_DATETIME_MIN,
                )
            else:
                raise FileNotFoundError(f"Directory {path} does not exist.")
        else:

            def _invoke_api() -> ObjectMetadata:
                response = self._s3_client.head_object(Bucket=bucket, Key=key)

                return ObjectMetadata(
                    key=path,
                    type="file",
                    content_length=response["ContentLength"],
                    content_type=response.get("ContentType"),
                    last_modified=response["LastModified"],
                    etag=response["ETag"].strip('"') if "ETag" in response else None,
                    storage_class=response.get("StorageClass"),
                    metadata=response.get("Metadata"),
                )

            try:
                return self._translate_errors(_invoke_api, operation="HEAD", bucket=bucket, key=key)
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
        follow_symlinks: bool = True,
    ) -> Iterator[ObjectMetadata]:
        bucket, prefix = split_path(path)

        # Get the prefix of the start_after and end_at paths relative to the bucket.
        if start_after:
            _, start_after = split_path(start_after)
        if end_at:
            _, end_at = split_path(end_at)

        def _invoke_api() -> Iterator[ObjectMetadata]:
            paginator = self._s3_client.get_paginator("list_objects_v2")
            if include_directories:
                page_iterator = paginator.paginate(
                    Bucket=bucket, Prefix=prefix, Delimiter="/", StartAfter=(start_after or "")
                )
            else:
                page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, StartAfter=(start_after or ""))

            for page in page_iterator:
                for item in page.get("CommonPrefixes", []):
                    yield ObjectMetadata(
                        key=os.path.join(bucket, item["Prefix"].rstrip("/")),
                        type="directory",
                        content_length=0,
                        last_modified=AWARE_DATETIME_MIN,
                    )

                # S3 guarantees lexicographical order for general purpose buckets (for
                # normal S3) but not directory buckets (for S3 Express One Zone).
                for response_object in page.get("Contents", []):
                    key = response_object["Key"]
                    if end_at is None or key <= end_at:
                        if key.endswith("/"):
                            if include_directories:
                                yield ObjectMetadata(
                                    key=os.path.join(bucket, key.rstrip("/")),
                                    type="directory",
                                    content_length=0,
                                    last_modified=response_object["LastModified"],
                                )
                        else:
                            yield ObjectMetadata(
                                key=os.path.join(bucket, key),
                                type="file",
                                content_length=response_object["Size"],
                                last_modified=response_object["LastModified"],
                                etag=response_object["ETag"].strip('"'),
                                storage_class=response_object.get("StorageClass"),  # Pass storage_class
                            )
                    else:
                        return

        return self._translate_errors(_invoke_api, operation="LIST", bucket=bucket, key=prefix)

    def _upload_file(
        self,
        remote_path: str,
        f: Union[str, IO],
        attributes: Optional[dict[str, str]] = None,
        content_type: Optional[str] = None,
    ) -> int:
        file_size: int = 0

        if isinstance(f, str):
            bucket, key = split_path(remote_path)
            file_size = os.path.getsize(f)

            # Upload small files
            if file_size <= self._transfer_config.multipart_threshold:
                if self._rust_client and not attributes and not content_type:
                    run_async_rust_client_method(self._rust_client, "upload", f, key)
                else:
                    with open(f, "rb") as fp:
                        self._put_object(remote_path, fp.read(), attributes=attributes, content_type=content_type)
                return file_size

            # Upload large files using TransferConfig
            def _invoke_api() -> int:
                extra_args = {}
                if content_type:
                    extra_args["ContentType"] = content_type
                if self._is_directory_bucket(bucket):
                    extra_args["StorageClass"] = EXPRESS_ONEZONE_STORAGE_CLASS
                validated_attributes = validate_attributes(attributes)
                if validated_attributes:
                    extra_args["Metadata"] = validated_attributes
                if self._rust_client and not extra_args:
                    run_async_rust_client_method(self._rust_client, "upload_multipart_from_file", f, key)
                else:
                    self._s3_client.upload_file(
                        Filename=f,
                        Bucket=bucket,
                        Key=key,
                        Config=self._transfer_config,
                        ExtraArgs=extra_args,
                    )

                return file_size

            return self._translate_errors(_invoke_api, operation="PUT", bucket=bucket, key=key)
        else:
            # Upload small files
            f.seek(0, io.SEEK_END)
            file_size = f.tell()
            f.seek(0)

            if file_size <= self._transfer_config.multipart_threshold:
                if isinstance(f, io.StringIO):
                    self._put_object(
                        remote_path, f.read().encode("utf-8"), attributes=attributes, content_type=content_type
                    )
                else:
                    self._put_object(remote_path, f.read(), attributes=attributes, content_type=content_type)
                return file_size

            # Upload large files using TransferConfig
            bucket, key = split_path(remote_path)

            def _invoke_api() -> int:
                extra_args = {}
                if content_type:
                    extra_args["ContentType"] = content_type
                if self._is_directory_bucket(bucket):
                    extra_args["StorageClass"] = EXPRESS_ONEZONE_STORAGE_CLASS
                validated_attributes = validate_attributes(attributes)
                if validated_attributes:
                    extra_args["Metadata"] = validated_attributes

                if self._rust_client and isinstance(f, io.BytesIO) and not extra_args:
                    data = f.getbuffer()
                    run_async_rust_client_method(self._rust_client, "upload_multipart_from_bytes", key, data)
                else:
                    self._s3_client.upload_fileobj(
                        Fileobj=f,
                        Bucket=bucket,
                        Key=key,
                        Config=self._transfer_config,
                        ExtraArgs=extra_args,
                    )

                return file_size

            return self._translate_errors(_invoke_api, operation="PUT", bucket=bucket, key=key)

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> int:
        if metadata is None:
            metadata = self._get_object_metadata(remote_path)

        if isinstance(f, str):
            bucket, key = split_path(remote_path)
            if os.path.dirname(f):
                os.makedirs(os.path.dirname(f), exist_ok=True)

            # Download small files
            if metadata.content_length <= self._transfer_config.multipart_threshold:
                if self._rust_client:
                    run_async_rust_client_method(self._rust_client, "download", key, f)
                else:
                    with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                        temp_file_path = fp.name
                        fp.write(self._get_object(remote_path))
                    os.rename(src=temp_file_path, dst=f)
                return metadata.content_length

            # Download large files using TransferConfig
            def _invoke_api() -> int:
                with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                    temp_file_path = fp.name
                    if self._rust_client:
                        run_async_rust_client_method(
                            self._rust_client, "download_multipart_to_file", key, temp_file_path
                        )
                    else:
                        self._s3_client.download_fileobj(
                            Bucket=bucket,
                            Key=key,
                            Fileobj=fp,
                            Config=self._transfer_config,
                        )

                os.rename(src=temp_file_path, dst=f)

                return metadata.content_length

            return self._translate_errors(_invoke_api, operation="GET", bucket=bucket, key=key)
        else:
            # Download small files
            if metadata.content_length <= self._transfer_config.multipart_threshold:
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

            # Download large files using TransferConfig
            bucket, key = split_path(remote_path)

            def _invoke_api() -> int:
                self._s3_client.download_fileobj(
                    Bucket=bucket,
                    Key=key,
                    Fileobj=f,
                    Config=self._transfer_config,
                )

                return metadata.content_length

            return self._translate_errors(_invoke_api, operation="GET", bucket=bucket, key=key)
