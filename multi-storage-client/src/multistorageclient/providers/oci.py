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
import posixpath
import tempfile
from collections.abc import Callable, Iterator
from typing import IO, Any, Optional, TypeVar, Union

import oci
from dateutil.parser import parse as dateutil_parser
from oci._vendor.requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    ContentDecodingError,
)
from oci.auth.signers import SecurityTokenSigner
from oci.exceptions import ServiceError
from oci.object_storage import ObjectStorageClient, UploadManager
from oci.retry import DEFAULT_RETRY_STRATEGY, RetryStrategyBuilder
from oci.signer import load_private_key_from_file

from ..constants import DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT
from ..telemetry import Telemetry
from ..types import (
    AWARE_DATETIME_MIN,
    CredentialsProvider,
    ObjectMetadata,
    PreconditionFailedError,
    Range,
    RetryableError,
)
from ..utils import safe_makedirs, split_path, validate_attributes
from .base import BaseStorageProvider

_T = TypeVar("_T")

MB = 1024 * 1024

MULTIPART_THRESHOLD = 64 * MB
MULTIPART_CHUNKSIZE = 32 * MB

PROVIDER = "oci"


class OracleStorageProvider(BaseStorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with
    Oracle Cloud Infrastructure (OCI) Object Storage.
    """

    _namespace: str
    _credentials_provider: Optional[CredentialsProvider]
    _oci_client: ObjectStorageClient

    def __init__(
        self,
        namespace: str,
        base_path: str = "",
        credentials_provider: Optional[CredentialsProvider] = None,
        retry_strategy: Optional[dict[str, Any]] = None,
        config_dict: Optional[dict[str, Any]] = None,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes an instance of :py:class:`OracleStorageProvider`.

        :param namespace: The OCI Object Storage namespace. This is a unique identifier assigned to each tenancy.
        :param base_path: The root prefix path within the bucket where all operations will be scoped.
        :param credentials_provider: The provider to retrieve OCI credentials.
        :param retry_strategy: ``oci.retry.RetryStrategyBuilder`` parameters.
        :param config_dict: Resolved MSC config.
        :param telemetry_provider: A function that provides a telemetry instance.
        """
        super().__init__(
            base_path=base_path,
            provider_name=PROVIDER,
            config_dict=config_dict,
            telemetry_provider=telemetry_provider,
        )

        self._namespace = namespace
        self._credentials_provider = credentials_provider
        self._retry_strategy = (
            DEFAULT_RETRY_STRATEGY
            if retry_strategy is None
            else RetryStrategyBuilder(**retry_strategy).get_retry_strategy()
        )
        self._timeout = kwargs.get("timeout")
        if self._timeout is None:
            self._timeout = (DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT)
        self._oci_client = self._create_oci_client()
        self._upload_manager = UploadManager(self._oci_client)
        self._multipart_threshold = int(kwargs.get("multipart_threshold", MULTIPART_THRESHOLD))
        self._multipart_chunksize = int(kwargs.get("multipart_chunksize", MULTIPART_CHUNKSIZE))

    def _create_oci_client(self) -> ObjectStorageClient:
        config = oci.config.from_file()
        kwargs = {"retry_strategy": self._retry_strategy}

        # OCI doesn't support `authentication_type=security_token` OCI config entries in their SDKs yet. Manually configure signers.
        #
        # https://github.com/oracle/oci-python-sdk/blob/v2.169.0/src/oci/util.py#L213-L225
        # https://github.com/oracle/oci-ruby-sdk/issues/70
        if "security_token_file" in config:
            with open(config["security_token_file"], "r") as security_token_file:
                kwargs["signer"] = SecurityTokenSigner(
                    private_key=load_private_key_from_file(
                        filename=config["key_file"],
                        pass_phrase=config.get("pass_phrase"),
                    ),
                    # The OCI documentation + CLI are unforgiving about newline-terminated security token files.
                    #
                    # Do not ignore trailing newlines in case future upstream automatic signer configuration does the same.
                    #
                    # https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdk_authentication_methods.htm#sdk_authentication_methods_session_token
                    # https://github.com/oracle/oci-cli/blob/v3.77.0/src/oci_cli/cli_session.py#L143-L144
                    token=security_token_file.read(),
                )

        client = ObjectStorageClient(config, **kwargs)
        client.base_client.timeout = self._timeout
        return client

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

    def _translate_errors(
        self,
        func: Callable[[], _T],
        operation: str,
        bucket: str,
        key: str,
    ) -> _T:
        """
        Translates errors like timeouts and client errors.

        :param func: The function that performs the actual object storage operation.
        :param operation: The type of operation being performed (e.g., "PUT", "GET", "DELETE").
        :param bucket: The name of the object storage bucket involved in the operation.
        :param key: The key of the object within the object storage bucket.

        :return: The result of the object storage operation, typically the return value of the `func` callable.
        """
        try:
            return func()
        except ServiceError as error:
            status_code = error.status
            request_id = error.request_id
            endpoint = error.request_endpoint
            error_info = f"request_id: {request_id}, endpoint: {endpoint}, status_code: {status_code}"

            if status_code == 404:
                raise FileNotFoundError(f"Object {bucket}/{key} does not exist. {error_info}")  # pylint: disable=raise-missing-from
            elif status_code == 412:
                raise PreconditionFailedError(
                    f"Failed to {operation} object(s) at {bucket}/{key}. {error_info}"
                ) from error
            elif status_code == 429:
                raise RetryableError(
                    f"Too many request to {operation} object(s) at {bucket}/{key}. {error_info}"
                ) from error
            else:
                raise RuntimeError(f"Failed to {operation} object(s) at {bucket}/{key}. {error_info}") from error
        except (ConnectionError, ChunkedEncodingError, ContentDecodingError) as error:
            raise RetryableError(
                f"Failed to {operation} object(s) at {bucket}/{key}, error type: {type(error).__name__}"
            ) from error
        except FileNotFoundError:
            raise
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
    ) -> int:
        bucket, key = split_path(path)
        self._refresh_oci_client_if_needed()

        # OCI only supports if_none_match=="*"
        # refer: https://docs.oracle.com/en-us/iaas/tools/python/2.150.0/api/object_storage/client/oci.object_storage.ObjectStorageClient.html?highlight=put_object#oci.object_storage.ObjectStorageClient.put_object
        def _invoke_api() -> int:
            validated_attributes = validate_attributes(attributes)
            self._oci_client.put_object(
                namespace_name=self._namespace,
                bucket_name=bucket,
                object_name=key,
                put_object_body=body,
                opc_meta=validated_attributes or {},  # Pass metadata or empty dict
                if_match=if_match,
                if_none_match=if_none_match,
            )

            return len(body)

        return self._translate_errors(_invoke_api, operation="PUT", bucket=bucket, key=key)

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

        return self._translate_errors(_invoke_api, operation="GET", bucket=bucket, key=key)

    def _copy_object(self, src_path: str, dest_path: str) -> int:
        src_bucket, src_key = split_path(src_path)
        dest_bucket, dest_key = split_path(dest_path)
        self._refresh_oci_client_if_needed()

        src_object = self._get_object_metadata(src_path)

        def _invoke_api() -> int:
            copy_details = oci.object_storage.models.CopyObjectDetails(
                source_object_name=src_key, destination_bucket=dest_bucket, destination_object_name=dest_key
            )

            self._oci_client.copy_object(
                namespace_name=self._namespace, bucket_name=src_bucket, copy_object_details=copy_details
            )

            return src_object.content_length

        return self._translate_errors(_invoke_api, operation="COPY", bucket=src_bucket, key=src_key)

    def _delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        bucket, key = split_path(path)
        self._refresh_oci_client_if_needed()

        def _invoke_api() -> None:
            namespace_name = self._namespace
            bucket_name = bucket
            object_name = key
            if if_match is not None:
                self._oci_client.delete_object(namespace_name, bucket_name, object_name, if_match=if_match)
            else:
                self._oci_client.delete_object(namespace_name, bucket_name, object_name)

        return self._translate_errors(_invoke_api, operation="DELETE", bucket=bucket, key=key)

    def _is_dir(self, path: str) -> bool:
        # Ensure the path ends with '/' to mimic a directory
        path = self._append_delimiter(path)

        bucket, key = split_path(path)
        self._refresh_oci_client_if_needed()

        def _invoke_api() -> bool:
            # List objects with the given prefix
            response = self._oci_client.list_objects(
                namespace_name=self._namespace,
                bucket_name=bucket,
                prefix=key,
                delimiter="/",
            )
            # Check if there are any contents or common prefixes
            if response:
                return bool(response.data.objects or response.data.prefixes)
            return False

        return self._translate_errors(_invoke_api, operation="LIST", bucket=bucket, key=key)

    def _make_symlink(self, path: str, target: str) -> None:
        bucket, key = split_path(path)
        target_bucket, target_key = split_path(target)
        if bucket != target_bucket:
            raise ValueError(f"Cannot create cross-bucket symlink: '{bucket}' -> '{target_bucket}'.")
        relative_target = posixpath.relpath(target_key, posixpath.dirname(key))
        self._refresh_oci_client_if_needed()

        def _invoke_api() -> None:
            self._oci_client.put_object(
                namespace_name=self._namespace,
                bucket_name=bucket,
                object_name=key,
                put_object_body=b"",
                opc_meta={"msc-symlink-target": relative_target},
            )

        self._translate_errors(_invoke_api, operation="PUT", bucket=bucket, key=key)

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
            self._refresh_oci_client_if_needed()

            def _invoke_api() -> ObjectMetadata:
                response = self._oci_client.head_object(
                    namespace_name=self._namespace, bucket_name=bucket, object_name=key
                )

                # Extract custom metadata from headers with 'opc-meta-' prefix
                attributes = {}
                if response.headers:  # pyright: ignore [reportOptionalMemberAccess]
                    for metadata_key, metadata_val in response.headers.items():  # pyright: ignore [reportOptionalMemberAccess]
                        if metadata_key.startswith("opc-meta-"):
                            # Remove the 'opc-meta-' prefix to get the original key
                            metadata_key = metadata_key[len("opc-meta-") :]
                            attributes[metadata_key] = metadata_val

                user_metadata = attributes if attributes else None
                symlink_target = user_metadata.get("msc-symlink-target") if user_metadata else None
                return ObjectMetadata(
                    key=path,
                    content_length=int(response.headers["Content-Length"]),  # pyright: ignore [reportOptionalMemberAccess]
                    content_type=response.headers.get("Content-Type", None),  # pyright: ignore [reportOptionalMemberAccess]
                    last_modified=dateutil_parser(response.headers["last-modified"]),  # pyright: ignore [reportOptionalMemberAccess]
                    etag=response.headers.get("etag", None),  # pyright: ignore [reportOptionalMemberAccess]
                    metadata=user_metadata,
                    symlink_target=symlink_target,
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
        self._refresh_oci_client_if_needed()

        def _invoke_api() -> Iterator[ObjectMetadata]:
            # ListObjects only includes object names by default.
            #
            # Request additional fields needed for creating an ObjectMetadata.
            fields = ",".join(
                [
                    "etag",
                    "name",
                    "size",
                    "timeModified",
                ]
            )
            next_start_with: Optional[str] = start_after
            while True:
                if include_directories:
                    response = self._oci_client.list_objects(
                        namespace_name=self._namespace,
                        bucket_name=bucket,
                        prefix=prefix,
                        # This is ≥ instead of >.
                        start=next_start_with,
                        delimiter="/",
                        fields=fields,
                    )
                else:
                    response = self._oci_client.list_objects(
                        namespace_name=self._namespace,
                        bucket_name=bucket,
                        prefix=prefix,
                        # This is ≥ instead of >.
                        start=next_start_with,
                        fields=fields,
                    )

                if not response:
                    return []

                if include_directories:
                    for directory in response.data.prefixes:
                        prefix_key = directory.rstrip("/")
                        # Filter by start_after and end_at if specified
                        if (start_after is None or start_after < prefix_key) and (
                            end_at is None or prefix_key <= end_at
                        ):
                            yield ObjectMetadata(
                                key=os.path.join(bucket, prefix_key),
                                type="directory",
                                content_length=0,
                                last_modified=AWARE_DATETIME_MIN,
                            )
                        elif end_at is not None and end_at < prefix_key:
                            return

                # OCI guarantees lexicographical order.
                for response_object in response.data.objects:  # pyright: ignore [reportOptionalMemberAccess]
                    key = response_object.name
                    if (start_after is None or start_after < key) and (end_at is None or key <= end_at):
                        if key.endswith("/"):
                            if include_directories:
                                yield ObjectMetadata(
                                    key=os.path.join(bucket, key.rstrip("/")),
                                    type="directory",
                                    content_length=0,
                                    last_modified=response_object.time_modified,
                                )
                        else:
                            yield ObjectMetadata(
                                key=os.path.join(bucket, key),
                                type="file",
                                content_length=response_object.size,
                                last_modified=response_object.time_modified,
                                etag=response_object.etag,
                            )
                    elif start_after != key:
                        return
                next_start_with = response.data.next_start_with  # pyright: ignore [reportOptionalMemberAccess]
                if next_start_with is None or (end_at is not None and end_at < next_start_with):
                    return

        return self._translate_errors(_invoke_api, operation="LIST", bucket=bucket, key=prefix)

    @property
    def supports_parallel_listing(self) -> bool:
        return True

    def _upload_file(self, remote_path: str, f: Union[str, IO], attributes: Optional[dict[str, str]] = None) -> int:
        bucket, key = split_path(remote_path)
        file_size: int = 0
        self._refresh_oci_client_if_needed()

        validated_attributes = validate_attributes(attributes)
        if isinstance(f, str):
            file_size = os.path.getsize(f)

            def _invoke_api() -> int:
                if file_size > self._multipart_threshold:
                    self._upload_manager.upload_file(
                        namespace_name=self._namespace,
                        bucket_name=bucket,
                        object_name=key,
                        file_path=f,
                        part_size=self._multipart_chunksize,
                        allow_parallel_uploads=True,
                        metadata=validated_attributes or {},
                    )
                else:
                    self._upload_manager.upload_file(
                        namespace_name=self._namespace,
                        bucket_name=bucket,
                        object_name=key,
                        file_path=f,
                        metadata=validated_attributes or {},
                    )

                return file_size

            return self._translate_errors(_invoke_api, operation="PUT", bucket=bucket, key=key)
        else:
            # Convert file-like object to BytesIO because stream_ref cannot work with StringIO.
            if isinstance(f, io.StringIO):
                f = io.BytesIO(f.getvalue().encode("utf-8"))

            f.seek(0, io.SEEK_END)
            file_size = f.tell()
            f.seek(0)

            def _invoke_api() -> int:
                if file_size > self._multipart_threshold:
                    self._upload_manager.upload_stream(
                        namespace_name=self._namespace,
                        bucket_name=bucket,
                        object_name=key,
                        stream_ref=f,
                        part_size=self._multipart_chunksize,
                        allow_parallel_uploads=True,
                        metadata=validated_attributes or {},
                    )
                else:
                    self._upload_manager.upload_stream(
                        namespace_name=self._namespace,
                        bucket_name=bucket,
                        object_name=key,
                        stream_ref=f,
                        metadata=validated_attributes or {},
                    )

                return file_size

            return self._translate_errors(_invoke_api, operation="PUT", bucket=bucket, key=key)

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> int:
        self._refresh_oci_client_if_needed()

        if metadata is None:
            metadata = self._get_object_metadata(remote_path)

        bucket, key = split_path(remote_path)

        if isinstance(f, str):
            if os.path.dirname(f):
                safe_makedirs(os.path.dirname(f))

            def _invoke_api() -> int:
                response = self._oci_client.get_object(
                    namespace_name=self._namespace, bucket_name=bucket, object_name=key
                )
                with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                    temp_file_path = fp.name
                    for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):  # pyright: ignore [reportOptionalMemberAccess]
                        fp.write(chunk)
                os.rename(src=temp_file_path, dst=f)

                return metadata.content_length

            return self._translate_errors(_invoke_api, operation="GET", bucket=bucket, key=key)
        else:

            def _invoke_api() -> int:
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

                return metadata.content_length

            return self._translate_errors(_invoke_api, operation="GET", bucket=bucket, key=key)
