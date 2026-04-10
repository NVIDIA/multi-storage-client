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
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from typing import IO, Any, Optional, TypeVar, Union
from urllib.parse import urlparse

from azure.core import MatchConditions
from azure.core.exceptions import AzureError, HttpResponseError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobPrefix, BlobServiceClient, generate_blob_sas
from azure.storage.blob._models import BlobSasPermissions

from ..constants import DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT
from ..signers.base import URLSigner
from ..telemetry import Telemetry
from ..types import (
    AWARE_DATETIME_MIN,
    Credentials,
    CredentialsProvider,
    ObjectMetadata,
    PreconditionFailedError,
    Range,
    SignerType,
)
from ..utils import safe_makedirs, split_path, validate_attributes
from .base import BaseStorageProvider

_T = TypeVar("_T")

PROVIDER = "azure"
AZURE_CONNECTION_STRING_KEY = "connection"
AZURE_CREDENTIAL_KEY = "azure_credential"

DEFAULT_PRESIGN_EXPIRES_IN = 3600

# How long before delegation key expiry we treat the cached key as stale.
_DELEGATION_KEY_REFRESH_BUFFER = timedelta(minutes=5)

# Azure's maximum allowed delegation key lifetime is 7 days.
_DELEGATION_KEY_LIFETIME = timedelta(days=7)


def _sas_permissions_for_method(method: str) -> BlobSasPermissions:
    """Return the minimal :class:`BlobSasPermissions` needed for *method*."""
    m = method.upper()
    if m in ("PUT", "POST"):
        return BlobSasPermissions(write=True, create=True)
    elif m == "DELETE":
        return BlobSasPermissions(delete=True)
    else:
        # GET, HEAD, and any unrecognised method → read-only
        return BlobSasPermissions(read=True)


def _parse_account_name_from_url(account_url: str) -> str:
    """Extract the storage account name from an Azure Blob Storage account URL."""
    hostname = urlparse(account_url).hostname
    if hostname is None:
        raise ValueError(f"Invalid Azure account URL: {account_url!r}")
    return hostname.split(".")[0]


def _parse_connection_string(conn_str: str) -> dict[str, str]:
    """Parse an Azure connection string (``AccountName=foo;AccountKey=bar;...``) into a dict."""
    return dict(part.split("=", 1) for part in conn_str.split(";") if "=" in part)


class AzureURLSigner(URLSigner):
    """
    Generates Azure Blob Storage SAS (Shared Access Signature) URLs.

    Supports two signing paths depending on which credential is provided:

    * **Account key** – uses a static storage account key (parsed from a connection string).
    * **User delegation key** – uses a time-limited key obtained via Azure Identity (e.g. workload
      identity, managed identity).  Callers are responsible for refreshing the signer when the
      delegation key approaches expiry; see :py:meth:`AzureBlobStorageProvider._generate_presigned_url`.
    """

    def __init__(
        self,
        account_name: str,
        account_url: str,
        *,
        account_key: Optional[str] = None,
        user_delegation_key: Optional[Any] = None,
        expires_in: int = DEFAULT_PRESIGN_EXPIRES_IN,
    ) -> None:
        if account_key is None and user_delegation_key is None:
            raise ValueError("Either account_key or user_delegation_key must be provided.")
        self._account_name = account_name
        self._account_url = account_url.rstrip("/")
        self._account_key = account_key
        self._user_delegation_key = user_delegation_key
        self._expires_in = expires_in

    def generate_presigned_url(self, path: str, *, method: str = "GET") -> str:
        """
        Generate a SAS URL for the given blob path.

        :param path: Path in the form ``container/blob/name``.
        :param method: HTTP method requested by the caller.
        :return: A fully-qualified SAS URL.
        """
        container_name, blob_name = split_path(path)
        expiry = datetime.now(timezone.utc) + timedelta(seconds=self._expires_in)

        sas_kwargs: dict[str, Any] = {
            "account_name": self._account_name,
            "container_name": container_name,
            "blob_name": blob_name,
            "permission": _sas_permissions_for_method(method),
            "expiry": expiry,
        }

        if self._account_key is not None:
            sas_kwargs["account_key"] = self._account_key
        else:
            sas_kwargs["user_delegation_key"] = self._user_delegation_key

        sas_token = generate_blob_sas(**sas_kwargs)
        blob_url = f"{self._account_url}/{container_name}/{blob_name}"
        return f"{blob_url}?{sas_token}"


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
            custom_fields={AZURE_CONNECTION_STRING_KEY: self._connection},
        )

    def refresh_credentials(self) -> None:
        pass


class DefaultAzureCredentialsProvider(CredentialsProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.CredentialsProvider` that uses Azure Identity's :py:class:`azure.identity.DefaultAzureCredential` to authenticate with Blob Storage.

    See :py:class:`azure.identity.DefaultAzureCredential` for provider options.
    """

    def __init__(self, **kwargs: dict[str, Any]):
        self._credential = DefaultAzureCredential(**kwargs)

    def get_credentials(self) -> Credentials:
        return Credentials(
            access_key="",
            secret_key="",
            token=None,
            expiration=None,
            custom_fields={AZURE_CREDENTIAL_KEY: self._credential},
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
        config_dict: Optional[dict[str, Any]] = None,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
        **kwargs: dict[str, Any],
    ):
        """
        Initializes the :py:class:`AzureBlobStorageProvider` with the endpoint URL and optional credentials provider.

        :param endpoint_url: The Azure storage account URL.
        :param base_path: The root prefix path within the container where all operations will be scoped.
        :param credentials_provider: The provider to retrieve Azure credentials.
        :param config_dict: Resolved MSC config.
        :param telemetry_provider: A function that provides a telemetry instance.
        """
        super().__init__(
            base_path=base_path,
            provider_name=PROVIDER,
            config_dict=config_dict,
            telemetry_provider=telemetry_provider,
        )

        self._account_url = endpoint_url
        self._credentials_provider = credentials_provider
        # Cache static connection-string signing material used for per-request signers.
        self._account_key_signing_material: Optional[tuple[str, str]] = None
        # Cached delegation key and its expiry for DefaultAzureCredentialsProvider.
        self._delegation_user_key: Optional[Any] = None
        self._delegation_signer_expiry: Optional[datetime] = None
        # https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob#optional-configuration
        client_optional_configuration_keys = {
            "retry_total",
            "retry_connect",
            "retry_read",
            "retry_status",
            "connection_timeout",
            "read_timeout",
        }
        self._client_optional_configuration: dict[str, Any] = {
            key: value for key, value in kwargs.items() if key in client_optional_configuration_keys
        }
        if "connection_timeout" not in self._client_optional_configuration:
            self._client_optional_configuration["connection_timeout"] = DEFAULT_CONNECT_TIMEOUT
        if "read_timeout" not in self._client_optional_configuration:
            self._client_optional_configuration["read_timeout"] = DEFAULT_READ_TIMEOUT
        self._blob_service_client = self._create_blob_service_client()

    def _create_blob_service_client(self) -> BlobServiceClient:
        """
        Creates and configures the Azure BlobServiceClient using the current credentials.

        :return: The configured BlobServiceClient.
        """
        if self._credentials_provider:
            credentials = self._credentials_provider.get_credentials()

            if isinstance(self._credentials_provider, StaticAzureCredentialsProvider):
                return BlobServiceClient.from_connection_string(
                    credentials.get_custom_field(AZURE_CONNECTION_STRING_KEY), **self._client_optional_configuration
                )
            elif isinstance(self._credentials_provider, DefaultAzureCredentialsProvider):
                return BlobServiceClient(
                    account_url=self._account_url,
                    credential=credentials.get_custom_field(AZURE_CREDENTIAL_KEY),
                    **self._client_optional_configuration,
                )
            else:
                # Fallback to connection string if no built-in credentials provider is provided
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

    def _translate_errors(
        self,
        func: Callable[[], _T],
        operation: str,
        container: str,
        blob: str,
    ) -> _T:
        """
        Translates errors like timeouts and client errors.

        :param func: The function that performs the actual Azure Blob Storage operation.
        :param operation: The type of operation being performed (e.g., "PUT", "GET", "DELETE").
        :param container: The name of the Azure container involved in the operation.
        :param blob: The name of the blob within the Azure container.

        :return The result of the Azure Blob Storage operation, typically the return value of the `func` callable.
        """
        try:
            return func()
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
            error_info = f"message: {error.message}"
            raise RuntimeError(f"Failed to {operation} object(s) at {container}/{blob}. {error_info}") from error
        except FileNotFoundError:
            raise
        except Exception as error:
            raise RuntimeError(
                f"Failed to {operation} object(s) at {container}/{blob}. error_type: {type(error).__name__}, error: {error}"
            ) from error

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

        return self._translate_errors(_invoke_api, operation="PUT", container=container_name, blob=blob_name)

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

        return self._translate_errors(_invoke_api, operation="GET", container=container_name, blob=blob_name)

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

        return self._translate_errors(_invoke_api, operation="COPY", container=src_container, blob=src_blob)

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

        return self._translate_errors(_invoke_api, operation="DELETE", container=container_name, blob=blob_name)

    def _delete_objects(self, paths: list[str]) -> None:
        if not paths:
            return

        by_container: dict[str, list[str]] = {}
        for p in paths:
            container_name, blob_name = split_path(p)
            by_container.setdefault(container_name, []).append(blob_name)
        self._refresh_blob_service_client_if_needed()

        AZURE_BATCH_LIMIT = 256

        def _invoke_api() -> None:
            for container_name, blob_names in by_container.items():
                container_client = self._blob_service_client.get_container_client(container=container_name)
                for i in range(0, len(blob_names), AZURE_BATCH_LIMIT):
                    chunk = blob_names[i : i + AZURE_BATCH_LIMIT]
                    container_client.delete_blobs(*chunk)

        container_desc = "(" + "|".join(by_container) + ")"
        blob_desc = "(" + "|".join(str(len(blob_names)) for blob_names in by_container.values()) + " keys)"
        self._translate_errors(_invoke_api, operation="DELETE_MANY", container=container_desc, blob=blob_desc)

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

        return self._translate_errors(_invoke_api, operation="LIST", container=container_name, blob=prefix)

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
                return self._translate_errors(_invoke_api, operation="HEAD", container=container_name, blob=blob_name)
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
        container_name, prefix = split_path(path)

        # Get the prefix of the start_after and end_at paths relative to the bucket.
        if start_after:
            _, start_after = split_path(start_after)
        if end_at:
            _, end_at = split_path(end_at)

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
                    prefix_key = blob.name.rstrip("/")
                    # Filter by start_after and end_at if specified
                    if (start_after is None or start_after < prefix_key) and (end_at is None or prefix_key <= end_at):
                        yield ObjectMetadata(
                            key=os.path.join(container_name, prefix_key),
                            type="directory",
                            content_length=0,
                            last_modified=AWARE_DATETIME_MIN,
                        )
                    elif end_at is not None and end_at < prefix_key:
                        return
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

        return self._translate_errors(_invoke_api, operation="LIST", container=container_name, blob=prefix)

    def _generate_presigned_url(
        self,
        path: str,
        *,
        method: str = "GET",
        signer_type: Optional[SignerType] = None,
        signer_options: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Generate a SAS URL for a blob in Azure Blob Storage.

        :param path: Path in the form ``container/blob/name``.
        :param method: HTTP method requested by the caller.
        :param signer_type: Must be ``None`` or :py:attr:`SignerType.AZURE`.
        :param signer_options: Optional dict; supports ``expires_in`` (int, seconds).
        :return: A fully-qualified SAS URL.
        :raises ValueError: If *signer_type* is not ``None`` / ``SignerType.AZURE``, or if the
            configured credential type does not support SAS generation.
        """
        if signer_type is not None and signer_type != SignerType.AZURE:
            raise ValueError(f"Unsupported signer type for Azure provider: {signer_type!r}")

        options = signer_options or {}
        expires_in = int(options.get("expires_in", DEFAULT_PRESIGN_EXPIRES_IN))

        self._refresh_blob_service_client_if_needed()

        if isinstance(self._credentials_provider, StaticAzureCredentialsProvider):
            # Account key path: cache parsed AccountName + AccountKey, then sign per request.
            if self._account_key_signing_material is None:
                conn_str = self._credentials_provider.get_credentials().get_custom_field(AZURE_CONNECTION_STRING_KEY)
                parsed = _parse_connection_string(conn_str)
                self._account_key_signing_material = (parsed["AccountName"], parsed["AccountKey"])
            account_name, account_key = self._account_key_signing_material
            signer = AzureURLSigner(
                account_name=account_name,
                account_url=self._account_url,
                account_key=account_key,
                expires_in=expires_in,
            )

        elif isinstance(self._credentials_provider, DefaultAzureCredentialsProvider):
            # User delegation key path: refresh when the cached key is within the
            # refresh buffer of its own expiry or has not been fetched yet.
            now = datetime.now(timezone.utc)
            if (
                self._delegation_user_key is None
                or self._delegation_signer_expiry is None
                or now >= self._delegation_signer_expiry - _DELEGATION_KEY_REFRESH_BUFFER
            ):
                key_expiry = now + _DELEGATION_KEY_LIFETIME
                self._delegation_user_key = self._blob_service_client.get_user_delegation_key(
                    key_start_time=now,
                    key_expiry_time=key_expiry,
                )
                self._delegation_signer_expiry = key_expiry
            signer = AzureURLSigner(
                account_name=_parse_account_name_from_url(self._account_url),
                account_url=self._account_url,
                user_delegation_key=self._delegation_user_key,
                expires_in=expires_in,
            )

        else:
            raise ValueError(
                "Azure presigned URLs require StaticAzureCredentialsProvider (connection string) or "
                "DefaultAzureCredentialsProvider (Azure Identity). "
                f"Got: {type(self._credentials_provider).__name__!r}"
            )

        return signer.generate_presigned_url(path, method=method)

    @property
    def supports_parallel_listing(self) -> bool:
        return True

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

            return self._translate_errors(_invoke_api, operation="PUT", container=container_name, blob=blob_name)
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

            return self._translate_errors(_invoke_api, operation="PUT", container=container_name, blob=blob_name)

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> int:
        if metadata is None:
            metadata = self._get_object_metadata(remote_path)

        container_name, blob_name = split_path(remote_path)
        self._refresh_blob_service_client_if_needed()

        if isinstance(f, str):
            if os.path.dirname(f):
                safe_makedirs(os.path.dirname(f))

            def _invoke_api() -> int:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                    temp_file_path = fp.name
                    stream = blob_client.download_blob()
                    fp.write(stream.readall())
                os.rename(src=temp_file_path, dst=f)

                return metadata.content_length

            return self._translate_errors(_invoke_api, operation="GET", container=container_name, blob=blob_name)
        else:

            def _invoke_api() -> int:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                stream = blob_client.download_blob()
                if isinstance(f, io.StringIO):
                    f.write(stream.readall().decode("utf-8"))
                else:
                    f.write(stream.readall())

                return metadata.content_length

            return self._translate_errors(_invoke_api, operation="GET", container=container_name, blob=blob_name)
