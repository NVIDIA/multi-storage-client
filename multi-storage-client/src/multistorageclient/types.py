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

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import IO, Any, NamedTuple, Optional, Tuple, Union

from dateutil.parser import parse as dateutil_parser

MSC_PROTOCOL_NAME = "msc"
MSC_PROTOCOL = MSC_PROTOCOL_NAME + "://"

DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0

# datetime.min is a naive datetime.
#
# This creates issues when doing datetime.astimezone(timezone.utc) since it assumes the local timezone for the naive datetime.
# If the local timezone is offset behind UTC, it attempts to subtract off the offset which goes below the representable limit (i.e. an underflow).
# A `ValueError: year 0 is out of range` is thrown as a result.
AWARE_DATETIME_MIN = datetime.min.replace(tzinfo=timezone.utc)


@dataclass
class Credentials:
    """
    A data class representing the credentials needed to access a storage provider.
    """

    #: The access key for authentication.
    access_key: str
    #: The secret key for authentication.
    secret_key: str
    #: An optional security token for temporary credentials.
    token: Optional[str]
    #: The expiration time of the credentials in ISO 8601 format.
    expiration: Optional[str]
    #: A dictionary for storing custom key-value pairs.
    custom_fields: dict[str, Any] = field(default_factory=dict)

    def is_expired(self) -> bool:
        """
        Checks if the credentials are expired based on the expiration time.

        :return: ``True`` if the credentials are expired, ``False`` otherwise.
        """
        expiry = dateutil_parser(self.expiration) if self.expiration else None
        if expiry is None:
            return False
        return expiry <= datetime.now(tz=timezone.utc)

    def get_custom_field(self, key: str, default: Any = None) -> Any:
        """
        Retrieves a value from custom fields by its key.

        :param key: The key to look up in custom fields.
        :param default: The default value to return if the key is not found.
        :return: The value associated with the key, or the default value if not found.
        """
        return self.custom_fields.get(key, default)


@dataclass
class ObjectMetadata:
    """
    A data class that represents the metadata associated with an object stored in a cloud storage service. This metadata
    includes both required and optional information about the object.
    """

    #: Relative path of the object.
    key: str
    #: The size of the object in bytes.
    content_length: int
    #: The timestamp indicating when the object was last modified.
    last_modified: datetime
    type: str = "file"
    #: The MIME type of the object.
    content_type: Optional[str] = field(default=None)
    #: The entity tag (ETag) of the object.
    etag: Optional[str] = field(default=None)
    #: The storage class of the object.
    storage_class: Optional[str] = field(default=None)

    metadata: Optional[dict[str, Any]] = field(default=None)

    @staticmethod
    def from_dict(data: dict) -> "ObjectMetadata":
        """
        Creates an ObjectMetadata instance from a dictionary (parsed from JSON).
        """
        try:
            last_modified = dateutil_parser(data["last_modified"])
            key = data.get("key")
            if key is None:
                raise ValueError("Missing required field: 'key'")
            return ObjectMetadata(
                key=key,
                content_length=data["content_length"],
                last_modified=last_modified,
                type=data.get("type", "file"),  # default to file
                content_type=data.get("content_type"),
                etag=data.get("etag"),
                storage_class=data.get("storage_class"),
                metadata=data.get("metadata"),
            )
        except KeyError as e:
            raise ValueError("Missing required field.") from e

    def to_dict(self) -> dict:
        data = asdict(self)
        data["last_modified"] = self.last_modified.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return {k: v for k, v in data.items() if v is not None}


class CredentialsProvider(ABC):
    """
    Abstract base class for providing credentials to access a storage provider.
    """

    @abstractmethod
    def get_credentials(self) -> Credentials:
        """
        Retrieves the current credentials.

        :return: The current credentials used for authentication.
        """
        pass

    @abstractmethod
    def refresh_credentials(self) -> None:
        """
        Refreshes the credentials if they are expired or about to expire.
        """
        pass


@dataclass
class Range:
    """
    A data class that represents a byte range for read operations.
    """

    #: The start offset in bytes.
    offset: int
    #: The number of bytes to read.
    size: int


class StorageProvider(ABC):
    """
    Abstract base class for interacting with a storage provider.
    """

    @abstractmethod
    def put_object(
        self,
        path: str,
        body: bytes,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Uploads an object to the storage provider.

        :param path: The path where the object will be stored.
        :param body: The content of the object to store.
        :param attributes: The attributes to add to the file
        """
        pass

    @abstractmethod
    def get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        """
        Retrieves an object from the storage provider.

        :param path: The path where the object is stored.

        :return: The content of the retrieved object.
        """
        pass

    @abstractmethod
    def copy_object(self, src_path: str, dest_path: str) -> None:
        """
        Copies an object from source to destination in the storage provider.

        :param src_path: The path of the source object to copy.
        :param dest_path: The path of the destination.
        """
        pass

    @abstractmethod
    def delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        """
        Deletes an object from the storage provider.

        :param path: The path of the object to delete.
        :param if_match: Optional if-match value to use for conditional deletion.
        """
        pass

    @abstractmethod
    def get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        """
        Retrieves metadata or information about an object stored in the provider.

        :param path: The path of the object.
        :param strict: If True, performs additional validation to determine whether the path refers to a directory.

        :return: A metadata object containing the information about the object.
        """
        pass

    @abstractmethod
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
        :param include_directories: Whether to include directories in the result. When True, directories are returned alongside objects.
        :param attribute_filter_expression: The attribute filter expression to apply to the result.
        :param show_attributes: Whether to return attributes in the result.  There will be performance impact if this is True as now we need to get object metadata for each object.
        :param follow_symlinks: Whether to follow symbolic links. Only applicable for POSIX file storage providers.

        :return: An iterator over objects metadata under the specified path.
        """
        pass

    @abstractmethod
    def upload_file(self, remote_path: str, f: Union[str, IO], attributes: Optional[dict[str, str]] = None) -> None:
        """
        Uploads a file from the local file system to the storage provider.

        :param remote_path: The path where the object will be stored.
        :param f: The source file to upload. This can either be a string representing the local
            file path, or a file-like object (e.g., an open file handle).
        :param attributes: The attributes to add to the file if a new file is created.
        """
        pass

    @abstractmethod
    def download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        """
        Downloads a file from the storage provider to the local file system.

        :param remote_path: The path of the file to download.
        :param f: The destination for the downloaded file. This can either be a string representing
            the local file path where the file will be saved, or a file-like object to write the
            downloaded content into.
        :param metadata: Metadata about the object to download.
        """
        pass

    @abstractmethod
    def glob(self, pattern: str, attribute_filter_expression: Optional[str] = None) -> list[str]:
        """
        Matches and retrieves a list of object keys in the storage provider that match the specified pattern.

        :param pattern: The pattern to match object keys against, supporting wildcards (e.g., ``*.txt``).
        :param attribute_filter_expression: The attribute filter expression to apply to the result.

        :return: A list of object keys that match the specified pattern.
        """
        pass

    @abstractmethod
    def is_file(self, path: str) -> bool:
        """
        Checks whether the specified key in the storage provider points to a file (as opposed to a folder or directory).

        :param path: The path to check.

        :return: ``True`` if the key points to a file, ``False`` if it points to a directory or folder.
        """
        pass


class ResolvedPath(NamedTuple):
    """
    Result of resolving a virtual path to a physical path.

    :param physical_path: The physical path in storage backend
    :param exists: Whether the path exists in metadata
    :param profile: Optional profile name for routing in CompositeStorageClient.
        None means use current client's storage provider.
        String means route to named child StorageClient.
    """

    physical_path: str
    exists: bool
    profile: Optional[str] = None


class MetadataProvider(ABC):
    """
    Abstract base class for accessing file metadata.
    """

    @abstractmethod
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
        Lists objects in the metadata provider under the specified path.

        :param path: The path to list objects under. The path must be a valid file or subdirectory path, cannot be partial or just "prefix".
        :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
        :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.
        :param include_directories: Whether to include directories in the result. When True, directories are returned alongside objects.
        :param attribute_filter_expression: The attribute filter expression to apply to the result.
        :param show_attributes: Whether to return attributes in the result.  Depend on implementation, there might be performance impact if this set to True.

        :return: A iterator over objects metadata under the specified path.
        """
        pass

    @abstractmethod
    def get_object_metadata(self, path: str, include_pending: bool = False) -> ObjectMetadata:
        """
        Retrieves metadata or information about an object stored in the provider.

        :param path: The path of the object.
        :param include_pending: Whether to include metadata that is not yet committed.

        :return: A metadata object containing the information about the object.
        """
        pass

    @abstractmethod
    def glob(self, pattern: str, attribute_filter_expression: Optional[str] = None) -> list[str]:
        """
        Matches and retrieves a list of object keys in the storage provider that match the specified pattern.

        :param pattern: The pattern to match object keys against, supporting wildcards (e.g., ``*.txt``).
        :param attribute_filter_expression: The attribute filter expression to apply to the result.

        :return: A list of object keys that match the specified pattern.
        """
        pass

    @abstractmethod
    def realpath(self, logical_path: str) -> ResolvedPath:
        """
        Resolves a logical path to its physical storage path.

        This method checks if the object exists in the committed state and returns
        the appropriate physical path. The exists flag indicates whether the path
        was found.

        :param logical_path: The user-facing logical path

        :return: ResolvedPath with physical_path and exists flag indicating if path was found.
            If exists=True, physical_path is the committed storage path.
            If exists=False, physical_path is typically the logical_path as fallback.
        """
        pass

    @abstractmethod
    def generate_physical_path(self, logical_path: str, for_overwrite: bool = False) -> ResolvedPath:
        """
        Generates a physical storage path for writing a new or overwritten object.

        This method is used for write operations to determine where the object should
        be physically stored. Implementations can use this to:
        - Generate UUID-based paths for deduplication
        - Create versioned paths (file-v1.txt, file-v2.txt) for time travel
        - Implement path rewriting strategies

        :param logical_path: The user-facing logical path
        :param for_overwrite: If True, indicates the path is for overwriting an existing object.
            Implementations may generate unique paths for overwrites to support versioning.

        :return: ResolvedPath with physical_path for writing. The exists flag indicates
            whether the logical path currently exists in committed state (for overwrite scenarios).
        """
        pass

    @abstractmethod
    def add_file(self, path: str, metadata: ObjectMetadata) -> None:
        """
        Add a file to be tracked by the :py:class:`MetadataProvider`. Does not have to be
        reflected in listing until a :py:meth:`MetadataProvider.commit_updates` forces a persist.
        This function must tolerate duplicate calls (idempotent behavior).

        :param path: User-supplied virtual path
        :param metadata: physical file metadata from StorageProvider
        """
        pass

    @abstractmethod
    def remove_file(self, path: str) -> None:
        """
        Remove a file tracked by the :py:class:`MetadataProvider`. Does not have to be
        reflected in listing until a :py:meth:`MetadataProvider.commit_updates` forces a persist.
        This function must tolerate duplicate calls (idempotent behavior).

        :param path: User-supplied virtual path
        """
        pass

    @abstractmethod
    def commit_updates(self) -> None:
        """
        Commit any newly adding files, used in conjunction with :py:meth:`MetadataProvider.add_file`.
        :py:class:`MetadataProvider` will persistently record any metadata changes.
        """
        pass

    @abstractmethod
    def is_writable(self) -> bool:
        """
        Returns ``True`` if the :py:class:`MetadataProvider` supports writes else ``False``.
        """
        pass

    @abstractmethod
    def allow_overwrites(self) -> bool:
        """
        Returns ``True`` if the :py:class:`MetadataProvider` allows overwriting existing files else ``False``.
        When ``True``, :py:meth:`add_file` will not raise an error if the file already exists.
        """
        pass


@dataclass
class StorageProviderConfig:
    """
    A data class that represents the configuration needed to initialize a storage provider.
    """

    #: The name or type of the storage provider (e.g., ``s3``, ``gcs``, ``oci``, ``azure``).
    type: str
    #: Additional options required to configure the storage provider (e.g., endpoint URLs, region, etc.).
    options: Optional[dict[str, Any]] = None


@dataclass
class StorageBackend:
    """
    Represents configuration for a single storage backend.
    """

    storage_provider_config: StorageProviderConfig
    credentials_provider: Optional[CredentialsProvider] = None
    replicas: list["Replica"] = field(default_factory=list)


class ProviderBundle(ABC):
    """
    Abstract base class that serves as a container for various providers (storage, credentials, and metadata)
    that interact with a storage service. The :py:class:`ProviderBundle` abstracts access to these providers, allowing for
    flexible implementations of cloud storage solutions.
    """

    @property
    @abstractmethod
    def storage_provider_config(self) -> StorageProviderConfig:
        """
        :return: The configuration for the storage provider, which includes the provider
                    name/type and additional options.
        """
        pass

    @property
    @abstractmethod
    def credentials_provider(self) -> Optional[CredentialsProvider]:
        """
        :return: The credentials provider responsible for managing authentication credentials
                    required to access the storage service.
        """
        pass

    @property
    @abstractmethod
    def metadata_provider(self) -> Optional[MetadataProvider]:
        """
        :return: The metadata provider responsible for retrieving metadata about objects in the storage service.
        """
        pass

    @property
    @abstractmethod
    def replicas(self) -> list["Replica"]:
        """
        :return: The replicas configuration for this provider bundle, if any.
        """
        pass


class ProviderBundleV2(ABC):
    """
    Abstract base class that serves as a container for various providers (storage, credentials, and metadata)
    that interact with one or multiple storage service. The :py:class:`ProviderBundleV2` abstracts access to these providers, allowing for
    flexible implementations of cloud storage solutions.

    """

    @property
    @abstractmethod
    def storage_backends(self) -> dict[str, StorageBackend]:
        """
        :return: Mapping of storage backend name -> StorageBackend. Must have at least one backend.
        """
        pass

    @property
    @abstractmethod
    def metadata_provider(self) -> Optional[MetadataProvider]:
        """
        :return: The metadata provider responsible for retrieving metadata about objects in the storage service. If there are multiple backends, this is required.
        """
        pass


@dataclass
class RetryConfig:
    """
    A data class that represents the configuration for retry strategy.
    """

    #: The number of attempts before giving up. Must be at least 1.
    attempts: int = DEFAULT_RETRY_ATTEMPTS
    #: The base delay (in seconds) for exponential backoff. Must be a non-negative value.
    delay: float = DEFAULT_RETRY_DELAY
    #: The backoff multiplier for exponential backoff. Must be at least 1.0.
    backoff_multiplier: float = DEFAULT_RETRY_BACKOFF_MULTIPLIER

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("Attempts must be at least 1.")
        if self.delay < 0:
            raise ValueError("Delay must be a non-negative number.")
        if self.backoff_multiplier < 1.0:
            raise ValueError("Backoff multiplier must be at least 1.0.")


class RetryableError(Exception):
    """
    Exception raised for errors that should trigger a retry.
    """

    pass


class PreconditionFailedError(Exception):
    """
    Exception raised when a precondition fails. e.g. if-match, if-none-match, etc.
    """

    pass


class NotModifiedError(Exception):
    """
    Raised when a conditional operation fails because the resource has not been modified.

    This typically occurs when using if-none-match with a specific generation/etag
    and the resource's current generation/etag matches the specified one.
    """

    pass


class SourceVersionCheckMode(Enum):
    """
    Enum for controlling source version checking behavior.
    """

    INHERIT = "inherit"  # Inherit from configuration (cache config)
    ENABLE = "enable"  # Always check source version
    DISABLE = "disable"  # Never check source version


@dataclass
class Replica:
    """
    A tier of storage that can be used to store data.
    """

    replica_profile: str
    read_priority: int


class AutoCommitConfig:
    """
    A data class that represents the configuration for auto commit.
    """

    interval_minutes: Optional[float]  # The interval in minutes for auto commit.
    at_exit: bool = False  # if True, commit on program exit

    def __init__(self, interval_minutes: Optional[float] = None, at_exit: bool = False) -> None:
        self.interval_minutes = interval_minutes
        self.at_exit = at_exit


class ExecutionMode(Enum):
    """
    Enum for controlling execution mode in sync operations.
    """

    LOCAL = "local"
    RAY = "ray"


class PatternType(Enum):
    """
    Type of pattern operation for include/exclude filtering.
    """

    INCLUDE = "include"
    EXCLUDE = "exclude"


# Type alias for pattern matching
PatternList = list[Tuple[PatternType, str]]
