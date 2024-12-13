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

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import IO, Any, Dict, Iterator, List, Optional, Tuple, Union

from dateutil.parser import parse as dateutil_parser

MSC_PROTOCOL_NAME = "msc"
MSC_PROTOCOL = MSC_PROTOCOL_NAME + "://"

DEFAULT_POSIX_PROFILE_NAME = "default"
DEFAULT_POSIX_PROFILE = {
    "profiles": {
        DEFAULT_POSIX_PROFILE_NAME: {
            "storage_provider": {
                "type": "file",
                "options": {
                    "base_path": "/"
                }
            }
        }
    }
}

DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1.0


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

    def is_expired(self) -> bool:
        """
        Checks if the credentials are expired based on the expiration time.

        :return: ``True`` if the credentials are expired, ``False`` otherwise.
        """
        expiry = dateutil_parser(self.expiration) if self.expiration else None
        if expiry is None:
            return False
        return expiry <= datetime.now(tz=timezone.utc)


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

    @staticmethod
    def from_dict(data: dict) -> 'ObjectMetadata':
        """
        Creates an ObjectMetadata instance from a dictionary (parsed from JSON).
        """
        try:
            last_modified = dateutil_parser(data['last_modified'])
            key = data.get('key')
            if key is None:
                raise ValueError("Missing required field: 'key'")
            return ObjectMetadata(
                key=key,
                content_length=data['content_length'],
                last_modified=last_modified,
                type=data.get("type", "file"),  # default to file
                content_type=data.get('content_type'),
                etag=data.get('etag')
            )
        except KeyError as e:
            raise ValueError("Missing required field.") from e

    def to_dict(self) -> dict:
        data = asdict(self)
        data['last_modified'] = self.last_modified.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
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
    Byte-range read.
    """
    offset: int
    size: int


class StorageProvider(ABC):
    """
    Abstract base class for interacting with a storage provider.
    """

    @abstractmethod
    def put_object(self, path: str, body: bytes) -> None:
        """
        Uploads an object to the storage provider.

        :param path: The path where the object will be stored.
        :param body: The content of the object to store.
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
    def delete_object(self, path: str) -> None:
        """
        Deletes an object from the storage provider.

        :param path: The path of the object to delete.
        """
        pass

    @abstractmethod
    def get_object_metadata(self, path: str) -> ObjectMetadata:
        """
        Retrieves metadata or information about an object stored in the provider.

        :param path: The path of the object.

        :return: A metadata object containing the information about the object.
        """
        pass

    @abstractmethod
    def list_objects(self, prefix: str, start_after: Optional[str] = None,
                     end_at: Optional[str] = None) -> Iterator[ObjectMetadata]:
        """
        Lists objects in the storage provider under the specified prefix.

        :param prefix: The prefix or path to list objects under.
        :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
        :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.

        :return: An iterator over objects metadata under the specified prefix.
        """
        pass

    @abstractmethod
    def upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        """
        Uploads a file from the local file system to the storage provider.

        :param remote_path: The path where the object will be stored.
        :param f: The source file to upload. This can either be a string representing the local
            file path, or a file-like object (e.g., an open file handle).
        """
        pass

    @abstractmethod
    def download_file(self,
                      remote_path: str,
                      f: Union[str, IO],
                      metadata: Optional[ObjectMetadata] = None) -> None:
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
    def glob(self, pattern: str) -> List[str]:
        """
        Matches and retrieves a list of object keys in the storage provider that match the specified pattern.

        :param pattern: The pattern to match object keys against, supporting wildcards (e.g., ``*.txt``).

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


class MetadataProvider(ABC):
    """
    Abstract base class for accessing file metadata.
    """

    @abstractmethod
    def list_objects(self, prefix: str, start_after: Optional[str] = None,
                     end_at: Optional[str] = None) -> Iterator[ObjectMetadata]:
        """
        Lists objects in the storage provider under the specified prefix.

        :param prefix: The prefix or path to list objects under.
        :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
        :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.

        :return: A iterator over objects metadata under the specified prefix.
        """
        pass

    @abstractmethod
    def get_object_metadata(self, path: str) -> ObjectMetadata:
        """
        Retrieves metadata or information about an object stored in the provider.

        :param path: The path of the object.

        :return: A metadata object containing the information about the object.
        """
        pass

    @abstractmethod
    def glob(self, pattern: str) -> List[str]:
        """
        Matches and retrieves a list of object keys in the storage provider that match the specified pattern.

        :param pattern: The pattern to match object keys against, supporting wildcards (e.g., ``*.txt``).

        :return: A list of object keys that match the specified pattern.
        """
        pass

    @abstractmethod
    def realpath(self, path: str) -> Tuple[str, bool]:
        """
        Returns the canonical, full real physical path for use by a
        :py:class:`StorageProvider`. This provides translation from user-visible paths to
        the canonical paths needed by a :py:class:`StorageProvider`.

        :param path: user-supplied virtual path

        :return: A canonical physical path and if the object at the path is valid
        """
        pass

    @abstractmethod
    def add_file(self, path: str, metadata: ObjectMetadata) -> None:
        """
        Add a file to be tracked by the :py:class:`MetadataProvider`. Does not have to be
        reflected in listing until a :py:meth:`MetadataProvider.commit_updates` forces a persist.

        :param path: User-supplied path
        :param metadata: file metadata
        """
        pass

    @abstractmethod
    def remove_file(self, path: str) -> None:
        """
        Remove a file tracked by the :py:class:`MetadataProvider`. Does not have to be
        reflected in listing until a :py:meth:`MetadataProvider.commit_updates` forces a persist.

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


@dataclass
class StorageProviderConfig:
    """
    A data class that represents the configuration needed to initialize a storage provider.
    """

    #: The name or type of the storage provider (e.g., ``s3``, ``gcs``, ``oci``, ``azure``).
    type: str
    #: Additional options required to configure the storage provider (e.g., endpoint URLs, region, etc.).
    options: Optional[Dict[str, Any]] = None


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


@dataclass
class RetryConfig:
    """
    A data class that represents the configuration for retry strategy.
    """

    #: The number of attempts before giving up. Must be at least 1.
    attempts: int = DEFAULT_RETRY_ATTEMPTS
    #: The delay (in seconds) between retry attempts. Must be a non-negative value.
    delay: float = DEFAULT_RETRY_DELAY

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("Attempts must be at least 1.")
        if self.delay < 0:
            raise ValueError("Delay must be a non-negative number.")


class RetryableError(Exception):
    """
    Exception raised for errors that should trigger a retry.
    """
    pass
