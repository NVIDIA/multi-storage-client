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

import os
import threading
from collections.abc import Iterator
from typing import Any, Optional, Union
from urllib.parse import ParseResult, urlparse

from .client import StorageClient
from .config import DEFAULT_POSIX_PROFILE_NAME, SUPPORTED_IMPLICIT_PROFILE_PROTOCOLS, StorageClientConfig
from .file import ObjectFile, PosixFile
from .telemetry import Telemetry
from .types import MSC_PROTOCOL, ExecutionMode, ObjectMetadata

_TELEMETRY: Optional[Telemetry] = None
_TELEMETRY_LOCK = threading.Lock()
_STORAGE_CLIENT_CACHE: dict[str, StorageClient] = {}
_STORAGE_CLIENT_CACHE_LOCK = threading.Lock()


def get_telemetry() -> Optional[Telemetry]:
    """
    Get the :py:class:``Telemetry`` instance to use for storage clients created by shortcuts.

    :return: A telemetry instance.
    """
    global _TELEMETRY

    return _TELEMETRY


def set_telemetry(telemetry: Optional[Telemetry]) -> None:
    """
    Set the :py:class:``Telemetry`` instance to use for storage clients created by shortcuts.

    :param telemetry: A telemetry instance.
    """
    global _TELEMETRY
    global _TELEMETRY_LOCK

    with _TELEMETRY_LOCK:
        _TELEMETRY = telemetry


def _build_full_path(original_url: str, pr: ParseResult) -> str:
    """
    Helper function to construct the full path from a parsed URL, including query and fragment.

    :param original_url: The original URL before parsing
    :param pr: The parsed URL result from urlparse
    :return: The complete path including query and fragment if present
    """
    path = pr.path
    if pr.query:
        path += "?" + pr.query
    elif original_url.endswith("?"):
        path += "?"  # handle the glob pattern that has a trailing question mark
    if pr.fragment:
        path += "#" + pr.fragment
    return path


def _resolve_msc_url(url: str) -> tuple[str, str]:
    """
    Resolve an MSC URL to a profile name and path.

    :param url: The MSC URL to resolve (msc://profile/path)
    :return: A tuple of (profile_name, path)
    """
    pr = urlparse(url)
    profile = pr.netloc
    path = _build_full_path(url, pr)
    if path.startswith("/"):
        path = path[1:]
    return profile, path


def _resolve_non_msc_url(url: str) -> tuple[str, str]:
    """
    Resolve a non-MSC URL to a profile name and path.

    Resolution process:
    1. First check if MSC config exists
    2. If config exists, check for possible path mapping
    3. If no mapping is found, fall back to default POSIX profile
       for file paths or create an implicit profile based on URL

    :param url: The non-MSC URL to resolve
    :return: A tuple of (profile_name, path)
    """
    # Check if we have a valid path mapping, if so check if there is a matching mapping
    path_mapping = StorageClientConfig.read_path_mapping()
    if path_mapping:
        # Look for a matching mapping
        possible_mapping = path_mapping.find_mapping(url)
        if possible_mapping:
            return possible_mapping  # return the profile name and path

    # For file paths, use the default POSIX profile
    if url.startswith("file://"):
        pr = urlparse(url)
        return DEFAULT_POSIX_PROFILE_NAME, _build_full_path(url, pr)
    elif url.startswith("/"):
        url = os.path.normpath(url)
        return DEFAULT_POSIX_PROFILE_NAME, url

    # For other URL protocol, create an implicit profile name
    pr = urlparse(url)
    protocol = pr.scheme.lower()

    # Translate relative paths to absolute paths
    if not protocol:
        return DEFAULT_POSIX_PROFILE_NAME, os.path.realpath(url)

    # Validate the protocol is supported
    if protocol not in SUPPORTED_IMPLICIT_PROFILE_PROTOCOLS:
        supported_protocols = ", ".join([f"{p}://" for p in SUPPORTED_IMPLICIT_PROFILE_PROTOCOLS])
        raise ValueError(
            f'Unknown URL "{url}", expecting "{MSC_PROTOCOL}" or a supported protocol ({supported_protocols}) or a POSIX path'
        )

    # Build the implicit profile name using the format _protocol-bucket
    bucket = pr.netloc
    if not bucket:
        raise ValueError(f'Invalid URL "{url}", bucket name is required for {protocol}:// URLs')

    profile_name = f"_{protocol}-{bucket}"

    # Return normalized path with leading slash removed
    path = pr.path
    if path.startswith("/"):
        path = path[1:]

    return profile_name, path


def resolve_storage_client(url: str) -> tuple[StorageClient, str]:
    """
    Build and return a :py:class:`multistorageclient.StorageClient` instance based on the provided URL or path.

    This function parses the given URL or path and determines the appropriate storage profile and path.
    It supports URLs with the protocol ``msc://``, as well as POSIX paths or ``file://`` URLs for local file
    system access. If the profile has already been instantiated, it returns the cached client. Otherwise,
    it creates a new :py:class:`StorageClient` and caches it.

    The function also supports implicit profiles for non-MSC URLs. When a non-MSC URL is provided (like s3://,
    gs://, ais://, file://), MSC will infer the storage provider based on the URL protocol and create an implicit
    profile with the naming convention "_protocol-bucket" (e.g., "_s3-bucket1", "_gs-bucket1").

    Path mapping defined in the MSC configuration are also applied before creating implicit profiles.
    This allows for explicit mappings between source paths and destination MSC profiles.

    :param url: The storage location, which can be:
                - A URL in the format ``msc://profile/path`` for object storage.
                - A local file system path (absolute POSIX path) or a ``file://`` URL.
                - A non-MSC URL with a supported protocol (s3://, gs://, ais://).

    :return: A tuple containing the :py:class:`multistorageclient.StorageClient` instance and the parsed path.

    :raises ValueError: If the URL's protocol is neither ``msc`` nor a valid local file system path
                        or a supported non-MSC protocol.
    """
    global _STORAGE_CLIENT_CACHE
    global _STORAGE_CLIENT_CACHE_LOCK

    # Normalize the path for msc:/ prefix due to pathlib.Path('msc://')
    if url.startswith("msc:/") and not url.startswith("msc://"):
        url = url.replace("msc:/", "msc://")

    # Resolve the URL to a profile name and path
    profile, path = _resolve_msc_url(url) if url.startswith(MSC_PROTOCOL) else _resolve_non_msc_url(url)

    # Check if the profile has already been instantiated
    if profile in _STORAGE_CLIENT_CACHE:
        return _STORAGE_CLIENT_CACHE[profile], path

    # Create a new StorageClient instance and cache it
    with _STORAGE_CLIENT_CACHE_LOCK:
        if profile in _STORAGE_CLIENT_CACHE:
            return _STORAGE_CLIENT_CACHE[profile], path
        else:
            client = StorageClient(config=StorageClientConfig.from_file(profile=profile, telemetry=get_telemetry()))
            _STORAGE_CLIENT_CACHE[profile] = client

    return client, path


def open(url: str, mode: str = "rb", **kwargs: Any) -> Union[PosixFile, ObjectFile]:
    """
    Open a file at the given URL using the specified mode.

    The function utilizes the :py:class:`multistorageclient.StorageClient` to open a file at the provided path.
    The URL is parsed, and the corresponding :py:class:`multistorageclient.StorageClient` is retrieved or built.

    :param url: The URL of the file to open. (example: ``msc://profile/prefix/dataset.tar``)
    :param mode: The file mode to open the file in.

    :return: A file-like object that allows interaction with the file.

    :raises ValueError: If the URL's protocol does not match the expected protocol ``msc``.
    """
    client, path = resolve_storage_client(url)
    return client.open(path, mode, **kwargs)


def glob(pattern: str, attribute_filter_expression: Optional[str] = None) -> list[str]:
    """
    Return a list of files matching a pattern.

    This function supports glob-style patterns for matching multiple files within a storage system. The pattern is
    parsed, and the associated :py:class:`multistorageclient.StorageClient` is used to retrieve the
    list of matching files.

    :param pattern: The glob-style pattern to match files. (example: ``msc://profile/prefix/**/*.tar``)
    :param attribute_filter_expression: The attribute filter expression to apply to the result.

    :return: A list of file paths matching the pattern.

    :raises ValueError: If the URL's protocol does not match the expected protocol ``msc``.
    """
    client, path = resolve_storage_client(pattern)
    if not pattern.startswith(MSC_PROTOCOL) and client.profile == DEFAULT_POSIX_PROFILE_NAME:
        return client.glob(path, include_url_prefix=False, attribute_filter_expression=attribute_filter_expression)
    else:
        return client.glob(path, include_url_prefix=True, attribute_filter_expression=attribute_filter_expression)


def upload_file(url: str, local_path: str, attributes: Optional[dict[str, str]] = None) -> None:
    """
    Upload a file to the given URL from a local path.

    The function utilizes the :py:class:`multistorageclient.StorageClient` to upload a file (object) to the
    provided path. The URL is parsed, and the corresponding :py:class:`multistorageclient.StorageClient`
    is retrieved or built.

    :param url: The URL of the file. (example: ``msc://profile/prefix/dataset.tar``)
    :param local_path: The local path of the file.

    :raises ValueError: If the URL's protocol does not match the expected protocol ``msc``.
    """
    client, path = resolve_storage_client(url)
    return client.upload_file(remote_path=path, local_path=local_path, attributes=attributes)


def download_file(url: str, local_path: str) -> None:
    """
    Download a file in a given remote_path to a local path

    The function utilizes the :py:class:`multistorageclient.StorageClient` to download a file (object) at the
    provided path. The URL is parsed, and the corresponding :py:class:`multistorageclient.StorageClient`
    is retrieved or built.

    :param url: The URL of the file to download. (example: ``msc://profile/prefix/dataset.tar``)
    :param local_path: The local path where the file should be downloaded.

    :raises ValueError: If the URL's protocol does not match the expected protocol ``msc``.
    """
    client, path = resolve_storage_client(url)
    return client.download_file(remote_path=path, local_path=local_path)


def is_empty(url: str) -> bool:
    """
    Checks whether the specified URL contains any objects.

    :param url: The URL to check, typically pointing to a storage location.
    :return: ``True`` if there are no objects/files under this URL, ``False`` otherwise.

    :raises ValueError: If the URL's protocol does not match the expected protocol ``msc``.
    """
    client, path = resolve_storage_client(url)
    return client.is_empty(path)


def is_file(url: str) -> bool:
    """
    Checks whether the specified url points to a file (rather than a directory or folder).

    The function utilizes the :py:class:`multistorageclient.StorageClient` to check if a file (object) exists
    at the provided path. The URL is parsed, and the corresponding :py:class:`multistorageclient.StorageClient`
    is retrieved or built.

    :param url: The URL to check the existence of a file. (example: ``msc://profile/prefix/dataset.tar``)
    """
    client, path = resolve_storage_client(url)
    return client.is_file(path=path)


def sync(
    source_url: str,
    target_url: str,
    delete_unmatched_files: bool = False,
    execution_mode: ExecutionMode = ExecutionMode.LOCAL,
) -> None:
    """
    Syncs files from the source storage to the target storage.

    :param source_url: The URL for the source storage.
    :param target_url: The URL for the target storage.
    :param delete_unmatched_files: Whether to delete files at the target that are not present at the source.
    :param execution_mode: The execution mode to use. Currently supports "local" and "ray".
    """
    source_client, source_path = resolve_storage_client(source_url)
    target_client, target_path = resolve_storage_client(target_url)
    target_client.sync_from(
        source_client, source_path, target_path, delete_unmatched_files, execution_mode=execution_mode
    )


def sync_replicas(
    source_url: str,
    replica_indices: Optional[list[int]] = None,
    delete_unmatched_files: bool = False,
    execution_mode: ExecutionMode = ExecutionMode.LOCAL,
) -> None:
    """
    Syncs files from the source storage to all the replicas.

    :param source_url: The URL for the source storage.
    :param replica_indices: Specify the indices of the replicas to sync to. If not provided, all replicas will be synced. Index starts from 0.
    :param delete_unmatched_files: Whether to delete files at the replicas that are not present at the source.
    :param execution_mode: The execution mode to use. Currently supports "local" and "ray".
    """
    source_client, source_path = resolve_storage_client(source_url)
    source_client.sync_replicas(
        source_path,
        replica_indices=replica_indices,
        delete_unmatched_files=delete_unmatched_files,
        execution_mode=execution_mode,
    )


def list(
    url: str,
    start_after: Optional[str] = None,
    end_at: Optional[str] = None,
    include_directories: bool = False,
    attribute_filter_expression: Optional[str] = None,
    show_attributes: bool = False,
) -> Iterator[ObjectMetadata]:
    """
    Lists the contents of the specified URL prefix.

    This function retrieves the corresponding :py:class:`multistorageclient.StorageClient`
    for the given URL and returns an iterator of objects (files or directories) stored under the provided prefix.

    :param url: The prefix to list objects under.
    :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
    :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.
    :param include_directories: Whether to include directories in the result. When True, directories are returned alongside objects.
    :param attribute_filter_expression: The attribute filter expression to apply to the result.

    :return: An iterator of :py:class:`ObjectMetadata` objects representing the files (and optionally directories)
             accessible under the specified URL prefix. The returned keys will always be prefixed with msc://.
    """
    client, path = resolve_storage_client(url)
    return client.list(
        path=path,
        start_after=start_after,
        end_at=end_at,
        include_directories=include_directories,
        include_url_prefix=True,
        attribute_filter_expression=attribute_filter_expression,
        show_attributes=show_attributes,
    )


def write(url: str, body: bytes, attributes: Optional[dict[str, str]] = None) -> None:
    """
    Writes an object to the storage provider at the specified path.

    :param url: The path where the object should be written.
    :param body: The content to write to the object.
    """
    client, path = resolve_storage_client(url)
    client.write(path=path, body=body, attributes=attributes)


def delete(url: str, recursive: bool = False) -> None:
    """
    Deletes the specified object(s) from the storage provider.

    This function retrieves the corresponding :py:class:`multistorageclient.StorageClient`
    for the given URL and deletes the object(s) at the specified path.

    :param url: The URL of the object to delete. (example: ``msc://profile/prefix/file.txt``)
    :param recursive: Whether to delete objects in the path recursively.
    """
    client, path = resolve_storage_client(url)
    client.delete(path, recursive=recursive)


def info(url: str) -> ObjectMetadata:
    """
    Retrieves metadata or information about an object stored at the specified path.

    :param url: The URL of the object to retrieve information about. (example: ``msc://profile/prefix/file.txt``)

    :return: An :py:class:`ObjectMetadata` object representing the object's metadata.
    """
    client, path = resolve_storage_client(url)
    return client.info(path)


def commit_metadata(url: str) -> None:
    """
    Commits the metadata updates for the specified storage client profile.

    :param url: The URL of the path to commit metadata for.
    """
    client, path = resolve_storage_client(url)
    client.commit_metadata(prefix=path)
