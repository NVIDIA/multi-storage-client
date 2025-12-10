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

import importlib.util
import io
import os
import tempfile
from collections.abc import Callable, Iterator
from typing import IO, Any, Optional, TypeVar, Union

from huggingface_hub import CommitOperationCopy, HfApi
from huggingface_hub.errors import EntryNotFoundError, HfHubHTTPError, RepositoryNotFoundError, RevisionNotFoundError
from huggingface_hub.hf_api import RepoFile, RepoFolder

from ..telemetry import Telemetry
from ..types import AWARE_DATETIME_MIN, Credentials, CredentialsProvider, ObjectMetadata, Range, RetryableError
from .base import BaseStorageProvider

_T = TypeVar("_T")

PROVIDER = "huggingface"

HF_TRANSFER_UNAVAILABLE_ERROR_MESSAGE = (
    "Fast transfer using 'hf_transfer' is enabled (HF_HUB_ENABLE_HF_TRANSFER=1) "
    "but 'hf_transfer' package is not available in your environment. "
    "Either install hf_transfer with 'pip install hf_transfer' or "
    "disable it by setting HF_HUB_ENABLE_HF_TRANSFER=0"
)


class HuggingFaceCredentialsProvider(CredentialsProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.CredentialsProvider` that provides HuggingFace credentials.
    """

    def __init__(self, access_token: str):
        """
        Initializes the :py:class:`HuggingFaceCredentialsProvider` with the provided access token.

        :param access_token: The HuggingFace access token for authentication.
        """
        self.token = access_token

    def get_credentials(self) -> Credentials:
        """
        Retrieves the current HuggingFace credentials.

        :return: The current credentials used for HuggingFace authentication.
        """
        return Credentials(
            access_key="",
            secret_key="",
            token=self.token,
            expiration=None,
        )

    def refresh_credentials(self) -> None:
        """
        Refreshes the credentials if they are expired or about to expire.

        Note: HuggingFace tokens typically don't expire, so this is a no-op.
        """
        pass


class HuggingFaceStorageProvider(BaseStorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with HuggingFace Hub repositories.
    """

    def __init__(
        self,
        repository_id: str,
        repo_type: str = "model",
        base_path: str = "",
        repo_revision: str = "main",
        credentials_provider: Optional[CredentialsProvider] = None,
        config_dict: Optional[dict[str, Any]] = None,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
    ):
        """
        Initializes the :py:class:`HuggingFaceStorageProvider` with repository information and optional credentials provider.

        :param repository_id: The HuggingFace repository ID (e.g., 'username/repo-name').
        :param repo_type: The type of repository ('dataset', 'model', 'space'). Defaults to 'model'.
        :param base_path: The root prefix path within the repository where all operations will be scoped.
        :param repo_revision: The git revision (branch, tag, or commit) to use. Defaults to 'main'.
        :param credentials_provider: The provider to retrieve HuggingFace credentials.
        :param config_dict: Resolved MSC config.
        :param telemetry_provider: A function that provides a telemetry instance.
        """

        # Validate repo_type
        allowed_repo_types = {"dataset", "model", "space"}
        if repo_type not in allowed_repo_types:
            raise ValueError(f"Invalid repo_type '{repo_type}'. Must be one of: {allowed_repo_types}")

        # Validate repository_id format
        if not repository_id or "/" not in repository_id:
            raise ValueError(f"Invalid repository_id '{repository_id}'. Expected format: 'username/repo-name'")

        self._validate_hf_transfer_availability()

        super().__init__(
            base_path=base_path,
            provider_name=PROVIDER,
            config_dict=config_dict,
            telemetry_provider=telemetry_provider,
        )

        self._repository_id = repository_id
        self._repo_type = repo_type
        self._repo_revision = repo_revision
        self._credentials_provider = credentials_provider

        self._hf_client: HfApi = self._create_hf_api_client()

    def _create_hf_api_client(self) -> HfApi:
        """
        Creates and configures the HuggingFace API client.

        Initializes the HfApi client with authentication token if credentials are provided,
        otherwise creates an unauthenticated client for public repositories.

        :return: Configured HfApi client instance.
        """

        token = None
        if self._credentials_provider:
            creds = self._credentials_provider.get_credentials()
            token = creds.token

        return HfApi(token=token)

    def _validate_hf_transfer_availability(self) -> None:
        """
        Validates that hf_transfer is available if it's enabled via environment variables.

        Raises:
            ValueError: If hf_transfer is enabled but not available.
        """
        # Check if hf_transfer is enabled via environment variable
        hf_transfer_enabled = os.environ.get("HF_HUB_ENABLE_HF_TRANSFER", "").lower() in ("1", "on", "true", "yes")

        if hf_transfer_enabled and importlib.util.find_spec("hf_transfer") is None:
            raise ValueError(HF_TRANSFER_UNAVAILABLE_ERROR_MESSAGE)

    def _parse_rate_limit_headers(self, response) -> str:
        """
        Parses HuggingFace rate limit headers and returns formatted information.

        HuggingFace returns rate limit information in these headers:
        - RateLimit: "api";r=0;t=142
          - r = requests remaining in the current window
          - t = seconds until rate limit resets
        - RateLimit-Policy: "fixed window";"api";q=10000;w=300
          - q = total requests allowed per window
          - w = window size in seconds

        Reference: https://huggingface.co/docs/hub/rate-limits

        :param response: The HTTP response object containing rate limit headers.
        :return: Formatted string with rate limit information, or empty string if headers not found.
        """

        try:
            headers = response.headers
        except Exception:
            return ""

        rate_limit_info = []

        # Note: HTTP headers are case-insensitive, but we use the canonical casing from HF docs
        if "RateLimit" in headers:
            rate_limit = headers["RateLimit"]
            # Extract r (remaining) and t (time until reset)
            remaining = None
            reset_seconds = None

            parts = rate_limit.split(";")
            for part in parts:
                part = part.strip()
                if part.startswith("r="):
                    try:
                        remaining = int(part[2:])
                    except ValueError:
                        pass
                elif part.startswith("t="):
                    try:
                        reset_seconds = int(part[2:])
                    except ValueError:
                        pass

            if remaining is not None:
                rate_limit_info.append(f"Requests remaining in current window: {remaining}")
            if reset_seconds is not None:
                rate_limit_info.append(f"Rate limit resets in: {reset_seconds} seconds")

        if "RateLimit-Policy" in headers:
            policy = headers["RateLimit-Policy"]
            # Extract q (quota) and w (window size)
            quota = None
            window_seconds = None

            parts = policy.split(";")
            for part in parts:
                part = part.strip()
                if part.startswith("q="):
                    try:
                        quota = int(part[2:])
                    except ValueError:
                        pass
                elif part.startswith("w="):
                    try:
                        window_seconds = int(part[2:])
                    except ValueError:
                        pass

            if quota is not None and window_seconds is not None:
                window_minutes = window_seconds / 60
                rate_limit_info.append(f"Rate limit policy: {quota} requests per {window_minutes:.0f}-minute window")

        if rate_limit_info:
            return " | ".join(rate_limit_info)

        return ""

    def _translate_errors(
        self,
        func: Callable[[], _T],
        operation: str,
        repo_id: str,
        path: str,
    ) -> _T:
        """
        Translates HuggingFace errors into standardized exceptions with retry logic.

        Parses HuggingFace rate limit headers (RateLimit and RateLimit-Policy) to provide
        detailed information about rate limiting to users. See https://huggingface.co/docs/hub/rate-limits

        :param func: The function that performs the actual HuggingFace operation.
        :param operation: The type of operation being performed (e.g., "upload", "download", "delete").
        :param repo_id: The HuggingFace repository ID.
        :param path: The path of the object within the repository.
        :return: The result of the HuggingFace operation.
        :raises RetryableError: For transient errors that can be retried (429, 503, connection errors).
        :raises FileNotFoundError: When the requested resource is not found.
        :raises RuntimeError: For other non-retryable errors.
        """
        try:
            return func()
        except RepositoryNotFoundError as error:
            raise FileNotFoundError(
                f"Repository not found or access denied: {repo_id}. "
                f"Verify the repository exists and you have access permissions."
            ) from error
        except RevisionNotFoundError as error:
            raise FileNotFoundError(
                f"Revision '{self._repo_revision}' not found in repository {repo_id}. "
                f"Verify the branch, tag, or commit exists."
            ) from error
        except EntryNotFoundError as error:
            raise FileNotFoundError(f"File not found in HuggingFace repository: {path}") from error
        except FileNotFoundError:
            raise
        except HfHubHTTPError as error:
            # Extract status code and parse rate limit headers
            # Don't use hasattr() - it's unreliable with response objects
            status_code = None
            response = None

            try:
                response = error.response
                if response is not None:
                    status_code = response.status_code
            except AttributeError:
                pass

            rate_limit_info = self._parse_rate_limit_headers(response)
            quota_suffix = f" | {rate_limit_info}" if rate_limit_info else ""

            error_info = f"repo_id: {repo_id}, path: {path}, status_code: {status_code}, error: {error}"

            if status_code == 404:
                raise FileNotFoundError(f"Object {repo_id}/{path} does not exist. {error_info}") from error
            elif status_code == 409:
                raise RetryableError(f"Conflict Error for {repo_id}. {error_info}{quota_suffix}") from error
            elif status_code == 429:
                base_message = f"Rate limit exceeded when {operation} object(s) at {repo_id}/{path}. {error_info}"
                raise RetryableError(f"{base_message}{quota_suffix}") from error
            elif status_code == 503:
                raise RetryableError(
                    f"Service unavailable when {operation} object(s) at {repo_id}/{path}. {error_info}{quota_suffix}"
                ) from error
            elif status_code in (408, 500, 502, 504):
                raise RetryableError(
                    f"Transient error ({status_code}) when {operation} object(s) at {repo_id}/{path}. {error_info}{quota_suffix}"
                ) from error
            else:
                raise RuntimeError(
                    f"HuggingFace API error during {operation} of {path}: {error}{quota_suffix}"
                ) from error
        except (ConnectionError, TimeoutError, OSError) as error:
            raise RetryableError(
                f"Connection error when {operation} object(s) at {repo_id}/{path}, error type: {type(error).__name__}"
            ) from error
        except Exception as error:
            raise RuntimeError(f"Unexpected error during {operation} of {path}: {error}") from error

    def _put_object(
        self,
        path: str,
        body: bytes,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
    ) -> int:
        """
        Uploads an object to the HuggingFace repository.

        :param path: The path where the object will be stored in the repository.
        :param body: The content of the object to store.
        :param if_match: Optional ETag for conditional uploads (not supported by HuggingFace).
        :param if_none_match: Optional ETag for conditional uploads (not supported by HuggingFace).
        :param attributes: Optional attributes for the object (not supported by HuggingFace).
        :return: Data size in bytes.
        :raises RuntimeError: If HuggingFace client is not initialized or API errors occur.
        :raises ValueError: If client attempts to create a directory.
        :raises ValueError: If conditional upload parameters are provided (not supported).
        """
        if not self._hf_client:
            raise RuntimeError("HuggingFace client not initialized")

        if if_match is not None or if_none_match is not None:
            raise ValueError(
                "HuggingFace provider does not support conditional uploads. "
                "if_match and if_none_match parameters are not supported."
            )

        if attributes is not None:
            raise ValueError(
                "HuggingFace provider does not support custom object attributes. "
                "Use commit messages or repository metadata instead."
            )

        if path.endswith("/"):
            raise ValueError(
                "HuggingFace Storage Provider does not support explicit directory creation. "
                "Directories are created implicitly when files are uploaded to paths within them."
            )

        path = self._normalize_path(path)

        def _invoke_api():
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(body)
                temp_file_path = temp_file.name

            try:
                self._hf_client.upload_file(
                    path_or_fileobj=temp_file_path,
                    path_in_repo=path,
                    repo_id=self._repository_id,
                    repo_type=self._repo_type,
                    revision=self._repo_revision,
                    commit_message=f"Upload {path}",
                    commit_description=None,
                    create_pr=False,
                )

                return len(body)

            finally:
                os.unlink(temp_file_path)

        return self._translate_errors(_invoke_api, "PUT", self._repository_id, path)

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        """
        Retrieves an object from the HuggingFace repository.

        :param path: The path of the object to retrieve from the repository.
        :param byte_range: Optional byte range for partial content (not supported by HuggingFace).
        :return: The content of the retrieved object.
        :raises RuntimeError: If HuggingFace client is not initialized or API errors occur.
        :raises ValueError: If a byte range is requested (HuggingFace doesn't support range reads).
        :raises FileNotFoundError: If the file doesn't exist in the repository.
        """

        if not self._hf_client:
            raise RuntimeError("HuggingFace client not initialized")

        if byte_range is not None:
            raise ValueError(
                "HuggingFace provider does not support partial range reads. "
                f"Requested range: offset={byte_range.offset}, size={byte_range.size}. "
                "To read the entire file, call get_object() without the byte_range parameter."
            )

        path = self._normalize_path(path)

        def _invoke_api():
            with tempfile.TemporaryDirectory() as temp_dir:
                downloaded_path = self._hf_client.hf_hub_download(
                    repo_id=self._repository_id,
                    filename=path,
                    repo_type=self._repo_type,
                    revision=self._repo_revision,
                    local_dir=temp_dir,
                )

                with open(downloaded_path, "rb") as f:
                    data = f.read()

                return data

        return self._translate_errors(_invoke_api, "GET", self._repository_id, path)

    def _copy_object(self, src_path: str, dest_path: str) -> int:
        """
        Copies an object within the HuggingFace repository using server-side copy.

        .. note::
            Copy behavior is size-dependent: files ≥10MB are copied remotely via
            metadata (LFS), while files <10MB are downloaded and re-uploaded.

        :param src_path: The source path of the object to copy.
        :param dest_path: The destination path for the copied object.
        :return: Data size in bytes.
        :raises RuntimeError: If HuggingFace client is not initialized or API errors occur.
        :raises FileNotFoundError: If the source file doesn't exist.
        """
        if not self._hf_client:
            raise RuntimeError("HuggingFace client not initialized")

        src_path = self._normalize_path(src_path)
        dest_path = self._normalize_path(dest_path)

        src_object = self._get_object_metadata(src_path)

        def _invoke_api():
            operations = [
                CommitOperationCopy(
                    src_path_in_repo=src_path,
                    path_in_repo=dest_path,
                )
            ]

            self._hf_client.create_commit(
                repo_id=self._repository_id,
                operations=operations,
                commit_message=f"Copy {src_path} to {dest_path}",
                repo_type=self._repo_type,
                revision=self._repo_revision,
            )

            return src_object.content_length

        return self._translate_errors(_invoke_api, "COPY", self._repository_id, f"{src_path} to {dest_path}")

    def _delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        """
        Deletes an object from the HuggingFace repository.

        :param path: The path of the object to delete from the repository.
        :param if_match: Optional ETag for conditional deletion (not supported by HuggingFace).
        :raises RuntimeError: If HuggingFace client is not initialized or API errors occur.
        :raises ValueError: If conditional deletion parameters are provided (not supported).
        :raises FileNotFoundError: If the file doesn't exist in the repository.
        """
        if not self._hf_client:
            raise RuntimeError("HuggingFace client not initialized")

        if if_match is not None:
            raise ValueError(
                "HuggingFace provider does not support conditional deletion. if_match parameter is not supported."
            )

        path = self._normalize_path(path)

        def _invoke_api():
            self._hf_client.delete_file(
                path_in_repo=path,
                repo_id=self._repository_id,
                repo_type=self._repo_type,
                revision=self._repo_revision,
                commit_message=f"Delete {path}",
            )

        self._translate_errors(_invoke_api, "DELETE", self._repository_id, path)

    def _item_to_metadata(self, item: Union[RepoFile, RepoFolder]) -> ObjectMetadata:
        """
        Convert a RepoFile or RepoFolder into ObjectMetadata.

        :param item: The RepoFile or RepoFolder item from HuggingFace API.
        :return: ObjectMetadata representing the item.
        """
        last_modified = AWARE_DATETIME_MIN

        if isinstance(item, RepoFile):
            etag = item.blob_id
            return ObjectMetadata(
                key=item.path,
                type="file",
                content_length=item.size,
                last_modified=last_modified,
                etag=etag,
                content_type=None,
                storage_class=None,
                metadata=None,
            )
        else:
            etag = item.tree_id
            return ObjectMetadata(
                key=item.path,
                type="directory",
                content_length=0,
                last_modified=last_modified,
                etag=etag,
                content_type=None,
                storage_class=None,
                metadata=None,
            )

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        """
        Retrieves metadata for an object in the HuggingFace repository.

        :param path: The path of the object to get metadata for.
        :param strict: Whether to raise an error if the object doesn't exist.
        :return: Metadata about the object.
        :raises RuntimeError: If HuggingFace client is not initialized or API errors occur.
        :raises FileNotFoundError: If the file doesn't exist and strict=True.
        """
        if not self._hf_client:
            raise RuntimeError("HuggingFace client not initialized")

        path = self._normalize_path(path)

        def _invoke_api():
            items = self._hf_client.get_paths_info(
                repo_id=self._repository_id,
                paths=[path],
                repo_type=self._repo_type,
                revision=self._repo_revision,
                expand=True,
            )

            if not items:
                raise FileNotFoundError(f"File not found in HuggingFace repository: {path}")

            item = items[0]
            return self._item_to_metadata(item)

        try:
            return self._translate_errors(_invoke_api, "HEAD", self._repository_id, path)
        except FileNotFoundError as error:
            if strict:
                dir_path = path.rstrip("/") + "/"
                if self._is_dir(dir_path):
                    return ObjectMetadata(
                        key=dir_path,
                        type="directory",
                        content_length=0,
                        last_modified=AWARE_DATETIME_MIN,
                        etag=None,
                        content_type=None,
                        storage_class=None,
                        metadata=None,
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
        """
        Lists objects in the HuggingFace repository under the specified path.

        :param path: The path to list objects under.
        :param start_after: The key to start listing after (exclusive, used as cursor).
        :param end_at: The key to end listing at (inclusive, used as cursor).
        :param include_directories: Whether to include directories in the listing.
        :return: An iterator over object metadata for objects under the specified path.
        :raises RuntimeError: If HuggingFace client is not initialized or API errors occur.

        .. note::
            HuggingFace Hub API does not natively support pagination parameters.
            This implementation fetches all items and uses cursor-based filtering,
            which may impact performance for large repositories. The ordering is
            directory-first, then files, with lexicographical ordering within each group.
        """
        if not self._hf_client:
            raise RuntimeError("HuggingFace client not initialized")

        path = self._normalize_path(path)

        try:
            metadata = self._get_object_metadata(path.rstrip("/"), strict=False)
            if metadata and metadata.type == "file":
                yield metadata
                return
        except FileNotFoundError:
            pass

        def _invoke_api():
            dir_path = path.rstrip("/")

            repo_items = self._hf_client.list_repo_tree(
                repo_id=self._repository_id,
                path_in_repo=dir_path + "/" if dir_path else None,
                repo_type=self._repo_type,
                revision=self._repo_revision,
                expand=True,
                recursive=not include_directories,
            )

            return list(repo_items)

        try:
            items = self._translate_errors(_invoke_api, "LIST", self._repository_id, path)

            # Use cursor-based pagination because HuggingFace returns items with
            # directory-first ordering (not pure lexicographical).
            seen_start = start_after is None
            seen_end = False

            for item in items:
                if seen_end:
                    break

                metadata = self._item_to_metadata(item)
                key = metadata.key

                if not seen_start:
                    if key == start_after:
                        seen_start = True
                    continue

                should_yield = False
                if include_directories and isinstance(item, RepoFolder):
                    should_yield = True
                elif isinstance(item, RepoFile):
                    should_yield = True

                if should_yield:
                    yield metadata

                if end_at is not None and key == end_at:
                    seen_end = True

        except FileNotFoundError:
            # Directory doesn't exist - return empty (matches POSIX behavior)
            pass

    def _upload_file(self, remote_path: str, f: Union[str, IO], attributes: Optional[dict[str, str]] = None) -> int:
        """
        Uploads a file to the HuggingFace repository.

        :param remote_path: The remote path where the file will be stored in the repository.
        :param f: File path or file object to upload.
        :param attributes: Optional attributes for the file (not supported by HuggingFace).
        :return: Data size in bytes.
        :raises RuntimeError: If HuggingFace client is not initialized or API errors occur.
        :raises ValueError: If client attempts to create a directory.
        :raises ValueError: If custom attributes are provided (not supported).
        """
        if not self._hf_client:
            raise RuntimeError("HuggingFace client not initialized")

        if attributes is not None:
            raise ValueError(
                "HuggingFace provider does not support custom file attributes. "
                "Use commit messages or repository metadata instead."
            )

        if remote_path.endswith("/"):
            raise ValueError(
                "HuggingFace Storage Provider does not support explicit directory creation. "
                "Directories are created implicitly when files are uploaded to paths within them."
            )

        remote_path = self._normalize_path(remote_path)

        def _invoke_api():
            if isinstance(f, str):
                file_size = os.path.getsize(f)

                self._hf_client.upload_file(
                    path_or_fileobj=f,
                    path_in_repo=remote_path,
                    repo_id=self._repository_id,
                    repo_type=self._repo_type,
                    revision=self._repo_revision,
                    commit_message=f"Upload {remote_path}",
                    commit_description=None,
                    create_pr=False,
                )

                return file_size

            else:
                content = f.read()

                if isinstance(content, str):
                    content_bytes = content.encode("utf-8")
                else:
                    content_bytes = content

                # Create temporary file since HfAPI.upload_file requires BinaryIO, not generic IO
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    temp_file.write(content_bytes)
                    temp_file_path = temp_file.name

                try:
                    self._hf_client.upload_file(
                        path_or_fileobj=temp_file_path,
                        path_in_repo=remote_path,
                        repo_id=self._repository_id,
                        repo_type=self._repo_type,
                        revision=self._repo_revision,
                        commit_message=f"Upload {remote_path}",
                        create_pr=False,
                    )

                    return len(content_bytes)

                finally:
                    os.unlink(temp_file_path)

        return self._translate_errors(_invoke_api, "PUT", self._repository_id, remote_path)

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> int:
        """
        Downloads a file from the HuggingFace repository.

        :param remote_path: The remote path of the file to download from the repository.
        :param f: Local file path or file object to write to.
        :param metadata: Optional object metadata (not used in this implementation).
        :return: Data size in bytes.
        """
        if not self._hf_client:
            raise RuntimeError("HuggingFace client not initialized")

        remote_path = self._normalize_path(remote_path)

        def _invoke_api():
            if isinstance(f, str):
                parent_dir = os.path.dirname(f)
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)

                target_dir = parent_dir if parent_dir else "."
                downloaded_path = self._hf_client.hf_hub_download(
                    repo_id=self._repository_id,
                    filename=remote_path,
                    repo_type=self._repo_type,
                    revision=self._repo_revision,
                    local_dir=target_dir,
                )

                if os.path.abspath(downloaded_path) != os.path.abspath(f):
                    os.rename(downloaded_path, f)

                return os.path.getsize(f)

            else:
                with tempfile.TemporaryDirectory() as temp_dir:
                    downloaded_path = self._hf_client.hf_hub_download(
                        repo_id=self._repository_id,
                        filename=remote_path,
                        repo_type=self._repo_type,
                        revision=self._repo_revision,
                        local_dir=temp_dir,
                    )

                    with open(downloaded_path, "rb") as src:
                        data = src.read()
                        if isinstance(f, io.TextIOBase):
                            f.write(data.decode("utf-8"))
                        else:
                            f.write(data)

                        return len(data)

        return self._translate_errors(_invoke_api, "GET", self._repository_id, remote_path)

    def _is_dir(self, path: str) -> bool:
        """
        Helper method to check if a path is a directory.

        :param path: The path to check.
        :return: True if the path appears to be a directory (has files under it).
        """
        path = path.rstrip("/")
        if not path:
            # The root of the repo is always a directory
            return True

        try:
            path_info = self._hf_client.get_paths_info(
                repo_id=self._repository_id,
                paths=[path],
                repo_type=self._repo_type,
                revision=self._repo_revision,
            )

            if not path_info:
                return False

            return isinstance(path_info[0], RepoFolder)

        except RepositoryNotFoundError as e:
            raise FileNotFoundError(
                f"Repository not found or access denied: {self._repository_id}. "
                f"Verify the repository exists and you have access permissions."
            ) from e
        except RevisionNotFoundError as e:
            raise FileNotFoundError(
                f"Revision '{self._repo_revision}' not found in repository {self._repository_id}. "
                f"Verify the branch, tag, or commit exists."
            ) from e
        except IndexError:
            return False
        except Exception as e:
            raise Exception(f"Unexpected error: {e}")

    def _normalize_path(self, path: str) -> str:
        """
        Normalize path for HuggingFace API by removing leading slashes.
        HuggingFace expects relative paths within repositories.
        """
        return path.lstrip("/")
