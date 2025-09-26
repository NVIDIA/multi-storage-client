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
import shutil
import tempfile
from collections.abc import Iterator, Sequence
from typing import IO, Optional, Union

import opentelemetry.metrics as api_metrics
from huggingface_hub import HfApi
from huggingface_hub.errors import HfHubHTTPError, RepositoryNotFoundError, RevisionNotFoundError

from ..telemetry import Telemetry
from ..telemetry.attributes.base import AttributesProvider
from ..types import (
    Credentials,
    CredentialsProvider,
    ObjectMetadata,
    Range,
)
from .base import BaseStorageProvider

PROVIDER = "huggingface"


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
            secret_key="",  # HF only uses access token
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
        metric_counters: dict[Telemetry.CounterName, api_metrics.Counter] = {},
        metric_gauges: dict[Telemetry.GaugeName, api_metrics._Gauge] = {},
        metric_attributes_providers: Sequence[AttributesProvider] = (),
    ):
        """
        Initializes the :py:class:`HuggingFaceStorageProvider` with repository information and optional credentials provider.

        :param repository_id: The HuggingFace repository ID (e.g., 'username/repo-name').
        :param repo_type: The type of repository ('dataset', 'model', 'space'). Defaults to 'model'.
        :param base_path: The root prefix path within the repository where all operations will be scoped.
        :param repo_revision: The git revision (branch, tag, or commit) to use. Defaults to 'main'.
        :param credentials_provider: The provider to retrieve HuggingFace credentials.
        :param metric_counters: Metric counters.
        :param metric_gauges: Metric gauges.
        :param metric_attributes_providers: Metric attributes providers.
        """

        # Validate repo_type
        allowed_repo_types = {"dataset", "model", "space"}
        if repo_type not in allowed_repo_types:
            raise ValueError(f"Invalid repo_type '{repo_type}'. Must be one of: {allowed_repo_types}")

        # Validate repository_id format
        if not repository_id or "/" not in repository_id:
            raise ValueError(f"Invalid repository_id '{repository_id}'. Expected format: 'username/repo-name'")

        super().__init__(
            base_path=base_path,
            provider_name=PROVIDER,
            metric_counters=metric_counters,
            metric_gauges=metric_gauges,
            metric_attributes_providers=metric_attributes_providers,
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

    # Abstract method implementations - minimal stubs for now
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

        try:
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

        except (RepositoryNotFoundError, RevisionNotFoundError) as e:
            raise FileNotFoundError(
                f"Repository or revision not found: {self._repository_id}@{self._repo_revision}"
            ) from e
        except HfHubHTTPError as e:
            raise RuntimeError(f"HuggingFace API error during upload of {path}: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error during upload of {path}: {e}") from e

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

        try:
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

        except (RepositoryNotFoundError, RevisionNotFoundError) as e:
            raise FileNotFoundError(f"File not found in HuggingFace repository: {path}") from e
        except HfHubHTTPError as e:
            raise RuntimeError(f"HuggingFace API error during download of {path}: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error during download of {path}: {e}") from e

    def _copy_object(self, src_path: str, dest_path: str) -> int:
        """
        Copies an object within the HuggingFace repository.

        :param src_path: The source path of the object to copy.
        :param dest_path: The destination path for the copied object.
        :return: Data size in bytes.
        :raises NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("HuggingFace provider not fully implemented yet")

    def _delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        """
        Deletes an object from the HuggingFace repository.

        :param path: The path of the object to delete from the repository.
        :param if_match: Optional ETag for conditional deletion (not supported by HuggingFace).
        :raises NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("HuggingFace provider not fully implemented yet")

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        """
        Retrieves metadata for an object in the HuggingFace repository.

        :param path: The path of the object to get metadata for.
        :param strict: Whether to raise an error if the object doesn't exist.
        :return: Metadata about the object.
        :raises NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("HuggingFace provider not fully implemented yet")

    def _list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
        """
        Lists objects in the HuggingFace repository under the specified path.

        :param path: The path to list objects under.
        :param start_after: The key to start listing after (not supported by HuggingFace).
        :param end_at: The key to end listing at (not supported by HuggingFace).
        :param include_directories: Whether to include directories in the listing.
        :return: An iterator over object metadata for objects under the specified path.
        :raises NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("HuggingFace provider not fully implemented yet")

    def _upload_file(self, remote_path: str, f: Union[str, IO], attributes: Optional[dict[str, str]] = None) -> int:
        """
        Uploads a file to the HuggingFace repository.

        :param remote_path: The remote path where the file will be stored in the repository.
        :param f: File path or file object to upload.
        :param attributes: Optional attributes for the file (not supported by HuggingFace).
        :return: Data size in bytes.
        :raises RuntimeError: If HuggingFace client is not initialized or API errors occur.
        :raises ValueError: If custom attributes are provided (not supported).
        """
        if not self._hf_client:
            raise RuntimeError("HuggingFace client not initialized")

        if attributes is not None:
            raise ValueError(
                "HuggingFace provider does not support custom file attributes. "
                "Use commit messages or repository metadata instead."
            )

        try:
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

        except (RepositoryNotFoundError, RevisionNotFoundError) as e:
            raise FileNotFoundError(
                f"Repository or revision not found: {self._repository_id}@{self._repo_revision}"
            ) from e
        except HfHubHTTPError as e:
            raise RuntimeError(f"HuggingFace API error during upload of {remote_path}: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error during upload of {remote_path}: {e}") from e

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

        try:
            if isinstance(f, str):
                if os.path.dirname(f):
                    os.makedirs(os.path.dirname(f), exist_ok=True)

                target_dir = os.path.dirname(f)

                downloaded_path = self._hf_client.hf_hub_download(
                    repo_id=self._repository_id,
                    filename=remote_path,
                    repo_type=self._repo_type,
                    revision=self._repo_revision,
                    local_dir=target_dir if target_dir else None,
                )

                if os.path.abspath(downloaded_path) != os.path.abspath(f):
                    shutil.move(downloaded_path, f)

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

        except (RepositoryNotFoundError, RevisionNotFoundError) as e:
            raise FileNotFoundError(f"File not found in HuggingFace repository: {remote_path}") from e
        except HfHubHTTPError as e:
            raise RuntimeError(f"HuggingFace API error during download: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error during download: {e}") from e
