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

"""
Hydra ConfigSource plugin for Multi-Storage Client.

This module provides a ConfigSource implementation that allows Hydra to load
configuration files from remote storage systems using Multi-Storage Client.
"""

import logging
from typing import List, Optional

from hydra.core.config_search_path import ConfigSearchPath, SearchPathElement
from hydra.core.object_type import ObjectType
from hydra.plugins.config_source import ConfigLoadError, ConfigResult, ConfigSource
from hydra.plugins.search_path_plugin import SearchPathPlugin
from omegaconf import OmegaConf

import multistorageclient as msc
from multistorageclient.shortcuts import resolve_storage_client
from multistorageclient.types import MSC_PROTOCOL, MSC_PROTOCOL_NAME
from multistorageclient.utils import join_paths

logger = logging.getLogger(__name__)


class MSCConfigSource(ConfigSource):
    """
    A Hydra :py:class:`hydra.plugins.config_source.ConfigSource` that uses Multi-Storage Client to read configuration files from remote storage systems.

    Supports loading configs from S3, GCS, Azure Blob Storage, and other MSC-supported storage backends.
    Must be used in conjunction with :py:class:`MSCSearchPathPlugin`.
    """

    def __init__(self, provider: str, path: str) -> None:
        """
        Initialize the MSC ConfigSource.

        :param provider: The provider name (should be ``main`` if the user specifies config path, or ``msc-universal`` if the user doesn't specify a config path).
        :param path: The base path for this source. It should be a full MSC URL (e.g., ``msc://dev/configs``) if the user specifies config path, or ``msc://`` if the user doesn't specify a config path and the universal MSC source is used.
        """
        if path.find("://") == -1:
            path = f"{MSC_PROTOCOL}{path}"
        super().__init__(provider=provider, path=path)

        # Store the base URL for resolving relative paths
        self.base_url = path
        logger.debug(f"Initialized MSCConfigSource with base URL: {self.base_url}")

    @staticmethod
    def scheme() -> str:
        """
        Return the URL scheme for this ConfigSource.

        :return: The scheme string 'msc'.
        """
        return MSC_PROTOCOL_NAME

    def _resolve_full_url(self, config_path: str) -> str:
        """
        Convert config_path to a full ``msc://`` URL.

        :param config_path: Either a relative path, config group reference (e.g., ``database: postgres``), or full ``msc://`` URL.
        :return: Full ``msc://`` URL that can be passed to ``multistorageclient.open()``.
        """
        if config_path.startswith(MSC_PROTOCOL):
            return config_path

        # Handle Hydra defaults syntax: "group: config" -> "group/config"
        if ": " in config_path:
            group, config_name = config_path.split(": ", 1)
            config_path = f"{group}/{config_name}"

        # Relative path - join with base URL using MSC's utility
        return join_paths(self.base_url, config_path)

    def load_config(self, config_path: str) -> ConfigResult:
        """
        Load a configuration file from MSC storage.

        :param config_path: Relative path to the config file, or full ``msc://`` URL.
        :return: The loaded configuration.
        :raises ConfigLoadError: If the config file cannot be loaded.
        """
        full_url = self._resolve_full_url(config_path)
        full_url = self._normalize_file_name(full_url)

        try:
            with msc.open(full_url, "r") as f:
                header_text = f.read(512)
                header = ConfigSource._get_header_dict(header_text)
                f.seek(0)
                cfg = OmegaConf.load(f)
                return ConfigResult(
                    provider=self.provider,
                    path=f"{self.scheme()}://{self.path}",
                    config=cfg,
                    header=header,
                )
        except Exception as e:
            raise ConfigLoadError(f"Failed to load config from {full_url}: {e}")

    def available(self) -> bool:
        """
        Check if the MSC config source is available.

        :return: ``True`` if the MSC config source can be accessed, ``False`` otherwise.
        """
        try:
            # Try to resolve the base URL to see if MSC can handle it
            resolve_storage_client(self.base_url)
            return True
        except Exception:
            logger.error("MSC config source not available", exc_info=True)
            return False

    def is_group(self, config_path: str) -> bool:
        """
        Check if the given path is a group (directory).

        :param config_path: Relative path or full ``msc://`` URL to check.
        :return: ``True`` if the path is a directory, ``False`` otherwise.
        """
        full_url = self._resolve_full_url(config_path)

        # Ensure path ends with "/" for directory check
        full_url = full_url.rstrip("/") + "/"

        try:
            # Use msc.info() directly to check if path is a directory
            metadata = msc.info(full_url)
            return metadata.type == "directory"
        except FileNotFoundError:
            return False
        except Exception:
            return False

    def is_config(self, config_path: str) -> bool:
        """
        Check if the given path is a config file.

        :param config_path: Relative path or full ``msc://`` URL to check.
        :return: ``True`` if the path is a config file, ``False`` otherwise.
        """
        # If there's a directory with the same name, directory takes precedence
        if self.is_group(config_path):
            return False

        full_url = self._resolve_full_url(config_path)
        full_url = self._normalize_file_name(full_url)

        try:
            return msc.is_file(full_url)
        except Exception:
            return False

    def list(self, config_path: str, results_filter: Optional[ObjectType]) -> List[str]:
        """
        List items under the specified config path.

        :param config_path: Relative path or full ``msc://`` URL to list.
        :param results_filter: Optional filter for the results.
        :return: List of config names and group names under the specified path.
        """
        full_url = self._resolve_full_url(config_path)
        files: List[str] = []

        try:
            # Use MSC to resolve client and list items directly
            # In this case, client.list() is simpler than msc.list() because
            # msc.list() returns keys with full msc:// URLs which would require
            # more complex path manipulation
            client, path = resolve_storage_client(full_url)

            # Add trailing slash to ensure we're listing directory contents
            list_path = path.rstrip("/") + "/" if path else ""

            # Get all items under this path
            for item in client.list(prefix=list_path, include_directories=True):
                # Get the relative path from the base path
                if item.key.startswith(list_path):
                    relative_path = item.key[len(list_path) :]
                elif path and item.key.startswith(path + "/"):
                    relative_path = item.key[len(path + "/") :]
                else:
                    continue

                # Skip empty paths or the directory itself
                if not relative_path or relative_path.endswith("/"):
                    continue

                # Get just the immediate file/directory name (no nested paths)
                file_name = relative_path.split("/")[0]
                if not file_name:
                    continue

                # Build the full config path for this item
                item_path = join_paths(config_path, file_name) if config_path else file_name

                self._list_add_result(
                    files=files,
                    file_path=item_path,
                    file_name=file_name,
                    results_filter=results_filter,
                )

        except Exception:
            logger.error(f"Failed to list MSC path '{full_url}'", exc_info=True)
            # Return empty list if we can't list the directory

        return sorted(list(set(files)))

    def __repr__(self) -> str:
        return f"MSCConfigSource(provider={self.provider}, path={self.scheme()}://{self.path})"


class MSCSearchPathPlugin(SearchPathPlugin):
    """
    A Hydra :py:class:`hydra.plugins.search_path_plugin.SearchPathPlugin` that enables MSC support.

    Fixes MSC URL mangling issues and ensures MSC sources are available for config loading.
    """

    def manipulate_search_path(self, search_path: ConfigSearchPath) -> None:
        """
        Enable MSC support by fixing mangled URLs and adding universal MSC source.

        Performs two operations:

        1. **Fixes mangled MSC URLs**: CLI path normalization can mangle ``msc://dev/configs`` to ``/current/dir/msc:/dev/configs``.
        2. **Adds universal MSC source**: Ensures :py:class:`MSCConfigSource` is available to handle any ``msc://`` URLs in config defaults.

        :param search_path: The :py:class:`hydra.core.config_search_path.ConfigSearchPath` to manipulate.
        """
        path_elements = search_path.get_path()

        # Step 1: Fix any mangled MSC URLs from CLI
        for i, element in enumerate(path_elements):
            path = element.path

            # Detect mangled MSC URLs: contains "msc:/" but doesn't start with "msc://"
            if path and "msc:/" in path and not path.startswith(MSC_PROTOCOL):
                # Extract the MSC URL from the mangled path
                msc_start = path.find("msc:/")
                msc_part = path[msc_start:]  # Everything from "msc:/" onwards

                # Fix the missing slash: "msc:/profile" -> "msc://profile"
                if msc_part.startswith("msc:/") and not msc_part.startswith(MSC_PROTOCOL):
                    fixed_url = msc_part.replace("msc:/", MSC_PROTOCOL, 1)
                else:
                    fixed_url = msc_part

                # Replace the element with a new one containing the fixed URL
                path_elements[i] = SearchPathElement(element.provider, fixed_url)

                logger.debug(f"Fixed mangled MSC URL: {path} → {fixed_url}")

        # Step 2: Ensure there's a universal MSC source for handling any msc:// URLs
        # Check if there's already an msc:// entry in the search path
        has_msc_source = any(element.path and element.path.startswith(MSC_PROTOCOL) for element in path_elements)

        if not has_msc_source:
            # Add a universal MSC source that can handle any msc:// URL
            # Use a generic base that MSCConfigSource can resolve dynamically
            search_path.append("msc-universal", MSC_PROTOCOL)
            logger.debug("Added universal MSC source to search path")
