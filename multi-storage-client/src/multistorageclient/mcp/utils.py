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

import sys

if sys.version_info >= (3, 10):
    """Shared utilities and helpers for Multi-Storage Client MCP server."""

    from typing import Any, Dict, List, Optional

    from multistorageclient import StorageClient, StorageClientConfig
    from multistorageclient.types import ObjectMetadata

    from .server import mcp_wrapper

    def metadata_to_dict(metadata: ObjectMetadata) -> Dict[str, Any]:
        """
        Convert ObjectMetadata to a dictionary for JSON serialization.

        :param metadata: ObjectMetadata instance
        :return: Dictionary representation
        """
        return {
            "key": metadata.key,
            "type": metadata.type,
            "content_length": metadata.content_length,
            "last_modified": metadata.last_modified.isoformat() if metadata.last_modified else None,
            "content_type": metadata.content_type,
            "etag": metadata.etag,
            "storage_class": metadata.storage_class,
            "metadata": metadata.metadata,
        }

    def get_msc_config_paths() -> Optional[List[str]]:
        """
        Get MSC configuration file paths using MCP server configuration.

        Uses mcp_wrapper.config_path if available, otherwise falls back
        to environment variables and standard configuration search paths.

        :return: List of config file paths to try, or None for default search paths
        """

        if mcp_wrapper.config_path:
            mcp_wrapper.logger.debug(f"Using MCP server config path: {mcp_wrapper.config_path}")
            return [str(mcp_wrapper.config_path)]
        else:
            # Fall back to standard search paths
            mcp_wrapper.logger.debug("Using standard MSC configuration search paths")
            return None

    def get_storage_client_for_url(url: str) -> tuple[StorageClient, str]:
        """
        Create a StorageClient instance for the given URL using MCP server configuration.

        This function uses get_msc_config_paths() to determine configuration file paths,
        then creates a StorageClient instance for the resolved profile.

        :param url: The storage URL to resolve
        :return: Tuple of (StorageClient instance, resolved path)
        """
        from multistorageclient import StorageClient
        from multistorageclient.shortcuts import MSC_PROTOCOL, _resolve_msc_url, _resolve_non_msc_url

        # Resolve the URL to a profile name and path
        if url.startswith("msc:/") and not url.startswith("msc://"):
            url = url.replace("msc:/", "msc://")

        profile, path = _resolve_msc_url(url) if url.startswith(MSC_PROTOCOL) else _resolve_non_msc_url(url)

        # Get configuration file paths using MCP server configuration
        config_file_paths = get_msc_config_paths()

        # Create StorageClient with the determined configuration
        config = StorageClientConfig.from_file(
            config_file_paths=config_file_paths,
            profile=profile,
        )
        client = StorageClient(config)

        return client, path
