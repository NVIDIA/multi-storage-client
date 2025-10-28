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

"""MCP tool definitions for Multi-Storage Client operations."""

import sys

if sys.version_info >= (3, 10):
    import json
    import logging
    from typing import Optional

    from .server import mcp
    from .utils import get_storage_client_for_url, metadata_to_dict

    logger = logging.getLogger(__name__)

    @mcp.tool
    def msc_list(
        url: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        attribute_filter_expression: Optional[str] = None,
        show_attributes: bool = False,
        limit: Optional[int] = None,
    ) -> str:
        """
        Lists the contents of the specified URL prefix in Multi-Storage Client.

        This function retrieves objects (files or directories) stored under the provided prefix
        from various storage backends like S3, GCS, Azure Blob Storage, local filesystem, etc.

        :param url: The URL prefix to list objects under (e.g., 'msc://profile/path/', 's3://bucket/prefix/')
        :param start_after: The key to start after (exclusive). An object with this key doesn't have to exist
        :param end_at: The key to end at (inclusive). An object with this key doesn't have to exist
        :param include_directories: Whether to include directories in the result
        :param attribute_filter_expression: Attribute filter expression to apply to results
        :param show_attributes: Whether to return attributes in the result
        :param limit: Maximum number of objects to return
        :return: JSON string containing list of objects with their metadata
        """
        try:
            logger.info(f"Listing objects at URL: {url}")

            client, path = get_storage_client_for_url(url)

            results_iter = client.list(
                path=path,
                start_after=start_after,
                end_at=end_at,
                include_directories=include_directories,
                include_url_prefix=True,
                attribute_filter_expression=attribute_filter_expression,
                show_attributes=show_attributes,
            )

            # Collect results with optional limit
            objects = []
            count = 0
            for obj_metadata in results_iter:
                obj_dict = metadata_to_dict(obj_metadata)
                objects.append(obj_dict)
                count += 1
                if limit and count >= limit:
                    break

            result = {"success": True, "url": url, "count": count, "objects": objects}

            if limit and count >= limit:
                result["truncated"] = True
                result["message"] = f"Results limited to {limit} objects"

            logger.info(f"Successfully listed {count} objects from {url}")
            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            logger.error(f"Error listing objects at {url}: {str(e)}")
            error_result = {"success": False, "error": str(e), "url": url}
            return json.dumps(error_result, indent=2)

    @mcp.tool
    def msc_info(url: str) -> str:
        """
        Retrieves metadata and information about an object stored at the specified URL.

        This function gets detailed metadata about a specific file or directory from various
        storage backends supported by Multi-Storage Client.

        :param url: The URL of the object to retrieve information about (e.g., 'msc://profile/path/file.txt')
        :return: JSON string containing object metadata including size, last modified time, type, etc.
        """
        try:
            logger.info(f"Getting info for URL: {url}")

            client, path = get_storage_client_for_url(url)

            metadata = client.info(path)

            result = {"success": True, "url": url, "metadata": metadata_to_dict(metadata)}

            logger.info(f"Successfully retrieved info for {url}")
            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            logger.error(f"Error getting info for {url}: {str(e)}")
            error_result = {"success": False, "error": str(e), "url": url}
            return json.dumps(error_result, indent=2)
