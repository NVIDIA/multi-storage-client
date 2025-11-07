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
    from typing import List, Optional

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

    @mcp.tool
    def msc_upload_file(url: str, local_path: str, attributes: Optional[dict] = None) -> str:
        """
        Upload a file to the given URL from a local path.

        This function uploads a file from the local filesystem to various storage backends
        supported by Multi-Storage Client.

        :param url: The destination URL where the file will be stored (e.g., 'msc://profile/path/file.txt')
        :param local_path: The local path of the file to upload
        :param attributes: Optional metadata attributes to add to the uploaded file
        :return: JSON string containing upload result and metadata
        """
        try:
            logger.info(f"Uploading file from {local_path} to {url}")

            client, path = get_storage_client_for_url(url)

            attrs = dict(attributes) if attributes else None

            client.upload_file(remote_path=path, local_path=local_path, attributes=attrs)

            metadata = client.info(path)

            result = {
                "success": True,
                "url": url,
                "local_path": local_path,
                "uploaded_metadata": metadata_to_dict(metadata),
            }

            if attrs:
                result["attributes"] = attrs

            logger.info(f"Successfully uploaded {local_path} to {url}")
            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            logger.error(f"Error uploading {local_path} to {url}: {str(e)}")
            error_result = {"success": False, "error": str(e), "url": url, "local_path": local_path}
            return json.dumps(error_result, indent=2)

    @mcp.tool
    def msc_download_file(url: str, local_path: str) -> str:
        """
        Download a file from the given remote URL to a local path.

        This function downloads a file from various storage backends supported by
        Multi-Storage Client to the local filesystem.

        :param url: The URL of the file to download (e.g., 'msc://profile/path/file.txt')
        :param local_path: The local path where the file should be downloaded
        :return: JSON string containing download result and metadata
        """
        try:
            logger.info(f"Downloading file from {url} to {local_path}")

            client, path = get_storage_client_for_url(url)

            metadata = client.info(path)

            client.download_file(remote_path=path, local_path=local_path)

            result = {
                "success": True,
                "url": url,
                "local_path": local_path,
                "downloaded_metadata": metadata_to_dict(metadata),
            }

            logger.info(f"Successfully downloaded {url} to {local_path}")
            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            logger.error(f"Error downloading {url} to {local_path}: {str(e)}")
            error_result = {"success": False, "error": str(e), "url": url, "local_path": local_path}
            return json.dumps(error_result, indent=2)

    @mcp.tool
    def msc_delete(url: str, recursive: bool = False) -> str:
        """
        Delete the specified object(s) from the storage provider.

        This function deletes files or directories from various storage backends
        supported by Multi-Storage Client.

        :param url: The URL of the object to delete (e.g., 'msc://profile/path/file.txt')
        :param recursive: Whether to delete objects in the path recursively
        :return: JSON string containing deletion result
        """
        try:
            logger.info(f"Deleting object at {url} (recursive={recursive})")

            client, path = get_storage_client_for_url(url)

            client.delete(path, recursive=recursive)

            result = {
                "success": True,
                "url": url,
                "recursive": recursive,
                "message": f"Successfully deleted {url}",
            }

            logger.info(f"Successfully deleted {url}")
            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            logger.error(f"Error deleting {url}: {str(e)}")
            error_result = {"success": False, "error": str(e), "url": url, "recursive": recursive}
            return json.dumps(error_result, indent=2)

    @mcp.tool
    def msc_copy(source_url: str, target_url: str) -> str:
        """
        Copy files from the source storage to the target storage.

        This function copies files between different storage locations using
        Multi-Storage Client's copy functionality for same-profile copies.

        :param source_url: The URL for the source file (e.g., 'msc://profile/path/file.txt')
        :param target_url: The URL for the target location (e.g., 'msc://profile/path/file_copy.txt')
        :return: JSON string containing copy result
        """
        try:
            logger.info(f"Copying from {source_url} to {target_url}")

            source_client, source_path = get_storage_client_for_url(source_url)
            target_client, target_path = get_storage_client_for_url(target_url)

            if source_client.profile != target_client.profile:
                error_message = (
                    f"Cross-profile copy not supported. Source profile '{source_client.profile}' differs from "
                    f"target profile '{target_client.profile}'. Use msc_sync for directory-level cross-profile copies, "
                    f"or use msc_download then msc_upload for single-file transfers."
                )
                logger.error(error_message)
                error_result = {
                    "success": False,
                    "error": error_message,
                    "source_url": source_url,
                    "target_url": target_url,
                    "source_profile": source_client.profile,
                    "target_profile": target_client.profile,
                }
                return json.dumps(error_result, indent=2)

            source_metadata = source_client.info(source_path)

            source_client.copy(source_path, target_path)

            target_metadata = source_client.info(target_path)

            result = {
                "success": True,
                "source_url": source_url,
                "target_url": target_url,
                "source_metadata": metadata_to_dict(source_metadata),
                "target_metadata": metadata_to_dict(target_metadata),
                "message": f"Successfully copied {source_url} to {target_url}",
            }

            logger.info(f"Successfully copied {source_url} to {target_url}")
            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            logger.error(f"Error copying {source_url} to {target_url}: {str(e)}")
            error_result = {
                "success": False,
                "error": str(e),
                "source_url": source_url,
                "target_url": target_url,
            }
            return json.dumps(error_result, indent=2)

    @mcp.tool
    def msc_is_file(url: str) -> str:
        """
        Check whether the specified URL points to a file (rather than a directory or folder).

        This function checks if a URL points to a file across various storage backends
        supported by Multi-Storage Client.

        :param url: The URL to check (e.g., 'msc://profile/path/file.txt')
        :return: JSON string containing the result of the file check
        """
        try:
            logger.info(f"Checking if {url} is a file")

            client, path = get_storage_client_for_url(url)

            is_file_result = client.is_file(path=path)

            result = {
                "success": True,
                "url": url,
                "is_file": is_file_result,
                "message": f"URL {url} {'is' if is_file_result else 'is not'} a file",
            }

            logger.info(f"Successfully checked {url}: is_file={is_file_result}")
            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            logger.error(f"Error checking if {url} is a file: {str(e)}")
            error_result = {"success": False, "error": str(e), "url": url}
            return json.dumps(error_result, indent=2)

    @mcp.tool
    def msc_is_empty(url: str) -> str:
        """
        Check whether the specified URL contains any objects.

        This function checks if a storage location is empty across various storage backends
        supported by Multi-Storage Client.

        :param url: The URL to check (e.g., 'msc://profile/path/')
        :return: JSON string containing the result of the empty check
        """
        try:
            logger.info(f"Checking if {url} is empty")

            client, path = get_storage_client_for_url(url)

            is_empty_result = client.is_empty(path)

            result = {
                "success": True,
                "url": url,
                "is_empty": is_empty_result,
                "message": f"URL {url} {'is empty' if is_empty_result else 'contains objects'}",
            }

            logger.info(f"Successfully checked {url}: is_empty={is_empty_result}")
            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            logger.error(f"Error checking if {url} is empty: {str(e)}")
            error_result = {"success": False, "error": str(e), "url": url}
            return json.dumps(error_result, indent=2)

    @mcp.tool
    def msc_sync(
        source_url: str,
        target_url: str,
        delete_unmatched_files: bool = False,
        preserve_source_attributes: bool = False,
    ) -> str:
        """
        Synchronize files from the source storage to the target storage.

        This function syncs files between different storage locations using
        Multi-Storage Client's sync functionality.

        :param source_url: The URL for the source storage (e.g., 'msc://profile1/path/')
        :param target_url: The URL for the target storage (e.g., 'msc://profile2/path/')
        :param delete_unmatched_files: Whether to delete files at target that are not present at source
        :param preserve_source_attributes: Whether to preserve source file metadata attributes
        :return: JSON string containing sync result
        """

        try:
            logger.info(f"Starting sync from {source_url} to {target_url}")

            source_client, source_path = get_storage_client_for_url(source_url)
            target_client, target_path = get_storage_client_for_url(target_url)

            target_client.sync_from(
                source_client,
                source_path,
                target_path,
                delete_unmatched_files=delete_unmatched_files,
                preserve_source_attributes=preserve_source_attributes,
            )

            result = {
                "success": True,
                "source_url": source_url,
                "target_url": target_url,
                "delete_unmatched_files": delete_unmatched_files,
                "preserve_source_attributes": preserve_source_attributes,
                "message": f"Successfully synced {source_url} to {target_url}",
            }

            logger.info(f"Successfully synced {source_url} to {target_url}")
            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            logger.error(f"Error syncing {source_url} to {target_url}: {str(e)}")
            error_result = {
                "success": False,
                "error": str(e),
                "source_url": source_url,
                "target_url": target_url,
            }
            return json.dumps(error_result, indent=2)

    @mcp.tool
    def msc_sync_replicas(
        source_url: str,
        replica_indices: Optional[List[int]] = None,
        delete_unmatched_files: bool = False,
    ) -> str:
        """
        Synchronize files from the source storage to configured replicas.

        This function syncs files from the primary storage to one or more replica storage locations
        configured in the storage profile. Replicas provide redundancy and improved read performance.

        :param source_url: The URL for the source storage (e.g., 'msc://profile/path/')
        :param replica_indices: Optional list of replica indices to sync to (0-based). If None, syncs to all replicas
        :param delete_unmatched_files: Whether to delete files at replicas that are not present at source
        :return: JSON string containing sync result
        """
        try:
            logger.info(f"Starting replica sync from {source_url}")

            client, source_path = get_storage_client_for_url(source_url)

            if not client.replicas:
                warning_msg = "No replicas configured for profile. Sync operation skipped."
                logger.warning(warning_msg)
                result = {
                    "success": True,
                    "source_url": source_url,
                    "message": warning_msg,
                    "replicas_synced": 0,
                }
                return json.dumps(result, indent=2, default=str)

            client.sync_replicas(
                source_path=source_path,
                replica_indices=replica_indices,
                delete_unmatched_files=delete_unmatched_files,
            )

            num_replicas = len(replica_indices) if replica_indices else len(client.replicas)

            result = {
                "success": True,
                "source_url": source_url,
                "replica_indices": replica_indices,
                "delete_unmatched_files": delete_unmatched_files,
                "replicas_synced": num_replicas,
                "message": f"Successfully synced {source_url} to {num_replicas} replica(s)",
            }

            logger.info(f"Successfully synced {source_url} to {num_replicas} replica(s)")
            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            logger.error(f"Error syncing replicas from {source_url}: {str(e)}")
            error_result = {
                "success": False,
                "error": str(e),
                "source_url": source_url,
                "replica_indices": replica_indices,
            }
            return json.dumps(error_result, indent=2)
