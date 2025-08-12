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

import json
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from multistorageclient.types import ObjectMetadata
from multistorageclient.utils import calculate_worker_processes_and_threads

from .. import StorageClient
from ..providers.manifest_metadata import DEFAULT_MANIFEST_BASE_DIR, ManifestMetadataProvider


class ManifestMetadataGenerator:
    """
    Generates a file metadata manifest for use with a :py:class:`multistorageclient.providers.ManifestMetadataProvider`.
    """

    @staticmethod
    def _generate_manifest_part_body(object_metadata: list[ObjectMetadata]) -> bytes:
        return "\n".join(
            [
                json.dumps({**metadata_dict, "size_bytes": metadata_dict.pop("content_length")})
                for metadata in object_metadata
                for metadata_dict in [metadata.to_dict()]
            ]
        ).encode(encoding="utf-8")

    @staticmethod
    def generate_and_write_manifest(
        data_storage_client: StorageClient,
        manifest_storage_client: StorageClient,
        partition_keys: Optional[list[str]] = None,
    ) -> None:
        """
        Generates a file metadata manifest.

        The data storage client's base path should be set to the root path for data objects (e.g. ``my-bucket/my-data-prefix``).

        The manifest storage client's base path should be set to the root path for manifest objects (e.g. ``my-bucket/my-manifest-prefix``).

        The following manifest objects will be written with the destination storage client (with the total number of manifest parts being variable)::

           .msc_manifests/
           ├── msc_manifest_index.json
           └── parts/
               ├── msc_manifest_part000001.jsonl
               ├── ...
               └── msc_manifest_part999999.jsonl

        :param data_storage_client: Storage client for reading data objects.
        :param manifest_storage_client: Storage client for writing manifest objects.
        :param partition_keys: Optional list of keys to partition the listing operation. If provided, objects will be listed concurrently using these keys as boundaries.
        """
        # Get respective StorageProviders. A StorageClient will always have a StorageProvider
        # TODO: Cleanup by exposing APIs from the client
        data_storage_provider = data_storage_client._storage_provider
        manifest_storage_provider = manifest_storage_client._storage_provider

        # Create a ManifestMetadataProvider for writing manifest, configure manifest storage provider
        # TODO(NGCDP-3018): Opportunity to split up the responsibilities of MetadataProvider
        manifest_metadata_provider = ManifestMetadataProvider(
            storage_provider=manifest_storage_provider, manifest_path="", writable=True
        )

        if partition_keys is not None:
            _, num_worker_threads = calculate_worker_processes_and_threads()

            boundaries = list(zip([""] + partition_keys, partition_keys + [None]))

            def process_partition(boundary):
                start_after, end_at = boundary
                for object_metadata in data_storage_provider.list_objects(
                    path="", start_after=start_after, end_at=end_at, show_attributes=True
                ):
                    if DEFAULT_MANIFEST_BASE_DIR not in object_metadata.key.split("/"):  # Do not track manifest files
                        manifest_metadata_provider.add_file(path=object_metadata.key, metadata=object_metadata)

            with ThreadPoolExecutor(max_workers=num_worker_threads) as executor:
                futures = [executor.submit(process_partition, boundary) for boundary in boundaries]
                for future in futures:
                    future.result()
        else:
            for object_metadata in data_storage_provider.list_objects(path="", show_attributes=True):
                if DEFAULT_MANIFEST_BASE_DIR not in object_metadata.key.split("/"):  # Do not track manifest files
                    manifest_metadata_provider.add_file(path=object_metadata.key, metadata=object_metadata)

        manifest_metadata_provider.commit_updates()
