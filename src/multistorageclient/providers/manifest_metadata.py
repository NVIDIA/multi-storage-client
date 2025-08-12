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

from __future__ import annotations  # Enables forward references in type hints

import io
import json
import logging
import os
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from ..types import MetadataProvider, ObjectMetadata, StorageProvider
from ..utils import create_attribute_filter_evaluator, glob, matches_attribute_filter_expression

logger = logging.getLogger(__name__)


DEFAULT_MANIFEST_BASE_DIR = ".msc_manifests"
MANIFEST_INDEX_FILENAME = "msc_manifest_index.json"
MANIFEST_PARTS_CHILD_DIR = "parts"
MANIFEST_PART_PREFIX = "msc_manifest_part"
MANIFEST_PART_SUFFIX = ".jsonl"  # Suffix for the manifest part files
SEQUENCE_PADDING = 6  # Define padding for the sequence number (e.g., 6 for "000001")


@dataclass
class ManifestPartReference:
    """
    A data class representing a reference to dataset manifest part.
    """

    #: The path of the manifest part relative to the main manifest.
    path: str

    @staticmethod
    def from_dict(data: dict[str, Any]) -> ManifestPartReference:
        """
        Creates a ManifestPartReference instance from a dictionary.
        """
        # Validate that the required 'path' field is present
        if "path" not in data:
            raise ValueError("Missing required field: 'path'")

        return ManifestPartReference(path=data["path"])

    def to_dict(self) -> dict:
        """
        Converts ManifestPartReference instance to a dictionary.
        """
        return {
            "path": self.path,
        }


@dataclass
class Manifest:
    """
    A data class representing a dataset manifest.
    """

    #: Defines the version of the manifest schema.
    version: str
    #: References to manifest parts.
    parts: list[ManifestPartReference]

    @staticmethod
    def from_dict(data: dict) -> "Manifest":
        """
        Creates a Manifest instance from a dictionary (parsed from JSON).
        """
        # Perform any necessary validation here
        try:
            version = data["version"]
            parts = [ManifestPartReference.from_dict(part) for part in data["parts"]]
        except KeyError as e:
            raise ValueError("Invalid manifest data: Missing required field") from e

        return Manifest(version=version, parts=parts)

    def to_json(self) -> str:
        # Convert dataclass to dict and parts to JSON-compatible format
        data = asdict(self)
        data["parts"] = [part.to_dict() for part in self.parts]
        return json.dumps(data)


def _metadata_to_manifest_dict(metadata: ObjectMetadata) -> dict:
    """
    Convert an ObjectMetadata instance to a dictionary suitable with manifest format,
    replacing 'content_length' with 'size_bytes' and removing 'content_length'.
    """
    metadata_dict = metadata.to_dict()
    # Pop out content_length, store it in size_bytes
    size_bytes = metadata_dict.pop("content_length", None)
    metadata_dict["size_bytes"] = size_bytes
    return metadata_dict


class ManifestMetadataProvider(MetadataProvider):
    _storage_provider: StorageProvider
    _files: dict[str, ObjectMetadata]
    _pending_adds: dict[str, ObjectMetadata]
    _pending_removes: set[str]
    _manifest_path: str
    _writable: bool

    def __init__(self, storage_provider: StorageProvider, manifest_path: str, writable: bool = False) -> None:
        """
        Creates a :py:class:`ManifestMetadataProvider`.

        :param storage_provider: Storage provider.
        :param manifest_path: Main manifest file path.
        :param writable: If true, allows modifications and new manifests to be written.
        """
        self._storage_provider = storage_provider
        self._files = {}
        self._pending_adds = {}
        self._pending_removes = set()
        self._manifest_path = manifest_path
        self._writable = writable

        self._load_manifest(storage_provider, self._manifest_path)

    def _load_manifest(self, storage_provider: StorageProvider, manifest_path: str) -> None:
        """
        Loads manifest.

        :param storage_provider: Storage provider.
        :param manifest_path: Main manifest file path
        """

        def helper_find_manifest_file(manifest_path: str) -> str:
            if storage_provider.is_file(manifest_path):
                return manifest_path

            if storage_provider.is_file(os.path.join(manifest_path, MANIFEST_INDEX_FILENAME)):
                return os.path.join(manifest_path, MANIFEST_INDEX_FILENAME)

            # Now go looking and select newest manifest.
            if DEFAULT_MANIFEST_BASE_DIR not in manifest_path.split("/"):
                manifest_path = os.path.join(manifest_path, DEFAULT_MANIFEST_BASE_DIR)

            candidates = storage_provider.glob(os.path.join(manifest_path, "*", MANIFEST_INDEX_FILENAME))
            candidates = sorted(candidates)
            return candidates[-1] if candidates else ""

        resolved_manifest_path = helper_find_manifest_file(manifest_path)
        if not resolved_manifest_path:
            logger.warning(f"No manifest found at '{manifest_path}'.")
            return

        file_content = storage_provider.get_object(resolved_manifest_path)

        prefix = os.path.dirname(resolved_manifest_path)
        _, file_extension = os.path.splitext(resolved_manifest_path)
        self._load_manifest_file(storage_provider, file_content, prefix, file_extension[1:])

    def _load_manifest_file(
        self, storage_provider: StorageProvider, file_content: bytes, manifest_base: str, file_type: str
    ) -> None:
        """
        Loads a manifest.

        :param storage_provider: Storage provider.
        :param file_content: Manifest file content bytes.
        :param manifest_base: Manifest file base path.
        :param file_type: Manifest file type.
        """
        if file_type == "json":
            manifest_dict = json.loads(file_content.decode("utf-8"))
            manifest = Manifest.from_dict(manifest_dict)

            # Check manifest version. Not needed once we make the manifest model use sum types/discriminated unions.
            if manifest.version != "1":
                raise ValueError(f"Manifest version {manifest.version} is not supported.")

            # Load manifest parts.
            for manifest_part_reference in manifest.parts:
                object_metadata: list[ObjectMetadata] = self._load_manifest_part_file(
                    storage_provider=storage_provider,
                    manifest_base=manifest_base,
                    manifest_part_reference=manifest_part_reference,
                )

                for object_metadatum in object_metadata:
                    self._files[object_metadatum.key] = object_metadatum
        else:
            raise NotImplementedError(f"Manifest file type {file_type} is not supported.")

    def _load_manifest_part_file(
        self, storage_provider: StorageProvider, manifest_base: str, manifest_part_reference: ManifestPartReference
    ) -> list[ObjectMetadata]:
        """
        Loads a manifest part.

        :param storage_provider: Storage provider.
        :param manifest_base: Manifest file base path. Prepend to manifest part reference paths.
        :param manifest_part_reference: Manifest part reference.
        """
        object_metadata = []

        if not os.path.isabs(manifest_part_reference.path):
            remote_path = os.path.join(manifest_base, manifest_part_reference.path)
        else:
            remote_path = manifest_part_reference.path
        manifest_part_file_content = storage_provider.get_object(remote_path)

        # The manifest part is a JSON lines file. Each line is a JSON-serialized ObjectMetadata.
        for line in io.TextIOWrapper(io.BytesIO(manifest_part_file_content), encoding="utf-8"):
            object_metadatum_dict = json.loads(line)
            object_metadatum_dict["content_length"] = object_metadatum_dict.pop("size_bytes")
            object_metadatum = ObjectMetadata.from_dict(object_metadatum_dict)
            object_metadata.append(object_metadatum)

        return object_metadata

    def _write_manifest_files(self, storage_provider: StorageProvider, object_metadata: list[ObjectMetadata]) -> None:
        """
        Writes the main manifest and its part files.

        :param storage_provider: The storage provider to use for writing.
        :param object_metadata: objects to include in manifest.
        """
        if not object_metadata:
            return

        base_path = self._manifest_path
        manifest_base_path = base_path

        base_path_parts = base_path.split(os.sep)
        if DEFAULT_MANIFEST_BASE_DIR in base_path_parts:
            manifests_index = base_path_parts.index(DEFAULT_MANIFEST_BASE_DIR)
            if manifests_index > 0:
                manifest_base_path = os.path.join(*base_path_parts[:manifests_index])
            else:
                manifest_base_path = ""
            if base_path.startswith(os.sep):
                manifest_base_path = os.sep + manifest_base_path

        current_time = datetime.now(timezone.utc)
        current_time_str = current_time.isoformat(timespec="seconds")
        manifest_folderpath = os.path.join(manifest_base_path, DEFAULT_MANIFEST_BASE_DIR, current_time_str)
        # We currently write only one part by default
        part_sequence_number = 1
        manifest_part_file_path = os.path.join(
            MANIFEST_PARTS_CHILD_DIR,
            f"{MANIFEST_PART_PREFIX}{part_sequence_number:0{SEQUENCE_PADDING}}{MANIFEST_PART_SUFFIX}",
        )

        manifest = Manifest(version="1", parts=[ManifestPartReference(path=manifest_part_file_path)])

        # Write single manifest part with metadata as JSON lines (each object on a new line)
        manifest_part_content = "\n".join(
            [json.dumps(_metadata_to_manifest_dict(metadata)) for metadata in object_metadata]
        )
        storage_provider.put_object(
            os.path.join(manifest_folderpath, manifest_part_file_path), manifest_part_content.encode("utf-8")
        )

        # Write the main manifest file
        manifest_file_path = os.path.join(manifest_folderpath, MANIFEST_INDEX_FILENAME)
        manifest_content = manifest.to_json()
        storage_provider.put_object(manifest_file_path, manifest_content.encode("utf-8"))

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
        List objects in the manifest.

        :param path: The path to filter objects by.
        :param start_after: The object to start after.
        :param end_at: The object to end at.
        :param include_directories: Whether to include directories.
        :param attribute_filter_expression: The attribute filter expression to filter objects by.
        :param show_attributes: This field is not used in this implementation - It will always return attributes.  This is present merely to satisfy the interface.
        """

        if (start_after is not None) and (end_at is not None) and not (start_after < end_at):
            raise ValueError(f"start_after ({start_after}) must be before end_at ({end_at})!")

        if path and not path.endswith("/"):
            path = path + "/"

        # create evaluator for attribute filter expression if present
        evaluator = (
            create_attribute_filter_evaluator(attribute_filter_expression) if attribute_filter_expression else None
        )

        # Note that this is a generator, not a tuple (there's no tuple comprehension).
        keys = (
            key
            for key, obj_metadata in self._files.items()
            if key.startswith(path)
            and (start_after is None or start_after < key)
            and (end_at is None or key <= end_at)
            and (
                evaluator is None or matches_attribute_filter_expression(obj_metadata, evaluator)
            )  # filter by evaluator if present
        )

        pending_directory: Optional[ObjectMetadata] = None
        for key in sorted(keys):
            if include_directories:
                relative = key[len(path) :].lstrip("/")
                subdirectory = relative.split("/", 1)[0] if "/" in relative else None

                if subdirectory:
                    directory_name = f"{path}{subdirectory}/"

                    if pending_directory and pending_directory.key != directory_name:
                        yield pending_directory

                    obj_metadata = self.get_object_metadata(key)
                    if not pending_directory or pending_directory.key != directory_name:
                        pending_directory = ObjectMetadata(
                            key=directory_name,
                            type="directory",
                            last_modified=obj_metadata.last_modified,
                            content_length=0,
                        )
                    else:
                        pending_directory.last_modified = max(
                            pending_directory.last_modified, obj_metadata.last_modified
                        )
                    continue  # Skip yielding this key as it's part of a directory

            obj = self._files[key]
            obj.key = key  # use key without base_path
            yield obj

        if include_directories and pending_directory:
            yield pending_directory

    def get_object_metadata(self, path: str, include_pending: bool = False) -> ObjectMetadata:
        if path in self._files:
            if include_pending and path in self._pending_removes:
                raise FileNotFoundError(f"Object {path} does not exist.")
            else:
                return self._files[path]
        elif include_pending and path in self._pending_adds:
            return self._pending_adds[path]
        else:
            raise FileNotFoundError(f"Object {path} does not exist.")

    def glob(self, pattern: str, attribute_filter_expression: Optional[str] = None) -> list[str]:
        """
        List objects in the manifest.

        :param pattern: The pattern to filter objects by.
        :param attribute_filter_expression: The attribute filter expression to filter objects by.
        """

        all_objects = [
            object.key for object in self.list_objects("", attribute_filter_expression=attribute_filter_expression)
        ]
        return [key for key in glob(all_objects, pattern)]

    def realpath(self, path: str) -> tuple[str, bool]:
        exists = path in self._files
        return path, exists

    def add_file(self, path: str, metadata: ObjectMetadata) -> None:
        if not self.is_writable():
            raise RuntimeError(f"Manifest update support not enabled in configuration. Attempted to add {path}.")
        self._pending_adds[path] = metadata

    def remove_file(self, path: str) -> None:
        if not self.is_writable():
            raise RuntimeError(f"Manifest update support not enabled in configuration. Attempted to remove {path}.")
        if path not in self._files:
            raise FileNotFoundError(f"Object {path} does not exist.")
        self._pending_removes.add(path)

    def is_writable(self) -> bool:
        return self._writable

    def commit_updates(self) -> None:
        if not self._pending_adds and not self._pending_removes:
            return

        if self._pending_adds:
            self._files.update(self._pending_adds)
            self._pending_adds = {}

        for path in self._pending_removes:
            self._files.pop(path)
        self._pending_removes = set()

        # Collect metadata for each object to write out in this part file.
        object_metadata = [
            ObjectMetadata(
                key=file_path,
                content_length=metadata.content_length,
                last_modified=metadata.last_modified,
                metadata=metadata.metadata,
            )
            for file_path, metadata in self._files.items()
        ]
        self._write_manifest_files(self._storage_provider, object_metadata)
