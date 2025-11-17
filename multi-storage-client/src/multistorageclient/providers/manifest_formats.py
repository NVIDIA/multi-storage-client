# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import Union

from ..types import ObjectMetadata


class ManifestFormat(str, Enum):
    JSONL = "jsonl"
    PARQUET = "parquet"


def _metadata_to_manifest_dict(metadata: ObjectMetadata) -> dict:
    metadata_dict = metadata.to_dict()
    metadata_dict["size_bytes"] = metadata_dict.pop("content_length")
    return metadata_dict


class ManifestFormatHandler(ABC):
    @abstractmethod
    def get_file_suffix(self) -> str:
        pass

    @abstractmethod
    def write_part(self, object_metadata: list[ObjectMetadata]) -> bytes:
        pass

    @abstractmethod
    def read_part(self, content: bytes) -> list[ObjectMetadata]:
        pass


class JsonlManifestFormatHandler(ManifestFormatHandler):
    def get_file_suffix(self) -> str:
        return ".jsonl"

    def write_part(self, object_metadata: list[ObjectMetadata]) -> bytes:
        lines = [json.dumps(_metadata_to_manifest_dict(metadata)) for metadata in object_metadata]
        return "\n".join(lines).encode("utf-8")

    def read_part(self, content: bytes) -> list[ObjectMetadata]:
        object_metadata = []
        for line in io.TextIOWrapper(io.BytesIO(content), encoding="utf-8"):
            object_metadatum_dict = json.loads(line)
            object_metadatum_dict["content_length"] = object_metadatum_dict.pop("size_bytes")

            # Extract physical_path before from_dict (to set as attribute after)
            physical_path = object_metadatum_dict.pop("physical_path", None)

            object_metadatum = ObjectMetadata.from_dict(object_metadatum_dict)

            # Preserve physical_path as attribute if present (for ManifestObjectMetadata)
            if physical_path is not None:
                object_metadatum.physical_path = physical_path  # type: ignore

            object_metadata.append(object_metadatum)
        return object_metadata


class ParquetManifestFormatHandler(ManifestFormatHandler):
    def __init__(self):
        self._check_pyarrow_available()

    def _check_pyarrow_available(self) -> None:
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            raise ImportError(
                "PyArrow is required for Parquet manifest support. "
                "Install it with: pip install multi-storage-client[parquet]"
            )

    def get_file_suffix(self) -> str:
        return ".parquet"

    def write_part(self, object_metadata: list[ObjectMetadata]) -> bytes:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        data = {
            "key": [m.key for m in object_metadata],
            "size_bytes": [m.content_length for m in object_metadata],
            "last_modified": [m.last_modified.isoformat() for m in object_metadata],
            "metadata": [json.dumps(m.metadata) if m.metadata else None for m in object_metadata],
            "physical_path": [getattr(m, "physical_path", m.key) for m in object_metadata],
        }
        table = pa.table(data)

        buf = io.BytesIO()
        pq.write_table(table, buf)
        return buf.getvalue()

    def read_part(self, content: bytes) -> list[ObjectMetadata]:
        import pyarrow.parquet as pq  # type: ignore
        from dateutil.parser import isoparse

        buf = io.BytesIO(content)
        table = pq.read_table(buf)

        object_metadata = []
        for i in range(len(table)):
            row = {col: table[col][i].as_py() for col in table.column_names}
            metadata_dict = json.loads(row["metadata"]) if row.get("metadata") else None

            obj = ObjectMetadata(
                key=row["key"],
                content_length=row["size_bytes"],
                last_modified=isoparse(row["last_modified"]),
                metadata=metadata_dict,
            )

            # Preserve physical_path as attribute if present (for ManifestObjectMetadata)
            if "physical_path" in row:
                obj.physical_path = row["physical_path"]  # type: ignore

            object_metadata.append(obj)
        return object_metadata


def get_format_handler(manifest_format: Union[ManifestFormat, str]) -> ManifestFormatHandler:
    if isinstance(manifest_format, str):
        try:
            manifest_format = ManifestFormat(manifest_format)
        except ValueError:
            raise ValueError(f"Unsupported manifest format: {manifest_format}")

    if manifest_format == ManifestFormat.PARQUET:
        return ParquetManifestFormatHandler()
    elif manifest_format == ManifestFormat.JSONL:
        return JsonlManifestFormatHandler()
