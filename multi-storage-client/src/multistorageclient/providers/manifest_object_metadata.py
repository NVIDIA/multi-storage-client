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

from dataclasses import dataclass
from typing import Optional

from ..types import ObjectMetadata


@dataclass
class ManifestObjectMetadata(ObjectMetadata):
    """
    Extended metadata for manifest files that tracks both logical and physical paths.

    This class extends ObjectMetadata to include physical path information,
    enabling advanced features like safe overwrites and path remapping.

    Inherits all fields from ObjectMetadata (key, content_length, last_modified, etc.)
    and adds physical_path to track the actual storage location.
    """

    # Extended manifest-specific fields
    physical_path: Optional[str] = None  # Physical storage path

    def __post_init__(self):
        """Initialize physical_path to match key if not provided."""
        if self.physical_path is None:
            self.physical_path = self.key

    def to_object_metadata(self) -> ObjectMetadata:
        """Convert to standard ObjectMetadata for external APIs."""
        return ObjectMetadata(
            key=self.key,
            content_length=self.content_length,
            last_modified=self.last_modified,
            content_type=self.content_type,
            etag=self.etag,
            metadata=self.metadata,
            type=self.type,
        )

    @classmethod
    def from_object_metadata(
        cls, obj_metadata: ObjectMetadata, physical_path: Optional[str] = None
    ) -> "ManifestObjectMetadata":
        """Create ManifestObjectMetadata from standard ObjectMetadata.

        If obj_metadata has a physical_path attribute (e.g., from dict with extra fields),
        use it. Otherwise, use the provided physical_path parameter, or default to key.
        """
        # Check if obj_metadata already has physical_path (for forward compatibility)
        final_physical_path: str
        if hasattr(obj_metadata, "physical_path") and obj_metadata.physical_path is not None:  # type: ignore
            final_physical_path = obj_metadata.physical_path  # type: ignore
        elif physical_path is not None:
            final_physical_path = physical_path
        else:
            final_physical_path = obj_metadata.key

        return cls(
            key=obj_metadata.key,
            content_length=obj_metadata.content_length,
            last_modified=obj_metadata.last_modified,
            content_type=obj_metadata.content_type,
            etag=obj_metadata.etag,
            metadata=obj_metadata.metadata,
            type=obj_metadata.type,
            physical_path=final_physical_path,
        )
