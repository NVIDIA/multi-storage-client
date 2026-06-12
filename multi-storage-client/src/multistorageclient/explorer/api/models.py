# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

from typing import Optional

from pydantic import BaseModel


class ListRequest(BaseModel):
    """Request model for listing files"""

    url: str
    start_after: Optional[str] = None
    end_at: Optional[str] = None
    include_directories: bool = True
    limit: Optional[int] = None


class InfoRequest(BaseModel):
    """Request model for getting file/directory info"""

    url: str


class DownloadRequest(BaseModel):
    """Request model for downloading a file"""

    url: str


class DeleteRequest(BaseModel):
    """Request model for deleting files/directories"""

    url: str
    recursive: bool = False


class CopyRequest(BaseModel):
    """Request model for copying files"""

    source_url: str
    target_url: str


class SyncRequest(BaseModel):
    """Request model for syncing files"""

    source_url: str
    target_url: str
    delete_unmatched_files: bool = False
    preserve_source_attributes: bool = False


class PreviewRequest(BaseModel):
    """Request model for previewing a file"""

    url: str
    max_bytes: int = 1048576  # 1 MB default


class ConfigUploadResponse(BaseModel):
    """Response model for config upload"""

    status: str
    message: str
    profiles: list[str]
