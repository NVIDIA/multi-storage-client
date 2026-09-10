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

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from huggingface_hub.hf_api import RepoFile, RepoFolder

from multistorageclient.providers.huggingface import HuggingFaceStorageProvider
from multistorageclient.types import AWARE_DATETIME_MIN

LAST_COMMIT = {"id": "c1", "title": "initial", "date": "2025-01-02T03:04:05.000Z"}


@pytest.fixture
def hf_provider():
    with patch("multistorageclient.providers.huggingface.HfApi", return_value=MagicMock()):
        yield HuggingFaceStorageProvider(repository_id="org/repo")


def test_item_to_metadata_uses_last_commit_date(hf_provider):
    """expand=True is requested precisely so that last_commit is available; its date must be surfaced."""
    file_item = RepoFile(path="data/a.bin", size=3, oid="blob1", lfs=None, last_commit=LAST_COMMIT, security=None)
    folder_item = RepoFolder(path="data", oid="tree1", last_commit=LAST_COMMIT)

    file_metadata = hf_provider._item_to_metadata(file_item)
    folder_metadata = hf_provider._item_to_metadata(folder_item)

    expected = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert file_metadata.type == "file"
    assert file_metadata.content_length == 3
    assert file_metadata.etag == "blob1"
    assert file_metadata.last_modified == expected
    assert folder_metadata.type == "directory"
    assert folder_metadata.etag == "tree1"
    assert folder_metadata.last_modified == expected


def test_item_to_metadata_without_last_commit_uses_sentinel(hf_provider):
    file_item = RepoFile(path="data/a.bin", size=3, oid="blob1", lfs=None, last_commit=None, security=None)

    assert hf_provider._item_to_metadata(file_item).last_modified == AWARE_DATETIME_MIN
