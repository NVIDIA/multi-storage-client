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

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from huggingface_hub.errors import RepositoryNotFoundError
from huggingface_hub.hf_api import RepoFile, RepoFolder

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers.huggingface import REPO_INFO_TIMEOUT_SECONDS, HuggingFaceStorageProvider
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

    expected = datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC)
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


def test_root_info_reports_repository_commit_and_license():
    """The root directory reports the commit the revision points to and the repository card's license."""
    hf_client = MagicMock()
    hf_client.repo_info.return_value.sha = "0123abc"
    hf_client.repo_info.return_value.card_data.license = "apache-2.0"
    options = {"repository_id": "org/repo", "repo_type": "dataset", "repo_revision": "v1", "base_path": ""}
    config = {"profiles": {"hf": {"storage_provider": {"type": "huggingface", "options": options}}}}
    with patch("multistorageclient.providers.huggingface.HfApi", return_value=hf_client):
        client = StorageClient(StorageClientConfig.from_dict(config, profile="hf"))

    expected = {
        "type": "huggingface",
        "options": {"base_path": "", "repository_id": "org/repo", "repo_type": "dataset", "repo_revision": "v1"},
        "properties": {"commit_sha": "0123abc", "licenses": ["apache-2.0"]},
    }
    for path in ("", "."):
        metadata = client.info(path, include_root_properties=True)
        assert metadata.type == "directory"
        assert metadata.metadata == {"hf": expected}
    hf_client.repo_info.assert_called_with(
        repo_id="org/repo", repo_type="dataset", revision="v1", timeout=REPO_INFO_TIMEOUT_SECONDS
    )

    # The reported type and options are a valid storage provider config.
    copy_config = {
        "profiles": {"copy": {"storage_provider": {"type": expected["type"], "options": expected["options"]}}}
    }
    with patch("multistorageclient.providers.huggingface.HfApi", return_value=hf_client):
        StorageClientConfig.from_dict(copy_config, profile="copy")


def test_root_info_without_properties_skips_repository_lookup():
    """By default, root info only reports the type and options, without a network call to the Hub."""
    hf_client = MagicMock()
    options = {"repository_id": "org/repo", "repo_type": "dataset", "repo_revision": "v1", "base_path": ""}
    config = {"profiles": {"hf": {"storage_provider": {"type": "huggingface", "options": options}}}}
    with patch("multistorageclient.providers.huggingface.HfApi", return_value=hf_client):
        client = StorageClient(StorageClientConfig.from_dict(config, profile="hf"))

    for metadata in (client.info(), client.info(""), client.info(".")):
        assert metadata.metadata == {"hf": {"type": "huggingface", "options": options}}
    hf_client.repo_info.assert_not_called()


def test_root_metadata_keeps_list_of_licenses(hf_provider):
    """Dataset cards can declare a list of licenses instead of one."""
    hf_provider._hf_client.repo_info.return_value.card_data.license = ["cc-by-4.0", "mit"]

    assert hf_provider.get_root_metadata()["properties"]["licenses"] == ["cc-by-4.0", "mit"]


def test_root_metadata_without_card_omits_licenses(hf_provider):
    hf_provider._hf_client.repo_info.return_value.card_data = None

    assert "licenses" not in hf_provider.get_root_metadata()["properties"]


def test_root_metadata_missing_repository_raises(hf_provider):
    hf_provider._hf_client.repo_info.side_effect = RepositoryNotFoundError("no such repository")

    with pytest.raises(FileNotFoundError):
        hf_provider.get_root_metadata()
