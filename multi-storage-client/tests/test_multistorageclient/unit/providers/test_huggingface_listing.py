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

from unittest.mock import MagicMock, patch

import pytest
from huggingface_hub.errors import EntryNotFoundError, RepositoryNotFoundError, RevisionNotFoundError
from huggingface_hub.hf_api import RepoFile

from multistorageclient.providers.huggingface import HuggingFaceStorageProvider

LAST_COMMIT = {"id": "c1", "title": "initial", "date": "2025-01-02T03:04:05.000Z"}


def _raising_tree(error: Exception):
    """Mimic ``HfApi.list_repo_tree``: a lazy iterator that raises only once it is consumed."""

    def tree(*_args, **_kwargs):
        raise error
        yield  # pragma: no cover - makes this a generator

    return tree


def _file(path: str) -> RepoFile:
    return RepoFile(path=path, size=len(path), oid=f"oid-{path}", lfs=None, last_commit=None, security=None)


def _paths_info_entry(path: str) -> dict:
    """What paths-info with expand returns for a path; paths without an extension are directories."""
    if "." not in path.rsplit("/", 1)[-1]:
        return {"type": "directory", "path": path, "oid": f"tree-{path}", "lastCommit": LAST_COMMIT}
    return {"type": "file", "path": path, "size": len(path), "oid": f"oid-{path}", "lastCommit": LAST_COMMIT}


@pytest.fixture
def paths_info_session():
    def post(_url, json, **_kwargs):
        response = MagicMock()
        response.json.return_value = [_paths_info_entry(path) for path in json["paths"]]
        return response

    session = MagicMock()
    session.post.side_effect = post
    return session


@pytest.fixture
def hf_provider(paths_info_session):
    hf_client = MagicMock()
    hf_client.endpoint = "https://huggingface.co"
    hf_client.token = "hf_test_token"
    hf_client.repo_info.return_value.sha = "0123abc"
    with patch("multistorageclient.providers.huggingface.HfApi", return_value=hf_client):
        provider = HuggingFaceStorageProvider(repository_id="org/repo")
    # The path is not a file, so listing proceeds to list_repo_tree.
    provider._get_object_metadata = MagicMock(side_effect=FileNotFoundError("not a file"))  # type: ignore
    with patch("multistorageclient.providers.huggingface.get_session", return_value=paths_info_session):
        yield provider


def test_list_missing_directory_returns_empty(hf_provider):
    hf_provider._hf_client.list_repo_tree.side_effect = _raising_tree(EntryNotFoundError("no such directory"))

    assert list(hf_provider.list_objects("missing/")) == []


@pytest.mark.parametrize(
    "error",
    [RepositoryNotFoundError("no such repository"), RevisionNotFoundError("no such revision")],
    ids=["repository", "revision"],
)
def test_list_missing_repository_or_revision_raises(hf_provider, error):
    """A misspelled repository or revision must surface as an error, not as an empty listing."""
    hf_provider._hf_client.list_repo_tree.side_effect = _raising_tree(error)

    with pytest.raises(FileNotFoundError):
        list(hf_provider.list_objects("data/"))


def test_list_returns_files(hf_provider):
    hf_provider._hf_client.list_repo_tree.return_value = [_file("data/a.bin"), _file("data/b.bin")]

    assert [obj.key for obj in hf_provider.list_objects("data/")] == ["data/a.bin", "data/b.bin"]
