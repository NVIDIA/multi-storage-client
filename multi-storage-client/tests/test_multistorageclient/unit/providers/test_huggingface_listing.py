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


def _raising_tree(error: Exception):
    """Mimic ``HfApi.list_repo_tree``: a lazy iterator that raises only once it is consumed."""

    def tree(*_args, **_kwargs):
        raise error
        yield  # pragma: no cover - makes this a generator

    return tree


@pytest.fixture
def hf_provider():
    with patch("multistorageclient.providers.huggingface.HfApi", return_value=MagicMock()):
        provider = HuggingFaceStorageProvider(repository_id="org/repo")
    # The path is not a file, so listing proceeds to list_repo_tree.
    provider._get_object_metadata = MagicMock(side_effect=FileNotFoundError("not a file"))  # type: ignore
    return provider


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
    hf_provider._hf_client.list_repo_tree.return_value = [
        RepoFile(path="data/a.bin", size=1, oid="a", lfs=None, last_commit=None, security=None),
        RepoFile(path="data/b.bin", size=2, oid="b", lfs=None, last_commit=None, security=None),
    ]

    assert [obj.key for obj in hf_provider.list_objects("data/")] == ["data/a.bin", "data/b.bin"]
