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
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.providers.oci import OracleStorageProvider

LAST_MODIFIED = datetime(2025, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def oci_provider():
    with patch.object(OracleStorageProvider, "_create_oci_client", return_value=MagicMock()):
        yield OracleStorageProvider(namespace="test-ns", base_path="mybucket")


def _obj(name: str):
    return SimpleNamespace(name=name, size=1, time_modified=LAST_MODIFIED, etag=name)


def test_oci_list_objects_strips_bucket_from_start_after_and_end_at(oci_provider):
    """start_after/end_at arrive bucket-qualified from BaseStorageProvider and must be compared bucket-relative."""
    oci_provider._oci_client.list_objects.return_value = SimpleNamespace(
        data=SimpleNamespace(prefixes=[], objects=[_obj("dir/b"), _obj("dir/c"), _obj("dir/d")], next_start_with=None)
    )

    keys = [obj.key for obj in oci_provider.list_objects("dir/", start_after="dir/a", end_at="dir/c")]

    assert keys == ["dir/b", "dir/c"]
    call_kwargs = oci_provider._oci_client.list_objects.call_args.kwargs
    assert call_kwargs["bucket_name"] == "mybucket"
    assert call_kwargs["prefix"] == "dir/"
    assert call_kwargs["start"] == "dir/a"
