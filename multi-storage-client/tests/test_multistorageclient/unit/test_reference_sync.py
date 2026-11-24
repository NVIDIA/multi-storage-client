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

from unittest.mock import Mock

import pytest

from multistorageclient import resolve_storage_client
from multistorageclient.types import SymlinkHandling
from test_multistorageclient.unit.utils import catalog


@pytest.mark.parametrize(
    ("key", "setup"),
    [
        ("public/../../outside/file", "none"),
        ("linked/file", "symlink_parent"),
        ("file", "symlink_file"),
        ("file", "directory"),
    ],
)
def test_reference_sync_confines_posix_writes_to_requested_directory(tmp_path, monkeypatch, key, setup):
    monkeypatch.setattr(catalog, "LOGICAL", key)
    source = catalog.configure_inventory(tmp_path, monkeypatch)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "file"
    sentinel.write_bytes(b"original")
    dest = tmp_path / "chosen"
    dest.mkdir()
    if setup == "symlink_parent":
        (dest / "linked").symlink_to(outside, target_is_directory=True)
    elif setup == "symlink_file":
        (dest / "file").symlink_to(sentinel)
    elif setup == "directory":
        (dest / "file").mkdir()
        (dest / "file" / catalog.PHYSICAL).symlink_to(sentinel)

    target, prefix = resolve_storage_client(str(dest))
    with pytest.raises(RuntimeError, match="POSIX"):
        target.sync_from(source, target_path=prefix, num_worker_processes=1, ignore_hidden=False)
    assert sentinel.read_bytes() == b"original"


def test_reference_sync_confines_deletions_through_directory_symlinks(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "LOGICAL", "new")
    source = catalog.configure_inventory(tmp_path, monkeypatch)
    dest = tmp_path / "chosen"
    dest.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "extra"
    sentinel.write_bytes(b"original")
    (dest / "linked").symlink_to(outside, target_is_directory=True)

    target, prefix = resolve_storage_client(str(dest))
    with pytest.raises(RuntimeError, match="escapes POSIX destination"):
        target.sync_from(source, target_path=prefix, delete_unmatched_files=True, num_worker_processes=1)
    assert sentinel.read_bytes() == b"original"


@pytest.mark.parametrize(("link_target", "allowed"), [("../inside", True), ("../../outside", False)])
def test_reference_sync_checks_preserved_symlink_target(tmp_path, monkeypatch, link_target, allowed):
    key = "nested/link"
    monkeypatch.setattr(catalog, "LOGICAL", key)
    source = catalog.configure_inventory(tmp_path, monkeypatch)
    inventory = source._metadata_provider
    assert isinstance(inventory, catalog.Inventory)
    inventory._files[key].symlink_target = link_target
    dest = tmp_path / "chosen"
    target, prefix = resolve_storage_client(str(dest))
    kwargs = {"target_path": prefix, "num_worker_processes": 1, "symlink_handling": SymlinkHandling.PRESERVE}
    if allowed:
        target.sync_from(source, **kwargs)
        assert (dest / key).readlink().as_posix() == link_target
    else:
        with pytest.raises(RuntimeError, match="escapes POSIX destination"):
            target.sync_from(source, **kwargs)
        assert not (dest / key).is_symlink()


def test_reference_sync_checks_symlinks_created_in_the_same_batch(tmp_path, monkeypatch):
    source = catalog.configure_inventory(tmp_path, monkeypatch)
    inventory = source._metadata_provider
    assert isinstance(inventory, catalog.Inventory)
    entry = inventory._files.pop(catalog.LOGICAL)
    inventory._files["dir/sub"] = entry.replace(key="dir/sub", symlink_target="..")
    inventory._files["dir/sub/link"] = entry.replace(key="dir/sub/link", symlink_target="../../outside")
    dest = tmp_path / "chosen"
    target, prefix = resolve_storage_client(str(dest))
    with pytest.raises(RuntimeError, match="POSIX"):
        target.sync_from(source, target_path=prefix, num_worker_processes=1, symlink_handling=SymlinkHandling.PRESERVE)
    assert (dest / "dir/sub").is_symlink()
    assert not (dest / "link").is_symlink()


def test_reference_sync_keeps_object_destination_key_literal(tmp_path, monkeypatch):
    key = "a/../../object"
    monkeypatch.setattr(catalog, "LOGICAL", key)
    source = catalog.configure_inventory(tmp_path, monkeypatch, multi=True)
    target, _ = resolve_storage_client(str(tmp_path))
    monkeypatch.setattr(target.delegate, "_is_posix_file_storage_provider", lambda: False)
    monkeypatch.setattr(target.delegate, "list_recursive", lambda **kwargs: iter(()))
    upload = Mock()
    monkeypatch.setattr(target.delegate, "upload_files", upload)
    target.sync_from(source, target_path="objects", num_worker_processes=1, ignore_hidden=False)
    assert upload.call_args.args[0] == ["objects/" + key]
