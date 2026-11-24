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

import builtins
import errno
import os
import stat
from io import BytesIO, StringIO

import pytest

import multistorageclient as msc
from multistorageclient import StorageClient, StorageClientConfig, resolve_storage_client
from multistorageclient.file import PosixFile
from multistorageclient.types import BatchTransferError, Range, SuffixRange, SyncError
from test_multistorageclient.unit.utils import catalog


def _bound_client(root):
    profile = {"storage_provider": {"type": "file", "options": {"base_path": str(root)}}}
    config = {"profiles": {"dataset": profile}}
    return StorageClient(StorageClientConfig._from_bound_profile(config, "dataset", catalog.ROOT, "alice"))


def _swap_on_open(monkeypatch, source, outside):
    ordinary_open = builtins.open

    def swap_and_open(path, *args, **kwargs):
        if path == str(source) and not source.is_symlink():
            source.rename(source.with_suffix(".original"))
            source.symlink_to(outside)
        return ordinary_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", swap_and_open)


@pytest.mark.parametrize("operation", ["open", "read", "range", "suffix", "download", "bytes", "text", "batch"])
def test_bound_posix_rejects_replacement_before_byte_open(tmp_path, monkeypatch, operation):
    root = tmp_path / "dataset"
    root.mkdir()
    source = root / "entry"
    source.write_bytes(b"inside")
    outside = tmp_path / "outside"
    outside.write_bytes(b"secret")
    dest = tmp_path / "download"
    buffer = StringIO() if operation == "text" else BytesIO()
    client = _bound_client(root)
    _swap_on_open(monkeypatch, source, outside)

    with pytest.raises(
        (PermissionError, RuntimeError, BatchTransferError), match="outside the bound storage directory"
    ):
        if operation == "open":
            with client.open("entry") as stream:
                stream.read()
        elif operation in ("read", "range", "suffix"):
            ranges = {"range": Range(0, 2), "suffix": SuffixRange(2)}
            client.read("entry", ranges.get(operation))
        elif operation == "batch":
            client.download_files(["entry"], [str(dest)])
        else:
            client.download_file("entry", str(dest) if operation == "download" else buffer)
    assert source.is_symlink()
    assert not dest.exists()
    assert not buffer.getvalue()


@pytest.mark.parametrize("replaced", ["dataset", "subdir", "item"])
def test_bound_posix_rejects_symlink_swap_after_resolving_contained_links(tmp_path, monkeypatch, replaced):
    root = tmp_path / "dataset"
    subdir = root / "subdir"
    subdir.mkdir(parents=True)
    item = subdir / "item"
    item.write_bytes(b"inside")
    (root / "alias").symlink_to(subdir, target_is_directory=True)
    (subdir / "linked").symlink_to("item")
    outside = tmp_path / "outside"
    (outside / "subdir").mkdir(parents=True)
    (outside / "subdir" / "item").write_bytes(b"secret")
    client = _bound_client(root)
    with client.open("alias/linked", mode="rt", encoding="utf-8") as stream:
        assert stream.read() == "inside"

    swap = {"dataset": root, "subdir": subdir, "item": item}[replaced]
    target = outside / swap.relative_to(root)
    fd_open = os.open

    def swap_before_component_open(path, flags, *args, **kwargs):
        if path == replaced and kwargs.get("dir_fd") is not None:
            swap.rename(swap.with_suffix(".original"))
            swap.symlink_to(target, target_is_directory=target.is_dir())
        return fd_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", swap_before_component_open)
    with pytest.raises(OSError) as error, client.open("alias/linked"):
        pytest.fail("Opened the replacement symlink")
    assert error.value.errno in (errno.ELOOP, errno.ENOTDIR)
    assert swap.is_symlink()


@pytest.mark.parametrize("posix_target", [True, False])
def test_reference_sync_reads_posix_source_through_confined_open(tmp_path, monkeypatch, posix_target):
    monkeypatch.setattr(catalog, "LOGICAL", "entry")
    client = catalog.configure_inventory(tmp_path, monkeypatch)
    source = tmp_path / "primary" / catalog.PHYSICAL
    outside = tmp_path / "outside"
    outside.write_bytes(b"secret")
    dest = tmp_path / "chosen"
    target, prefix = resolve_storage_client(str(dest))
    if not posix_target:
        monkeypatch.setattr(target.delegate, "_is_posix_file_storage_provider", lambda: False)
    _swap_on_open(monkeypatch, source, outside)

    with pytest.raises(RuntimeError, match="outside the bound storage directory"):
        target.sync_from(client, target_path=prefix, num_worker_processes=1)
    assert source.is_symlink()
    assert not (dest / "entry").exists()


def test_reference_posix_sync_preserves_file_mode(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "LOGICAL", "entry")
    client = catalog.configure_inventory(tmp_path, monkeypatch)
    source = tmp_path / "primary" / catalog.PHYSICAL
    source.chmod(0o751)
    dest = tmp_path / "chosen"
    target, prefix = resolve_storage_client(str(dest))

    target.sync_from(client, target_path=prefix, num_worker_processes=1)
    copied = dest / "entry"
    assert copied.read_bytes() == b"image"
    assert stat.S_IMODE(copied.stat().st_mode) == 0o751


def test_reference_posix_sync_replaces_hardlinked_destination(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "LOGICAL", "entry")
    source = catalog.configure_inventory(tmp_path, monkeypatch)
    outside = tmp_path / "outside"
    outside.write_bytes(b"original outside contents")
    dest = tmp_path / "chosen"
    dest.mkdir()
    entry = dest / "entry"
    entry.hardlink_to(outside)
    target, prefix = resolve_storage_client(str(dest))

    target.sync_from(source, target_path=prefix, num_worker_processes=1)

    assert outside.read_bytes() == b"original outside contents"
    assert entry.read_bytes() == b"image"


def test_reference_posix_sync_preserves_destination_after_partial_copy(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "LOGICAL", "entry")
    source = catalog.configure_inventory(tmp_path, monkeypatch)
    dest = tmp_path / "chosen"
    dest.mkdir()
    entry = dest / "entry"
    entry.write_bytes(b"previous contents")
    target, prefix = resolve_storage_client(str(dest))
    original_read = PosixFile.read

    def fail_after_prefix(stream, size=-1):
        if stream.tell():
            raise OSError("injected source read failure")
        return original_read(stream, 2)

    monkeypatch.setattr(PosixFile, "read", fail_after_prefix)
    with pytest.raises(SyncError, match="injected source read failure"):
        target.sync_from(source, target_path=prefix, num_worker_processes=1)

    assert entry.read_bytes() == b"previous contents"
    assert list(dest.iterdir()) == [entry]


@pytest.mark.parametrize("access", ["get_posix_path", "as_posix", "load", "memmap"])
def test_reference_denies_backing_path_exports(tmp_path, monkeypatch, access):
    import numpy as np

    key = "input.npy"
    monkeypatch.setattr(catalog, "LOGICAL", key)
    client = catalog.configure_inventory(tmp_path, monkeypatch)
    source = tmp_path / "primary" / catalog.PHYSICAL
    with source.open("wb") as stream:
        np.save(stream, np.array([7]))
    url = client.url(key)
    with pytest.raises(PermissionError, match="download_file"):
        if access == "get_posix_path":
            client.get_posix_path(key)
        elif access == "as_posix":
            msc.Path(url).as_posix()
        elif access == "load":
            mapped = msc.numpy.load(url, mmap_mode="r+")
            mapped[0] = 11
            mapped.flush()
        else:
            msc.numpy.memmap(url, mode="r")
    with client.open(key, "rb") as stream:
        assert np.load(stream).tolist() == [7]
