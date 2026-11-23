# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import multiprocessing
import os
import pickle
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from multistorageclient import Path, StorageClient, StorageClientConfig, resolve_storage_client
from multistorageclient.client.composite import CompositeStorageClient
from multistorageclient.file import ObjectFile
from multistorageclient.types import (
    ObjectMetadata,
    ResolvedPath,
    ResolvedPathState,
    StorageProvider,
)
from test_multistorageclient.unit.utils.catalog import (
    ENCODED,
    LOGICAL,
    PHYSICAL,
    ROOT,
    CatalogResolver,
    Inventory,
    configure_inventory,
)


@pytest.mark.parametrize("multi", [False, True])
def test_inventory_routes_reads_and_signing_without_writes(tmp_path, monkeypatch, multi):
    client = configure_inventory(tmp_path, monkeypatch, multi)
    delegate = client.delegate
    backend = delegate._child_clients["primary"] if isinstance(delegate, CompositeStorageClient) else delegate
    assert client.url(LOGICAL) == ROOT + ENCODED
    assert [item.key for item in client.list(include_url_prefix=True)] == [ROOT + ENCODED]
    assert [item.key for item in client.list_recursive(include_url_prefix=True)] == [ROOT + ENCODED]
    assert client.glob("./images/../caf*", include_url_prefix=True) == [ROOT + ENCODED]
    with (
        patch.object(backend._storage_provider, "generate_presigned_url", return_value="signed") as signer,
        patch("multistorageclient.replica_manager._REPLICA_THREAD_POOL.submit") as submit,
    ):
        assert client.read(LOGICAL) == b"image"
        with client.open(LOGICAL) as stream:
            assert stream.read() == b"image"
        for method in ("GET", "HEAD"):
            assert client.generate_presigned_url(LOGICAL, method=method) == "signed"
            signer.assert_called_with(PHYSICAL, method=method, signer_type=None, signer_options=None)
        for operation in (client.read, client.open, client.generate_presigned_url):
            with pytest.raises(FileNotFoundError):
                operation("untracked")
        assert signer.call_count == 2
        assert not backend.writable_replicas
        assert client.replicas == delegate.replicas == backend.replicas == []
        replica_file = tmp_path / "mirror" / PHYSICAL
        replica_file.write_bytes(b"replica bytes")
        assert client.read(LOGICAL) == b"replica bytes"
        with pytest.raises(FileNotFoundError):
            client.read(PHYSICAL)
        replica_file.unlink()
        replica = backend._replicas[0]
        for handle in (client, delegate, backend, replica, replica.delegate, StorageClient(client._config)):
            assert handle.profile == client.profile
            for name, args in (
                ("write", ("file", b"new")),
                ("copy", ("old", "new")),
                ("delete", ("file",)),
                ("delete_many", (["file"],)),
                ("make_symlink", ("link", "file")),
                ("upload_file", ("file", __file__)),
                ("upload_files", (["file"], [__file__])),
                ("commit_metadata", ()),
                ("sync_replicas", ("",)),
                ("sync_from", (client,)),
            ):
                with pytest.raises(PermissionError, match="read-only"):
                    getattr(handle, name)(*args)
            for mode in ("wb", "ab", "xb", "rb+", "r+"):
                with pytest.raises(PermissionError, match="read-only"):
                    handle.open("file", mode)
            with pytest.raises(PermissionError, match="read-only"):
                handle.generate_presigned_url("untracked", method="PUT")
        submit.assert_not_called()
    assert list((tmp_path / "mirror").iterdir()) == []
    assert sorted(file.name for file in (tmp_path / "primary").iterdir()) == [PHYSICAL, "untracked"]


@pytest.mark.parametrize("location", ["root", "replica"])
def test_reject_metadata_autocommit_before_registering_work(tmp_path, monkeypatch, location):
    setting = {"at_exit": True, "interval_minutes": 1}
    with (
        patch("multistorageclient.client.single.atexit.register") as register,
        patch("multistorageclient.client.single.threading.Thread.start") as start,
        pytest.raises(ValueError, match="autocommit"),
    ):
        configure_inventory(
            tmp_path,
            monkeypatch,
            autocommit=setting if location == "root" else None,
            replica_autocommit=setting if location == "replica" else None,
        )
    start.assert_not_called()
    register.assert_not_called()


@pytest.mark.parametrize("multi", [False, True])
def test_pickle_and_sync_rewrap_keep_binding_and_backend(tmp_path, monkeypatch, multi):
    client = configure_inventory(tmp_path, monkeypatch, multi)
    delegate = client.delegate
    backend = delegate._child_clients["primary"] if isinstance(delegate, CompositeStorageClient) else delegate
    replica = backend._replicas[0]
    for handle in (client, client._config, replica, replica._config, backend, delegate):
        serialized = pickle.dumps(handle)
        assert os.fsencode(tmp_path) not in serialized
        restored = pickle.loads(serialized)
        if isinstance(restored, StorageClientConfig):
            restored = StorageClient(restored)
        assert restored.url() == ROOT
        assert restored.profile == client.profile
        if handle is replica or handle is replica._config:
            assert not restored.is_file(PHYSICAL)
        elif multi and handle is backend:
            assert restored.read(PHYSICAL) == b"image"
        else:
            assert restored.read(LOGICAL) == b"image"
        with pytest.raises(PermissionError, match="read-only"):
            restored.delete("untracked")

    state = client._config.__getstate__()
    state["resolver_identity"] = ("catalog", "different")
    with pytest.raises(ValueError, match="identity"):
        StorageClientConfig.__new__(StorageClientConfig).__setstate__(state)


def _try_read(func, *args):
    try:
        return func(*args)
    except PermissionError:
        return "denied"


def _fork_worker(output, scope, client, delegate, config, path, listing, posix_stream, object_stream, fd, inode):
    os.environ["MSC_TEST_SCOPE"] = scope
    before = len(CatalogResolver.binds)
    paths = {path}
    prior_hash = hash(path)
    try:
        inherited_fd = os.fstat(fd).st_ino == inode
    except OSError:
        inherited_fd = False
    output.send(
        (
            inherited_fd,
            _try_read(next, listing),
            _try_read(posix_stream.read),
            _try_read(object_stream.read),
            _try_read(path.read_bytes),
            _try_read(client.read, LOGICAL),
            _try_read(delegate.read, LOGICAL),
            _try_read(lambda: StorageClient(config).read(LOGICAL)),
            path in paths and hash(path) == prior_hash and Path(path) in paths and path.parent / path.name in paths,
            len(CatalogResolver.binds) - before,
            Path(str(path)) in paths,
        )
    )
    output.close()


@pytest.mark.skipif("fork" not in multiprocessing.get_all_start_methods(), reason="fork unavailable")
@pytest.mark.parametrize(
    ("parent_scope", "child_scope", "allowed"), [("alice", "alice", True), ("alice", "bob", False), ("", "", False)]
)
def test_forked_handles_authorize_prior_scope_and_discard_streams(
    tmp_path, monkeypatch, parent_scope, child_scope, allowed
):
    monkeypatch.setenv("MSC_TEST_SCOPE", parent_scope)
    client = configure_inventory(tmp_path, monkeypatch)
    ctx = multiprocessing.get_context("fork")
    receiver, sender = ctx.Pipe(duplex=False)
    with client.open(LOGICAL) as posix_stream, ObjectFile(client, LOGICAL) as object_stream:
        assert object_stream.read(0) == b""
        listing = client.list()
        assert next(listing).key == LOGICAL
        fd = posix_stream.fileno()
        process = ctx.Process(
            target=_fork_worker,
            args=(
                sender,
                child_scope,
                client,
                client.delegate,
                client._config,
                Path(client.url(LOGICAL)),
                listing,
                posix_stream,
                object_stream,
                fd,
                os.fstat(fd).st_ino,
            ),
        )
        process.start()
        sender.close()
        try:
            assert receiver.poll(30)
            result = receiver.recv()
        finally:
            process.join(10)
            receiver.close()
    assert process.exitcode == 0
    value = b"image" if allowed else "denied"
    assert result == (False, "denied", "denied", "denied", value, value, value, value, True, int(allowed), allowed)


def _spawn_worker(output, payload):
    try:
        client = pickle.loads(payload)
        output.send((client.url(), client.read(LOGICAL)))
    except PermissionError:
        output.send("denied")
    finally:
        output.close()


def _read_cached_reference(url, scope):
    os.environ["MSC_TEST_SCOPE"] = scope
    before = len(CatalogResolver.binds)
    client, key = resolve_storage_client(url)
    with patch.object(client.delegate, "_storage_provider", spec=StorageProvider) as origin:
        origin.get_object.return_value = b"origin"
        value = client.read(key)
    return client.profile, value, [(ref.root_url, ref.access_scope) for ref in CatalogResolver.binds[before:]]


@pytest.mark.parametrize(
    "method", [method for method in ("fork", "spawn") if method in multiprocessing.get_all_start_methods()]
)
def test_worker_authorizes_and_reads_parent_disk_cache(tmp_path, catalog_config, method):
    profile = {
        "storage_provider": {"type": "file", "options": {"base_path": str(tmp_path)}},
        "metadata_provider": {"type": f"{Inventory.__module__}.Inventory"},
        "caching_enabled": True,
    }
    catalog_config(
        options={"profile": profile}, cache={"location": str(tmp_path / "cache"), "check_source_version": False}
    )
    client, key = resolve_storage_client(ROOT + ENCODED)
    with patch.object(client.delegate, "_storage_provider", spec=StorageProvider) as origin:
        origin.get_object.return_value = b"parent"
        assert client.read(key) == b"parent"
    with multiprocessing.get_context(method).Pool(1) as pool:
        alice = pool.apply_async(_read_cached_reference, (client.url(key), "alice")).get(timeout=30)
        bob = pool.apply_async(_read_cached_reference, (client.url(key), "bob")).get(timeout=30)
    assert alice == (client.profile, b"parent", [(ROOT, "alice")])
    assert bob[0] != client.profile
    assert bob[1:] == (b"origin", [(ROOT, "bob")])


@pytest.mark.parametrize("scope", ["new-user", "denied"])
def test_spawn_uses_receiving_credentials_and_reopens_pinned_revision(tmp_path, monkeypatch, scope):
    payload = pickle.dumps(configure_inventory(tmp_path, monkeypatch))
    monkeypatch.setenv("MSC_TEST_REVISION", "r2")
    monkeypatch.setenv("MSC_TEST_SCOPE", scope)
    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    process = ctx.Process(target=_spawn_worker, args=(sender, payload))
    process.start()
    sender.close()
    try:
        assert receiver.poll(30)
        result = receiver.recv()
    finally:
        process.join(10)
        receiver.close()
    assert process.exitcode == 0
    assert result == ("denied" if scope == "denied" else (ROOT, b"image"))


def test_bound_posix_storage_stays_inside_configured_root(tmp_path):
    directory = tmp_path / "dataset"
    directory.mkdir()
    (directory / "item").write_bytes(b"ok")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"secret")
    (directory / "link").symlink_to(outside, target_is_directory=True)
    config = {"profiles": {"dataset": {"storage_provider": {"type": "file", "options": {"base_path": str(directory)}}}}}
    client = StorageClient(StorageClientConfig._from_bound_profile(config, "dataset", ROOT, "alice"))
    for key in ("../outside/secret", "link/secret"):
        for method in (
            client.read,
            client.info,
            client.open,
            lambda path: list(client.list(path)),
        ):
            with pytest.raises(PermissionError, match="outside the bound storage directory"):
                method(key)
    assert client.read("item") == b"ok"
    assert client.glob("**/*") == ["item"]
    with pytest.raises(PermissionError):
        client.glob("../outside/*")

    ordinary = StorageClient(StorageClientConfig.from_dict(config, "dataset"))
    assert ordinary.read("link/secret") == b"secret"
    assert ordinary.get_posix_path("item") == str(directory / "item")
    with ordinary.open("item") as stream:
        assert stream.resolve_filesystem_path() == str(directory / "item")


def test_scoped_cache_separates_object_keys_and_authorizes_cached_opens(tmp_path):
    from unittest.mock import Mock

    from multistorageclient.types import Range

    cache_root = tmp_path / "cache"
    profile = {
        "storage_provider": {"type": "file", "options": {"base_path": str(tmp_path)}},
        "metadata_provider": {"type": f"{Inventory.__module__}.Inventory"},
        "caching_enabled": True,
    }
    config = {"profiles": {"dataset": profile}, "cache": {"location": str(cache_root), "check_source_version": False}}
    client = StorageClient(StorageClientConfig._from_bound_profile(config, "dataset", ROOT, "alice"))
    physical = "../../secret"
    data = b"bytes"
    provider = Mock(spec=StorageProvider)
    provider.get_object.side_effect = lambda key, byte_range=None: data
    provider.get_object_metadata.return_value = ObjectMetadata(
        key=physical, content_length=len(data), last_modified=datetime.now(UTC)
    )
    client._storage_provider = provider
    metadata = client._metadata_provider
    assert metadata is not None
    lookup = Mock(return_value=ResolvedPath(physical, ResolvedPathState.EXISTS))
    metadata.realpath = lookup

    # Range misses must use the physical object key on the provider even though the disk filename is hashed.
    assert client.read(LOGICAL, Range(1, 2)) == b"yt"
    assert provider.get_object.call_args.args[0] == physical
    cache = client._cache_manager
    assert cache is not None
    cache.set(physical, data)
    cache.set("secret", b"different")
    assert not (tmp_path / "secret").exists()
    assert cache.read(physical) == data and cache.read("secret") == b"different"
    provider.get_object.reset_mock()
    assert client.read(LOGICAL) == data
    with client.open(LOGICAL) as stream:
        assert stream.read() == data
        with pytest.raises(PermissionError, match="download_file"):
            stream.resolve_filesystem_path()
    assert cache.read(physical) == data
    provider.get_object.assert_not_called()
    provider.download_file.assert_not_called()

    lookup.side_effect = PermissionError("revoked")
    for access in (client.read, client.open):
        with pytest.raises(PermissionError, match="revoked"):
            access(LOGICAL)
    provider.get_object.assert_not_called()
    provider.download_file.assert_not_called()
