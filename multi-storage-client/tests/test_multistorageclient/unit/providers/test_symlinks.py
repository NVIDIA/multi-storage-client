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

import os
import tempfile
import uuid

import pytest

import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.types import SymlinkHandling
from test_multistorageclient.utils.wait import wait_for_is_file


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryAzureBlobStorageContainer],
        [tempdatastore.TemporaryGoogleCloudStorageBucket],
        [tempdatastore.TemporaryGoogleCloudStorageS3Bucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
def test_list_symlinks_from_object_storage(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """
    Verify that symlinks pointing to both *files* and *directories* are
    surfaced correctly by :py:meth:`StorageClient.info` and
    :py:meth:`StorageClient.list` on an object-storage backend.

    Object stores have no native symlink concept, so MSC represents a symlink
    as an empty object whose user metadata stores the relative target path
    (see ``msc-symlink-target`` in the provider implementations). This works
    the same regardless of whether the target is a file or a directory --
    the target is just a string.

    The test sets up the following layout under a unique random prefix::

        <prefix>/
        ├── target.txt              (regular file)
        ├── symlink_to_file         (symlink object whose target is "target.txt")
        ├── subdir/
        │   └── file.txt            (regular file inside subdir)
        └── symlink_to_subdir       (symlink object whose target is "subdir")

    The flat listing ``list(prefix=<prefix>)`` yields exactly these four
    entries (symlink objects appear alongside regular files, with their
    ``symlink_target`` populated):

    ================================  ===============  ====================
    key                               type             symlink_target
    ================================  ===============  ====================
    ``<prefix>/target.txt``           ``file``         ``None``
    ``<prefix>/symlink_to_file``      ``file``         ``"target.txt"``
    ``<prefix>/subdir/file.txt``      ``file``         ``None``
    ``<prefix>/symlink_to_subdir``    ``file``         ``"subdir"``
    ================================  ===============  ====================

    The hierarchical listing
    ``list(prefix=<prefix>/, include_directories=True)`` yields exactly these
    four entries -- the real ``subdir`` is collapsed into a single
    ``directory`` entry, but both symlink objects stay visible as their own
    entries (directory symlinks are **not** collapsed into the directory
    listing):

    ================================  ===============  ====================
    key                               type             symlink_target
    ================================  ===============  ====================
    ``<prefix>/target.txt``           ``file``         ``None``
    ``<prefix>/symlink_to_file``      ``file``         ``"target.txt"``
    ``<prefix>/subdir``               ``directory``    ``None``
    ``<prefix>/symlink_to_subdir``    ``file``         ``"subdir"``
    ================================  ===============  ====================

    Also asserts that ``info(...).symlink_target`` returns the relative
    target for both the file symlink (``"target.txt"``) and the directory
    symlink (``"subdir"``), and that ``is_file`` reports ``True`` for both
    because each symlink itself is an empty object.
    """
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        symlink_prefix = f"{uuid.uuid4().hex}"
        target_file_path = f"{symlink_prefix}/target.txt"
        symlink_to_file_path = f"{symlink_prefix}/symlink_to_file"
        subdir = f"{symlink_prefix}/subdir"
        subdir_file_path = f"{subdir}/file.txt"
        symlink_to_subdir_path = f"{symlink_prefix}/symlink_to_subdir"

        storage_client.write(path=target_file_path, body=b"target content")
        wait_for_is_file(storage_client=storage_client, path=target_file_path, is_file=True)

        storage_client.write(path=subdir_file_path, body=b"file content")
        wait_for_is_file(storage_client=storage_client, path=subdir_file_path, is_file=True)

        storage_client.make_symlink(path=symlink_to_file_path, target=target_file_path)
        wait_for_is_file(storage_client=storage_client, path=symlink_to_file_path, is_file=True)

        storage_client.make_symlink(path=symlink_to_subdir_path, target=subdir)
        wait_for_is_file(storage_client=storage_client, path=symlink_to_subdir_path, is_file=True)

        assert storage_client.is_file(path=symlink_to_file_path)
        assert storage_client.info(path=symlink_to_file_path).symlink_target == "target.txt"
        assert storage_client.is_file(path=symlink_to_subdir_path)
        assert storage_client.info(path=symlink_to_subdir_path).symlink_target == "subdir"

        # Flat listing returns all underlying objects under the prefix: the
        # two regular files plus the two symlink objects. Only the symlinks
        # carry a non-None symlink_target.
        objects = list(storage_client.list(prefix=symlink_prefix))
        objects_by_key = {o.key: o for o in objects}
        assert len(objects) == 4
        assert objects_by_key[target_file_path].type == "file"
        assert objects_by_key[target_file_path].symlink_target is None
        assert objects_by_key[symlink_to_file_path].type == "file"
        assert objects_by_key[symlink_to_file_path].symlink_target == "target.txt"
        assert objects_by_key[subdir_file_path].type == "file"
        assert objects_by_key[subdir_file_path].symlink_target is None
        assert objects_by_key[symlink_to_subdir_path].type == "file"
        assert objects_by_key[symlink_to_subdir_path].symlink_target == "subdir"

        # Listing with include_directories=True at the top-level prefix
        # returns the real subdir as a directory entry and the two symlink
        # objects (including the directory symlink) as separate file entries
        # that still carry their symlink targets.
        entries = list(storage_client.list(prefix=f"{symlink_prefix}/", include_directories=True))
        entries_by_key = {e.key: e for e in entries}
        assert len(entries) == 4
        assert entries_by_key[subdir].type == "directory"
        assert entries_by_key[subdir].symlink_target is None
        assert entries_by_key[target_file_path].type == "file"
        assert entries_by_key[target_file_path].symlink_target is None
        assert entries_by_key[symlink_to_file_path].type == "file"
        assert entries_by_key[symlink_to_file_path].symlink_target == "target.txt"
        assert entries_by_key[symlink_to_subdir_path].type == "file"
        assert entries_by_key[symlink_to_subdir_path].symlink_target == "subdir"

        # Delete the symlinks and the target file.
        storage_client.delete(path=symlink_to_file_path)
        storage_client.delete(path=symlink_to_subdir_path)
        storage_client.delete(path=target_file_path)
        storage_client.delete(path=subdir_file_path)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryAzureBlobStorageContainer],
        [tempdatastore.TemporaryGoogleCloudStorageBucket],
        [tempdatastore.TemporaryGoogleCloudStorageS3Bucket],
        [tempdatastore.TemporarySwiftStackBucket],
        [tempdatastore.TemporaryAIStoreBucket],
    ],
)
def test_make_symlink_relative_target_is_portable(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """
    Verify that :py:meth:`StorageClient.make_symlink` stores a *portable*
    relative target on every storage backend.

    MSC defines the stored symlink target as the logical target's path
    expressed **relative to the symlink's own parent directory**. Every
    provider's ``_make_symlink`` computes this with ``posixpath.relpath``
    (object stores) or ``os.path.relpath`` (POSIX), so the resulting
    string is identical regardless of backend. This portability is what
    lets a symlink survive being copied or synced between two different
    providers (POSIX <-> S3 <-> Azure <-> GCS ...) without rewriting its
    target -- the relative path resolves the same way under any root.

    This test mirrors
    :py:func:`test_posix_file.test_make_symlink_creates_parent_directories`
    but exercises every supported backend instead of only POSIX. The
    layout created under a unique random prefix is::

        <prefix>/
        ├── target.txt               (regular file)
        └── sub/
            └── dir/
                └── link.txt  -> "../../target.txt"

    The symlink sits two directory levels below the target so the
    relative target must contain two ``..`` hops. The test asserts that
    :py:meth:`StorageClient.info` reports
    ``symlink_target == "../../target.txt"`` on every provider, and that
    :py:meth:`StorageClient.is_file` returns ``True`` for the symlink
    (the symlink itself is a zero-byte marker object on object stores
    and a native OS symlink on POSIX).
    """
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        prefix = f"{uuid.uuid4().hex}"
        target_path = f"{prefix}/target.txt"
        symlink_path = f"{prefix}/sub/dir/link.txt"

        storage_client.write(path=target_path, body=b"target content")
        wait_for_is_file(storage_client=storage_client, path=target_path, is_file=True)

        storage_client.make_symlink(path=symlink_path, target=target_path)
        wait_for_is_file(storage_client=storage_client, path=symlink_path, is_file=True)

        assert storage_client.is_file(path=symlink_path)
        assert storage_client.info(path=symlink_path).symlink_target == "../../target.txt"

        storage_client.delete(path=symlink_path)
        storage_client.delete(path=target_path)


def _build_posix_storage_client_with_symlink_tree(
    temp_data_store: tempdatastore.TemporaryDataStore,
) -> StorageClient:
    """
    Build a :py:class:`StorageClient` whose underlying POSIX ``base_path``
    contains the standard symlink tree used by the POSIX
    ``symlink_handling`` tests. The layout mirrors the example documented in
    ``Symlink_Support_in_MSC.md``::

        base_dir/
        ├── c.txt
        ├── d.txt  -> dir2/b.txt      (file symlink)
        ├── dir1/
        │   └── a.txt
        ├── dir2/
        │   └── b.txt
        └── dir3   -> dir1            (directory symlink)

    ``make_symlink`` is used so the provider creates *relative* native OS
    symlinks, matching how real-world POSIX trees look.

    The fixture's ``base_path`` is resolved via :py:func:`os.path.realpath`
    before constructing the client -- this matters on macOS where ``/var``
    is itself a symlink to ``/private/var``. The POSIX provider calls
    ``os.path.realpath`` on symlink targets during listing and compares the
    result to ``base_path``; without the up-front resolution, every symlink
    would appear to point *outside* ``base_path``.
    """
    profile = "data"
    profile_config = temp_data_store.profile_config_dict()
    profile_config["storage_provider"]["options"]["base_path"] = os.path.realpath(
        profile_config["storage_provider"]["options"]["base_path"]
    )
    config_dict = {"profiles": {profile: profile_config}}
    storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

    storage_client.write(path="c.txt", body=b"c content")
    storage_client.write(path="dir1/a.txt", body=b"a content")
    storage_client.write(path="dir2/b.txt", body=b"b content")
    storage_client.make_symlink(path="d.txt", target="dir2/b.txt")
    storage_client.make_symlink(path="dir3", target="dir1")

    return storage_client


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryPOSIXDirectory]],
)
def test_list_symlinks_from_posix_with_follow(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """
    ``symlink_handling=SymlinkHandling.FOLLOW`` (the default) makes symlinks
    completely transparent on POSIX: file symlinks resolve to their target's
    content and directory symlinks are traversed. No ``symlink_target`` is
    ever reported, and content duplication is expected when directory
    symlinks point into the listed tree.

    For the standard test tree, the listing yields exactly these five
    entries (``dir3/a.txt`` is the same file as ``dir1/a.txt``, reached
    through the directory symlink ``dir3 -> dir1``):

    ======================  ===============  =========================
    key                     type             symlink_target
    ======================  ===============  =========================
    ``c.txt``               ``file``         ``None``
    ``d.txt``               ``file``         ``None``
    ``dir1/a.txt``          ``file``         ``None``
    ``dir2/b.txt``          ``file``         ``None``
    ``dir3/a.txt``          ``file``         ``None``
    ======================  ===============  =========================

    This is the backward-compatible behavior that matches the legacy
    ``follow_symlinks=True`` listing semantics.
    """
    with temp_data_store_type() as temp_data_store:
        storage_client = _build_posix_storage_client_with_symlink_tree(temp_data_store)

        objects = list(storage_client.list(prefix="", symlink_handling=SymlinkHandling.FOLLOW))
        objects_by_key = {o.key: o for o in objects}

        assert set(objects_by_key.keys()) == {
            "c.txt",
            "d.txt",
            "dir1/a.txt",
            "dir2/b.txt",
            "dir3/a.txt",
        }
        for o in objects:
            assert o.type == "file"
            assert o.symlink_target is None


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryPOSIXDirectory]],
)
def test_list_symlinks_from_posix_with_skip(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """
    ``symlink_handling=SymlinkHandling.SKIP`` excludes every symlink from the
    listing, regardless of whether it points at a file or a directory. File
    symlinks are dropped and directory symlinks are neither emitted nor
    traversed.

    For the standard test tree, the listing yields exactly these three
    entries -- the real files only (``d.txt`` and everything under the
    ``dir3`` directory symlink are absent):

    ======================  ===============  =========================
    key                     type             symlink_target
    ======================  ===============  =========================
    ``c.txt``               ``file``         ``None``
    ``dir1/a.txt``          ``file``         ``None``
    ``dir2/b.txt``          ``file``         ``None``
    ======================  ===============  =========================

    This matches the legacy ``follow_symlinks=False`` listing semantics.
    """
    with temp_data_store_type() as temp_data_store:
        storage_client = _build_posix_storage_client_with_symlink_tree(temp_data_store)

        objects = list(storage_client.list(prefix="", symlink_handling=SymlinkHandling.SKIP))
        objects_by_key = {o.key: o for o in objects}

        assert set(objects_by_key.keys()) == {"c.txt", "dir1/a.txt", "dir2/b.txt"}
        for o in objects:
            assert o.symlink_target is None


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryPOSIXDirectory]],
)
def test_list_symlinks_from_posix_with_preserve(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """
    ``symlink_handling=SymlinkHandling.PRESERVE`` surfaces symlinks as leaf
    entries with ``ObjectMetadata.symlink_target`` populated, and never
    recurses into directory symlinks.

    For the standard test tree, the listing yields exactly these five
    entries (symlinks are emitted regardless of ``include_directories`` --
    see the note in ``Symlink_Support_in_MSC.md``):

    ======================  ===============  =========================
    key                     type             symlink_target
    ======================  ===============  =========================
    ``c.txt``               ``file``         ``None``
    ``d.txt``               ``file``         ``"dir2/b.txt"``
    ``dir1/a.txt``          ``file``         ``None``
    ``dir2/b.txt``          ``file``         ``None``
    ``dir3``                ``directory``    ``"dir1"``
    ======================  ===============  =========================

    Critically, ``dir3/a.txt`` is **absent** -- the directory symlink is a
    leaf entry and is not traversed, so content duplication is avoided.
    """
    with temp_data_store_type() as temp_data_store:
        storage_client = _build_posix_storage_client_with_symlink_tree(temp_data_store)

        objects = list(storage_client.list(prefix="", symlink_handling=SymlinkHandling.PRESERVE))
        objects_by_key = {o.key: o for o in objects}

        assert set(objects_by_key.keys()) == {
            "c.txt",
            "d.txt",
            "dir1/a.txt",
            "dir2/b.txt",
            "dir3",
        }

        assert objects_by_key["c.txt"].type == "file"
        assert objects_by_key["c.txt"].symlink_target is None

        assert objects_by_key["dir1/a.txt"].type == "file"
        assert objects_by_key["dir1/a.txt"].symlink_target is None

        assert objects_by_key["dir2/b.txt"].type == "file"
        assert objects_by_key["dir2/b.txt"].symlink_target is None

        assert objects_by_key["d.txt"].type == "file"
        assert objects_by_key["d.txt"].symlink_target == "dir2/b.txt"

        assert objects_by_key["dir3"].type == "directory"
        assert objects_by_key["dir3"].symlink_target == "dir1"


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryPOSIXDirectory]],
)
def test_list_symlinks_from_posix_with_preserve_raises_on_external_target(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
):
    """
    ``symlink_handling=SymlinkHandling.PRESERVE`` must raise ``ValueError``
    when a symlink's resolved target lives outside ``base_path``.

    MSC surfaces preserved symlinks as portable relative paths that must
    resolve to a key inside ``base_path``; a target outside ``base_path``
    has no valid relative key to record, so the provider fails fast with a
    message telling the caller to switch to ``FOLLOW`` (to dereference the
    content) or ``SKIP`` (to ignore the symlink).

    Layout created by this test::

        base_dir/
        ├── c.txt
        └── escaping    -> <outside_dir>/outside.txt   (points OUTSIDE base_dir)
        <outside_dir>/
        └── outside.txt

    The symlink is created directly via :py:func:`os.symlink` so the
    on-disk target unambiguously escapes ``base_dir``; this mirrors how
    external symlinks end up on a real POSIX tree.
    """
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        profile_config = temp_data_store.profile_config_dict()
        base_path = os.path.realpath(profile_config["storage_provider"]["options"]["base_path"])
        profile_config["storage_provider"]["options"]["base_path"] = base_path
        config_dict = {"profiles": {profile: profile_config}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        storage_client.write(path="c.txt", body=b"c content")

        with tempfile.TemporaryDirectory() as outside_dir:
            outside_file = os.path.join(os.path.realpath(outside_dir), "outside.txt")
            with open(outside_file, "wb") as f:
                f.write(b"outside content")

            os.symlink(outside_file, os.path.join(base_path, "escaping"))

            with pytest.raises(ValueError, match="points outside the base directory"):
                list(storage_client.list(prefix="", symlink_handling=SymlinkHandling.PRESERVE))


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryPOSIXDirectory]],
)
def test_list_symlinks_from_posix_with_preserve_raises_on_broken_target(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
):
    """
    ``symlink_handling=SymlinkHandling.PRESERVE`` must raise ``ValueError``
    when a symlink's target does not exist (broken / dangling symlink).

    MSC surfaces preserved symlinks as leaf entries backed by a real target;
    a broken symlink has no target to record size or type for and cannot be
    dereferenced by ``FOLLOW`` either, so the provider fails fast with a
    message telling the caller to switch to ``SKIP`` to ignore the symlink.

    Layout created by this test::

        base_dir/
        ├── c.txt
        └── dangling   -> missing.txt   (target does not exist)
    """
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        profile_config = temp_data_store.profile_config_dict()
        base_path = os.path.realpath(profile_config["storage_provider"]["options"]["base_path"])
        profile_config["storage_provider"]["options"]["base_path"] = base_path
        config_dict = {"profiles": {profile: profile_config}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        storage_client.write(path="c.txt", body=b"c content")

        os.symlink(os.path.join(base_path, "missing.txt"), os.path.join(base_path, "dangling"))

        with pytest.raises(ValueError, match="Broken symlink"):
            list(storage_client.list(prefix="", symlink_handling=SymlinkHandling.PRESERVE))


@pytest.mark.serial
def test_sync_from_posix_to_s3_to_posix_preserves_symlinks():
    """
    End-to-end round trip that verifies :py:attr:`SymlinkHandling.PRESERVE`
    survives a POSIX → S3 → POSIX sync sequence.

    The source POSIX layout (``posix_src/``) matches the standard tree used
    by the POSIX symlink tests plus one extra symlink inside a subdirectory
    to exercise non-root key translation::

        posix_src/
        ├── c.txt                                (regular file)
        ├── d.txt               -> dir2/b.txt    (file symlink)
        ├── dir1/
        │   └── a.txt                            (regular file)
        ├── dir2/
        │   └── b.txt                            (regular file)
        ├── dir3                -> dir1          (directory symlink)
        └── subdir/
            ├── file.txt                         (regular file)
            └── link_to_sibling -> file.txt      (file symlink in subdir)

    Expected end-state on the destination POSIX (``posix_dst/``) after the
    round trip::

        posix_dst/
        ├── c.txt                                (regular file, content = "c content")
        ├── d.txt               -> dir2/b.txt    (file symlink)
        ├── dir1/
        │   └── a.txt                            (regular file, content = "a content")
        ├── dir2/
        │   └── b.txt                            (regular file, content = "b content")
        ├── dir3                -> dir1          (directory symlink)
        └── subdir/
            ├── file.txt                         (regular file, content = "subdir file")
            └── link_to_sibling -> file.txt      (file symlink in subdir)

    Assertions:

    * On the S3 bucket, every symlink key materialises as a **zero-byte
      object** carrying a ``msc-symlink-target`` user metadata entry whose
      value is the symlink's target expressed **relative to the symlink's
      own directory** (``dir2/b.txt``, ``dir1``, and ``file.txt``
      respectively). Regular files round-trip unchanged.
    * On the destination POSIX directory, every symlink reappears as a
      real native OS symlink (``os.path.islink``) with a **relative**
      target (``os.readlink``), and reading through the symlink returns
      the same bytes as the source file. Regular files also match their
      source content byte-for-byte.
    """
    with (
        tempdatastore.TemporaryPOSIXDirectory() as posix_src_store,
        tempdatastore.TemporaryAWSS3Bucket() as s3_store,
        tempdatastore.TemporaryPOSIXDirectory() as posix_dst_store,
    ):
        src_profile = "posix-src"
        s3_profile = "s3-mid"
        dst_profile = "posix-dst"

        src_profile_config = posix_src_store.profile_config_dict()
        src_profile_config["storage_provider"]["options"]["base_path"] = os.path.realpath(
            src_profile_config["storage_provider"]["options"]["base_path"]
        )
        dst_profile_config = posix_dst_store.profile_config_dict()
        dst_profile_config["storage_provider"]["options"]["base_path"] = os.path.realpath(
            dst_profile_config["storage_provider"]["options"]["base_path"]
        )

        src_config_dict = {"profiles": {src_profile: src_profile_config}}
        s3_config_dict = {"profiles": {s3_profile: s3_store.profile_config_dict()}}
        dst_config_dict = {"profiles": {dst_profile: dst_profile_config}}

        src_client = StorageClient(
            config=StorageClientConfig.from_dict(config_dict=src_config_dict, profile=src_profile)
        )
        s3_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=s3_config_dict, profile=s3_profile))
        dst_client = StorageClient(
            config=StorageClientConfig.from_dict(config_dict=dst_config_dict, profile=dst_profile)
        )

        # Create data and symlinks in the source POSIX directory
        src_client.write(path="c.txt", body=b"c content")
        src_client.write(path="dir1/a.txt", body=b"a content")
        src_client.write(path="dir2/b.txt", body=b"b content")
        src_client.write(path="subdir/file.txt", body=b"subdir file")
        src_client.make_symlink(path="d.txt", target="dir2/b.txt")
        src_client.make_symlink(path="dir3", target="dir1")
        src_client.make_symlink(path="subdir/link_to_sibling", target="subdir/file.txt")

        # Upload to S3
        s3_prefix = "uploaded"
        s3_client.sync_from(
            src_client,
            source_path="",
            target_path=s3_prefix,
            symlink_handling=SymlinkHandling.PRESERVE,
        )

        s3_objects_by_key = {entry.key: entry for entry in s3_client.list(prefix=s3_prefix)}
        expected_s3_keys = {
            f"{s3_prefix}/c.txt": (b"c content", None),
            f"{s3_prefix}/dir1/a.txt": (b"a content", None),
            f"{s3_prefix}/dir2/b.txt": (b"b content", None),
            f"{s3_prefix}/subdir/file.txt": (b"subdir file", None),
            f"{s3_prefix}/d.txt": (b"", "dir2/b.txt"),
            f"{s3_prefix}/dir3": (b"", "dir1"),
            f"{s3_prefix}/subdir/link_to_sibling": (b"", "file.txt"),
        }
        assert set(s3_objects_by_key.keys()) == set(expected_s3_keys.keys())
        for key, (expected_body, expected_symlink_target) in expected_s3_keys.items():
            entry = s3_objects_by_key[key]
            info = s3_client.info(path=key)
            assert info.symlink_target == expected_symlink_target, (
                f"S3 key {key} has unexpected symlink_target: {info.symlink_target}"
            )
            if expected_symlink_target is None:
                assert entry.content_length == len(expected_body)
                actual_body = s3_client.read(path=key)
                assert actual_body == expected_body, f"S3 key {key} content mismatch"
            else:
                assert entry.content_length == 0, f"Symlink key {key} should be a zero-byte object"

        # Download from S3
        dst_client.sync_from(
            s3_client,
            source_path=s3_prefix,
            target_path="",
            symlink_handling=SymlinkHandling.PRESERVE,
        )

        # Verify the destination via POSIX APIs
        dst_base_path = dst_profile_config["storage_provider"]["options"]["base_path"]
        expected_regular_files = {
            "c.txt": b"c content",
            "dir1/a.txt": b"a content",
            "dir2/b.txt": b"b content",
            "subdir/file.txt": b"subdir file",
        }
        expected_symlinks = {
            "d.txt": ("dir2/b.txt", "dir2/b.txt", b"b content"),
            "dir3": ("dir1", "dir1", None),
            "subdir/link_to_sibling": ("file.txt", "subdir/file.txt", b"subdir file"),
        }

        for rel_path, expected_content in expected_regular_files.items():
            physical_path = os.path.join(dst_base_path, rel_path)
            assert os.path.isfile(physical_path), f"Missing regular file: {rel_path}"
            assert not os.path.islink(physical_path), f"Expected non-symlink: {rel_path}"
            with open(physical_path, "rb") as f:
                assert f.read() == expected_content, f"Content mismatch: {rel_path}"

        for rel_path, (
            expected_readlink,
            expected_target_logical_key,
            expected_dereferenced,
        ) in expected_symlinks.items():
            physical_path = os.path.join(dst_base_path, rel_path)
            assert os.path.islink(physical_path), f"Expected symlink at: {rel_path}"
            actual_target = os.readlink(physical_path)
            assert not os.path.isabs(actual_target), f"Symlink {rel_path} must be relative, got {actual_target}"
            assert actual_target == expected_readlink, (
                f"Symlink {rel_path} readlink={actual_target!r}, expected {expected_readlink!r}"
            )
            resolved = os.path.normpath(os.path.join(os.path.dirname(physical_path), actual_target))
            expected_resolved = os.path.normpath(os.path.join(dst_base_path, expected_target_logical_key))
            assert resolved == expected_resolved, (
                f"Symlink {rel_path} resolves to {resolved}, expected {expected_resolved}"
            )
            if expected_dereferenced is not None:
                with open(physical_path, "rb") as f:
                    assert f.read() == expected_dereferenced, f"Dereferenced content mismatch for symlink {rel_path}"

        # Verify the destination via list() with PRESERVE mode
        dst_objects = list(dst_client.list(path="", symlink_handling=SymlinkHandling.PRESERVE))
        dst_objects_by_key = {o.key: o for o in dst_objects}
        assert len(dst_objects) == len(expected_regular_files) + len(expected_symlinks)
        for rel_path, expected_content in expected_regular_files.items():
            assert dst_objects_by_key[rel_path].content_length == len(expected_content)
            assert dst_client.read(path=rel_path) == expected_content
        for rel_path, (
            expected_readlink,
            expected_target_logical_key,
            expected_dereferenced,
        ) in expected_symlinks.items():
            assert dst_objects_by_key[rel_path].content_length == 0
            # ``symlink_target`` is reported relative to the symlink's own
            # parent directory (matching the on-disk ``os.readlink`` value and
            # the object-store ``msc-symlink-target`` convention).
            assert dst_objects_by_key[rel_path].symlink_target == expected_readlink
            if expected_dereferenced is not None:
                assert dst_client.read(path=rel_path) == expected_dereferenced

        # Verify the destination via list() with FOLLOW mode
        expected_follow_files = {
            **expected_regular_files,
            "d.txt": b"b content",
            "subdir/link_to_sibling": b"subdir file",
            "dir3/a.txt": b"a content",
        }
        dst_objects = list(dst_client.list(path="", symlink_handling=SymlinkHandling.FOLLOW))
        dst_objects_by_key = {o.key: o for o in dst_objects}
        assert set(dst_objects_by_key.keys()) == set(expected_follow_files.keys())
        for rel_path, expected_content in expected_follow_files.items():
            assert dst_objects_by_key[rel_path].content_length == len(expected_content)
            assert dst_objects_by_key[rel_path].symlink_target is None
            assert dst_client.read(path=rel_path) == expected_content
