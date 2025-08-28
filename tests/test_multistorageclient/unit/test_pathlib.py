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

import os
import pickle
import shutil
import stat
import tempfile
import uuid
from pathlib import Path

import pytest

import multistorageclient as msc
from test_multistorageclient.unit.utils import config, tempdatastore

MB = 1024 * 1024


def test_path_basic():
    with tempfile.TemporaryDirectory() as temp_dir:
        os.makedirs(f"{temp_dir}/dir1")

        # create a file in the temporary directory
        with open(f"{temp_dir}/dir1/testfile.txt", "w") as f:
            f.write("1.1.1.1")

        path1 = msc.Path(f"{temp_dir}/dir1/testfile.txt")
        assert path1.exists()
        assert path1.is_file()
        assert not path1.is_dir()
        assert path1.suffix == ".txt"
        assert path1.stem == "testfile"
        assert path1.name == "testfile.txt"
        assert path1.parts[-2:] == ("dir1", "testfile.txt")
        assert path1.as_posix() == f"{temp_dir}/dir1/testfile.txt"
        assert path1.is_absolute()
        assert path1.absolute() == path1
        assert not path1.is_reserved()
        assert path1.match("*.txt")
        assert not path1.match("*.bin")
        assert path1.with_suffix(".bin").suffix == ".bin"
        assert path1.with_stem("testfile2").stem == "testfile2"
        assert isinstance(path1.resolve(), msc.Path)
        assert not path1.is_symlink()
        assert not path1.is_block_device()
        assert not path1.is_char_device()
        assert not path1.is_fifo()
        assert not path1.is_socket()
        assert not path1.is_mount()
        assert path1.samefile(path1)
        assert path1.resolve() == msc.Path(f"{Path(temp_dir).resolve()}/dir1/testfile.txt")
        assert path1.absolute() == path1

        path2 = msc.Path(f"{temp_dir}/dir1/")
        assert path2.exists()
        assert path2.is_dir()
        assert not path2.is_file()

        path3 = msc.Path(f"{temp_dir}/dir1") / "testfile.txt"
        assert path3.exists()
        assert path1 == path3

        path4 = msc.Path(f"{temp_dir}/dir1") / Path("testfile.txt")
        assert path4.exists()
        assert path1 == path4

        path5 = msc.Path(f"{temp_dir}/dir2/doesnotexist")
        assert not path5.exists()

        path6 = msc.Path(f"/{uuid.uuid4()}/")
        assert not path6.exists()

        with pytest.raises(FileNotFoundError):
            list(path6.iterdir())

        path7 = path1.rename(f"{temp_dir}/dir1/testfile-1.txt")
        assert not path1.exists()
        assert path7.exists()

        path8 = msc.Path(f"{temp_dir}/dir1/testfile-3.txt")
        assert path8.as_posix() == f"{temp_dir}/dir1/testfile-3.txt"

        path9 = msc.Path(f"{temp_dir}/dir1/testfile-4.txt")
        assert not path9.exists()
        path9.touch()
        assert path9.exists()
        assert path9.is_file()
        assert path9.stat().st_size == 0


def test_path_hierarchy(file_storage_config):
    with tempfile.TemporaryDirectory() as temp_dir:
        path = msc.Path(f"{temp_dir}/dir1/dir2/dir3/testfile.txt")
        assert path.parent == msc.Path(f"{temp_dir}/dir1/dir2/dir3")
        assert list(path.parents[:4]) == [
            msc.Path(f"{temp_dir}/dir1/dir2/dir3"),
            msc.Path(f"{temp_dir}/dir1/dir2"),
            msc.Path(f"{temp_dir}/dir1"),
            msc.Path(temp_dir),
        ]


def test_path_iterdir(file_storage_config):
    with tempfile.TemporaryDirectory() as temp_dir:
        os.makedirs(f"{temp_dir}/dir1/dir2/dir3")

        path = msc.Path(f"{temp_dir}/")
        assert list(path.iterdir()) == [msc.Path(f"{temp_dir}/dir1")]


def test_path_open(file_storage_config):
    with tempfile.TemporaryDirectory() as temp_dir:
        path = msc.Path(f"{temp_dir}/dir1/testfile.txt")
        create_file(path)
        with path.open("r") as f:
            assert f.read() == "1.1.1.1"


def test_path_mkdir_rmdir(file_storage_config):
    with tempfile.TemporaryDirectory() as temp_dir:
        path = msc.Path(f"{temp_dir}/dir1/dir2/dir3")
        path.mkdir(parents=True, exist_ok=True)
        assert path.exists()
        assert path.is_dir()
        path.rmdir()


def test_path_glob(file_storage_config):
    with tempfile.TemporaryDirectory() as temp_dir:
        create_file(msc.Path(f"{temp_dir}/dir1/testfile.txt"))
        create_file(msc.Path(f"{temp_dir}/dir1/dir2/testfile.txt"))
        create_file(msc.Path(f"{temp_dir}/dir1/dir3/testfile.txt"))

        path = msc.Path(f"{temp_dir}/")
        assert list(path.glob("*")) == [msc.Path(f"{temp_dir}/dir1")]
        assert list(path.glob("dir1/*")) == [
            msc.Path(f"{temp_dir}/dir1/dir2"),
            msc.Path(f"{temp_dir}/dir1/dir3"),
            msc.Path(f"{temp_dir}/dir1/testfile.txt"),
        ]
        assert list(path.glob("dir1/dir2/*")) == [msc.Path(f"{temp_dir}/dir1/dir2/testfile.txt")]
        assert list(path.glob("**/*.txt")) == [
            msc.Path(f"{temp_dir}/dir1/testfile.txt"),
            msc.Path(f"{temp_dir}/dir1/dir2/testfile.txt"),
            msc.Path(f"{temp_dir}/dir1/dir3/testfile.txt"),
        ]
        assert list(path.rglob("*.txt")) == [
            msc.Path(f"{temp_dir}/dir1/testfile.txt"),
            msc.Path(f"{temp_dir}/dir1/dir2/testfile.txt"),
            msc.Path(f"{temp_dir}/dir1/dir3/testfile.txt"),
        ]


def test_shutil_rmtree(file_storage_config):
    with tempfile.TemporaryDirectory() as temp_dir:
        create_file(msc.Path(f"{temp_dir}/dir1/dir2/testfile.txt"))
        create_file(msc.Path(f"{temp_dir}/dir1/dir3/testfile.txt"))

        path = msc.Path(f"{temp_dir}/")
        shutil.rmtree(path)  # noqa: F821
        assert not path.exists()


def test_relative_path():
    path = msc.Path("./workspace/datasets/file.txt")
    assert path.as_posix() == os.path.realpath("./workspace/datasets/file.txt")


def test_hashable_path():
    path1 = msc.Path("/tmp/testfile")
    path2 = msc.Path("msc://default/tmp/testfile")
    assert hash(path1) == hash(path2)
    assert path1 == path2

    paths = {path1, path2}
    assert len(paths) == 1


def test_pickable_path():
    local_path = msc.Path("/tmp/test/file.txt")
    pickled = pickle.dumps(local_path)
    unpickled = pickle.loads(pickled)

    assert local_path == unpickled
    assert str(local_path) == str(unpickled)
    assert unpickled._storage_client


def create_file(path: msc.Path):
    with path.open("w") as f:
        f.write("1.1.1.1")


def verify_pathlib(profile: str, prefix: str):
    body = b"A" * (4 * MB)

    # open files
    for i in range(10):
        with msc.Path(f"msc://{profile}/{prefix}/data-{i}.bin").open("wb") as fp:
            fp.write(body)

    # glob, rglob
    assert len(list(msc.Path(f"msc://{profile}/{prefix}").glob("**/*.bin"))) == 10
    assert len(list(msc.Path(f"msc://{profile}/{prefix}").rglob("*.bin"))) == 10
    assert len(list(msc.Path(f"msc://{profile}/{prefix}/").iterdir())) == 10

    # match
    assert msc.Path(f"msc://{profile}/{prefix}/data-0.bin").match("data-*.bin")

    # stat and lstat
    assert msc.Path(f"msc://{profile}/{prefix}/data-0.bin").stat().st_size == 4 * MB
    assert msc.Path(f"msc://{profile}/{prefix}/data-0.bin").lstat().st_size == 4 * MB
    assert msc.Path(f"msc://{profile}/{prefix}/data-0.bin").stat().st_mtime > 0
    assert (
        msc.Path(f"msc://{profile}/{prefix}/data-0.bin").stat().st_mtime
        == msc.Path(f"msc://{profile}/{prefix}/data-0.bin").lstat().st_mtime
    )
    assert msc.Path(f"msc://{profile}/{prefix}/data-0.bin").stat().st_mode == stat.S_IFREG | 0o644

    # Path operations
    path = msc.Path(f"msc://{profile}/{prefix}/data-0.bin")
    assert str(path) == f"msc://{profile}/{prefix}/data-0.bin"
    assert path.exists()
    assert path.resolve() == path
    assert path.absolute() == path
    assert path.parent == msc.Path(f"msc://{profile}/{prefix}")
    assert path.parents == [msc.Path(f"msc://{profile}/{prefix}"), msc.Path(f"msc://{profile}")]
    assert path.is_file()
    assert path.read_bytes() == body
    assert path.read_text() == body.decode()
    with path.open("rb") as fp:
        assert fp.read() == body
    path.unlink()
    assert not path.exists()
    assert len(list(msc.Path(f"msc://{profile}/{prefix}").iterdir())) == 9

    path2 = msc.Path(f"msc://{profile}/{prefix}/data-99")
    assert not path2.exists()
    assert not path2.is_dir()
    assert path2.with_name("data-22.bin") == msc.Path(f"msc://{profile}/{prefix}/data-22.bin")
    assert path2.with_suffix(".bin") == msc.Path(f"msc://{profile}/{prefix}/data-99.bin")
    assert path2.with_stem("data-66") == msc.Path(f"msc://{profile}/{prefix}/data-66")
    assert path2.samefile(msc.Path(f"msc://{profile}/{prefix}/data-99"))

    path3 = msc.Path(f"msc://{profile}/{uuid.uuid4()}/")
    assert not path3.exists()
    assert not path3.is_file()
    assert list(path3.iterdir()) == []

    path4 = msc.Path(f"msc://{profile}/{prefix}/data-#!-_.*'()&$@=;/:+,?\\{{}}%`]<>~|#.bin")
    assert str(path4) == f"msc://{profile}/{prefix}/data-#!-_.*'()&$@=;/:+,?\\{{}}%`]<>~|#.bin"

    # rename
    path5 = msc.Path(f"msc://{profile}/{prefix}/data-2.bin")
    path6 = path5.rename(f"msc://{profile}/{prefix}/data-3.bin")
    assert not path5.exists()
    assert path6 == msc.Path(f"msc://{profile}/{prefix}/data-3.bin")
    with path6.open("rb") as fp:
        assert fp.read() == body

    # filesystem path
    path7 = msc.Path(f"msc://{profile}/{prefix}/data-4.bin")
    path7.as_posix().startswith(tempfile.gettempdir())
    assert Path(path7.as_posix()).exists()

    # touch
    path8 = msc.Path(f"msc://{profile}/{prefix}/data-1.bin")
    path8.touch()
    assert path8.exists()
    path9 = msc.Path(f"msc://{profile}/{prefix}/data-99.bin")
    assert not path9.exists()
    path9.touch()
    assert path9.exists()
    assert path9.stat().st_size == 0


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_pathlib_local_and_remote(temp_data_store_type: type[tempdatastore.TemporaryDataStore]) -> None:
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(config_dict={"profiles": {"test": temp_data_store.profile_config_dict()}})
        verify_pathlib(profile="test", prefix="files")
