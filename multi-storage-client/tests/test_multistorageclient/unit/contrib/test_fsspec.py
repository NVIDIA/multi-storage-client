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
import tempfile
import uuid

import pytest

import multistorageclient as msc
from test_multistorageclient.unit.utils import config, tempdatastore


def generate_file(fs, path):
    with fs.open(path, "w") as fp:
        fp.write(str(uuid.uuid4()))


def verify_fsspec_implementation(profile: str):
    fs = msc.async_fs.MultiStorageAsyncFileSystem()

    # Create 15 files in total
    generate_file(fs, f"{profile}/f1.txt")
    generate_file(fs, f"{profile}/f2.txt")
    generate_file(fs, f"{profile}/f3.txt")
    generate_file(fs, f"{profile}/a/f1.txt")
    generate_file(fs, f"{profile}/a/f2.txt")
    generate_file(fs, f"{profile}/b/f3.txt")
    generate_file(fs, f"{profile}/b/f4.txt")
    generate_file(fs, f"{profile}/c/f5.txt")
    generate_file(fs, f"{profile}/c/f6.txt")
    generate_file(fs, f"{profile}/a/0001/f1.txt")
    generate_file(fs, f"{profile}/a/0001/f2.txt")
    generate_file(fs, f"{profile}/a/b/0002/f1.txt")
    generate_file(fs, f"{profile}/a/b/0002/f2.txt")
    generate_file(fs, f"{profile}/a/b/c/0003/f1.txt")
    generate_file(fs, f"{profile}/a/b/c/0003/f2.txt")

    # List files
    assert len(fs.ls(f"{profile}/")) == 6
    assert len(fs.ls(f"{profile}/a")) == 4
    assert len(fs.find(f"{profile}/")) == 15
    assert len(fs.glob(f"{profile}/*")) == 6
    assert len(fs.glob(f"{profile}/**")) == 24
    assert len(fs.glob(f"{profile}/**/*")) == 23

    # Get and Put
    with tempfile.TemporaryDirectory() as tmpdir:
        fs.get(f"{profile}/f1.txt", os.path.join(tmpdir, "f1.txt"))
        assert os.path.exists(os.path.join(tmpdir, "f1.txt"))
        fs.put(os.path.join(tmpdir, "f1.txt"), f"{profile}/f1.txt")

    # Cat
    assert len(fs.cat_file(f"{profile}/a/0001/f1.txt")) == 36

    # Info
    fileinfo = fs.info(f"{profile}/f1.txt")
    assert fileinfo["size"] == 36

    # Exists
    assert fs.exists(f"{profile}/a/b/c/0003/f2.txt")
    assert not fs.exists(f"{profile}/a/b/c/0003/f3.txt")

    # mkdir is no-op for object store
    fs.mkdir(f"{profile}/a/b/c/d/e/")
    with pytest.raises(FileNotFoundError):
        fs.info(f"{profile}/a/b/c/d/e/")

    # Pipe and Delete
    fs.pipe_file(f"{profile}/pipe_file.txt", uuid.uuid4().bytes)
    assert fs.exists(f"{profile}/pipe_file.txt")
    fs.rm(f"{profile}/pipe_file.txt")
    assert not fs.exists(f"{profile}/pipe_file.txt")

    # Move a single file
    fs.pipe_file(f"{profile}/pipe_file.txt", uuid.uuid4().bytes)
    fs.mv(f"{profile}/pipe_file.txt", f"{profile}/pipe_file_rename.txt")
    assert not fs.exists(f"{profile}/pipe_file.txt")
    assert fs.exists(f"{profile}/pipe_file_rename.txt")
    fs.rm(f"{profile}/pipe_file_rename.txt")

    # Delete a directory
    fs.rm(f"{profile}/a/b/c", recursive=True)
    assert len(fs.glob(f"{profile}/a/b/c")) == 0

    # Move a directory
    fs.mv(f"{profile}/a/b", f"{profile}/dir1", recursive=True)
    assert len(fs.find(f"{profile}/a/b")) == 0
    assert len(fs.find(f"{profile}/dir1")) == 2
    assert len(fs.glob(f"{profile}/dir1/*")) == 1
    assert len(fs.glob(f"{profile}/dir1/**/*")) == 3


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_fsspec_implementation(temp_data_store_type: type[tempdatastore.TemporaryDataStore]) -> None:
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    profile: temp_data_store.profile_config_dict(),
                }
            }
        )

        verify_fsspec_implementation(profile=profile)
