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

import mmap
import os
import tempfile

import pytest

import multistorageclient as msc
from multistorageclient import StorageClient, StorageClientConfig
from test_multistorageclient.unit.utils import tempdatastore


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_storage_client_basic_usage(temp_data_store_type: type[tempdatastore.TemporaryDataStore]) -> None:
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={"profiles": {profile: temp_data_store.profile_config_dict()}}, profile=profile
            )
        )

        file_path = "file.txt"
        file_content_length = 1
        file_body_bytes = b"\x00" * file_content_length
        file_body_string = file_body_bytes.decode()

        # Open a file for writes (bytes).
        with storage_client.open(path=file_path, mode="wb") as file:
            assert not file.closed
            assert not file.readable()
            assert file.name == file_path
            assert file.writable()
            file.write(file_body_bytes)
            assert file.tell() == file_content_length

        # Check if the file's persisted.
        file_info = storage_client.info(path=file_path)
        assert file_info is not None
        assert file_info.content_length == file_content_length

        # Open the file for reads (bytes).
        with storage_client.open(path=file_path, mode="rb", buffering=0) as file:
            assert not file.isatty()
            assert file.readable()
            assert not file.writable()
            assert file.read() == file_body_bytes
            assert file.seekable()
            file.seek(0)
            assert file.readall() == file_body_bytes
            file.seek(0)
            buffer = bytearray(file_content_length)
            file.readinto(buffer)
            assert buffer == file_body_bytes
            file.seek(0)
            assert file.readline() == file_body_bytes
            file.seek(0)
            assert file.readlines() == [file_body_bytes]

            # Check if it works with mmap.
            if temp_data_store_type is tempdatastore.TemporaryPOSIXDirectory:
                with mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_file:
                    content = mmap_file[:]
                    assert content == file_body_bytes

        # Delete the file.
        storage_client.delete(path=file_path)
        with pytest.raises(FileNotFoundError):
            with storage_client.open(path=file_path, mode="rb") as file:
                pass

        # Open a file for writes (string).
        with storage_client.open(path=file_path, mode="w") as file:
            assert not file.readable()
            assert file.writable()
            file.write(file_body_string)
            assert file.tell() == file_content_length

        # Check if the file's persisted.
        file_info = storage_client.info(path=file_path)
        assert file_info is not None
        assert file_info.content_length == file_content_length

        # Open the file for reads (string).
        with storage_client.open(path=file_path, mode="r") as file:
            assert not file.isatty()
            assert file.readable()
            assert not file.writable()
            assert file.read() == file_body_string
            assert file.seekable()
            file.seek(0)
            assert file.read() == file_body_string
            file.seek(0)
            assert file.readline() == file_body_string
            file.seek(0)
            assert file.readlines() == [file_body_string]

        # Delete the file.
        storage_client.delete(path=file_path)
        with pytest.raises(FileNotFoundError):
            with storage_client.open(path=file_path, mode="r") as file:
                pass

        # Verify the file creation is atomic.
        fp1 = storage_client.open(path=file_path, mode="w")
        fp1.write(file_body_string)

        with pytest.raises(FileNotFoundError):
            storage_client.info(path=file_path)

        # The file is written only after the file is closed.
        fp1.close()

        file_info = storage_client.info(path=file_path)
        assert file_info is not None
        assert file_info.content_length == file_content_length


def test_msc_shortcuts() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        file1 = os.path.join(temp_dir, "file1.txt")
        with msc.open(file1, "w") as f:
            f.write("Hello, world!")

        with msc.open(file1, "r") as f:
            assert f.read() == "Hello, world!"
