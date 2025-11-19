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

import functools
import mmap
import os

import pytest

import multistorageclient.telemetry as telemetry
import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.file import PosixFile
from test_multistorageclient.unit.utils.telemetry.metrics.export import InMemoryMetricExporter


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryAzureBlobStorageContainer],
        [tempdatastore.TemporaryGoogleCloudStorageBucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
def test_file_open(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={
                    "profiles": {profile: temp_data_store.profile_config_dict()},
                    "opentelemetry": {
                        "metrics": {
                            "attributes": [
                                {"type": "static", "options": {"attributes": {"cluster": "local"}}},
                                {"type": "host", "options": {"attributes": {"node": "name"}}},
                                {"type": "process", "options": {"attributes": {"process": "pid"}}},
                            ],
                            "exporter": {"type": telemetry._fully_qualified_name(InMemoryMetricExporter)},
                        },
                    },
                },
                profile=profile,
                telemetry_provider=functools.partial(telemetry.init, mode=telemetry.TelemetryMode.LOCAL),
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
            #
            # Only works with PosixFile.
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

        # Check if tell() returns the correct position during iteration
        with storage_client.open(path=file_path, mode="rb") as file:
            expected = 0
            for line in file:
                expected += len(line)
                assert file.tell() == expected

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


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryPOSIXDirectory]],
)
def test_file_open_atomic(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={"profiles": {profile: temp_data_store.profile_config_dict()}}, profile=profile
            )
        )

        # Open a file for writes (atomic=False)
        with storage_client.open(path="file.txt", mode="wb", atomic=False) as file:
            assert not hasattr(file, "_temp_path"), "File should not have a temporary path"
            file.write(b"\x00" * 1024)

        with storage_client.open(path="file.txt", mode="rb") as file:
            assert file.read() == b"\x00" * 1024

        # Open a file for writes (atomic=True)
        with storage_client.open(path="file.txt", mode="wb", atomic=True) as file:
            assert hasattr(file, "_temp_path"), "File should have a temporary path"
            file.write(b"\x00" * 2048)

        with storage_client.open(path="file.txt", mode="rb") as file:
            assert file.read() == b"\x00" * 2048


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryPOSIXDirectory]],
)
def test_file_discard(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={"profiles": {profile: temp_data_store.profile_config_dict()}}, profile=profile
            )
        )

        # Open a file for writes (atomic=True)
        fp = storage_client.open(path="file.txt", mode="wb", atomic=True)
        assert isinstance(fp, PosixFile)
        assert hasattr(fp, "_temp_path"), "File should have a temporary path"
        fp.write(b"\x00" * 2048)
        fp.discard()
        assert fp._file.closed
        assert not os.path.exists(fp._temp_path)
