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
from unittest.mock import Mock, patch

import pytest

from multistorageclient import StorageClient, StorageClientConfig, telemetry
from multistorageclient.file import ObjectFile, PosixFile, RemoteFileReader
from multistorageclient.providers.base import BaseStorageProvider
from test_multistorageclient.unit.utils import tempdatastore
from test_multistorageclient.unit.utils.telemetry.metrics.export import InMemoryMetricExporter


def _posix_client_with_file_descriptor_metrics(tmp_path):
    duration_gauge = Mock()
    open_counter = Mock()
    telemetry_provider = Mock(spec=telemetry.Telemetry)
    telemetry_provider.gauge.side_effect = lambda config, name: (
        duration_gauge if name is telemetry.Telemetry.GaugeName.FILE_DESCRIPTOR_DURATION else Mock()
    )
    telemetry_provider.counter.return_value = Mock()
    telemetry_provider.up_down_counter.return_value = open_counter

    profile = "data"
    config = StorageClientConfig.from_dict(
        config_dict={
            "profiles": {profile: {"storage_provider": {"type": "file", "options": {"base_path": str(tmp_path)}}}},
            "opentelemetry": {
                "metrics": {
                    "attributes": [
                        {"type": "host", "options": {"attributes": {"node": "name"}}},
                        {"type": "process", "options": {"attributes": {"process": "pid"}}},
                    ],
                    "exporter": {"type": "console"},
                }
            },
        },
        profile=profile,
        telemetry_provider=lambda: telemetry_provider,
    )
    return StorageClient(config=config), duration_gauge, open_counter


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
        with pytest.raises(FileNotFoundError), storage_client.open(path=file_path, mode="rb") as file:
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
        with pytest.raises(FileNotFoundError), storage_client.open(path=file_path, mode="r") as file:
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


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryPOSIXDirectory]],
)
def test_file_read_does_not_create_parent_dirs(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict={"profiles": {profile: temp_data_store.profile_config_dict()}}, profile=profile
            )
        )

        nonexistent_dir = "nonexisting_dir"
        nonexistent_file_path = os.path.join(nonexistent_dir, "test.txt")

        base_path = temp_data_store.profile_config_dict()["storage_provider"]["options"]["base_path"]
        full_dir_path = os.path.join(base_path, nonexistent_dir)

        assert not os.path.exists(full_dir_path)

        with pytest.raises(FileNotFoundError), storage_client.open(nonexistent_file_path, "r") as f:
            f.read()

        assert not os.path.exists(full_dir_path)


def test_posix_file_descriptor_metrics_context_manager_and_attributes(tmp_path):
    storage_client, duration_gauge, open_counter = _posix_client_with_file_descriptor_metrics(tmp_path)

    with storage_client.open("file.txt", "wb") as file:
        assert isinstance(file, PosixFile)
        assert [metric_call.args[0] for metric_call in open_counter.add.call_args_list] == [1]

    assert [metric_call.args[0] for metric_call in open_counter.add.call_args_list] == [1, -1]
    assert sum(metric_call.args[0] for metric_call in open_counter.add.call_args_list) == 0
    assert duration_gauge.set.call_count == 1

    opened_attributes = open_counter.add.call_args_list[0].kwargs["attributes"]
    closed_attributes = open_counter.add.call_args_list[1].kwargs["attributes"]
    duration_attributes = duration_gauge.set.call_args.kwargs["attributes"]
    assert opened_attributes is closed_attributes
    assert opened_attributes is duration_attributes
    assert opened_attributes["multistorageclient.provider"] == "file"
    assert opened_attributes["multistorageclient.file.mode"] == "wb"
    assert "multistorageclient.version" in opened_attributes
    assert "node" in opened_attributes
    assert "process" in opened_attributes
    assert all("path" not in key for key in opened_attributes)


def test_posix_file_descriptor_metrics_explicit_close_only_records_once(tmp_path):
    storage_client, duration_gauge, open_counter = _posix_client_with_file_descriptor_metrics(tmp_path)

    file = storage_client.open("file.txt", "wb")
    file.close()
    file.close()
    file.discard()

    assert [metric_call.args[0] for metric_call in open_counter.add.call_args_list] == [1, -1]
    assert duration_gauge.set.call_count == 1


def test_posix_file_descriptor_metrics_discard_only_records_once(tmp_path):
    storage_client, duration_gauge, open_counter = _posix_client_with_file_descriptor_metrics(tmp_path)

    file = storage_client.open("file.txt", "wb")
    file.write(b"discarded")
    file.discard()
    file.discard()
    file.close()

    assert [metric_call.args[0] for metric_call in open_counter.add.call_args_list] == [1, -1]
    assert duration_gauge.set.call_count == 1
    assert not (tmp_path / "file.txt").exists()


def test_posix_file_descriptor_metrics_open_failure_does_not_increment_counter(tmp_path):
    storage_client, duration_gauge, open_counter = _posix_client_with_file_descriptor_metrics(tmp_path)

    with pytest.raises(FileNotFoundError):
        storage_client.open("missing.txt", "rb")

    open_counter.add.assert_not_called()
    duration_gauge.set.assert_not_called()


def test_posix_file_descriptor_counter_increment_failure_does_not_decrement(tmp_path):
    storage_client, duration_gauge, open_counter = _posix_client_with_file_descriptor_metrics(tmp_path)
    open_counter.add.side_effect = RuntimeError("counter unavailable")

    with storage_client.open("file.txt", "wb") as file:
        file.write(b"content")

    assert file.closed
    assert [metric_call.args[0] for metric_call in open_counter.add.call_args_list] == [1]
    assert duration_gauge.set.call_count == 1


def test_posix_file_descriptor_duration_excludes_atomic_rename(tmp_path):
    storage_client, duration_gauge, _ = _posix_client_with_file_descriptor_metrics(tmp_path)
    clock = [10.0]
    real_rename = os.rename

    def delayed_rename(source, destination):
        clock[0] = 100.0
        real_rename(source, destination)

    with (
        patch("multistorageclient.file.time.perf_counter", side_effect=lambda: clock[0]),
        patch("multistorageclient.file.os.rename", side_effect=delayed_rename),
    ):
        file = storage_client.open("file.txt", "wb")
        clock[0] = 14.0
        file.close()

    assert duration_gauge.set.call_args.args[0] == 4.0
    assert clock[0] == 100.0


def test_object_file_fileno_is_stable_for_in_memory_files():
    """fileno() on an in-memory ObjectFile must reuse one temporary descriptor instead of leaking one per call."""
    storage_client = Mock()
    storage_client._storage_provider = Mock(spec=BaseStorageProvider)
    storage_client._cache_manager = None

    object_file = ObjectFile(storage_client=storage_client, remote_path="object", mode="wb")
    open_files_before = len(object_file._open_files)

    descriptors = {object_file.fileno() for _ in range(5)}

    assert len(descriptors) == 1
    assert len(object_file._open_files) == open_files_before + 1
    object_file.close()


def test_object_file_close_releases_descriptors_when_upload_fails():
    """A failed upload must not leak the fileno() placeholder or other tracked local files."""
    storage_client = Mock()
    storage_client._storage_provider = Mock(spec=BaseStorageProvider)
    storage_client._cache_manager = None
    storage_client.upload_file.side_effect = RuntimeError("upload failed")

    object_file = ObjectFile(storage_client=storage_client, remote_path="object", mode="wb")
    object_file.write(b"payload")
    object_file.fileno()
    tracked = list(object_file._open_files)
    assert tracked

    with pytest.raises(RuntimeError, match="upload failed"):
        object_file.close()

    assert all(fp.closed for fp in tracked)


def test_object_file_append_removes_staging_file_when_upload_fails(tmp_path):
    """Append mode stages the merged content in a temp file; it must be removed when the upload raises."""
    storage_client = Mock()
    storage_client._storage_provider = Mock(spec=BaseStorageProvider)
    storage_client._cache_manager = None
    storage_client.download_file.side_effect = FileNotFoundError("no existing object")
    storage_client.upload_file.side_effect = RuntimeError("upload failed")
    staging_path = tmp_path / "staging.bin"

    object_file = ObjectFile(storage_client=storage_client, remote_path="object", mode="ab")
    object_file._generate_temp_file_path = lambda: str(staging_path)  # type: ignore
    object_file.write(b"appended")

    with pytest.raises(RuntimeError, match="upload failed"):
        object_file.close()

    storage_client.upload_file.assert_called_once()
    assert not staging_path.exists()


def test_non_posix_files_do_not_record_file_descriptor_metrics():
    provider = Mock(spec=BaseStorageProvider)
    storage_client = Mock()
    storage_client._storage_provider = provider
    storage_client._cache_manager = None

    object_file = ObjectFile(storage_client=storage_client, remote_path="object", mode="wb")
    object_file.close()

    remote_file = RemoteFileReader(remote_path="remote", file_size=0, storage_client=storage_client)
    remote_file.fileno()
    remote_file.close()

    provider._record_file_descriptor_open.assert_not_called()
    provider._record_file_descriptor_close.assert_not_called()


def test_posix_file_descriptor_telemetry_failures_do_not_affect_file_lifecycle(tmp_path):
    storage_client, _, _ = _posix_client_with_file_descriptor_metrics(tmp_path)
    provider = storage_client._storage_provider

    with patch.object(provider, "_record_file_descriptor_open", side_effect=RuntimeError("open telemetry failed")):
        file = storage_client.open("file.txt", "wb")
        file.write(b"content")
        file.close()

    with patch.object(provider, "_record_file_descriptor_close", side_effect=RuntimeError("close telemetry failed")):
        another_file = storage_client.open("another-file.txt", "wb")
        another_file.write(b"other content")
        another_file.close()

    assert file.closed
    assert another_file.closed
    assert (tmp_path / "file.txt").read_bytes() == b"content"
    assert (tmp_path / "another-file.txt").read_bytes() == b"other content"


def test_posix_file_readall_emits_read_metrics_once(tmp_path):
    """readall() is instrumented itself and must not also go through the instrumented read()."""
    config = StorageClientConfig.from_dict(
        {"profiles": {"data": {"storage_provider": {"type": "file", "options": {"base_path": str(tmp_path)}}}}},
        profile="data",
    )
    storage_client = StorageClient(config)
    storage_client.write("file.txt", b"content")
    provider = storage_client._storage_provider
    assert isinstance(provider, BaseStorageProvider)

    with (
        patch.object(provider, "_emit_metrics", wraps=provider._emit_metrics) as emit_metrics,
        storage_client.open("file.txt", "rb") as f,
    ):
        assert isinstance(f, PosixFile)
        emit_metrics.reset_mock()
        assert f.readall() == b"content"
        assert emit_metrics.call_count == 1
