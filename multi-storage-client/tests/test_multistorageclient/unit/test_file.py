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
import io
import json
import mmap
import os
import threading
from array import array
from typing import cast
from unittest.mock import Mock, call, patch

import pytest

from multistorageclient import StorageClient, StorageClientConfig, telemetry
from multistorageclient.cache import CacheManager
from multistorageclient.caching.cache_config import CacheConfig
from multistorageclient.client.single import SingleStorageClient
from multistorageclient.constants import MEMORY_LOAD_LIMIT
from multistorageclient.file import ObjectFile, PosixFile, RemoteFileReader
from multistorageclient.providers.base import BaseStorageProvider
from multistorageclient.types import Range, ResolvedPath, ResolvedPathState, SourceVersionCheckMode
from test_multistorageclient.unit.utils import tempdatastore
from test_multistorageclient.unit.utils.telemetry.metrics.export import InMemoryMetricExporter


@pytest.mark.parametrize("rust_enabled", [False, True])
@pytest.mark.parametrize("buffer_type", [bytes, memoryview])
def test_remote_file_reader_returns_bytes(rust_enabled, buffer_type):
    storage_client = Mock()
    storage_client.read.return_value = buffer_type(b"abc")
    storage_client._is_rust_client_enabled.return_value = rust_enabled
    reader = RemoteFileReader("file", 3, storage_client)

    result = reader.read()

    assert isinstance(result, bytes)
    assert result == b"abc"
    assert reader.tell() == 3
    assert reader.read() == b""


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


def test_object_file_missing_cached_object_does_not_wait_during_cleanup():
    storage_client = Mock()
    storage_client._cache_manager = Mock()
    storage_client._cache_manager.prefetch_file.return_value = True
    storage_client._cache_manager.check_source_version.return_value = True
    storage_client.info.side_effect = FileNotFoundError("missing object")
    download_complete = Mock()
    object_file = ObjectFile.__new__(ObjectFile)

    with (
        patch("multistorageclient.file.threading.Event", return_value=download_complete),
        pytest.raises(FileNotFoundError, match="missing object"),
    ):
        object_file.__init__(storage_client=storage_client, remote_path="missing", mode="rb")

    object_file.close()
    assert object_file.closed
    download_complete.wait.assert_not_called()


@pytest.mark.timeout(5)
@pytest.mark.parametrize("cache_enabled", [False, True])
def test_object_file_large_text_read_propagates_background_error(cache_enabled):
    storage_client = Mock()
    cache_manager = Mock() if cache_enabled else None
    storage_client._cache_manager = cache_manager
    storage_client.info.return_value = Mock(content_length=2)
    if cache_manager:
        cache_manager.prefetch_file.return_value = True
        cache_manager.check_source_version.return_value = True
        cache_manager.get_max_cache_size.return_value = 1

    object_file = ObjectFile(
        storage_client=storage_client,
        remote_path="large.txt",
        mode="r",
        memory_load_limit=1,
    )

    with pytest.raises(OSError, match="Failed to download file large.txt") as exc_info:
        object_file.read()

    assert isinstance(exc_info.value.__cause__, ValueError)
    assert object_file._download_complete.is_set()
    object_file._download_thread.join(timeout=1)
    assert not object_file._download_thread.is_alive()
    object_file.close()


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


@pytest.mark.parametrize("mode", ["rb", "ab"])
@pytest.mark.parametrize(
    "state_kind",
    [
        "recognized",
        "cleanup_failure",
        "missing",
        "corrupt",
        "unreadable",
        "marker_symlink",
        "directory_symlink",
    ],
)
def test_object_file_failed_download_cleans_owned_staging_state(tmp_path, monkeypatch, caplog, mode, state_kind):
    if state_kind == "cleanup_failure":
        monkeypatch.setattr(
            "multistorageclient.file.shutil.rmtree", Mock(side_effect=PermissionError("cleanup failed"))
        )
    storage_client = Mock()
    storage_client._storage_provider = Mock(spec=BaseStorageProvider)
    cache_manager = Mock() if mode == "rb" else None
    storage_client._cache_manager = cache_manager
    storage_client.info.return_value = Mock(content_length=11, etag="revision-1")
    staging_path = tmp_path / "staging.bin"
    state_dir = tmp_path / ".staging.bin.msc-dl"
    if cache_manager:
        cache_manager.prefetch_file.return_value = True
        cache_manager.check_source_version.return_value = True
        cache_manager.contains.return_value = False
        cache_manager.get_max_cache_size.return_value = 100
        cache_manager.generate_temp_file_path.return_value = str(staging_path)
        cache_manager.acquire_lock.return_value = threading.Lock()

    def fail_download(*_):
        staging_path.write_bytes(b"partial staging file")
        actual_dir = tmp_path / "unrelated" if state_kind == "directory_symlink" else state_dir
        actual_dir.mkdir()
        if state_kind == "directory_symlink":
            state_dir.symlink_to(actual_dir, target_is_directory=True)
        state: dict[str, object] = {
            "format": "msc-download-v1",
            "key": "object",
            "size": 11,
            "etag": "revision-1",
            "version": None,
            "chunk_size": 4,
        }
        marker = state_dir / "state.json"
        if state_kind == "marker_symlink":
            external_marker = tmp_path / "external-marker.json"
            external_marker.write_text(json.dumps(state))
            marker.symlink_to(external_marker)
        elif state_kind != "missing":
            marker.write_text("{broken" if state_kind == "corrupt" else json.dumps(state))
        if state_kind == "unreadable":
            original_open = open

            def guarded_open(path, *args, **kwargs):
                if path == str(marker) or path == marker:
                    raise PermissionError("unreadable marker")
                return original_open(path, *args, **kwargs)

            monkeypatch.setattr("builtins.open", guarded_open)
        (state_dir / "000000.chunk").write_bytes(b"abcd")
        raise RuntimeError("download failed")

    storage_client.download_file.side_effect = fail_download
    object_file = ObjectFile(storage_client=storage_client, remote_path="object", mode=mode)
    try:
        if mode == "rb":
            with pytest.raises(OSError, match="Failed to download file") as exc:
                object_file.read()
            assert str(exc.value.__cause__) == "download failed"
        else:
            object_file._generate_temp_file_path = lambda: str(staging_path)  # type: ignore
            object_file.write(b"appended")
            with pytest.raises(RuntimeError, match="download failed"):
                object_file.close()
        assert not staging_path.exists()
        assert state_dir.exists() is (state_kind in ("cleanup_failure", "directory_symlink"))
        if state_kind in ("cleanup_failure", "directory_symlink"):
            assert (state_dir / "000000.chunk").read_bytes() == b"abcd"
        if state_kind == "marker_symlink":
            assert json.loads((tmp_path / "external-marker.json").read_text())["format"] == "msc-download-v1"
        if state_kind == "cleanup_failure":
            assert "Could not remove temporary download state" in caplog.text
        if cache_manager:
            cache_manager.set.assert_not_called()
        storage_client.upload_file.assert_not_called()
    finally:
        object_file.close()


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


def _object_file_client(tmp_path):
    return SingleStorageClient(
        StorageClientConfig.from_dict(
            {"profiles": {"data": {"storage_provider": {"type": "file", "options": {"base_path": str(tmp_path)}}}}},
            profile="data",
        )
    )


@pytest.mark.parametrize("memory_load_limit", [0, MEMORY_LOAD_LIMIT])
def test_object_file_without_cache_reads_ranges_when_prefetch_disabled(tmp_path, memory_load_limit, monkeypatch):
    monkeypatch.setenv("MSC_REMOTE_READ_BUFFER_SIZE", "0")
    (tmp_path / "object.bin").write_bytes(b"abcdef")
    client = _object_file_client(tmp_path)
    provider = cast(BaseStorageProvider, client._storage_provider)
    with (
        patch.object(client, "_is_posix_file_storage_provider", return_value=False),
        patch.object(provider, "download_file", wraps=provider.download_file) as download,
        patch.object(provider, "get_object", wraps=provider.get_object) as get_object,
        client.open("object.bin", "rb", prefetch_file=False, memory_load_limit=memory_load_limit) as file,
    ):
        assert isinstance(file, ObjectFile)
        assert file.seekable()
        assert isinstance(file._file, RemoteFileReader)
        get_object.assert_not_called()
        assert file.seek(-10, os.SEEK_END) == 0
        assert file.read(2) == b"ab"
        assert file.tell() == 2
        with pytest.raises(ValueError, match="negative seek"):
            file.seek(-1)
        with pytest.raises(ValueError, match="invalid whence"):
            file.seek(0, 4)
        assert file.tell() == 2
        assert file.seek(-10, os.SEEK_CUR) == 0
        assert file.seek(-2, os.SEEK_END) == 4
        assert file.read(10) == b"ef"
        assert file.tell() == 6
        assert file.read() == b""
        descriptor = file.fileno()

    assert file.closed
    with pytest.raises(ValueError, match="closed file"):
        file.read()
    with pytest.raises(OSError):
        os.fstat(descriptor)

    download.assert_not_called()
    assert get_object.call_args_list == [
        call("object.bin", byte_range=Range(offset=0, size=2)),
        call("object.bin", byte_range=Range(offset=4, size=2)),
    ]


def test_remote_file_reader_uses_default_buffer_size(monkeypatch):
    monkeypatch.delenv("MSC_REMOTE_READ_BUFFER_SIZE", raising=False)
    block_size = 8 * 1024 * 1024
    storage_client = Mock(_cache_manager=None, _replica_manager=None)
    storage_client.read.return_value = b"a" * block_size

    with RemoteFileReader("object.bin", 2 * block_size, storage_client) as file:
        storage_client.read.assert_not_called()
        assert file.read(1) == b"a"
        assert file.seek(block_size - 1) == block_size - 1
        assert file.read(1) == b"a"

    storage_client.read.assert_called_once_with(
        "object.bin", byte_range=Range(offset=0, size=block_size), check_source_version=SourceVersionCheckMode.INHERIT
    )


def test_object_file_range_reader_reuses_one_block_across_reads_and_seeks(tmp_path, monkeypatch):
    monkeypatch.setenv("MSC_REMOTE_READ_BUFFER_SIZE", "4")
    (tmp_path / "object.bin").write_bytes(b"0123456789abcd")
    client = _object_file_client(tmp_path)
    provider = cast(BaseStorageProvider, client._storage_provider)
    with (
        patch.object(client, "_is_posix_file_storage_provider", return_value=False),
        patch.object(provider, "get_object", wraps=provider.get_object) as get_object,
        client.open("object.bin", "rb", prefetch_file=False) as file,
    ):
        assert isinstance(file, ObjectFile)
        assert file.read(0) == b""
        get_object.assert_not_called()
        assert file.read(1) == b"0"
        target = bytearray(2)
        assert file.readinto(target) == 2
        assert target == b"12"
        assert file.seek(-2, os.SEEK_CUR) == 1
        assert file.read(2) == b"12"
        assert get_object.call_args_list == [call("object.bin", byte_range=Range(offset=0, size=4))]

        assert file.read(2) == b"34"
        assert file.seek(7) == 7
        assert file.read(1) == b"7"
        assert file.seek(-2, os.SEEK_END) == 12
        assert file.read(1) == b"c"
        assert file.read(None) == b"d"
        assert file.read(1) == b""
        assert file.tell() == 14

        assert file.seek(1) == 1
        assert file.read(8) == b"12345678"
        assert file.read() == b"9abcd"
        assert get_object.call_args_list == [
            call("object.bin", byte_range=Range(offset=0, size=4)),
            call("object.bin", byte_range=Range(offset=4, size=4)),
            call("object.bin", byte_range=Range(offset=12, size=2)),
            call("object.bin", byte_range=Range(offset=1, size=8)),
            call("object.bin", byte_range=Range(offset=9, size=5)),
        ]


def test_remote_file_reader_reads_into_writable_byte_view():
    target = array("I", [0, 0])
    data = b"x" * (len(target) * target.itemsize)
    storage_client = Mock()
    storage_client.read.return_value = data
    with RemoteFileReader("object.bin", len(data), storage_client) as file:
        with pytest.raises(TypeError, match="writable buffer"):
            file.readinto(bytes(2))
        with pytest.raises(TypeError):
            file.readinto(memoryview(bytearray(4))[::2])
        assert file.tell() == 0
        storage_client.read.assert_not_called()
        assert file.readinto(target) == len(data)
        assert target.tobytes() == data
        assert file.tell() == len(data)
    storage_client.read.assert_called_once_with(
        "object.bin", byte_range=Range(offset=0, size=len(data)), check_source_version=SourceVersionCheckMode.INHERIT
    )


def test_remote_file_reader_rechecks_source_after_peek_and_readline():
    policy = SourceVersionCheckMode.ENABLE
    storage_client = Mock()
    storage_client.read.side_effect = [b"a\nold", b"a\nold", b"new"]
    with RemoteFileReader("object.bin", 5, storage_client, policy) as file:
        assert file.peek() == b"a\nold"
        assert file.tell() == 0
        assert file.readline() == b"a\n"
        assert file.read(None) == b"new"
    assert storage_client.read.call_args_list == [
        call("object.bin", byte_range=Range(offset=0, size=5), check_source_version=policy),
        call("object.bin", byte_range=Range(offset=0, size=5), check_source_version=policy),
        call("object.bin", byte_range=Range(offset=2, size=3), check_source_version=policy),
    ]


@pytest.mark.parametrize("route", ["cache", "replicas", "version_check"])
def test_remote_file_reader_skips_configured_buffer_for_existing_read_policies(route, monkeypatch):
    monkeypatch.setenv("MSC_REMOTE_READ_BUFFER_SIZE", "4")
    storage_client = Mock(
        _cache_manager=Mock() if route == "cache" else None,
        _replica_manager=Mock() if route == "replicas" else None,
    )
    storage_client.read.side_effect = [b"a", b"b"]
    version_check = SourceVersionCheckMode.ENABLE if route == "version_check" else SourceVersionCheckMode.INHERIT

    with RemoteFileReader("object.bin", 6, storage_client, version_check) as file:
        assert file.read(1) == b"a"
        assert file.read(1) == b"b"

    assert storage_client.read.call_args_list == [
        call("object.bin", byte_range=Range(offset=0, size=1), check_source_version=version_check),
        call("object.bin", byte_range=Range(offset=1, size=1), check_source_version=version_check),
    ]


@pytest.mark.parametrize("buffer_size", ["-1", "invalid"])
def test_remote_file_reader_rejects_invalid_buffer_size(monkeypatch, buffer_size):
    monkeypatch.setenv("MSC_REMOTE_READ_BUFFER_SIZE", buffer_size)
    storage_client = Mock(_cache_manager=None, _replica_manager=None)
    with pytest.raises(ValueError, match="MSC_REMOTE_READ_BUFFER_SIZE"):
        RemoteFileReader("object.bin", 6, storage_client)
    storage_client.read.assert_not_called()


def test_remote_file_reader_copies_only_returned_lines(monkeypatch):
    copied_sizes = []

    class TrackedBytes(bytes):
        def __getitem__(self, key):
            result = super().__getitem__(key)
            if isinstance(result, bytes):
                copied_sizes.append(len(result))
            return result

    line = b"x" * 63 + b"\n"
    block = TrackedBytes(line * 3)
    monkeypatch.setenv("MSC_REMOTE_READ_BUFFER_SIZE", str(len(block)))
    storage_client = Mock(_cache_manager=None, _replica_manager=None)

    with (
        RemoteFileReader("object.bin", len(block), storage_client) as file,
        patch.object(file, "_read_range", return_value=block) as read_range,
    ):
        assert file.readline() == line
        assert next(file) == line
        assert file.readlines() == [line]

    assert copied_sizes == [len(line)] * 3
    read_range.assert_called_once_with(0, len(block))


@pytest.mark.parametrize("buffer_size", [0, io.DEFAULT_BUFFER_SIZE])
def test_object_file_range_reader_preserves_binary_lines(tmp_path, monkeypatch, buffer_size):
    monkeypatch.setenv("MSC_REMOTE_READ_BUFFER_SIZE", str(buffer_size))
    long_line = b"x" * (io.DEFAULT_BUFFER_SIZE + 1) + b"\n"
    content = b"ab\ncd\n" + long_line + b"tail"
    (tmp_path / "object.bin").write_bytes(content)
    client = _object_file_client(tmp_path)
    provider = cast(BaseStorageProvider, client._storage_provider)
    with (
        patch.object(client, "_is_posix_file_storage_provider", return_value=False),
        patch.object(provider, "get_object", wraps=provider.get_object) as get_object,
        client.open("object.bin", "rb", prefetch_file=False) as file,
    ):
        assert file.readline(0) == b""
        get_object.assert_not_called()
        assert file.readline(1) == b"a"
        assert file.read(1) == b"b"
        assert file.readline() == b"\n"
        assert file.tell() == 3
        assert next(file) == b"cd\n"
        assert next(file) == long_line
        assert file.readlines() == [b"tail"]
        assert get_object.call_args_list == [
            call("object.bin", byte_range=Range(offset=0, size=io.DEFAULT_BUFFER_SIZE)),
            call(
                "object.bin",
                byte_range=Range(offset=io.DEFAULT_BUFFER_SIZE, size=len(content) - io.DEFAULT_BUFFER_SIZE),
            ),
        ]

        assert file.seek(3) == 3
        assert file.readline() == b"cd\n"
        assert list(file) == [long_line, b"tail"]
        assert file.seek(6) == 6
        assert file.readline(io.DEFAULT_BUFFER_SIZE + 1) == long_line[:-1]
        assert file.tell() == 6 + len(long_line) - 1
        assert file.readline() == b"\n"


def test_object_file_without_cache_opens_empty_file_when_prefetch_disabled(tmp_path):
    (tmp_path / "empty.bin").touch()
    client = _object_file_client(tmp_path)
    provider = cast(BaseStorageProvider, client._storage_provider)
    with (
        patch.object(client, "_is_posix_file_storage_provider", return_value=False),
        patch.object(provider, "download_file", wraps=provider.download_file) as download,
        patch.object(provider, "get_object", wraps=provider.get_object) as get_object,
        client.open("empty.bin", "rb", prefetch_file=False) as file,
    ):
        assert file.seek(0, os.SEEK_END) == 0
        assert file.read() == b""
        assert file.readline() == b""

    download.assert_not_called()
    get_object.assert_not_called()


@pytest.mark.parametrize("cache_enabled", [False, True])
def test_object_file_download_reuses_open_metadata(tmp_path, cache_enabled):
    content = b"cached file content\n"
    (tmp_path / "object.txt").write_bytes(content)
    client = _object_file_client(tmp_path)
    if cache_enabled:
        client._cache_manager = CacheManager(
            profile="data",
            cache_config=CacheConfig(
                location=str(tmp_path / "cache"), size="10M", cache_line_size="1M", check_source_version=False
            ),
        )
    provider = cast(BaseStorageProvider, client._storage_provider)
    with (
        patch.object(client, "_is_posix_file_storage_provider", return_value=False),
        patch.object(provider, "_get_object_metadata", wraps=provider._get_object_metadata) as metadata_lookup,
    ):
        with client.open("object.txt", "rb") as file:
            assert file.read() == content
        assert metadata_lookup.call_count == 1


@pytest.mark.parametrize("route", ["metadata", "replica"])
def test_object_file_metadata_reuse_keeps_routed_downloads(tmp_path, route):
    content = b"routed content"
    (tmp_path / "physical").write_bytes(content)
    client = _object_file_client(tmp_path)
    metadata = client.info("physical")
    routing = Mock()
    if route == "metadata":
        client._metadata_provider = routing
        routing.get_object_metadata.return_value = metadata
        routing.realpath.return_value = ResolvedPath("physical", ResolvedPathState.EXISTS)
    elif route == "replica":
        client._replica_manager = routing
        routing.download_from_replica_or_primary.side_effect = lambda path, destination, provider: (
            provider.download_file("physical", destination)
        )
    path = "logical" if route == "metadata" else "physical"
    provider = cast(BaseStorageProvider, client._storage_provider)
    with patch.object(provider, "_download_file", wraps=provider._download_file) as download:
        with ObjectFile(client, path, prefetch_file=route != "replica") as file:
            assert file.read(2) + file.read(2) + file.read() == content
        download.assert_called_once()
        assert download.call_args.args[2].content_length == len(content)
    if route == "metadata":
        routing.realpath.assert_called_once_with("logical")
        assert routing.get_object_metadata.call_count == 2
    elif route == "replica":
        routing.download_from_replica_or_primary.assert_called_once()
