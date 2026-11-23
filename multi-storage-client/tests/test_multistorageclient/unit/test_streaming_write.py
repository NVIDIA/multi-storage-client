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

import array
import gc
import io
import logging
import multiprocessing
import os
import pickle
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from typing import Any, cast
from unittest.mock import patch

import pytest
import torch as torch_module
import torch.distributed.checkpoint as dcp

import multistorageclient as msc
from multistorageclient import StorageClient, StorageClientConfig, StreamingWriterOptions
from multistorageclient.file import ObjectFile
from multistorageclient.providers import s3 as s3_provider
from multistorageclient.providers.ais import StaticAISCredentialProvider
from multistorageclient.providers.ais_s3 import AIStoreS3StorageProvider
from multistorageclient.providers.s3 import S3StorageProvider
from multistorageclient.providers.s8k import S8KStorageProvider
from multistorageclient.types import RetryableError
from test_multistorageclient.unit.utils import config, tempdatastore
from test_multistorageclient.unit.utils.streaming_processes import (
    fork_writers,
    payload,
    run_in_processes,
    write_object,
)

torch = cast(Any, torch_module)

MiB = 1024 * 1024
PART = 5 * MiB
OPTIONS = StreamingWriterOptions(part_size_bytes=PART, max_inflight_parts=2)


@contextmanager
def _streaming_bucket() -> Iterator[tuple[tempdatastore.TemporaryAWSS3Bucket, StorageClient]]:
    with tempdatastore.TemporaryAWSS3Bucket(enable_rust_client=True) as bucket:
        profile = bucket.profile_config_dict()
        client = StorageClient(StorageClientConfig.from_dict({"profiles": {"stream": profile}}, profile="stream"))
        yield bucket, client


def _multipart_uploads(bucket: tempdatastore.TemporaryAWSS3Bucket) -> list:
    return bucket._client.list_multipart_uploads(Bucket=bucket._bucket_name).get("Uploads", [])


def _stats(f: Any) -> dict[str, Any]:
    # The native writer's counters; the Python streaming interface has no stats().
    return f._file._writer._native.stats()


@pytest.mark.parametrize("size", [0, 1, PART - 1, PART, PART + 1, 3 * PART, 3 * PART + 7])
@pytest.mark.parametrize("write_size", [64 * 1024, None])
def test_streaming_write_roundtrip(size: int, write_size: int | None):
    data = payload(size)
    with _streaming_bucket() as (bucket, client):
        with client.open("dir/object.bin", "wb", attributes={"owner": "test"}, writer_options=OPTIONS) as f:
            assert isinstance(f, ObjectFile)
            step = write_size or max(size, 1)
            for offset in range(0, size, step):
                assert f.write(data[offset : offset + step]) == len(data[offset : offset + step])
            assert f.tell() == size
            stats = _stats(f)
        assert stats["multipart"] == (size >= PART)
        assert stats["peak_retained_bytes"] <= 3 * PART
        assert stats["peak_inflight_parts"] <= 2
        assert client.read("dir/object.bin") == data
        assert client.info("dir/object.bin").metadata == {"owner": "test"}
        assert _multipart_uploads(bucket) == []


def test_streaming_write_uploads_before_close_without_whole_object_calls():
    calls: list[str] = []
    original = s3_provider.run_async_rust_client_method

    def record(rust_client: Any, method: str, *args: Any, **kwargs: Any) -> Any:
        calls.append(method)
        return original(rust_client, method, *args, **kwargs)

    data = payload(8 * PART + 3)
    with _streaming_bucket() as (_, client):
        with (
            patch.object(s3_provider, "run_async_rust_client_method", record),
            client.open("big.bin", "wb", writer_options=OPTIONS) as f,
        ):
            for offset in range(0, len(data), MiB):
                f.write(data[offset : offset + MiB])
            f.flush()
            f.fsync()
            flushed = _stats(f)
        assert flushed["completed_bytes"] == 8 * PART
        assert flushed["inflight_parts"] == 0
        assert flushed["retained_bytes"] == 3
        assert flushed["first_part_complete_ns"] > 0
        assert flushed["peak_retained_bytes"] <= 3 * PART
        assert not {"put", "upload", "upload_multipart_from_bytes", "upload_multipart_from_file"} & set(calls)
        assert client.read("big.bin") == data


def test_streaming_file_is_forward_only():
    with _streaming_bucket() as (_, client):
        with client.open("forward.bin", "wb", writer_options=OPTIONS) as f:
            assert f.writable() and not f.readable() and not f.seekable()
            f.write(b"abc")
            assert f.seek(3) == 3
            assert f.seek(0, os.SEEK_CUR) == 3
            for operation in (lambda: f.seek(0), lambda: f.truncate()):
                with pytest.raises(io.UnsupportedOperation):
                    operation()
            # A placeholder descriptor, as for buffered files, so DCP's os.fsync(fileno()) needs no host-wide sync.
            os.fsync(f.fileno())
            f.fsync()
        assert f.closed
        f.close()
        with pytest.raises(ValueError):
            f.write(b"more")
        assert client.read("forward.bin") == b"abc"


def test_streaming_writer_owns_accepted_bytes():
    with _streaming_bucket() as (_, client):
        buffer = bytearray(payload(PART + 11))
        floats = array.array("f", [1.5, -2.25, 3.0])
        strided = memoryview(bytes(range(16)))[::2]
        with client.open("owned.bin", "wb", writer_options=OPTIONS) as f:
            f.write(buffer)
            buffer[:] = b"\0" * len(buffer)
            f.write(memoryview(floats))
            f.write(strided)
        assert client.read("owned.bin") == payload(PART + 11) + floats.tobytes() + bytes(range(0, 16, 2))


def test_streaming_exception_or_discard_aborts_and_keeps_old_object():
    with _streaming_bucket() as (bucket, client):
        client.write("ckpt.bin", b"old")
        with pytest.raises(KeyError), client.open("ckpt.bin", "wb", writer_options=OPTIONS) as f:
            f.write(payload(3 * PART))
            raise KeyError("serialization failed")
        assert client.read("ckpt.bin") == b"old"

        f = client.open("ckpt.bin", "wb", writer_options=OPTIONS)
        f.write(payload(2 * PART))
        f.discard()
        assert f.closed
        f.close()
        assert client.read("ckpt.bin") == b"old"
        assert _multipart_uploads(bucket) == []


def test_streaming_unclosed_file_is_aborted_not_published(caplog):
    with _streaming_bucket() as (_, client):
        f = client.open("unclosed.bin", "wb", writer_options=OPTIONS)
        f.write(payload(2 * PART))
        with caplog.at_level(logging.WARNING):
            del f
            gc.collect()
        assert "was not closed" in caplog.text
        assert not client.is_file("unclosed.bin")


def test_streaming_failure_is_sticky():
    with _streaming_bucket() as (bucket, client):
        bucket._client.delete_bucket(Bucket=bucket._bucket_name)
        f = client.open("missing.bin", "wb", writer_options=OPTIONS)
        with pytest.raises(RuntimeError) as first:
            f.write(payload(2 * PART))
            f.close()
        assert not isinstance(first.value, RetryableError)
        empty_strided = memoryview(b"")[::2]
        for operation in (
            lambda: f.write(b"x"),
            lambda: f.write(b""),
            lambda: f.write(empty_strided),
            f.tell,
            f.flush,
            f.close,
            f.close,
        ):
            with pytest.raises(Exception) as later:
                operation()
            assert later.type in (RuntimeError, first.type)
        assert not f.closed
        f.discard()
        assert f.closed


def test_streaming_is_opt_in_and_binary_write_only():
    with _streaming_bucket() as (_, client):
        with client.open("legacy.bin", "wb") as f:
            assert isinstance(f._file, io.BytesIO)
            f.write(b"abc")
            f.seek(0)
            f.write(b"x")
        assert client.read("legacy.bin") == b"xbc"
        for mode in ("w", "ab", "rb"):
            with pytest.raises(ValueError, match='mode "wb"'):
                client.open("legacy.bin", mode, writer_options=OPTIONS)


@pytest.mark.parametrize(
    "options",
    [
        {"part_size_bytes": 1024},
        {"part_size_bytes": 5 * 1024 * MiB + 1},
        {"max_inflight_parts": 0},
        {"max_inflight_parts": True},
        {"max_object_size_bytes": -1},
    ],
)
def test_streaming_writer_options_are_validated(options: dict[str, Any]):
    with pytest.raises((TypeError, ValueError)):
        StreamingWriterOptions(**options)


def test_streaming_writer_options_are_picklable():
    assert pickle.loads(pickle.dumps(OPTIONS)) == OPTIONS


def test_streaming_rejects_unsupported_providers():
    options: dict[str, Any] = {"endpoint_url": "http://localhost:7070", "base_path": "bucket"}
    rust_client = {"allow_http": True}
    with pytest.raises(NotImplementedError, match="infer_content_type"):
        S8KStorageProvider(**options, rust_client=dict(rust_client), infer_content_type=True).open_streaming_writer(
            "key", None, OPTIONS
        )
    with pytest.raises(NotImplementedError, match="Rust client"):
        S3StorageProvider(**options).open_streaming_writer("key", None, OPTIONS)
    with pytest.raises(NotImplementedError, match="JWT"):
        AIStoreS3StorageProvider(
            **options, credentials_provider=StaticAISCredentialProvider(token="t"), rust_client=dict(rust_client)
        ).open_streaming_writer("key", None, OPTIONS)
    with pytest.raises(NotImplementedError, match="directory bucket"):
        S3StorageProvider(**{**options, "base_path": "b--x-s3"}, rust_client=dict(rust_client)).open_streaming_writer(
            "key", None, OPTIONS
        )

    with tempdatastore.TemporaryAWSS3Bucket() as bucket:
        client = StorageClient(
            StorageClientConfig.from_dict({"profiles": {"p": bucket.profile_config_dict()}}, profile="p")
        )
        with pytest.raises(NotImplementedError, match="Rust client"):
            client.open("buffered.bin", "wb", writer_options=OPTIONS)
        assert not client.is_file("buffered.bin")

    with tempdatastore.TemporaryPOSIXDirectory() as directory:
        client = StorageClient(
            StorageClientConfig.from_dict({"profiles": {"p": directory.profile_config_dict()}}, profile="p")
        )
        with pytest.raises(NotImplementedError, match="POSIX"):
            client.open("local.bin", "wb", writer_options=OPTIONS)


def test_streaming_max_object_size_raises_the_part_size():
    with _streaming_bucket() as (_, client):
        hint = replace(OPTIONS, max_object_size_bytes=10_000 * PART + 1)
        with client.open("hint.bin", "wb", writer_options=hint) as f:
            assert _stats(f)["part_size"] > PART
            f.write(b"not a limit")
        assert client.read("hint.bin") == b"not a limit"


def test_streaming_write_registers_with_metadata_provider():
    with tempdatastore.TemporaryAWSS3Bucket(enable_rust_client=True) as bucket:
        profile = bucket.profile_config_dict() | {
            "metadata_provider": {"type": "manifest", "options": {"manifest_path": ".msc_manifests", "writable": True}}
        }
        client = StorageClient(StorageClientConfig.from_dict({"profiles": {"m": profile}}, profile="m"))
        single = cast(Any, client)._delegate
        register = single._register_written_file
        calls: list[tuple] = []

        def flaky_register(*args: Any) -> None:
            calls.append(args)
            if len(calls) == 1:
                raise RuntimeError("registration failed")
            register(*args)

        data = payload(PART + 1)
        with patch.object(single, "_register_written_file", flaky_register):
            f = client.open("dir/object.bin", "wb", attributes={"owner": "test"}, writer_options=OPTIONS)
            f.write(data)
            with pytest.raises(RuntimeError, match="registration failed"):
                f.close()
            # The object is already published; a retried close only retries registration.
            f.close()
        assert f.closed and len(calls) == 2
        client.commit_metadata()
        assert client.read("dir/object.bin") == data
        assert client.info("dir/object.bin").metadata == {"owner": "test"}
        physical_path = calls[0][1]
        assert not single._storage_provider.get_object_metadata(physical_path).metadata
        assert _multipart_uploads(bucket) == []


def test_streaming_copies_non_contiguous_buffers_in_chunks():
    import numpy as np

    # 24 MiB of every other column: several one-part conversion chunks of whole rows.
    strided = np.resize(np.arange(251, dtype=np.uint8), (6144, 8192))[:, ::2]
    with _streaming_bucket() as (_, client):
        with client.open("strided.bin", "wb", writer_options=OPTIONS) as f:
            assert f.write(strided) == strided.nbytes
        assert client.read("strided.bin") == strided.tobytes()


def test_streaming_bounds_non_contiguous_conversion_to_one_part():
    import tracemalloc

    import numpy as np

    with _streaming_bucket() as (_, client):
        # A 1-D strided view of three parts is split by element; Python-side conversion peaks within one part.
        flat = np.resize(np.arange(251, dtype=np.uint8), 6 * PART)[::2]
        with client.open("flat.bin", "wb", writer_options=OPTIONS) as f:
            tracemalloc.start()
            try:
                assert f.write(flat) == flat.nbytes
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
        assert peak <= PART + MiB, peak
        assert client.read("flat.bin") == flat.tobytes()

        # Rows or elements larger than half a part cannot be split; they are rejected before any bytes are accepted.
        wide = np.zeros((2, 4 * PART), dtype=np.uint8)[:, ::2]
        big_items = np.zeros(4, dtype=[("a", "u1", (PART,))])[::2]
        for name, buffer in (("wide.bin", wide), ("items.bin", big_items)):
            with client.open(name, "wb", writer_options=OPTIONS) as f:
                with pytest.raises(ValueError, match="pass a contiguous buffer"):
                    f.write(buffer)
                assert f.tell() == 0
            assert client.read(name) == b""


def test_streaming_rechecks_the_overwrite_policy_before_publishing():
    with tempdatastore.TemporaryAWSS3Bucket(enable_rust_client=True) as bucket:
        profile = bucket.profile_config_dict() | {
            "metadata_provider": {"type": "manifest", "options": {"manifest_path": ".msc_manifests", "writable": True}}
        }
        client = StorageClient(StorageClientConfig.from_dict({"profiles": {"m": profile}}, profile="m"))
        first = client.open("same.bin", "wb", writer_options=OPTIONS)
        second = client.open("same.bin", "wb", writer_options=OPTIONS)
        first.write(payload(PART + 1))
        second.write(b"loser")
        first.close()
        client.commit_metadata()
        with pytest.raises(FileExistsError):
            second.close()
        second.discard()
        assert client.read("same.bin") == payload(PART + 1)
        assert _multipart_uploads(bucket) == []


def test_streaming_writer_rejects_pickling_and_other_processes():
    with _streaming_bucket() as (_, client):
        f = client.open("pid.bin", "wb", writer_options=OPTIONS)
        f.write(b"parent")
        native = cast(Any, f)._file._writer._native
        with pytest.raises(TypeError):
            pickle.dumps(native)

        def child(queue: Any) -> None:
            try:
                native.write(b"child")
                queue.put("wrote")
            except RuntimeError as error:
                queue.put(str(error))

        ctx = multiprocessing.get_context("fork")
        queue = ctx.Queue()
        process = ctx.Process(target=child, args=(queue,), daemon=True)
        process.start()
        try:
            assert "cannot be used in process" in queue.get(timeout=30)
            process.join(10)
            assert process.exitcode == 0
        finally:
            if process.is_alive():
                process.kill()
                process.join()

        f.write(b"-more")
        f.close()
        assert client.read("pid.bin") == b"parent-more"


def test_streaming_write_in_child_processes():
    with _streaming_bucket() as (_, client):
        client.write("warmup", b"parent uses the client first")
        config_dict = {"profiles": {"stream": client._config._config_dict["profiles"]["stream"]}}  # pyright: ignore
        spawn = multiprocessing.get_context("spawn")
        spawned = [(f"spawn-{i}.bin", PART + i) for i in range(2)]
        forked = [(f"fork-{i}.bin", PART + i) for i in range(2)]
        args = [(config_dict, path, size, OPTIONS) for path, size in spawned]
        assert run_in_processes(spawn, write_object, args, timeout=45) == [size for _, size in spawned]
        sizes = run_in_processes(spawn, fork_writers, [(config_dict, forked, OPTIONS, 30)], timeout=50)
        assert sizes == [[size for _, size in forked]]
        for path, size in spawned + forked:
            assert client.read(path) == payload(size)


def test_streaming_write_releases_gil_while_waiting():
    ticks = 0
    stop = threading.Event()

    def spin() -> None:
        nonlocal ticks
        while not stop.is_set():
            ticks += 1

    with _streaming_bucket() as (_, client):
        thread = threading.Thread(target=spin)
        thread.start()
        try:
            data = payload(20 * PART)
            with client.open("gil.bin", "wb", writer_options=replace(OPTIONS, max_inflight_parts=1)) as f:
                before = ticks
                f.write(data)
                during = ticks - before
        finally:
            stop.set()
            thread.join()
        assert during > 1000
        assert _stats(f)["capacity_wait_ns"] > 0


class _Model(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.linear = torch.nn.Linear(512, 4096)


def test_streaming_dcp_and_torch_save_roundtrip():
    opened: list[tuple[str, StreamingWriterOptions]] = []
    open_streaming_writer = S3StorageProvider.open_streaming_writer

    def record(self: S3StorageProvider, path: str, attributes: Any, options: StreamingWriterOptions) -> Any:
        opened.append((path, options))
        return open_streaming_writer(self, path, attributes, options)

    with _streaming_bucket() as (bucket, client):
        config.setup_msc_config(
            config_dict={"profiles": {"stream": client._config._config_dict["profiles"]["stream"]}}  # pyright: ignore
        )
        torch.manual_seed(0)
        model = _Model()
        prefix = f"msc://stream/checkpoint-{uuid.uuid4()}"
        with patch.object(S3StorageProvider, "open_streaming_writer", record):
            writer = msc.torch.MultiStorageFileSystemWriter(prefix, writer_options=OPTIONS)  # type: ignore[reportPrivateImportUsage]
            dcp.save({"model": model}, storage_writer=writer)  # type: ignore[reportPrivateImportUsage]
            msc.torch.save(model.state_dict(), f"{prefix}/model.pt", writer_options=OPTIONS)
        assert {path.rsplit(".", 1)[-1] for path, _ in opened} == {"distcp", "tmp", "pt"}
        assert all(options is OPTIONS for _, options in opened)

        restored = _Model()
        state = {"model": restored}
        dcp.load(state, storage_reader=msc.torch.MultiStorageFileSystemReader(prefix, thread_count=2))  # type: ignore[reportPrivateImportUsage]
        for name, tensor in model.state_dict().items():
            assert torch.equal(tensor, restored.state_dict()[name]), name

        loaded = msc.torch.load(f"{prefix}/model.pt")
        assert all(torch.equal(loaded[k], v) for k, v in model.state_dict().items())
        assert _multipart_uploads(bucket) == []
