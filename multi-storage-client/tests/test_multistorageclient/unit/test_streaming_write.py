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
from typing import Any, cast
from unittest.mock import patch

import pytest
import torch as torch_module
import torch.distributed.checkpoint as dcp

import multistorageclient as msc
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.file import ObjectFile
from multistorageclient.providers import s3 as s3_provider
from multistorageclient.providers.s3 import S3StorageProvider
from multistorageclient.providers.s8k import S8KStorageProvider
from test_multistorageclient.unit.utils import config, tempdatastore

torch = cast(Any, torch_module)

MiB = 1024 * 1024
PART = 5 * MiB


def _payload(size: int) -> bytes:
    return (bytes(range(251)) * (size // 251 + 1))[:size]


@contextmanager
def _streaming_bucket(**streaming: Any) -> Iterator[tuple[tempdatastore.TemporaryAWSS3Bucket, StorageClient]]:
    with tempdatastore.TemporaryAWSS3Bucket(enable_rust_client=True) as bucket:
        profile = bucket.profile_config_dict()
        profile["storage_provider"]["options"]["rust_client"]["streaming_write"] = {
            "enabled": True,
            "part_size_bytes": PART,
            "max_inflight_parts": 2,
            **streaming,
        }
        client = StorageClient(StorageClientConfig.from_dict({"profiles": {"stream": profile}}, profile="stream"))
        yield bucket, client


def _multipart_uploads(bucket: tempdatastore.TemporaryAWSS3Bucket) -> list:
    return bucket._client.list_multipart_uploads(Bucket=bucket._bucket_name).get("Uploads", [])


def _stats(f: Any) -> dict[str, Any]:
    return f._stream._writer.stats()


@pytest.mark.parametrize("size", [0, 1, PART - 1, PART, PART + 1, 3 * PART, 3 * PART + 7])
@pytest.mark.parametrize("write_size", [64 * 1024, None])
def test_streaming_write_roundtrip(size: int, write_size: int | None):
    data = _payload(size)
    with _streaming_bucket() as (bucket, client):
        with client.open("dir/object.bin", "wb", attributes={"owner": "test"}) as f:
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

    data = _payload(8 * PART + 3)
    with _streaming_bucket() as (_, client):
        with patch.object(s3_provider, "run_async_rust_client_method", record):
            with client.open("big.bin", "wb") as f:
                for offset in range(0, len(data), MiB):
                    f.write(data[offset : offset + MiB])
                f.flush()
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
        with client.open("forward.bin", "wb") as f:
            assert f.writable() and not f.readable() and not f.seekable()
            f.write(b"abc")
            assert f.seek(3) == 3
            assert f.seek(0, os.SEEK_CUR) == 3
            for operation in (lambda: f.seek(0), lambda: f.truncate(), f.fileno):
                with pytest.raises(io.UnsupportedOperation):
                    operation()
            f.fsync()
        assert f.closed
        f.close()
        with pytest.raises(ValueError):
            f.write(b"more")
        assert client.read("forward.bin") == b"abc"


def test_streaming_writer_owns_accepted_bytes():
    with _streaming_bucket() as (_, client):
        buffer = bytearray(_payload(PART + 11))
        floats = array.array("f", [1.5, -2.25, 3.0])
        strided = memoryview(bytes(range(16)))[::2]
        with client.open("owned.bin", "wb") as f:
            f.write(buffer)
            buffer[:] = b"\0" * len(buffer)
            f.write(memoryview(floats))
            f.write(strided)
        assert client.read("owned.bin") == _payload(PART + 11) + floats.tobytes() + bytes(range(0, 16, 2))


def test_streaming_exception_or_discard_aborts_and_keeps_old_object():
    with _streaming_bucket() as (bucket, client):
        client.write("ckpt.bin", b"old")
        with pytest.raises(KeyError):
            with client.open("ckpt.bin", "wb") as f:
                f.write(_payload(3 * PART))
                raise KeyError("serialization failed")
        assert client.read("ckpt.bin") == b"old"

        f = client.open("ckpt.bin", "wb")
        f.write(_payload(2 * PART))
        f.discard()
        assert f.closed
        f.close()
        assert client.read("ckpt.bin") == b"old"
        assert _multipart_uploads(bucket) == []


def test_streaming_unclosed_file_is_aborted_not_published(caplog):
    with _streaming_bucket() as (_, client):
        f = client.open("unclosed.bin", "wb")
        f.write(_payload(2 * PART))
        with caplog.at_level(logging.WARNING):
            del f
            gc.collect()
        assert "was not closed" in caplog.text
        assert not client.is_file("unclosed.bin")


def test_streaming_failure_is_sticky():
    with _streaming_bucket() as (bucket, client):
        bucket._client.delete_bucket(Bucket=bucket._bucket_name)
        f = client.open("missing.bin", "wb")
        with pytest.raises(Exception) as first:
            f.write(_payload(2 * PART))
            f.close()
        for operation in (lambda: f.write(b"x"), f.flush, f.close, f.close):
            with pytest.raises(Exception) as later:
                operation()
            assert later.type in (RuntimeError, first.type)
        assert not f.closed
        f.discard()
        assert f.closed


def test_streaming_is_opt_in_and_binary_write_only():
    with tempdatastore.TemporaryAWSS3Bucket(enable_rust_client=True) as bucket:
        client = StorageClient(
            StorageClientConfig.from_dict({"profiles": {"p": bucket.profile_config_dict()}}, profile="p")
        )
        with client.open("legacy.bin", "wb") as f:
            assert isinstance(f._file, io.BytesIO)
            f.write(b"abc")
            f.seek(0)
            f.write(b"x")
        assert client.read("legacy.bin") == b"xbc"

    with _streaming_bucket() as (_, client):
        for mode, expected in (("w", io.StringIO), ("ab", io.BytesIO)):
            with client.open(f"buffered-{mode}", mode) as f:
                assert isinstance(f._file, expected)


@pytest.mark.parametrize(
    "streaming",
    [{"part_size_bytes": 1024}, {"max_inflight_parts": 0}, {"unknown": 1}, {"enabled": "yes"}],
)
def test_streaming_config_is_validated(streaming: dict[str, Any]):
    with tempdatastore.TemporaryAWSS3Bucket(enable_rust_client=True) as bucket:
        profile = bucket.profile_config_dict()
        profile["storage_provider"]["options"]["rust_client"]["streaming_write"] = {"enabled": True, **streaming}
        with pytest.raises(RuntimeError, match="Failed to validate"):
            StorageClientConfig.from_dict({"profiles": {"p": profile}}, profile="p")


def test_streaming_rejects_unsupported_providers():
    options: dict[str, Any] = {"endpoint_url": "http://localhost:7070", "base_path": "bucket"}
    rust_client = {"allow_http": True, "streaming_write": {"enabled": True}}
    with pytest.raises(ValueError, match="infer_content_type"):
        S8KStorageProvider(**options, rust_client=dict(rust_client), infer_content_type=True)
    with pytest.raises(NotImplementedError):
        S3StorageProvider(**options).open_streaming_writer("key")
    with pytest.raises(NotImplementedError, match="directory bucket"):
        S3StorageProvider(**{**options, "base_path": "b--x-s3"}, rust_client=dict(rust_client)).open_streaming_writer(
            "key"
        )


def test_streaming_writer_rejects_pickling_and_other_processes():
    with _streaming_bucket() as (_, client):
        f = client.open("pid.bin", "wb")
        f.write(b"parent")
        native = cast(Any, f)._stream._writer._native
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
        process = ctx.Process(target=child, args=(queue,))
        process.start()
        process.join(30)
        assert process.exitcode == 0
        assert "cannot be used in process" in queue.get(timeout=5)

        f.write(b"-more")
        f.close()
        assert client.read("pid.bin") == b"parent-more"


def _child_write(config_dict: dict[str, Any], path: str, size: int) -> int:
    client = StorageClient(StorageClientConfig.from_dict(config_dict, profile="stream"))
    with client.open(path, "wb") as f:
        f.write(_payload(size))
        return f.tell()


def _fork_writers(config_dict: dict[str, Any], count: int, results: Any) -> None:
    # Forked children cannot use a Rust runtime their parent already started, as with buffered Rust uploads,
    # so this parent opens no streams before forking, like NVRx's multiprocess writer.
    StorageClient(StorageClientConfig.from_dict(config_dict, profile="stream"))
    with multiprocessing.get_context("fork").Pool(count) as pool:
        results.put(pool.starmap(_child_write, [(config_dict, f"fork-{i}.bin", PART + i) for i in range(count)]))


def test_streaming_write_in_child_processes():
    with _streaming_bucket() as (_, client):
        client.write("warmup", b"parent uses the client first")
        config_dict = {"profiles": {"stream": client._config._config_dict["profiles"]["stream"]}}  # pyright: ignore
        spawn = multiprocessing.get_context("spawn")
        with spawn.Pool(2) as pool:
            sizes = pool.starmap(_child_write, [(config_dict, f"spawn-{i}.bin", PART + i) for i in range(2)])
        assert sizes == [PART, PART + 1]
        results = spawn.Queue()
        parent = spawn.Process(target=_fork_writers, args=(config_dict, 2, results))
        parent.start()
        assert results.get(timeout=120) == [PART, PART + 1]
        parent.join(30)
        assert parent.exitcode == 0
        for prefix in ("spawn", "fork"):
            for i in range(2):
                assert client.read(f"{prefix}-{i}.bin") == _payload(PART + i)


def test_streaming_write_releases_gil_while_waiting():
    ticks = 0
    stop = threading.Event()

    def spin() -> None:
        nonlocal ticks
        while not stop.is_set():
            ticks += 1

    with _streaming_bucket(max_inflight_parts=1) as (_, client):
        thread = threading.Thread(target=spin)
        thread.start()
        try:
            with client.open("gil.bin", "wb") as f:
                before = ticks
                f.write(_payload(20 * PART))
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
    with _streaming_bucket() as (bucket, client):
        config.setup_msc_config(
            config_dict={"profiles": {"stream": client._config._config_dict["profiles"]["stream"]}}  # pyright: ignore
        )
        torch.manual_seed(0)
        model = _Model()
        prefix = f"msc://stream/checkpoint-{uuid.uuid4()}"
        with patch.object(os, "sync") as sync:
            dcp.save({"model": model}, storage_writer=msc.torch.MultiStorageFileSystemWriter(prefix))  # type: ignore[reportPrivateImportUsage]
        assert sync.called

        restored = _Model()
        state = {"model": restored}
        dcp.load(state, storage_reader=msc.torch.MultiStorageFileSystemReader(prefix, thread_count=2))  # type: ignore[reportPrivateImportUsage]
        for name, tensor in model.state_dict().items():
            assert torch.equal(tensor, restored.state_dict()[name]), name

        msc.torch.save(model.state_dict(), f"{prefix}/model.pt")
        loaded = msc.torch.load(f"{prefix}/model.pt")
        assert all(torch.equal(loaded[k], v) for k, v in model.state_dict().items())
        assert _multipart_uploads(bucket) == []
