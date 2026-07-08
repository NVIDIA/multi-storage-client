# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, cast

import pytest
from fsspec.callbacks import Callback

from multistorageclient.cache import CacheManager
from multistorageclient.caching.cache_config import CacheConfig
from multistorageclient.client.single import SingleStorageClient
from multistorageclient.contrib.async_fs import MultiStorageAsyncFileSystem
from multistorageclient.file import ObjectFile, RemoteFileReader
from multistorageclient.replica_manager import ReplicaManager
from multistorageclient.types import (
    MAX_SYMLINK_DEPTH,
    ObjectMetadata,
    Range,
    RetryableError,
    RetryConfig,
    SourceVersionCheckMode,
)


class _RangeClient:
    def __init__(self, content: bytes) -> None:
        self._content = content
        self.info_calls: list[str] = []
        self.open_calls: list[tuple[str, str]] = []
        self.open_kwargs: list[dict[str, object]] = []
        self.range_requests: list[tuple[int, int]] = []
        self.full_read_calls = 0

    def info(self, path: str) -> ObjectMetadata:
        self.info_calls.append(path)
        return ObjectMetadata(
            key=path,
            content_length=len(self._content),
            last_modified=datetime.now(timezone.utc),
        )

    def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        if byte_range is None:
            self.full_read_calls += 1
            return self._content
        return self._read_range(byte_range.offset, byte_range.size)

    def open(self, path: str, mode: str = "rb", **kwargs) -> RemoteFileReader:
        self.open_calls.append((path, mode))
        self.open_kwargs.append(dict(kwargs))
        return RemoteFileReader(path, len(self._content), read_range=self._read_range)

    def _read_range(self, offset: int, size: int) -> bytes:
        self.range_requests.append((offset, size))
        return self._content[offset : offset + size]


class _TrackingReader(io.BytesIO):
    def __init__(self, content: bytes) -> None:
        super().__init__(content)
        self.size = len(content)
        self.read_calls: list[tuple[int, int]] = []

    def read(self, size: int = -1) -> bytes:
        chunk = super().read(size)
        self.read_calls.append((size, len(chunk)))
        return chunk


class _OpenOnlyClient:
    def __init__(self, content: bytes) -> None:
        self._content = content
        self.reader: Optional[_TrackingReader] = None

    def open(self, path: str, mode: str = "rb", **kwargs) -> _TrackingReader:
        assert path == "objects/data.bin"
        assert mode == "rb"
        self.reader = _TrackingReader(self._content)
        return self.reader


class _OrdinaryObjectFileClient(_RangeClient):
    """Ordinary-provider test double whose default ObjectFile behavior is eager prefetch."""

    def __init__(self, content: bytes) -> None:
        super().__init__(content)
        self._cache_manager = None
        self.download_calls = 0
        self.download_destinations: list[str] = []

    def read(self, path: str, byte_range: Optional[Range] = None, **_kwargs: object) -> bytes:
        return super().read(path, byte_range)

    def _read_uncached(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        return super().read(path, byte_range)

    def download_file(self, _path: str, destination: Any) -> None:
        self.download_calls += 1
        if isinstance(destination, str):
            self.download_destinations.append(destination)
            Path(destination).write_bytes(self._content)
        else:
            destination.write(self._content)

    def _is_rust_client_enabled(self) -> bool:
        return False

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> ObjectFile:
        self.open_calls.append((path, mode))
        self.open_kwargs.append(dict(kwargs))
        return ObjectFile(cast(Any, self), path, mode=mode, **kwargs)


class _InfoFailingNativeDownloadClient(_OrdinaryObjectFileClient):
    def info(self, path: str) -> ObjectMetadata:
        self.info_calls.append(path)
        raise RetryableError("metadata preflight is transiently unavailable")


class _RustBytesLike:
    """Minimal stand-in for the bytes wrapper returned by the PyO3 client."""

    def __init__(self, content: bytes) -> None:
        self._content = content

    def to_bytes(self) -> bytes:
        return self._content


class _RustBytesUncachedObjectFileClient(_OrdinaryObjectFileClient):
    def __init__(self, content: bytes) -> None:
        super().__init__(content)
        self.uncached_range_requests: list[tuple[int, int]] = []

    def _read_uncached(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        if byte_range is None:
            return cast(bytes, _RustBytesLike(self._content))
        self.uncached_range_requests.append((byte_range.offset, byte_range.size))
        return cast(
            bytes,
            _RustBytesLike(self._content[byte_range.offset : byte_range.offset + byte_range.size]),
        )


class _MarkerSymlinkObjectFileClient:
    """Object-storage-shaped client that follows marker symlinks for reads."""

    def __init__(
        self,
        objects: dict[str, bytes],
        symlink_targets: dict[str, str],
        cache_manager: Any = None,
    ) -> None:
        self._objects = objects
        self._symlink_targets = symlink_targets
        self._cache_manager = cache_manager
        self.info_calls: list[str] = []
        self.open_calls: list[tuple[str, str]] = []
        self.open_kwargs: list[dict[str, object]] = []
        self.range_requests: list[tuple[str, int, int]] = []
        self.download_calls = 0

    def info(self, path: str) -> ObjectMetadata:
        self.info_calls.append(path)
        if path in self._symlink_targets:
            return ObjectMetadata(
                key=path,
                content_length=0,
                last_modified=datetime.now(timezone.utc),
                symlink_target=self._symlink_targets[path],
            )
        return ObjectMetadata(
            key=path,
            content_length=len(self._objects[path]),
            last_modified=datetime.now(timezone.utc),
        )

    def _resolve_read_path(self, path: str) -> str:
        resolved_path = path
        while resolved_path in self._symlink_targets:
            resolved_path = ObjectMetadata.resolve_symlink_target(
                resolved_path,
                self._symlink_targets[resolved_path],
            )
        return resolved_path

    def read(self, path: str, byte_range: Optional[Range] = None, **_kwargs: object) -> bytes:
        resolved_path = self._resolve_read_path(path)
        content = self._objects[resolved_path]
        if byte_range is None:
            return content
        self.range_requests.append((resolved_path, byte_range.offset, byte_range.size))
        return content[byte_range.offset : byte_range.offset + byte_range.size]

    def _read_uncached(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        return self.read(path, byte_range)

    def download_file(self, path: str, destination: Any) -> None:
        self.download_calls += 1
        content = self._objects[self._resolve_read_path(path)]
        if isinstance(destination, str):
            Path(destination).write_bytes(content)
        else:
            destination.write(content)

    def _is_rust_client_enabled(self) -> bool:
        return False

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> ObjectFile:
        self.open_calls.append((path, mode))
        self.open_kwargs.append(dict(kwargs))
        return ObjectFile(cast(Any, self), path, mode=mode, **kwargs)


class _FailingOpenClient:
    def open(self, path: str, mode: str = "rb", **kwargs) -> RemoteFileReader:
        raise FileNotFoundError(path)


class _FailingNativeDownloadClient(_RangeClient):
    def __init__(self, content: bytes, error: Exception) -> None:
        super().__init__(content)
        self._error = error
        self.download_destinations: list[str] = []

    def download_file(self, _path: str, destination: str) -> None:
        self.download_destinations.append(destination)
        Path(destination).write_bytes(self._content)
        raise self._error


class _FailingReader(_TrackingReader):
    def read(self, size: int = -1) -> bytes:
        if self.read_calls:
            raise OSError("injected source read failure")
        return super().read(size)


class _FailingReadClient(_OpenOnlyClient):
    def open(self, path: str, mode: str = "rb", **kwargs) -> _FailingReader:
        self.reader = _FailingReader(self._content)
        return self.reader


class _PrefixWritingDestination(io.BytesIO):
    def write(self, data: bytes) -> int:
        return super().write(data[: max(1, len(data) // 2)])


class _NoneWritingDestination:
    def __init__(self) -> None:
        self.writes: list[bytes] = []
        self.data = bytearray()

    def read(self, _size: int = -1) -> bytes:
        return b""

    def write(self, data: bytes) -> None:
        payload = bytes(data)
        self.writes.append(payload)
        self.data.extend(payload)

    def tell(self) -> int:
        return 0

    def close(self) -> None:
        return


class _ZeroWritingDestination(_NoneWritingDestination):
    def write(self, data: bytes) -> int:
        self.writes.append(bytes(data))
        return 0


class _ReplicaRangeProvider:
    is_read_only = False

    def __init__(
        self,
        content: bytes,
        *,
        etag: str = "source-r1",
        range_results: Optional[list[bytes]] = None,
        range_failures: Optional[list[Exception]] = None,
    ) -> None:
        self._content = content
        self._etag = etag
        self._range_results = list(range_results) if range_results is not None else []
        self._range_failures = list(range_failures) if range_failures is not None else []
        self.range_requests: list[Range] = []
        self.full_read_calls = 0
        self.metadata_calls = 0

    def get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        assert path == "objects/data.bin"
        self.metadata_calls += 1
        return ObjectMetadata(
            key=path,
            content_length=len(self._content),
            last_modified=datetime.now(timezone.utc),
            etag=self._etag,
        )

    def get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        assert path == "objects/data.bin"
        if byte_range is None:
            self.full_read_calls += 1
            return self._content
        self.range_requests.append(byte_range)
        if self._range_failures:
            raise self._range_failures.pop(0)
        if self._range_results:
            return self._range_results.pop(0)
        return self._content[byte_range.offset : byte_range.offset + byte_range.size]

    def download_file(self, path: str, destination: Any) -> None:
        assert path == "objects/data.bin"
        if isinstance(destination, str):
            Path(destination).write_bytes(self._content)
        else:
            destination.write(self._content)


class _ReplicaRangeClient:
    def __init__(
        self,
        profile: str,
        content: bytes,
        *,
        is_file_results: Optional[list[bool]] = None,
        failures: int = 0,
        range_results: Optional[list[bytes]] = None,
        range_failures: Optional[list[Exception]] = None,
    ) -> None:
        self.profile = profile
        self._content = content
        self._is_file_results = list(is_file_results) if is_file_results is not None else None
        self._failures = failures
        self._range_results = list(range_results) if range_results is not None else []
        self._range_failures = list(range_failures) if range_failures is not None else []
        self.is_file_calls: list[str] = []
        self.range_requests: list[Range] = []
        self.download_calls: list[str] = []

    def is_file(self, path: str) -> bool:
        self.is_file_calls.append(path)
        if self._is_file_results:
            return self._is_file_results.pop(0)
        return True

    def read(self, path: str, byte_range: Optional[Range] = None, **_kwargs: object) -> bytes:
        assert path == "objects/data.bin"
        if byte_range is not None:
            self.range_requests.append(byte_range)
        self._raise_if_configured()
        if self._range_failures:
            raise self._range_failures.pop(0)
        if self._range_results:
            return self._range_results.pop(0)
        if byte_range is None:
            return self._content
        return self._content[byte_range.offset : byte_range.offset + byte_range.size]

    def download_file(self, path: str, destination: Any) -> None:
        assert path == "objects/data.bin"
        self.download_calls.append(path)
        self._raise_if_configured()
        destination.write(self._content)

    def _raise_if_configured(self) -> None:
        if self._failures:
            self._failures -= 1
            raise OSError(f"{self.profile} is unavailable")


def _replica_backed_single_client(
    primary_content: bytes,
    replicas: list[_ReplicaRangeClient],
    *,
    primary: Optional[_ReplicaRangeProvider] = None,
) -> tuple[SingleStorageClient, _ReplicaRangeProvider]:
    primary = primary or _ReplicaRangeProvider(primary_content)
    client = SingleStorageClient.__new__(SingleStorageClient)
    client._storage_provider = cast(Any, primary)
    client._metadata_provider = None
    client._cache_manager = None
    client._retry_config = None
    client._replicas = cast(Any, replicas)
    client._replica_manager = ReplicaManager(client)
    client._stop_event = None
    return client, primary


def _filesystem_for(storage_client: object, resolved_path: str = "objects/data.bin") -> MultiStorageAsyncFileSystem:
    filesystem = MultiStorageAsyncFileSystem(skip_instance_cache=True)
    cast(Any, filesystem).resolve_path_and_storage_client = lambda path: (storage_client, resolved_path)
    return filesystem


def _assert_incremental_reads(reader: _TrackingReader, blocksize: int, total_size: int) -> None:
    assert reader.read_calls
    assert all(requested_size == blocksize for requested_size, _ in reader.read_calls)

    positive_read_sizes = [returned_size for _, returned_size in reader.read_calls if returned_size > 0]
    assert positive_read_sizes[:-1] == [blocksize] * (len(positive_read_sizes) - 1)
    assert positive_read_sizes[-1] == (total_size % blocksize or blocksize)


@pytest.mark.parametrize(
    ("start", "end"),
    [
        pytest.param(None, None, id="omitted-bounds"),
        pytest.param(None, 3, id="omitted-start"),
        pytest.param(7, None, id="omitted-end"),
        pytest.param(2, 7, id="positive-end-exclusive"),
        pytest.param(-100, 100, id="clamped-both-sides"),
        pytest.param(4, 4, id="equal-bounds"),
        pytest.param(7, 3, id="reversed-bounds"),
        pytest.param(100, 200, id="positive-out-of-range"),
        pytest.param(-200, -100, id="negative-out-of-range"),
        pytest.param(-6, -2, id="both-negative"),
        pytest.param(2, -2, id="mixed-positive-negative"),
        pytest.param(-8, 7, id="mixed-negative-positive"),
    ],
)
def test_cat_file_honors_standard_byte_slice_bounds(start: int | None, end: int | None) -> None:
    content = b"0123456789"
    storage_client = _RangeClient(content)
    filesystem = _filesystem_for(storage_client)
    normalized_start, normalized_end, _ = slice(start, end).indices(len(content))
    expected = content[normalized_start:normalized_end]

    result = filesystem.cat_file("objects/data.bin", start=start, end=end)

    if normalized_start >= normalized_end:
        assert storage_client.full_read_calls == 0
        assert storage_client.range_requests == []
    elif start is None and end is None:
        assert storage_client.full_read_calls == 0
        assert storage_client.range_requests == [(0, len(content))]
    else:
        assert storage_client.full_read_calls == 0
        assert storage_client.range_requests == [(normalized_start, normalized_end - normalized_start)]

    assert storage_client.open_calls == [("objects/data.bin", "rb")]
    assert result == expected


@pytest.mark.asyncio
async def test_async_cat_file_honors_negative_end_exclusive_bounds() -> None:
    content = b"0123456789"
    storage_client = _RangeClient(content)
    filesystem = _filesystem_for(storage_client)

    assert await filesystem._cat_file("objects/data.bin", start=-4, end=-1) == b"678"
    assert storage_client.full_read_calls == 0
    assert storage_client.range_requests == [(6, 3)]
    assert storage_client.open_calls == [("objects/data.bin", "rb")]


def test_cat_file_without_bounds_streams_via_open_and_forwards_kwargs() -> None:
    content = b"0123456789"
    storage_client = _RangeClient(content)
    filesystem = _filesystem_for(storage_client)

    assert filesystem.cat_file("objects/data.bin", prefetch_file=False) == content

    assert storage_client.full_read_calls == 0
    assert storage_client.open_calls == [("objects/data.bin", "rb")]
    assert storage_client.open_kwargs == [{"prefetch_file": False}]


def test_bounded_cat_file_uses_the_length_of_a_seekable_reader_without_a_size_property() -> None:
    content = b"0123456789"
    storage_client = _OpenOnlyClient(content)
    filesystem = _filesystem_for(storage_client)

    assert filesystem.cat_file("objects/data.bin", start=2, end=-2) == b"234567"
    assert storage_client.reader is not None
    assert storage_client.reader.read_calls == [(6, 6)]


def test_get_file_streams_into_file_like_destination_and_reports_progress() -> None:
    content = b"0123456789"
    storage_client = _OpenOnlyClient(content)
    filesystem = _filesystem_for(storage_client)
    filesystem.blocksize = 3
    destination = io.BytesIO()
    callback = Callback()

    filesystem.get_file("objects/data.bin", destination, callback=callback)

    assert destination.getvalue() == content
    assert not destination.closed
    assert callback.size == len(content)
    assert callback.value == len(content)
    assert storage_client.reader is not None
    _assert_incremental_reads(storage_client.reader, filesystem.blocksize, len(content))


@pytest.mark.asyncio
async def test_async_get_file_streams_into_file_like_destination_and_reports_progress() -> None:
    content = b"0123456789"
    storage_client = _OpenOnlyClient(content)
    filesystem = _filesystem_for(storage_client)
    filesystem.blocksize = 3
    destination = io.BytesIO()
    callback = Callback()

    await filesystem._get_file("objects/data.bin", destination, callback=callback)

    assert destination.getvalue() == content
    assert not destination.closed
    assert callback.size == len(content)
    assert callback.value == len(content)
    assert storage_client.reader is not None
    _assert_incremental_reads(storage_client.reader, filesystem.blocksize, len(content))


def test_get_file_retries_short_file_like_writes_until_the_full_chunk_is_stored() -> None:
    content = b"0123456789"
    storage_client = _OpenOnlyClient(content)
    filesystem = _filesystem_for(storage_client)
    filesystem.blocksize = 4
    destination = _PrefixWritingDestination()
    callback = Callback()

    filesystem.get_file("objects/data.bin", destination, callback=callback)

    assert destination.getvalue() == content
    assert callback.value == len(content)


def test_get_file_accepts_a_file_like_destination_that_returns_none_after_consuming_a_chunk() -> None:
    content = b"0123456789"
    storage_client = _OpenOnlyClient(content)
    filesystem = _filesystem_for(storage_client)
    destination = _NoneWritingDestination()
    callback = Callback()

    filesystem.get_file("objects/data.bin", cast(Any, destination), callback=callback)

    assert bytes(destination.data) == content
    assert destination.writes == [content]
    assert callback.value == len(content)


def test_get_file_rejects_a_file_like_destination_that_returns_zero_write_progress() -> None:
    content = b"0123456789"
    storage_client = _OpenOnlyClient(content)
    filesystem = _filesystem_for(storage_client)
    destination = _ZeroWritingDestination()

    with pytest.raises(OSError, match="no progress"):
        filesystem.get_file("objects/data.bin", cast(Any, destination))

    assert destination.writes == [content]


def test_get_file_streams_an_ordinary_object_file_provider_into_a_file_like_destination() -> None:
    """Caller-owned file-like destinations retain uncached, windowed reads."""
    content = b"0123456789"
    storage_client = _OrdinaryObjectFileClient(content)
    filesystem = _filesystem_for(storage_client)
    filesystem.blocksize = 3
    destination = io.BytesIO()

    filesystem.get_file("objects/data.bin", destination, prefetch_file=True, disable_read_cache=False)

    assert destination.getvalue() == content
    assert storage_client.open_kwargs == [{"prefetch_file": False, "disable_read_cache": True}]
    assert storage_client.download_calls == 0
    assert storage_client.range_requests


def test_get_file_path_uses_native_download_and_atomically_replaces_the_destination(tmp_path: Path) -> None:
    """Path downloads preserve optimized native transfers while publishing only completed output."""
    content = b"0123456789"
    storage_client = _OrdinaryObjectFileClient(content)
    filesystem = _filesystem_for(storage_client)
    target = tmp_path / "download.bin"
    target.write_bytes(b"old-target")
    callback = Callback()

    filesystem.get_file("objects/data.bin", target, callback=callback)

    assert target.read_bytes() == content
    assert storage_client.download_calls == 1
    assert storage_client.open_calls == []
    assert len(storage_client.download_destinations) == 1
    temporary_path = Path(storage_client.download_destinations[0])
    assert temporary_path.parent == tmp_path
    assert temporary_path != target
    assert not temporary_path.exists()
    assert storage_client.info_calls == []
    assert callback.size == len(content)
    assert callback.value == len(content)


def test_get_file_path_uses_native_download_without_an_info_preflight_when_info_is_retryable(tmp_path: Path) -> None:
    """Native downloads remain available when an unrelated metadata preflight would retry or fail."""
    content = b"0123456789"
    storage_client = _InfoFailingNativeDownloadClient(content)
    filesystem = _filesystem_for(storage_client)
    target = tmp_path / "download.bin"
    callback = Callback()

    filesystem.get_file("objects/data.bin", target, callback=callback)

    assert target.read_bytes() == content
    assert storage_client.download_calls == 1
    assert storage_client.info_calls == []
    assert callback.size == len(content)
    assert callback.value == len(content)


def test_get_file_streams_to_a_caller_owned_outfile() -> None:
    """An outfile supplied by fsspec remains owned by the caller and is never replaced."""
    content = b"0123456789"
    storage_client = _OrdinaryObjectFileClient(content)
    filesystem = _filesystem_for(storage_client)
    destination = io.BytesIO()

    filesystem.get_file("objects/data.bin", "ignored-local-path", outfile=destination)

    assert destination.getvalue() == content
    assert storage_client.download_calls == 0
    assert storage_client.open_kwargs == [{"prefetch_file": False, "disable_read_cache": True}]


@pytest.mark.parametrize(
    ("start", "end"),
    [
        pytest.param(2, 7, id="positive-end-exclusive"),
        pytest.param(-6, -2, id="negative-end-exclusive"),
    ],
)
def test_bounded_cat_file_forces_windowed_streaming_for_an_ordinary_object_file_provider(start: int, end: int) -> None:
    content = b"0123456789"
    storage_client = _OrdinaryObjectFileClient(content)
    filesystem = _filesystem_for(storage_client)
    normalized_start, normalized_end, _ = slice(start, end).indices(len(content))

    assert filesystem.cat_file("objects/data.bin", start=start, end=end, prefetch_file=True) == content[start:end]
    assert storage_client.open_kwargs == [{"prefetch_file": False, "buffering": 0}]
    assert storage_client.download_calls == 0
    assert storage_client.range_requests == [(normalized_start, normalized_end - normalized_start)]


def test_bounded_cat_file_normalizes_uncached_rust_bytes_like_range_results() -> None:
    content = b"0123456789"
    storage_client = _RustBytesUncachedObjectFileClient(content)
    filesystem = _filesystem_for(storage_client)

    result = filesystem.cat_file(
        "objects/data.bin",
        start=2,
        end=7,
        disable_read_cache=True,
    )

    assert type(result) is bytes
    assert result == b"23456"
    assert storage_client.open_kwargs == [{"disable_read_cache": True, "prefetch_file": False, "buffering": 0}]
    assert storage_client.uncached_range_requests == [(2, 5)]


def test_bounded_cat_file_reads_the_exact_range_from_a_replica() -> None:
    primary_content = b"primary-0123456789"
    replica_content = b"replica-0123456789"
    replica = _ReplicaRangeClient("replica-1", replica_content)
    storage_client, primary = _replica_backed_single_client(primary_content, [replica])
    filesystem = _filesystem_for(storage_client)

    assert filesystem.cat_file("objects/data.bin", start=3, end=10) == replica_content[3:10]
    assert replica.range_requests == [Range(offset=3, size=7)]
    assert replica.download_calls == []
    assert primary.range_requests == []
    assert primary.full_read_calls == 0


def test_replica_backed_stream_pins_one_range_source_across_seek_and_read() -> None:
    primary_content = b"primary-0123456789"
    replica_content = b"replica-0123456789"
    replica = _ReplicaRangeClient("replica-1", replica_content)
    storage_client, primary = _replica_backed_single_client(primary_content, [replica])

    with storage_client.open("objects/data.bin", "rb", prefetch_file=False, buffering=0) as source:
        source.seek(2)
        assert source.read(4) == replica_content[2:6]
        assert source.read(3) == replica_content[6:9]

    assert replica.range_requests == [Range(offset=2, size=4), Range(offset=6, size=3)]
    assert replica.is_file_calls == ["objects/data.bin"]
    assert primary.range_requests == []


def test_file_like_get_file_keeps_one_replica_selected_for_every_range() -> None:
    primary_content = b"primary--"
    first_replica_content = b"first----"
    second_replica_content = b"second---"
    first_replica = _ReplicaRangeClient("replica-1", first_replica_content, is_file_results=[True, False, False])
    second_replica = _ReplicaRangeClient("replica-2", second_replica_content)
    storage_client, primary = _replica_backed_single_client(primary_content, [first_replica, second_replica])
    filesystem = _filesystem_for(storage_client)
    filesystem.blocksize = 3
    destination = io.BytesIO()

    filesystem.get_file("objects/data.bin", destination, buffering=0)

    assert destination.getvalue() == first_replica_content
    assert first_replica.range_requests == [Range(offset=0, size=3), Range(offset=3, size=3), Range(offset=6, size=3)]
    assert first_replica.is_file_calls == ["objects/data.bin"]
    assert second_replica.range_requests == []
    assert primary.range_requests == []


def test_bounded_cat_file_fails_over_to_the_next_replica_without_reading_primary() -> None:
    primary_content = b"primary-0123456789"
    first_replica = _ReplicaRangeClient("replica-1", b"broken--0123456789", failures=1)
    second_replica_content = b"replica-0123456789"
    second_replica = _ReplicaRangeClient("replica-2", second_replica_content)
    storage_client, primary = _replica_backed_single_client(primary_content, [first_replica, second_replica])
    filesystem = _filesystem_for(storage_client)

    assert filesystem.cat_file("objects/data.bin", start=4, end=9) == second_replica_content[4:9]
    assert first_replica.range_requests == [Range(offset=4, size=5)]
    assert second_replica.range_requests == [Range(offset=4, size=5)]
    assert primary.range_requests == []


@pytest.mark.parametrize("malformed", [b"abc", b"abcde"], ids=["short", "overlong"])
def test_replica_range_reader_fails_over_from_a_malformed_initial_replica(malformed: bytes) -> None:
    first_replica = _ReplicaRangeClient("replica-1", b"first-data", range_results=[malformed])
    second_replica_content = b"second-data"
    second_replica = _ReplicaRangeClient("replica-2", second_replica_content)
    storage_client, primary = _replica_backed_single_client(b"primary-data", [first_replica, second_replica])
    filesystem = _filesystem_for(storage_client)

    assert filesystem.cat_file("objects/data.bin", start=1, end=5) == second_replica_content[1:5]
    assert first_replica.range_requests == [Range(offset=1, size=4)]
    assert second_replica.range_requests == [Range(offset=1, size=4)]
    assert primary.range_requests == []


@pytest.mark.parametrize("malformed", [b"abc", b"abcde"], ids=["short", "overlong"])
def test_replica_range_reader_rejects_a_malformed_primary_response(malformed: bytes) -> None:
    primary = _ReplicaRangeProvider(b"primary-data", range_results=[malformed])
    unavailable_replica = _ReplicaRangeClient("replica-1", b"replica-data", is_file_results=[False])
    storage_client, _ = _replica_backed_single_client(
        b"primary-data",
        [unavailable_replica],
        primary=primary,
    )
    reader = storage_client._replica_range_reader("objects/data.bin")

    with pytest.raises(IOError, match="expected 4 bytes"):
        reader(1, 4)

    assert unavailable_replica.range_requests == []
    assert primary.range_requests == [Range(offset=1, size=4)]


def test_replica_range_reader_does_not_fall_back_after_its_pinned_source_becomes_malformed() -> None:
    first_replica = _ReplicaRangeClient("replica-1", b"first-data", range_results=[b"irst", b"x"])
    second_replica = _ReplicaRangeClient("replica-2", b"second-data")
    storage_client, primary = _replica_backed_single_client(b"primary-data", [first_replica, second_replica])
    reader = storage_client._replica_range_reader("objects/data.bin")

    assert reader(1, 4) == b"irst"
    with pytest.raises(IOError, match="expected 4 bytes"):
        reader(5, 4)

    assert first_replica.range_requests == [Range(offset=1, size=4), Range(offset=5, size=4)]
    assert second_replica.range_requests == []
    assert primary.range_requests == []


def test_replica_range_reader_rejects_a_malformed_pinned_primary_response() -> None:
    primary = _ReplicaRangeProvider(b"primary-data", range_results=[b"rima", b"x"])
    unavailable_replica = _ReplicaRangeClient("replica-1", b"replica-data", is_file_results=[False])
    storage_client, primary = _replica_backed_single_client(
        b"primary-data",
        [unavailable_replica],
        primary=primary,
    )
    reader = storage_client._replica_range_reader("objects/data.bin")

    assert reader(1, 4) == b"rima"
    with pytest.raises(IOError, match="expected 4 bytes"):
        reader(5, 4)

    assert unavailable_replica.range_requests == []
    assert primary.range_requests == [Range(offset=1, size=4), Range(offset=5, size=4)]


def test_replica_direct_range_read_clips_at_end_of_file() -> None:
    replica_content = b"replica-data"
    replica = _ReplicaRangeClient("replica-1", replica_content)
    storage_client, primary = _replica_backed_single_client(b"primary-data", [replica])

    assert storage_client.read("objects/data.bin", byte_range=Range(offset=10, size=8)) == replica_content[10:]
    assert replica.range_requests == [Range(offset=10, size=8), Range(offset=10, size=2)]
    assert primary.range_requests == [Range(offset=10, size=8)]


@pytest.mark.parametrize(
    ("check_source_version", "cache_checks_source_version", "expected_metadata_calls"),
    [
        pytest.param(SourceVersionCheckMode.DISABLE, True, 0, id="source-version-disabled"),
        pytest.param(SourceVersionCheckMode.ENABLE, True, 1, id="source-version-enabled"),
    ],
)
def test_replica_range_read_uses_a_valid_full_cache_entry_before_remote_sources(
    tmp_path: Path,
    check_source_version: SourceVersionCheckMode,
    cache_checks_source_version: bool,
    expected_metadata_calls: int,
) -> None:
    content = b"cached-replica-content"
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(
            size="10M",
            cache_line_size="1M",
            check_source_version=cache_checks_source_version,
            location=str(tmp_path),
        ),
    )
    cache_manager.set("objects/data.bin", content, source_version="source-r1")
    primary = _ReplicaRangeProvider(content, range_failures=[OSError("primary unavailable")])
    replica = _ReplicaRangeClient(
        "replica-1",
        content,
        range_failures=[OSError("replica unavailable")],
    )
    storage_client, primary = _replica_backed_single_client(content, [replica], primary=primary)
    storage_client._cache_manager = cache_manager

    assert (
        storage_client.read(
            "objects/data.bin",
            byte_range=Range(offset=3, size=7),
            check_source_version=check_source_version,
        )
        == content[3:10]
    )

    assert primary.metadata_calls == expected_metadata_calls
    assert primary.range_requests == []
    assert replica.range_requests == []


@pytest.mark.parametrize(
    "check_source_version",
    [SourceVersionCheckMode.DISABLE, SourceVersionCheckMode.ENABLE],
    ids=["source-version-disabled", "source-version-enabled"],
)
def test_replica_range_cache_populates_on_a_miss_and_serves_a_nonprefetched_stream_when_sources_fail(
    tmp_path: Path,
    check_source_version: SourceVersionCheckMode,
) -> None:
    content = bytes(range(256)) * 8192
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(
            size="10M",
            cache_line_size="1M",
            check_source_version=check_source_version == SourceVersionCheckMode.ENABLE,
            location=str(tmp_path),
        ),
    )
    replica = _ReplicaRangeClient("replica-1", content)
    storage_client, primary = _replica_backed_single_client(content, [replica])
    storage_client._cache_manager = cache_manager
    requested_range = Range(offset=1024 * 1024, size=8)

    assert (
        storage_client.read(
            "objects/data.bin",
            byte_range=requested_range,
            check_source_version=check_source_version,
        )
        == content[1024 * 1024 : 1024 * 1024 + 8]
    )

    replica._range_failures.append(OSError("replica unavailable"))
    primary._range_failures.append(OSError("primary unavailable"))
    with storage_client.open(
        "objects/data.bin",
        "rb",
        prefetch_file=False,
        buffering=0,
        check_source_version=check_source_version,
    ) as source:
        source.seek(1024 * 1024)
        assert source.read(8) == content[1024 * 1024 : 1024 * 1024 + 8]

    assert replica.range_requests == [Range(offset=1024 * 1024, size=1024 * 1024)]
    assert primary.range_requests == []


def test_replica_stream_reuses_a_stale_range_cache_entry_when_inherit_disables_source_checks(tmp_path: Path) -> None:
    content = bytes(range(256)) * 8192
    cache_line_size = 1024 * 1024
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(
            size="10M",
            cache_line_size="1M",
            check_source_version=False,
            location=str(tmp_path),
        ),
    )
    cache_path = cache_manager._get_cache_file_path("objects/data.bin")
    cache_identity = cache_manager._cache_identity("objects/data.bin")
    cache_manager._write_chunk_to_cache(
        cache_manager._get_chunk_path(cache_path, 1, cache_identity),
        content[cache_line_size : 2 * cache_line_size],
        "stale-source-r0",
        1,
        cache_line_size,
        len(content),
        cache_identity=cache_identity,
    )
    replica = _ReplicaRangeClient(
        "replica-1",
        content,
        range_failures=[OSError("replica unavailable")],
    )
    primary = _ReplicaRangeProvider(
        content,
        range_failures=[OSError("primary unavailable")],
    )
    storage_client, primary = _replica_backed_single_client(content, [replica], primary=primary)
    storage_client._cache_manager = cache_manager

    with storage_client.open(
        "objects/data.bin",
        "rb",
        prefetch_file=False,
        buffering=0,
        check_source_version=SourceVersionCheckMode.INHERIT,
    ) as source:
        source.seek(cache_line_size)
        assert source.read(8) == content[cache_line_size : cache_line_size + 8]

    assert replica.range_requests == []
    assert primary.range_requests == []


def test_replica_stream_retries_its_pinned_primary_range_source(monkeypatch: pytest.MonkeyPatch) -> None:
    primary_content = b"primary-0123456789"
    primary = _ReplicaRangeProvider(
        primary_content,
        range_failures=[RetryableError("primary is temporarily unavailable")],
    )
    unavailable_replica = _ReplicaRangeClient("replica-1", b"replica-0123456789", failures=1)
    storage_client, primary = _replica_backed_single_client(
        primary_content,
        [unavailable_replica],
        primary=primary,
    )
    storage_client._retry_config = RetryConfig(attempts=2, delay=0, backoff_multiplier=1)
    monkeypatch.setattr("multistorageclient.retry.sleep_before_retry", lambda *_args: None)

    with storage_client.open("objects/data.bin", "rb", prefetch_file=False, buffering=0) as source:
        assert source.read(7) == primary_content[:7]

    assert unavailable_replica.range_requests == [Range(offset=0, size=7)]
    assert primary.range_requests == [Range(offset=0, size=7), Range(offset=0, size=7)]


def test_replica_direct_range_read_uses_one_retry_boundary_and_one_selection_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = _ReplicaRangeProvider(
        b"primary-0123456789",
        range_failures=[RetryableError("primary unavailable")] * 4,
    )
    unavailable_replica = _ReplicaRangeClient(
        "replica-1",
        b"replica-0123456789",
        is_file_results=[False, False],
    )
    storage_client, primary = _replica_backed_single_client(
        b"primary-0123456789",
        [unavailable_replica],
        primary=primary,
    )
    storage_client._retry_config = RetryConfig(attempts=2, delay=0, backoff_multiplier=1)
    monkeypatch.setattr("multistorageclient.retry.sleep_before_retry", lambda *_args: None)

    with pytest.raises(RetryableError, match="primary unavailable"):
        storage_client.read("objects/data.bin", byte_range=Range(offset=1, size=4))

    assert primary.range_requests == [Range(offset=1, size=4), Range(offset=1, size=4)]
    assert unavailable_replica.is_file_calls == ["objects/data.bin"]
    assert unavailable_replica.range_requests == []


def test_replica_backed_stream_materialization_reuses_the_pinned_range_source() -> None:
    primary_content = b"primary-abcdef"
    replica_content = b"replica-abcdef"
    replica = _ReplicaRangeClient("replica-1", replica_content, is_file_results=[True, False])
    storage_client, primary = _replica_backed_single_client(primary_content, [replica])

    with storage_client.open("objects/data.bin", "rb", prefetch_file=False, buffering=0) as source:
        assert source.read(4) == replica_content[:4]
        materialized_path = source.resolve_filesystem_path()

        assert Path(materialized_path).read_bytes() == replica_content
        assert source.tell() == 4
        assert source.read() == replica_content[4:]
        assert source.resolve_filesystem_path() == materialized_path

    assert replica.is_file_calls == ["objects/data.bin"]
    assert primary.range_requests == []


def test_get_file_materializes_a_relative_object_storage_symlink_marker(tmp_path: Path) -> None:
    link_path = "objects/links/link.bin"
    target_path = "objects/payload.bin"
    content = b"symlink target contents"
    storage_client = _MarkerSymlinkObjectFileClient(
        {target_path: content},
        {link_path: "../payload.bin"},
    )
    filesystem = _filesystem_for(storage_client, link_path)
    filesystem.blocksize = 5
    destination = tmp_path / "download.bin"

    filesystem.get_file(link_path, destination)

    assert destination.read_bytes() == content
    assert storage_client.open_kwargs == []
    assert storage_client.download_calls == 1
    assert storage_client.range_requests == []


def test_get_file_ignores_a_cached_marker_payload_when_streaming_a_symlink(tmp_path: Path) -> None:
    link_path = "objects/links/link.bin"
    target_path = "objects/payload.bin"
    content = b"symlink target contents"
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=False, location=str(tmp_path)),
    )
    cache_manager.set(link_path, b"")
    storage_client = _MarkerSymlinkObjectFileClient(
        {target_path: content},
        {link_path: "../payload.bin"},
        cache_manager,
    )
    filesystem = _filesystem_for(storage_client, link_path)
    destination = io.BytesIO()

    filesystem.get_file(link_path, destination)

    assert destination.getvalue() == content
    assert storage_client.range_requests == [(target_path, 0, len(content))]


def test_bounded_cat_file_reads_an_absolute_object_storage_symlink_marker_by_range() -> None:
    link_path = "objects/links/link.bin"
    target_path = "/objects/payload.bin"
    content = b"0123456789"
    storage_client = _MarkerSymlinkObjectFileClient(
        {target_path: content},
        {link_path: target_path},
    )
    filesystem = _filesystem_for(storage_client, link_path)

    assert filesystem.cat_file(link_path, start=-6, end=-2, prefetch_file=True) == b"4567"
    assert storage_client.open_kwargs == [{"prefetch_file": False, "buffering": 0}]
    assert storage_client.download_calls == 0
    assert storage_client.range_requests == [(target_path, 4, 4)]


def test_object_file_streaming_resolves_marker_symlink_chains_and_rejects_cycles() -> None:
    target_path = "objects/payload.bin"
    content = b"symlink target contents"
    chained_client = _MarkerSymlinkObjectFileClient(
        {target_path: content},
        {
            "objects/links/first.bin": "second.bin",
            "objects/links/second.bin": "../payload.bin",
        },
    )
    chained_filesystem = _filesystem_for(chained_client, "objects/links/first.bin")

    assert chained_filesystem.cat_file("objects/links/first.bin", start=1, end=8) == content[1:8]
    assert chained_client.range_requests == [(target_path, 1, 7)]

    cyclic_client = _MarkerSymlinkObjectFileClient(
        {},
        {
            "objects/links/first.bin": "second.bin",
            "objects/links/second.bin": "first.bin",
        },
    )
    cyclic_filesystem = _filesystem_for(cyclic_client, "objects/links/first.bin")

    with pytest.raises(ValueError, match="Symlink cycle detected"):
        cyclic_filesystem.cat_file("objects/links/first.bin", start=0, end=1)

    deep_client = _MarkerSymlinkObjectFileClient(
        {},
        {f"objects/links/{index}.bin": f"{index + 1}.bin" for index in range(MAX_SYMLINK_DEPTH + 1)},
    )
    deep_filesystem = _filesystem_for(deep_client, "objects/links/0.bin")

    with pytest.raises(ValueError, match="Too many levels of symlinks"):
        deep_filesystem.cat_file("objects/links/0.bin", start=0, end=1)


def test_get_file_path_does_not_truncate_an_existing_target_when_native_download_fails(tmp_path: Path) -> None:
    target = tmp_path / "target.bin"
    target.write_bytes(b"old-target")
    filesystem = _filesystem_for(_FailingNativeDownloadClient(b"partial-target", FileNotFoundError("objects/data.bin")))

    with pytest.raises(FileNotFoundError):
        filesystem.get_file("objects/data.bin", target)

    assert target.read_bytes() == b"old-target"
    assert set(tmp_path.iterdir()) == {target}


def test_get_file_file_like_destination_surfaces_streaming_failure_without_a_temp_path(tmp_path: Path) -> None:
    filesystem = _filesystem_for(_FailingReadClient(b"new-target"))
    filesystem.blocksize = 3
    destination = io.BytesIO()

    with pytest.raises(OSError, match="injected source read failure"):
        filesystem.get_file("objects/data.bin", destination)

    assert destination.getvalue() == b"new"
    assert list(tmp_path.iterdir()) == []
