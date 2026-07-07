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
from multistorageclient.contrib.async_fs import MultiStorageAsyncFileSystem
from multistorageclient.file import ObjectFile, RemoteFileReader
from multistorageclient.types import MAX_SYMLINK_DEPTH, ObjectMetadata, Range, RetryableError


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

    def read(self, _size: int = -1) -> bytes:
        return b""

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    def tell(self) -> int:
        return 0

    def close(self) -> None:
        return


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


def test_get_file_rejects_a_file_like_destination_that_reports_no_write_progress() -> None:
    content = b"0123456789"
    storage_client = _OpenOnlyClient(content)
    filesystem = _filesystem_for(storage_client)
    destination = _NoneWritingDestination()

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
