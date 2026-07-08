# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import contextlib
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, cast

import pytest

import multistorageclient.cache as cache_module
from multistorageclient.cache import CacheManager
from multistorageclient.caching.cache_config import CacheConfig
from multistorageclient.file import ObjectFile, RemoteFileReader
from multistorageclient.types import ObjectMetadata, Range, SourceVersionCheckMode


class _StreamingCacheManager:
    def prefetch_file(self) -> bool:
        return False

    def check_source_version(self) -> bool:
        return False

    def contains(self, *args, **kwargs) -> bool:
        return False


class _PrefetchingCacheManager(_StreamingCacheManager):
    def __init__(self, content: bytes, cache_path: Path) -> None:
        self._content = content
        self._cache_path = cache_path

    def get_max_cache_size(self) -> int:
        return len(self._content) + 1

    def acquire_lock(self, path: str):
        return contextlib.nullcontext()

    def generate_temp_file_path(self) -> str:
        return str(self._cache_path)

    def set(self, *args, **kwargs) -> None:
        return None

    def open(self, *args, **kwargs) -> io.StringIO:
        return io.StringIO(self._content.decode("utf-8"))


class _ValidatedOpenMissCacheManager(_StreamingCacheManager):
    """Test double that loses a previously reported cache hit before it can be opened."""

    def __init__(self, cache_path: Path) -> None:
        self._cache_path = cache_path
        self._cached_content: Optional[bytes] = None
        self.contains_calls = 0
        self.open_calls = 0

    def prefetch_file(self) -> bool:
        return True

    def get_max_cache_size(self) -> int:
        return 1024

    def contains(self, *args, **kwargs) -> bool:
        self.contains_calls += 1
        return self.contains_calls > 1

    def acquire_lock(self, path: str):
        return contextlib.nullcontext()

    def generate_temp_file_path(self) -> str:
        return str(self._cache_path)

    def set(self, _key: str, source: str, _source_version: Optional[str] = None) -> None:
        self._cached_content = Path(source).read_bytes()

    def open(self, *args, **kwargs) -> Optional[io.BytesIO]:
        self.open_calls += 1
        if self._cached_content is None:
            return None
        return io.BytesIO(self._cached_content)


class _PostSetOpenMissCacheManager(_StreamingCacheManager):
    """Test double that loses a freshly published entry before ObjectFile can reopen it."""

    def __init__(self, cache_path: Path) -> None:
        self._cache_path = cache_path
        self.open_calls = 0
        self.set_calls = 0

    def prefetch_file(self) -> bool:
        return True

    def get_max_cache_size(self) -> int:
        return 1024

    def acquire_lock(self, path: str):
        return contextlib.nullcontext()

    def generate_temp_file_path(self) -> str:
        return str(self._cache_path)

    def set(self, *_args, **_kwargs) -> None:
        self.set_calls += 1

    def open(self, *args, **kwargs) -> None:
        self.open_calls += 1
        return None


class _RangeStorageClient:
    def __init__(self, content: bytes, cache_manager: Optional[Any] = None) -> None:
        self._content = content
        self._cache_manager = cache_manager
        self.range_requests: list[tuple[int, int]] = []
        self.uncached_range_requests: list[tuple[int, int]] = []
        self.download_calls = 0

    def info(self, path: str) -> ObjectMetadata:
        return ObjectMetadata(
            key=path,
            content_length=len(self._content),
            last_modified=datetime.now(timezone.utc),
        )

    def read(
        self,
        path: str,
        byte_range: Optional[Range] = None,
        check_source_version: SourceVersionCheckMode = SourceVersionCheckMode.INHERIT,
    ) -> bytes:
        if byte_range is None:
            return self._content

        self.range_requests.append((byte_range.offset, byte_range.size))
        return self._content[byte_range.offset : byte_range.offset + byte_range.size]

    def download_file(self, path: str, destination: str | io.IOBase) -> None:
        self.download_calls += 1
        if isinstance(destination, str):
            Path(destination).write_bytes(self._content)
        else:
            destination.write(self._content)

    def _read_uncached(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        if byte_range is None:
            return self._content
        self.uncached_range_requests.append((byte_range.offset, byte_range.size))
        return self._content[byte_range.offset : byte_range.offset + byte_range.size]

    def _is_rust_client_enabled(self) -> bool:
        return False


class _RustBytesLike:
    """Minimal stand-in for the bytes wrapper returned by the PyO3 client."""

    def __init__(self, content: bytes) -> None:
        self._content = content

    def to_bytes(self) -> bytes:
        return self._content


class _RustBytesRangeStorageClient(_RangeStorageClient):
    def _read_uncached(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        return cast(bytes, _RustBytesLike(super()._read_uncached(path, byte_range)))


class _VersionedRangeStorageClient(_RangeStorageClient):
    def __init__(self, content: bytes, cache_manager: CacheManager, etag: Optional[str]) -> None:
        super().__init__(content, cache_manager=cache_manager)
        self._etag = etag

    def info(self, path: str) -> ObjectMetadata:
        metadata = super().info(path)
        metadata.etag = self._etag
        return metadata


class _FailingDownloadStorageClient(_RangeStorageClient):
    def download_file(self, path: str, destination: str | io.IOBase) -> None:
        raise OSError(f"injected prefetch failure for {path}")


def _object_file(storage_client: object, *args: Any, **kwargs: Any) -> ObjectFile:
    return ObjectFile(cast(Any, storage_client), *args, **kwargs)


def test_prefetch_download_retries_a_validated_cache_open_miss_as_a_cache_miss(tmp_path: Path) -> None:
    """A cache hit lost between validation and open falls through to a locked download."""
    content = b"recovered after replacement"
    cache_manager = _ValidatedOpenMissCacheManager(tmp_path / "downloaded.bin")
    storage_client = _RangeStorageClient(content, cache_manager=cache_manager)
    stream = _object_file(storage_client, "virtual/data.bin", mode="rb", prefetch_file=True)

    assert stream.read() == content
    assert storage_client.download_calls == 1
    assert cache_manager.contains_calls == 1
    assert cache_manager.open_calls == 3
    stream.close()


def test_prefetch_falls_back_to_an_uncached_stream_after_a_post_set_cache_miss(tmp_path: Path) -> None:
    """Eviction immediately after set() cannot turn a successful prefetch into FileNotFoundError."""
    content = b"recovered after post-set eviction"
    cache_manager = _PostSetOpenMissCacheManager(tmp_path / "downloaded.bin")
    storage_client = _RangeStorageClient(content, cache_manager=cache_manager)
    stream = _object_file(storage_client, "virtual/data.bin", mode="rb", prefetch_file=True)

    assert stream.read() == content
    assert storage_client.download_calls == 1
    assert cache_manager.set_calls == 1
    assert cache_manager.open_calls == 3
    assert storage_client.uncached_range_requests
    assert storage_client.range_requests == []
    stream.close()


def test_prefetch_cleans_a_rejected_cache_temp_path_before_uncached_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_config = CacheConfig(size="10M", cache_line_size="1M", check_source_version=False, location=str(tmp_path))
    owner = CacheManager(profile="owner", cache_config=cache_config)
    alias = CacheManager(profile="alias", cache_config=cache_config)
    owner_key = "virtual/data.bin"
    alias_key = "virtual/alias.bin"
    owner.set(owner_key, b"owner data")
    owner_path = owner._get_cache_file_path(owner_key)
    monkeypatch.setattr(alias, "_get_cache_file_path", lambda _key: owner_path)
    storage_client = _RangeStorageClient(b"alias data", cache_manager=alias)
    stream = _object_file(storage_client, alias_key, mode="rb", prefetch_file=True)

    assert stream.read() == b"alias data"
    assert owner.read(owner_key, check_source_version=SourceVersionCheckMode.DISABLE) == b"owner data"
    assert storage_client.uncached_range_requests
    assert list(Path(alias._cache_temp_dir).iterdir()) == []
    stream.close()


def test_prefetch_reuses_portable_full_cache_metadata_without_xattrs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A prefetched full object is reusable when xattrs are unavailable on the cache filesystem."""
    content = b"portable prefetch contents"
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmp_path)),
    )
    storage_client = _VersionedRangeStorageClient(content, cache_manager, etag="revision-1")

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)

    first = _object_file(
        storage_client,
        "virtual/data.bin",
        mode="rb",
        prefetch_file=True,
        check_source_version=SourceVersionCheckMode.ENABLE,
    )
    assert first.read() == content
    first.close()

    second = _object_file(
        storage_client,
        "virtual/data.bin",
        mode="rb",
        prefetch_file=True,
        check_source_version=SourceVersionCheckMode.ENABLE,
    )
    assert second.read() == content
    second.close()

    assert storage_client.download_calls == 1
    assert os.path.exists(
        cache_manager._get_chunk_metadata_path(cache_manager._get_cache_file_path("virtual/data.bin"))
    )


@pytest.mark.parametrize("etag", [None, ""], ids=["none", "empty"])
def test_prefetch_required_version_without_a_revision_uses_one_uncached_stream_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, etag: Optional[str]
) -> None:
    """Required cache validation cannot turn a missing provider ETag into a redundant full download."""
    content = b"uncached because the required revision is unavailable"
    cache_manager = CacheManager(
        profile="test",
        cache_config=CacheConfig(size="10M", cache_line_size="1M", check_source_version=True, location=str(tmp_path)),
    )
    storage_client = _VersionedRangeStorageClient(content, cache_manager, etag=etag)

    def raise_xattr_error(*_args, **_kwargs):
        raise OSError("xattr unsupported")

    monkeypatch.setattr(cache_module.xattr, "setxattr", raise_xattr_error)
    monkeypatch.setattr(cache_module.xattr, "getxattr", raise_xattr_error)
    cache_manager.set("virtual/data.bin", b"stale", source_version="stale-revision")

    stream = _object_file(
        storage_client,
        "virtual/data.bin",
        mode="rb",
        prefetch_file=True,
        check_source_version=SourceVersionCheckMode.ENABLE,
    )

    assert stream.read() == content
    assert storage_client.download_calls == 0
    assert storage_client.uncached_range_requests == [(0, len(content))]
    stream.close()


def test_remote_file_reader_uses_injected_range_reader_and_raw_io_lifecycle() -> None:
    content = b"abcdef"
    range_requests: list[tuple[int, int]] = []

    def read_range(offset: int, size: int) -> bytes:
        range_requests.append((offset, size))
        return content[offset : offset + size]

    reader = RemoteFileReader("virtual.bin", len(content), read_range=read_range)

    assert isinstance(reader, io.RawIOBase)
    assert reader.readable()
    assert reader.seekable()
    assert not reader.writable()
    assert reader.read(2) == b"ab"

    destination = bytearray(3)
    assert reader.readinto(destination) == 3
    assert bytes(destination) == b"cde"

    assert reader.seek(-1, os.SEEK_END) == 5
    assert reader.read() == b"f"
    assert range_requests == [(0, 2), (2, 3), (5, 1)]

    reader.close()
    assert reader.closed
    with pytest.raises(ValueError):
        reader.read(1)


def test_remote_file_reader_normalizes_rust_bytes_like_injected_range_results() -> None:
    content = b"abcdef"

    def read_range(offset: int, size: int) -> bytes:
        return cast(bytes, _RustBytesLike(content[offset : offset + size]))

    reader = RemoteFileReader("virtual.bin", len(content), read_range=read_range)

    first = reader.read(2)
    destination = bytearray(3)

    assert type(first) is bytes
    assert first == b"ab"
    assert reader.readinto(destination) == 3
    assert bytes(destination) == b"cde"


@pytest.mark.parametrize("size", [-1, -2, -100])
def test_remote_file_reader_treats_every_negative_read_size_as_read_all(size: int) -> None:
    content = b"abcdef"
    range_requests: list[tuple[int, int]] = []

    def read_range(offset: int, requested_size: int) -> bytes:
        range_requests.append((offset, requested_size))
        return content[offset : offset + requested_size]

    reader = RemoteFileReader("virtual.bin", len(content), read_range=read_range)

    assert reader.read(size) == content
    assert range_requests == [(0, len(content))]


@pytest.mark.parametrize("whence", [-1, 3, 99])
def test_remote_file_reader_rejects_invalid_seek_whence(whence: int) -> None:
    reader = RemoteFileReader("virtual.bin", 6, read_range=lambda offset, size: b"abcdef"[offset : offset + size])

    with pytest.raises(ValueError):
        reader.seek(0, whence)


@pytest.mark.parametrize(
    ("offset", "whence"),
    [
        pytest.param(-1, os.SEEK_SET, id="before-start-from-beginning"),
        pytest.param(-1, os.SEEK_CUR, id="before-start-from-current"),
        pytest.param(-7, os.SEEK_END, id="before-start-from-end"),
    ],
)
def test_remote_file_reader_rejects_seeks_before_start(offset: int, whence: int) -> None:
    reader = RemoteFileReader("virtual.bin", 6, read_range=lambda start, size: b"abcdef"[start : start + size])

    with pytest.raises(ValueError):
        reader.seek(offset, whence)


@pytest.mark.parametrize(
    ("method", "args"),
    [
        pytest.param("read", (1,), id="read"),
        pytest.param("readinto", (bytearray(1),), id="readinto"),
        pytest.param("seek", (0,), id="seek"),
        pytest.param("tell", (), id="tell"),
        pytest.param("flush", (), id="flush"),
        pytest.param("isatty", (), id="isatty"),
    ],
)
def test_remote_file_reader_rejects_io_after_close(method: str, args: tuple[object, ...]) -> None:
    reader = RemoteFileReader("virtual.bin", 6, read_range=lambda offset, size: b"abcdef"[offset : offset + size])
    reader.close()

    with pytest.raises(ValueError):
        getattr(reader, method)(*args)


def test_object_file_streaming_honors_binary_buffering() -> None:
    content = b"abcdefgh"

    unbuffered_client = _RangeStorageClient(content)
    unbuffered_file = _object_file(
        unbuffered_client,
        "virtual.bin",
        mode="rb",
        memory_load_limit=0,
        prefetch_file=False,
        buffering=0,
    )
    assert unbuffered_file.read(1) == b"a"
    assert unbuffered_file.read(1) == b"b"
    assert unbuffered_client.range_requests == [(0, 1), (1, 1)]
    unbuffered_file.close()

    buffered_client = _RangeStorageClient(content)
    buffered_file = _object_file(
        buffered_client,
        "virtual.bin",
        mode="rb",
        memory_load_limit=0,
        prefetch_file=False,
        buffering=4,
    )
    assert buffered_file.read(1) == b"a"
    assert buffered_file.read(1) == b"b"
    assert buffered_client.range_requests == [(0, 4)]
    buffered_file.close()


def test_unbuffered_uncached_object_file_normalizes_rust_bytes_like_range_results() -> None:
    content = b"abcdef"
    storage_client = _RustBytesRangeStorageClient(content)
    stream = _object_file(
        storage_client,
        "virtual.bin",
        mode="rb",
        memory_load_limit=0,
        prefetch_file=False,
        disable_read_cache=True,
        buffering=0,
    )

    first = stream.read(2)
    destination = bytearray(3)

    assert type(first) is bytes
    assert first == b"ab"
    assert stream.readinto(destination) == 3
    assert bytes(destination) == b"cde"
    assert storage_client.uncached_range_requests == [(0, 2), (2, 3)]
    stream.close()


def test_object_file_streams_utf8_jsonl_lines_across_byte_boundaries(tmp_path: Path) -> None:
    content = ' {"name":"café"}\n{"name":"naïve"}\n'.encode("utf-8")
    storage_client = _RangeStorageClient(
        content,
        cache_manager=_PrefetchingCacheManager(content, tmp_path / "prefetched.jsonl"),
    )
    stream = _object_file(
        storage_client,
        "records.jsonl",
        mode="r",
        encoding="utf-8",
        memory_load_limit=0,
        prefetch_file=False,
        buffering=2,
    )

    assert [json.loads(line) for line in stream] == [{"name": "café"}, {"name": "naïve"}]
    assert storage_client.range_requests
    assert storage_client.download_calls == 0

    read_boundaries = {offset + size for offset, size in storage_client.range_requests}
    multibyte_start = content.index("é".encode("utf-8"))
    assert multibyte_start + 1 in read_boundaries

    json_token = b'"name"'
    json_token_start = content.index(json_token)
    assert any(json_token_start < boundary < json_token_start + len(json_token) for boundary in read_boundaries)
    stream.close()


def test_object_file_text_buffering_one_uses_line_buffering_without_one_byte_range_reads() -> None:
    content = b"first line\nsecond line\n"
    storage_client = _RangeStorageClient(content)
    stream = _object_file(
        storage_client,
        "records.txt",
        mode="r",
        encoding="utf-8",
        memory_load_limit=0,
        prefetch_file=False,
        buffering=1,
    )

    assert stream.readline() == "first line\n"
    assert cast(io.TextIOWrapper, stream._file).line_buffering is True
    assert storage_client.range_requests == [(0, len(content))]
    stream.close()


def test_streamed_text_uses_universal_newline_translation_like_normal_text_io(tmp_path: Path) -> None:
    content = b"first\r\nsecond\rthird\n"
    normal_path = tmp_path / "normal.txt"
    normal_path.write_bytes(content)
    storage_client = _RangeStorageClient(content)
    stream = _object_file(
        storage_client,
        "records.txt",
        mode="r",
        encoding="utf-8",
        memory_load_limit=0,
        prefetch_file=False,
        buffering=3,
    )

    with normal_path.open("r", encoding="utf-8", newline=None) as normal_file:
        expected = normal_file.read()

    assert stream.read() == expected
    stream.seek(0)
    assert stream.readlines() == ["first\n", "second\n", "third\n"]
    stream.close()


@pytest.mark.parametrize(
    ("operation", "args"),
    [
        pytest.param("read", (1,), id="read"),
        pytest.param("seek", (0,), id="seek"),
        pytest.param("tell", (), id="tell"),
        pytest.param("readline", (), id="readline"),
        pytest.param("readinto", (bytearray(1),), id="readinto"),
    ],
)
def test_prefetch_failure_is_raised_to_foreground_operations_as_io_error(
    operation: str, args: tuple[object, ...]
) -> None:
    stream = _object_file(
        _FailingDownloadStorageClient(b"unavailable"),
        "broken.bin",
        mode="rb",
        prefetch_file=True,
    )

    with pytest.raises(OSError, match="injected prefetch failure"):
        getattr(stream, operation)(*args)

    stream.close()


def test_streamed_object_file_materializes_a_persistent_filesystem_path() -> None:
    content = b"persistent stream contents"
    storage_client = _RangeStorageClient(content, cache_manager=_StreamingCacheManager())
    stream = _object_file(storage_client, "virtual/data.bin", mode="rb", prefetch_file=False)

    assert stream.read(4) == content[:4]
    materialized_path = stream.resolve_filesystem_path()

    assert Path(materialized_path).is_file()
    assert Path(materialized_path).read_bytes() == content
    assert stream.tell() == 4
    assert stream.resolve_filesystem_path() == materialized_path

    stream.close()
    assert Path(materialized_path).read_bytes() == content


def test_prefetched_binary_object_file_materializes_the_opened_snapshot_without_redownload() -> None:
    """Materializing an in-memory prefetched file preserves the opened bytes after the remote object mutates."""
    opened_content = b"opened snapshot\r\n"
    storage_client = _RangeStorageClient(opened_content)
    stream = _object_file(storage_client, "virtual/data.bin", mode="rb", prefetch_file=True)

    assert stream.read(6) == opened_content[:6]
    position = stream.tell()
    storage_client._content = b"changed source contents\n"

    materialized_path = stream.resolve_filesystem_path()

    assert Path(materialized_path).read_bytes() == opened_content
    assert stream.tell() == position
    assert storage_client.download_calls == 1
    stream.close()


def test_prefetched_text_object_file_materializes_exact_original_bytes_and_preserves_position() -> None:
    """Text prefetch retains raw bytes, including newlines, instead of encoding translated text again."""
    opened_content = "café\r\nsecond\rline\n".encode("utf-8")
    storage_client = _RangeStorageClient(opened_content)
    stream = _object_file(
        storage_client,
        "virtual/data.txt",
        mode="r",
        encoding="utf-8",
        prefetch_file=True,
    )

    assert stream.readline() == "café\n"
    position = stream.tell()
    storage_client._content = b"changed\n"

    materialized_path = stream.resolve_filesystem_path()

    assert Path(materialized_path).read_bytes() == opened_content
    assert stream.tell() == position
    assert stream.read() == "second\nline\n"
    assert storage_client.download_calls == 1
    stream.close()


def test_text_object_file_materializes_original_bytes_without_newline_translation() -> None:
    content = "café\r\nsecond\rline\n".encode("utf-8")
    storage_client = _RangeStorageClient(content, cache_manager=_StreamingCacheManager())
    stream = _object_file(
        storage_client,
        "virtual/data.txt",
        mode="r",
        encoding="utf-8",
        prefetch_file=False,
        buffering=3,
    )

    assert stream.readline() == "café\n"
    position = stream.tell()
    materialized_path = stream.resolve_filesystem_path()

    assert Path(materialized_path).read_bytes() == content
    assert stream.tell() == position
    assert storage_client.download_calls == 0

    stream.close()
