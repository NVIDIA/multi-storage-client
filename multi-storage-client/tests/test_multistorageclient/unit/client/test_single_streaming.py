# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import multistorageclient.retry as retry_module
from multistorageclient import StorageClientConfig
from multistorageclient.cache import CacheManager
from multistorageclient.caching.cache_config import CacheConfig
from multistorageclient.client.single import SingleStorageClient
from multistorageclient.types import ObjectMetadata, Range, ResolvedPath, ResolvedPathState, RetryableError, RetryConfig


@pytest.fixture
def single_backend_config() -> StorageClientConfig:
    return StorageClientConfig.from_yaml(
        """
        profiles:
          test-single:
            storage_provider:
              type: file
              options:
                base_path: /tmp/test
        """,
        profile="test-single",
    )


@pytest.mark.parametrize(
    ("provider_default", "caller_prefetch", "expected_prefetch"),
    [
        pytest.param(False, None, False, id="provider-default"),
        pytest.param(True, None, True, id="provider-default-true"),
        pytest.param(False, True, True, id="caller-true-overrides-provider"),
        pytest.param(True, False, False, id="caller-false-overrides-provider"),
        pytest.param(None, None, None, id="legacy-cache-default"),
    ],
)
def test_single_open_applies_provider_prefetch_default_with_caller_precedence(
    single_backend_config: StorageClientConfig,
    provider_default: bool | None,
    caller_prefetch: bool | None,
    expected_prefetch: bool | None,
) -> None:
    client = SingleStorageClient(single_backend_config)
    provider = MagicMock()
    provider.default_read_prefetch = provider_default
    provider.is_read_only = False
    client._storage_provider = provider

    with patch("multistorageclient.client.single.ObjectFile", autospec=True) as object_file:
        client.open("virtual.bin", mode="rb", buffering=64, prefetch_file=caller_prefetch)

    assert object_file.call_args.kwargs["prefetch_file"] is expected_prefetch
    assert object_file.call_args.kwargs["buffering"] == 64


@pytest.mark.parametrize("mode", ["w", "wb", "a", "ab"])
def test_single_open_rejects_writes_to_read_only_provider_before_constructing_file(
    single_backend_config: StorageClientConfig, mode: str
) -> None:
    client = SingleStorageClient(single_backend_config)
    provider = MagicMock()
    provider.is_read_only = True
    client._storage_provider = provider

    with patch("multistorageclient.client.single.ObjectFile", autospec=True) as object_file:
        with pytest.raises(NotImplementedError, match="read-only"):
            client.open("virtual.bin", mode=mode)

    object_file.assert_not_called()
    provider.get_object_metadata.assert_not_called()


def test_single_sync_from_rejects_a_read_only_target_before_constructing_sync_manager(
    single_backend_config: StorageClientConfig,
) -> None:
    """Syncing to a manifest fails before source enumeration or worker setup."""
    client = SingleStorageClient(single_backend_config)
    provider = MagicMock()
    provider.is_read_only = True
    client._storage_provider = provider
    source_client = MagicMock()

    with patch("multistorageclient.client.single.SyncManager", autospec=True) as sync_manager:
        with pytest.raises(NotImplementedError, match="read-only"):
            client.sync_from(source_client)

    sync_manager.assert_not_called()
    source_client.assert_not_called()


def test_single_streaming_open_with_cache_disabled_bypasses_cache_after_logical_resolution(
    single_backend_config: StorageClientConfig,
    tmp_path: Path,
) -> None:
    client = SingleStorageClient(single_backend_config)
    cache_manager = CacheManager(
        profile="test-single",
        cache_config=CacheConfig(
            size="10M",
            cache_line_size="1M",
            check_source_version=False,
            location=str(tmp_path),
        ),
    )
    cache_manager.set("physical/data.bin", b"stale-cache")
    provider = MagicMock()

    def get_object(_path: str, byte_range: Range | None = None) -> bytes:
        assert byte_range is not None
        return b"fresh-source"[byte_range.offset : byte_range.offset + byte_range.size]

    provider.get_object.side_effect = get_object
    metadata = ObjectMetadata(
        key="logical/data.bin",
        content_length=len(b"fresh-source"),
        last_modified=datetime.now(timezone.utc),
    )
    metadata_provider = MagicMock()
    metadata_provider.get_object_metadata.return_value = metadata
    metadata_provider.realpath.return_value = ResolvedPath(
        physical_path="physical/data.bin",
        state=ResolvedPathState.EXISTS,
    )
    client._cache_manager = cache_manager
    client._storage_provider = provider
    client._metadata_provider = metadata_provider
    client._is_posix_file_storage_provider = lambda: False  # type: ignore[method-assign]

    with patch.object(cache_manager, "read", wraps=cache_manager.read) as read_cache:
        with client.open(
            "logical/data.bin",
            mode="rb",
            disable_read_cache=True,
            memory_load_limit=0,
            prefetch_file=False,
            buffering=0,
        ) as stream:
            assert stream.read(5) == b"fresh"

    metadata_provider.realpath.assert_called_once_with("logical/data.bin")
    provider.get_object.assert_called_once_with("physical/data.bin", byte_range=Range(offset=0, size=5))
    read_cache.assert_not_called()


def test_single_direct_cached_replica_range_read_retries_only_at_the_outer_boundary(
    single_backend_config: StorageClientConfig,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retryable source failure must not trigger either cache fallback before the outer retry."""
    client = SingleStorageClient(single_backend_config)
    cache_manager = CacheManager(
        profile="test-single",
        cache_config=CacheConfig(
            size="10M",
            cache_line_size="1M",
            check_source_version=False,
            location=str(tmp_path),
        ),
    )
    provider = MagicMock()
    provider.get_object.side_effect = RetryableError("transient range read failure")
    provider.get_object_metadata.return_value = ObjectMetadata(
        key="remote.bin",
        content_length=4,
        last_modified=datetime.now(timezone.utc),
    )
    replica_manager = MagicMock()
    selected_paths: list[str] = []

    def select_range_reader(path: str, _provider: MagicMock, *, primary_range_reader):
        selected_paths.append(path)
        return primary_range_reader

    replica_manager.range_reader_from_replica_or_primary.side_effect = select_range_reader
    client._cache_manager = cache_manager
    client._metadata_provider = None
    client._replica_manager = replica_manager
    client._retry_config = RetryConfig(attempts=2, delay=0, backoff_multiplier=1)
    client._storage_provider = provider
    client._is_posix_file_storage_provider = lambda: False  # type: ignore[method-assign]
    monkeypatch.setattr(retry_module, "sleep_before_retry", lambda *_args: None)

    with pytest.raises(RetryableError, match="transient range read failure"):
        client.read("remote.bin", byte_range=Range(offset=0, size=4))

    assert provider.get_object.call_count == 2
    assert replica_manager.range_reader_from_replica_or_primary.call_count == 1
    assert selected_paths == ["remote.bin"]


def test_single_cached_range_read_clips_past_eof_before_planning_chunks(
    single_backend_config: StorageClientConfig,
    tmp_path: Path,
) -> None:
    """A wide cached range fetches and stores only the source-backed final chunk."""
    client = SingleStorageClient(single_backend_config)
    cache_line_size = 1024 * 1024
    cache_manager = CacheManager(
        profile="test-single",
        cache_config=CacheConfig(
            size="10M",
            cache_line_size="1M",
            check_source_version=False,
            location=str(tmp_path),
        ),
    )
    provider = MagicMock()
    source = b"x"
    provider.get_object.side_effect = lambda _path, byte_range: source[
        byte_range.offset : byte_range.offset + byte_range.size
    ]
    provider.get_object_metadata.return_value = ObjectMetadata(
        key="remote.bin",
        content_length=len(source),
        last_modified=datetime.now(timezone.utc),
    )
    client._cache_manager = cache_manager
    client._metadata_provider = None
    client._replica_manager = None
    client._storage_provider = provider
    client._is_posix_file_storage_provider = lambda: False  # type: ignore[method-assign]

    with patch.object(
        cache_manager,
        "_fetch_and_cache_chunk",
        wraps=cache_manager._fetch_and_cache_chunk,
    ) as fetch_chunk:
        assert client.read("remote.bin", byte_range=Range(offset=0, size=10 * cache_line_size)) == source

    provider.get_object.assert_called_once_with("remote.bin", byte_range=Range(offset=0, size=len(source)))
    provider.get_object_metadata.assert_called_once_with("remote.bin")
    assert fetch_chunk.call_count == 1
    cache_path = cache_manager._get_cache_file_path("remote.bin")
    assert all(not Path(cache_manager._get_chunk_path(cache_path, chunk_idx)).exists() for chunk_idx in range(1, 10))


@pytest.mark.parametrize(
    ("byte_range", "metadata_calls"),
    [
        pytest.param(Range(offset=0, size=0), 0, id="zero-length"),
        pytest.param(Range(offset=1, size=1), 1, id="offset-at-eof"),
    ],
)
def test_single_cached_range_read_does_not_fetch_empty_ranges(
    single_backend_config: StorageClientConfig,
    tmp_path: Path,
    byte_range: Range,
    metadata_calls: int,
) -> None:
    """Zero-length and EOF ranges avoid source reads and cache publication."""
    client = SingleStorageClient(single_backend_config)
    cache_manager = CacheManager(
        profile="test-single",
        cache_config=CacheConfig(
            size="10M",
            cache_line_size="1M",
            check_source_version=False,
            location=str(tmp_path),
        ),
    )
    provider = MagicMock()
    provider.get_object.return_value = b"x"
    provider.get_object_metadata.return_value = ObjectMetadata(
        key="remote.bin",
        content_length=1,
        last_modified=datetime.now(timezone.utc),
    )
    client._cache_manager = cache_manager
    client._metadata_provider = None
    client._replica_manager = None
    client._storage_provider = provider
    client._is_posix_file_storage_provider = lambda: False  # type: ignore[method-assign]

    assert client.read("remote.bin", byte_range=byte_range) == b""

    provider.get_object.assert_not_called()
    assert provider.get_object_metadata.call_count == metadata_calls
    cache_path = cache_manager._get_cache_file_path("remote.bin")
    assert not Path(cache_path).exists()
    assert not Path(cache_manager._get_chunk_path(cache_path, 0)).exists()


def test_single_cached_range_read_uses_a_cached_chunk_size_without_metadata(
    single_backend_config: StorageClientConfig,
    tmp_path: Path,
) -> None:
    """A cached short final chunk bounds later wide reads without a metadata request."""
    client = SingleStorageClient(single_backend_config)
    cache_line_size = 1024 * 1024
    cache_manager = CacheManager(
        profile="test-single",
        cache_config=CacheConfig(
            size="10M",
            cache_line_size="1M",
            check_source_version=False,
            location=str(tmp_path),
        ),
    )
    provider = MagicMock()
    source = b"a" * cache_line_size + b"b"
    provider.get_object.side_effect = lambda _path, byte_range: source[
        byte_range.offset : byte_range.offset + byte_range.size
    ]
    provider.get_object_metadata.return_value = ObjectMetadata(
        key="remote.bin",
        content_length=len(source),
        last_modified=datetime.now(timezone.utc),
    )
    client._cache_manager = cache_manager
    client._metadata_provider = None
    client._replica_manager = None
    client._storage_provider = provider
    client._is_posix_file_storage_provider = lambda: False  # type: ignore[method-assign]

    assert client.read("remote.bin", byte_range=Range(offset=cache_line_size, size=1)) == b"b"
    provider.reset_mock()

    assert client.read("remote.bin", byte_range=Range(offset=0, size=10 * cache_line_size)) == source

    provider.get_object.assert_called_once_with("remote.bin", byte_range=Range(offset=0, size=cache_line_size))
    provider.get_object_metadata.assert_not_called()
    cache_path = cache_manager._get_cache_file_path("remote.bin")
    assert all(not Path(cache_manager._get_chunk_path(cache_path, chunk_idx)).exists() for chunk_idx in range(2, 10))


@pytest.mark.parametrize(
    "error_factory",
    [
        pytest.param(lambda: FileNotFoundError("missing object"), id="not-found"),
        pytest.param(lambda: IOError("TLS protocol failure"), id="protocol-io"),
    ],
)
def test_single_cached_range_read_propagates_nonretryable_source_errors_once(
    single_backend_config: StorageClientConfig,
    tmp_path: Path,
    error_factory,
) -> None:
    """Source failures bypass both cache fallback layers without duplicate reads."""
    client = SingleStorageClient(single_backend_config)
    cache_manager = CacheManager(
        profile="test-single",
        cache_config=CacheConfig(
            size="10M",
            cache_line_size="1M",
            check_source_version=False,
            location=str(tmp_path),
        ),
    )
    source_error = error_factory()
    provider = MagicMock()
    provider.get_object.side_effect = source_error
    provider.get_object_metadata.return_value = ObjectMetadata(
        key="remote.bin",
        content_length=1,
        last_modified=datetime.now(timezone.utc),
    )
    client._cache_manager = cache_manager
    client._metadata_provider = None
    client._replica_manager = None
    client._storage_provider = provider
    client._is_posix_file_storage_provider = lambda: False  # type: ignore[method-assign]

    with pytest.raises(type(source_error)) as raised:
        client.read("remote.bin", byte_range=Range(offset=0, size=1))

    assert raised.value is source_error
    provider.get_object.assert_called_once_with("remote.bin", byte_range=Range(offset=0, size=1))


def test_single_cached_range_read_propagates_source_size_errors_once(
    single_backend_config: StorageClientConfig,
    tmp_path: Path,
) -> None:
    """A source-size failure bypasses cache fallback without attempting a range read."""
    client = SingleStorageClient(single_backend_config)
    cache_manager = CacheManager(
        profile="test-single",
        cache_config=CacheConfig(
            size="10M",
            cache_line_size="1M",
            check_source_version=False,
            location=str(tmp_path),
        ),
    )
    source_error = FileNotFoundError("missing object metadata")
    provider = MagicMock()
    provider.get_object.side_effect = AssertionError("range source must not be read")
    provider.get_object_metadata.side_effect = source_error
    client._cache_manager = cache_manager
    client._metadata_provider = None
    client._replica_manager = None
    client._storage_provider = provider
    client._is_posix_file_storage_provider = lambda: False  # type: ignore[method-assign]

    with pytest.raises(FileNotFoundError) as raised:
        client.read("remote.bin", byte_range=Range(offset=0, size=1))

    assert raised.value is source_error
    provider.get_object_metadata.assert_called_once_with("remote.bin")
    provider.get_object.assert_not_called()
