# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient import StorageClientConfig
from multistorageclient.cache import CacheManager
from multistorageclient.caching.cache_config import CacheConfig
from multistorageclient.client.single import SingleStorageClient
from multistorageclient.types import ObjectMetadata, Range, ResolvedPath, ResolvedPathState


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
