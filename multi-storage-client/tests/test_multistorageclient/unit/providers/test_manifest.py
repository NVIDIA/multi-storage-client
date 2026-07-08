# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import io
import json
import os
import pickle
import threading
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional, cast
from unittest.mock import MagicMock
from urllib.parse import parse_qsl, urlsplit

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import multistorageclient.config as config_module
import multistorageclient.shortcuts as shortcuts
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.client.single import SingleStorageClient
from multistorageclient.manifest import ManifestValidationError
from multistorageclient.manifest.bindings import ServiceBinding, SourceBinding
from multistorageclient.manifest.models import QueryParameter
from multistorageclient.providers.base import BaseStorageProvider
from multistorageclient.providers.manifest import ManifestStorageProvider
from multistorageclient.providers.manifest_metadata import ManifestMetadataProvider
from multistorageclient.providers.posix_file import PosixFileStorageProvider
from multistorageclient.providers.s3 import S3StorageProvider
from multistorageclient.types import (
    Credentials,
    CredentialsProvider,
    ObjectMetadata,
    Range,
    RetryableError,
    RetryConfig,
    SourceVersionCheckMode,
    SymlinkHandling,
)
from test_multistorageclient.unit.manifest.helpers import object_row, service_row, write_manifest
from test_multistorageclient.unit.utils.config import setup_msc_config

_LAST_MODIFIED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
_DOWNLOAD_WINDOW_SIZE = 8 * 1024 * 1024


class _ConcurrencyProbe:
    def __init__(self, parties: Optional[int] = None) -> None:
        self._lock = threading.Lock()
        self._barrier = threading.Barrier(parties, timeout=2) if parties is not None else None
        self._completion_events: dict[int, threading.Event] = {}
        self.active = 0
        self.maximum = 0
        self.completion_order: list[int] = []

    @contextmanager
    def hold(self, operation_index: Optional[int]) -> Iterator[None]:
        with self._lock:
            self.active += 1
            self.maximum = max(self.maximum, self.active)
            if operation_index is not None:
                pair_start = operation_index - (operation_index % 2)
                pair_event = self._completion_events.setdefault(pair_start, threading.Event())
        try:
            if self._barrier is not None:
                assert operation_index is not None
                self._barrier.wait()
                if operation_index % 2 == 0 and not pair_event.wait(timeout=2):
                    raise TimeoutError("paired range read did not complete")
            yield
        finally:
            with self._lock:
                self.active -= 1
                if operation_index is not None:
                    self.completion_order.append(operation_index)
                    if operation_index % 2 == 1:
                        pair_event.set()


class _RecordingRangeReader:
    def __init__(
        self,
        objects: Mapping[str, bytes],
        *,
        binding_identity: str,
        probe: Optional[_ConcurrencyProbe] = None,
        synchronization_order: Optional[Mapping[str, int]] = None,
        truncate_ranges: bool = False,
        extend_ranges: bool = False,
        fail_on_range_call: Optional[int] = None,
        retryable_fail_on_range_call: Optional[int] = None,
    ) -> None:
        self._objects = dict(objects)
        self._binding_identity = binding_identity
        self._probe = probe or _ConcurrencyProbe()
        self._synchronization_order = dict(synchronization_order or {})
        self._truncate_ranges = truncate_ranges
        self._extend_ranges = extend_ranges
        self._fail_on_range_call = fail_on_range_call
        self._retryable_fail_on_range_call = retryable_fail_on_range_call
        self._lock = threading.Lock()
        self.calls: list[tuple[str, Optional[Range]]] = []
        self._range_calls = 0

    @property
    def binding_identity(self) -> str:
        return self._binding_identity

    def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        with self._lock:
            self.calls.append((path, byte_range))
            if byte_range is not None:
                self._range_calls += 1
                range_call = self._range_calls
            else:
                range_call = 0

        with self._probe.hold(self._synchronization_order.get(path)):
            if byte_range is not None and self._fail_on_range_call == range_call:
                raise OSError("injected range-read failure")
            if byte_range is not None and self._retryable_fail_on_range_call == range_call:
                raise RetryableError("injected retryable range-read failure")

            try:
                content = self._objects[path]
            except KeyError as exc:
                raise FileNotFoundError(path) from exc

            if byte_range is None:
                return content

            result = content[byte_range.offset : byte_range.offset + byte_range.size]
            if self._truncate_ranges and result:
                return result[:-1]
            if self._extend_ranges:
                return result + b"!"
            return result

    def info(self, path: str) -> ObjectMetadata:
        try:
            content = self._objects[path]
        except KeyError as exc:
            raise FileNotFoundError(path) from exc
        return ObjectMetadata(
            key=path,
            content_length=len(content),
            last_modified=_LAST_MODIFIED,
            etag=f'"{len(content)}"',
        )


class _InstrumentedPosixStorageProvider(PosixFileStorageProvider):
    _calls_lock = threading.Lock()
    get_calls: list[tuple[str, Optional[Range]]] = []
    download_calls: list[str] = []

    @classmethod
    def reset_calls(cls) -> None:
        with cls._calls_lock:
            cls.get_calls.clear()
            cls.download_calls.clear()

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        with self._calls_lock:
            self.get_calls.append((path, byte_range))
        return super()._get_object(path, byte_range)

    def _download_file(
        self,
        remote_path: str,
        f: Any,
        metadata: Optional[ObjectMetadata] = None,
    ) -> int:
        with self._calls_lock:
            self.download_calls.append(remote_path)
        return super()._download_file(remote_path, f, metadata)


class _RecordingServiceReader:
    def __init__(
        self,
        objects: Mapping[tuple[str, tuple[tuple[str, str], ...]], bytes],
        *,
        binding_identity: str,
        allowed_path_prefixes: Sequence[str] = ("render/",),
        allowed_query_parameters: Sequence[str] = ("frame", "variant"),
        truncate_ranges: bool = False,
        extend_ranges: bool = False,
    ) -> None:
        self._objects = dict(objects)
        self._binding_identity = binding_identity
        self._allowed_path_prefixes = tuple(allowed_path_prefixes)
        self._allowed_query_parameters = frozenset(allowed_query_parameters)
        self._truncate_ranges = truncate_ranges
        self._extend_ranges = extend_ranges
        self.validations: list[tuple[str, tuple[tuple[str, str], ...]]] = []
        self.calls: list[tuple[str, tuple[tuple[str, str], ...], Range, int]] = []

    @property
    def binding_identity(self) -> str:
        return self._binding_identity

    @staticmethod
    def _query_tuple(query: Sequence[QueryParameter]) -> tuple[tuple[str, str], ...]:
        return tuple((item.name, item.value) for item in query)

    def validate(self, path: str, query: Sequence[QueryParameter]) -> None:
        query_tuple = self._query_tuple(query)
        self.validations.append((path, query_tuple))
        if not any(path.startswith(prefix) for prefix in self._allowed_path_prefixes):
            raise ValueError(f"Service path {path!r} is not allowlisted")
        invalid_names = [name for name, _ in query_tuple if name not in self._allowed_query_parameters]
        if invalid_names:
            raise ValueError(f"Service query parameter {invalid_names[0]!r} is not allowlisted")

    def read(
        self,
        path: str,
        query: Sequence[QueryParameter],
        byte_range: Range,
        total_size: int,
    ) -> bytes:
        query_tuple = self._query_tuple(query)
        self.calls.append((path, query_tuple, byte_range, total_size))
        content = self._objects[(path, query_tuple)]
        result = content[byte_range.offset : byte_range.offset + byte_range.size]
        if self._truncate_ranges and result:
            return result[:-1]
        if self._extend_ranges:
            return result + b"!"
        return result


class _CredentialSpy(CredentialsProvider):
    instances: dict[tuple[str, str], "_CredentialSpy"] = {}

    def __init__(self, spy_id: str, role: str, credential_set: str) -> None:
        self.spy_id = spy_id
        self.role = role
        self.credential_set = credential_set
        self.get_calls = 0
        self.refresh_calls = 0
        self.instances[(spy_id, role)] = self

    def get_credentials(self) -> Credentials:
        self.get_calls += 1
        return Credentials(
            access_key=f"{self.role}-{self.credential_set}-access",
            secret_key=f"{self.role}-{self.credential_set}-secret",
            token=f"{self.role}-{self.credential_set}-token",
            expiration=None,
        )

    def refresh_credentials(self) -> None:
        self.refresh_calls += 1


class _AuthenticatedFailOnceStorageProvider(BaseStorageProvider):
    datasets: dict[tuple[str, str], dict[str, bytes]] = {}
    instances: dict[tuple[str, str], "_AuthenticatedFailOnceStorageProvider"] = {}

    def __init__(
        self,
        base_path: str,
        spy_id: str,
        role: str,
        credentials_provider: Optional[CredentialsProvider] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(base_path=base_path, provider_name=f"spy-{role}")
        if credentials_provider is None:
            raise AssertionError(f"{role} provider must receive its configured credentials provider")
        self._credentials_provider = credentials_provider
        self._objects = self.datasets[(spy_id, role)]
        self.read_attempts: list[tuple[str, Optional[Range]]] = []
        self.authenticated_access_keys: list[str] = []
        self.instances[(spy_id, role)] = self

    def _authenticate(self) -> None:
        credentials = self._credentials_provider.get_credentials()
        self.authenticated_access_keys.append(credentials.access_key)

    def _put_object(self, path: str, body: bytes, **kwargs: Any) -> int:
        raise NotImplementedError

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        self._authenticate()
        self.read_attempts.append((path, byte_range))
        if len(self.read_attempts) == 1:
            raise RetryableError(f"fail once while reading {path}")
        content = self._objects[path]
        if byte_range is None:
            return content
        return content[byte_range.offset : byte_range.offset + byte_range.size]

    def _copy_object(self, src_path: str, dest_path: str) -> int:
        raise NotImplementedError

    def _delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        raise NotImplementedError

    def _make_symlink(self, path: str, target: str) -> None:
        raise NotImplementedError

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        self._authenticate()
        content = self._objects[path]
        return ObjectMetadata(
            key=path,
            content_length=len(content),
            last_modified=_LAST_MODIFIED,
            etag=f'"{len(content)}"',
        )

    def _list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
    ) -> Iterator[ObjectMetadata]:
        return iter(())

    def _upload_file(self, remote_path: str, f: Any, attributes: Optional[dict[str, str]] = None) -> int:
        raise NotImplementedError

    def _download_file(
        self,
        remote_path: str,
        f: Any,
        metadata: Optional[ObjectMetadata] = None,
    ) -> int:
        raise NotImplementedError


class _RustRangeClient:
    def __init__(self, objects: Mapping[str, bytes]) -> None:
        self._objects = dict(objects)
        self.calls: list[tuple[str, Optional[Range]]] = []

    async def get(self, key: str, byte_range: Optional[Range] = None) -> bytes:
        self.calls.append((key, byte_range))
        content = self._objects[key]
        if byte_range is None:
            return content
        return content[byte_range.offset : byte_range.offset + byte_range.size]


def _provider(
    rows: Sequence[Mapping[str, Any]],
    *,
    source_bindings: Optional[Mapping[str, SourceBinding]] = None,
    service_bindings: Optional[Mapping[str, ServiceBinding]] = None,
    max_workers: int = 4,
    manifest_row_group_cache_size_bytes: int = 64 * 1024 * 1024,
    row_group_size: Optional[int] = None,
) -> ManifestStorageProvider:
    manifest_bytes = write_manifest(rows, row_group_size=row_group_size).getvalue()
    manifest_reader = _RecordingRangeReader(
        {"catalog.parquet": manifest_bytes},
        binding_identity="file:///manifests",
    )
    return ManifestStorageProvider(
        manifest_path="catalog.parquet",
        manifest_reader=manifest_reader,
        source_bindings=source_bindings or {},
        service_bindings=service_bindings or {},
        max_workers=max_workers,
        manifest_row_group_cache_size_bytes=manifest_row_group_cache_size_bytes,
    )


@pytest.mark.parametrize("invalid_budget", [-1, (1 << 63), True, 1.5, "64M"])
def test_manifest_provider_rejects_invalid_row_group_cache_budgets(invalid_budget: object) -> None:
    manifest_reader = MagicMock()

    with pytest.raises(ValueError, match="manifest_row_group_cache_size_bytes"):
        ManifestStorageProvider(
            manifest_path="catalog.parquet",
            manifest_reader=manifest_reader,
            source_bindings={},
            service_bindings={},
            manifest_row_group_cache_size_bytes=cast(Any, invalid_budget),
        )

    manifest_reader.info.assert_not_called()


def test_manifest_provider_preserves_existing_positional_constructor_arguments() -> None:
    manifest_bytes = write_manifest([]).getvalue()
    reader = _RecordingRangeReader({"catalog.parquet": manifest_bytes}, binding_identity="file:///manifests")
    config_dict = {"profile": "sentinel"}
    telemetry_provider = MagicMock()

    provider = ManifestStorageProvider(
        "catalog.parquet",
        reader,
        {},
        {},
        4,
        config_dict,
        telemetry_provider,
    )

    assert provider._config_dict is config_dict
    assert provider._telemetry_provider is telemetry_provider
    assert provider._manifest_row_group_cache_size_bytes == 64 * 1024 * 1024


@pytest.mark.parametrize(
    "manifest_path",
    [
        "",
        "/catalog.parquet",
        "../catalog.parquet",
        "datasets/../catalog.parquet",
        "datasets//catalog.parquet",
        "datasets\\catalog.parquet",
        "datasets/catalog.json",
        "datasets/catalog.parquet\n",
    ],
)
def test_manifest_provider_rejects_invalid_manifest_paths_before_reader_io(manifest_path: str) -> None:
    """The direct provider constructor enforces the same safe manifest-path invariant as config loading."""
    manifest_reader = MagicMock()

    with pytest.raises(ValueError):
        ManifestStorageProvider(
            manifest_path=manifest_path,
            manifest_reader=manifest_reader,
            source_bindings={},
            service_bindings={},
        )

    manifest_reader.info.assert_not_called()


@pytest.mark.parametrize("missing_operation", ["info", "read"])
def test_manifest_provider_rejects_malformed_manifest_readers_before_reader_io(missing_operation: str) -> None:
    """Direct construction rejects a non-callable manifest reader operation before invoking either one."""
    info = MagicMock()
    read = MagicMock()
    manifest_reader = MagicMock()
    manifest_reader.info = info
    manifest_reader.read = read
    setattr(manifest_reader, missing_operation, None)

    with pytest.raises(ValueError, match=rf"callable {missing_operation}"):
        ManifestStorageProvider(
            manifest_path="catalog.parquet",
            manifest_reader=cast(Any, manifest_reader),
            source_bindings={},
            service_bindings={},
        )

    info.assert_not_called()
    read.assert_not_called()


@pytest.mark.parametrize(
    ("source_bindings", "service_bindings"),
    [
        pytest.param(
            {
                "objects": SourceBinding(
                    cast(Any, type("ReaderWithoutRead", (), {"binding_identity": "file:///objects"})()), "r1"
                )
            },
            {},
            id="source-read",
        ),
        pytest.param(
            {},
            {
                "renderer": ServiceBinding(
                    cast(
                        Any,
                        type(
                            "ReaderWithoutValidate",
                            (),
                            {
                                "binding_identity": "https://renderer.example.test/v1",
                                "read": lambda _self, _path, _query, _byte_range, _total_size: b"",
                            },
                        )(),
                    ),
                    "r1",
                )
            },
            id="service-validate",
        ),
    ],
)
def test_manifest_provider_rejects_malformed_bindings_before_manifest_reader_io(
    source_bindings: Mapping[str, SourceBinding],
    service_bindings: Mapping[str, ServiceBinding],
) -> None:
    """Binding configuration fails before the provider reads manifest metadata or bytes."""

    class CountingManifestReader:
        binding_identity = "file:///manifests"

        def __init__(self) -> None:
            self.info_calls = 0
            self.read_calls = 0
            self._manifest = write_manifest([]).getvalue()

        def info(self, path: str) -> ObjectMetadata:
            self.info_calls += 1
            return ObjectMetadata(key=path, content_length=len(self._manifest), last_modified=_LAST_MODIFIED)

        def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
            self.read_calls += 1
            if byte_range is None:
                return self._manifest
            return self._manifest[byte_range.offset : byte_range.offset + byte_range.size]

    manifest_reader = CountingManifestReader()

    with pytest.raises(ValueError, match="callable"):
        ManifestStorageProvider(
            manifest_path="catalog.parquet",
            manifest_reader=manifest_reader,
            source_bindings=source_bindings,
            service_bindings=service_bindings,
        )

    assert manifest_reader.info_calls == 0
    assert manifest_reader.read_calls == 0


@pytest.mark.parametrize(
    "content_length",
    [
        pytest.param(-1, id="negative"),
        pytest.param(True, id="boolean"),
        pytest.param(1.5, id="float"),
        pytest.param("10", id="string"),
    ],
)
def test_manifest_provider_rejects_invalid_manifest_content_lengths_before_range_io(content_length: object) -> None:
    """Direct construction requires a concrete nonnegative integer manifest size."""
    manifest_reader = MagicMock()
    manifest_reader.info.return_value = MagicMock(content_length=content_length)

    with pytest.raises(ValueError, match="content length"):
        ManifestStorageProvider(
            manifest_path="catalog.parquet",
            manifest_reader=manifest_reader,
            source_bindings={},
            service_bindings={},
        )

    manifest_reader.read.assert_not_called()


def _single_object_rows(
    key: str,
    source_path: str,
    content: bytes,
    *,
    metadata: Optional[dict[str, Any]] = None,
    content_type: str = "application/octet-stream",
) -> list[dict[str, Any]]:
    return [
        object_row(
            key=key,
            size_bytes=len(content),
            last_modified=_LAST_MODIFIED,
            content_type=content_type,
            metadata=metadata,
            chunk_size_bytes=len(content),
            source_profile="objects",
            source_path=source_path,
            source_offset=0,
        )
    ]


def test_manifest_provider_lists_info_glob_and_attributes() -> None:
    objects = {
        "alpha.txt": b"a",
        "dir/a.txt": b"abc",
        "dir/nested/b.bin": b"wxyz",
    }
    rows = [
        *_single_object_rows("alpha.txt", "alpha.txt", objects["alpha.txt"], metadata={"color": "red"}),
        *_single_object_rows("dir/a.txt", "dir/a.txt", objects["dir/a.txt"], metadata={"color": "blue"}),
        *_single_object_rows(
            "dir/nested/b.bin",
            "dir/nested/b.bin",
            objects["dir/nested/b.bin"],
            metadata={"color": "green"},
        ),
    ]
    source = _RecordingRangeReader(objects, binding_identity="file:///objects")
    provider = _provider(rows, source_bindings={"objects": SourceBinding(source, "objects-r1")})

    metadata = provider.get_object_metadata("dir/a.txt")
    assert metadata.key == "dir/a.txt"
    assert metadata.content_length == 3
    assert metadata.content_type == "application/octet-stream"
    assert metadata.metadata == {"color": "blue"}
    assert metadata.etag

    shallow = list(provider.list_objects("", include_directories=True))
    assert [(item.key, item.type) for item in shallow] == [
        ("alpha.txt", "file"),
        ("dir", "directory"),
    ]
    expected_recursive = ["alpha.txt", "dir/a.txt", "dir/nested/b.bin"]
    assert [item.key for item in provider.list_objects("")] == expected_recursive
    assert [item.key for item in provider.list_objects_recursive("")] == expected_recursive
    assert [item.key for item in provider.list_objects("", start_after="alpha.txt", end_at="dir/nested/b.bin")] == [
        "dir/a.txt",
        "dir/nested/b.bin",
    ]
    assert provider.glob("**/*.bin") == ["dir/nested/b.bin"]
    assert [
        item.key
        for item in provider.list_objects(
            "",
            attribute_filter_expression='color = "blue"',
            show_attributes=True,
        )
    ] == ["dir/a.txt"]

    assert provider.get_object("dir/a.txt", Range(offset=3, size=1)) == b""
    assert provider.get_object("dir/a.txt", Range(offset=30, size=1)) == b""
    with pytest.raises(FileNotFoundError):
        provider.get_object_metadata("missing.bin")


def test_manifest_provider_handles_an_empty_dataset() -> None:
    provider = _provider([])

    assert list(provider.list_objects("")) == []
    assert list(provider.list_objects_recursive("")) == []
    assert provider.glob("**/*") == []
    with pytest.raises(FileNotFoundError):
        provider.get_object("missing.bin")


def test_manifest_provider_list_objects_returns_an_exact_file_path_with_bounds() -> None:
    """Direct manifest listing returns a requested file rather than only treating it as a prefix."""
    source = _RecordingRangeReader(
        {"exact.bin": b"exact", "neighbor.bin": b"neighbor"},
        binding_identity="file:///objects",
    )
    provider = _provider(
        [
            *_single_object_rows("dir/exact.bin", "exact.bin", b"exact"),
            *_single_object_rows("dir/neighbor.bin", "neighbor.bin", b"neighbor"),
        ],
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
    )

    assert [item.key for item in provider.list_objects("dir/exact.bin")] == ["dir/exact.bin"]
    assert [
        item.key for item in provider.list_objects("dir/exact.bin", start_after="dir/alpha.bin", end_at="dir/exact.bin")
    ] == ["dir/exact.bin"]
    assert list(provider.list_objects("dir/exact.bin", start_after="dir/exact.bin")) == []
    assert list(provider.list_objects("dir/exact.bin", end_at="dir/alpha.bin")) == []


@pytest.mark.parametrize(
    ("include_exact", "start_after", "end_at", "expected"),
    [
        pytest.param(
            False,
            None,
            None,
            [("a", "directory"), ("a-b", "file")],
            id="implicit-directory",
        ),
        pytest.param(
            False,
            "a",
            "a-b",
            [("a-b", "file")],
            id="implicit-directory-bounded",
        ),
        pytest.param(
            False,
            None,
            "a",
            [("a", "directory")],
            id="implicit-directory-end-bound",
        ),
        pytest.param(
            True,
            None,
            None,
            [("a", "file"), ("a-b", "file")],
            id="exact-file-precedence",
        ),
        pytest.param(
            True,
            "a",
            "a-b",
            [("a-b", "file")],
            id="exact-file-precedence-bounded",
        ),
        pytest.param(
            True,
            None,
            "a",
            [("a", "file")],
            id="exact-file-precedence-end-bound",
        ),
    ],
)
def test_manifest_provider_shallow_listing_orders_prefix_collisions_and_applies_bounds(
    include_exact: bool,
    start_after: Optional[str],
    end_at: Optional[str],
    expected: list[tuple[str, str]],
) -> None:
    """Shallow listing orders synthesized parents before punctuation siblings without duplicating exact files."""
    rows = [
        *_single_object_rows("a-b", "a-b.bin", b"dash"),
        *_single_object_rows("a/b", "a-child.bin", b"child"),
    ]
    objects = {"a-b.bin": b"dash", "a-child.bin": b"child"}
    if include_exact:
        rows = [*_single_object_rows("a", "a.bin", b"exact"), *rows]
        objects["a.bin"] = b"exact"

    source = _RecordingRangeReader(objects, binding_identity="file:///objects")
    provider = _provider(rows, source_bindings={"objects": SourceBinding(source, "objects-r1")})

    entries = list(provider.list_objects("", start_after=start_after, end_at=end_at, include_directories=True))

    assert [(entry.key, entry.type) for entry in entries] == expected


def test_manifest_provider_shallow_listing_scans_descendant_timestamps_only_for_winning_punctuation_prefixes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sibling punctuation probes inspect descendant timestamps only after their lexical winner is known."""
    logical_keys = [
        "nested/a-b/leaf.bin",
        "nested/a-c/leaf.bin",
        "nested/a-d/leaf.bin",
        "nested/a/child.bin",
        "nested/é-b/leaf.bin",
        "nested/é-c/leaf.bin",
        "nested/é/child.bin",
    ]
    objects = {key: key.encode() for key in logical_keys}
    rows = [row for key in logical_keys for row in _single_object_rows(key, key, objects[key])]
    source = _RecordingRangeReader(objects, binding_identity="file:///objects")
    provider = _provider(rows, source_bindings={"objects": SourceBinding(source, "objects-r1")})
    timestamp_prefixes: list[str] = []
    original = provider._catalog.directory_last_modified

    def track_timestamp_scan(key: str) -> Optional[datetime]:
        timestamp_prefixes.append(key)
        return original(key)

    monkeypatch.setattr(provider._catalog, "directory_last_modified", track_timestamp_scan)

    entries = list(
        provider.list_objects(
            "nested",
            start_after="nested/a",
            end_at="nested/é-c",
            include_directories=True,
        )
    )

    assert [(entry.key, entry.type) for entry in entries] == [
        ("nested/a-b", "directory"),
        ("nested/a-c", "directory"),
        ("nested/a-d", "directory"),
        ("nested/é", "directory"),
        ("nested/é-b", "directory"),
        ("nested/é-c", "directory"),
    ]
    assert timestamp_prefixes == ["nested/a", "nested/é"]


def test_manifest_provider_list_objects_recursive_returns_an_exact_file_path_with_bounds() -> None:
    """Recursive manifest listing applies the same exact-path and bound semantics as shallow listing."""
    source = _RecordingRangeReader(
        {"exact.bin": b"exact", "neighbor.bin": b"neighbor"},
        binding_identity="file:///objects",
    )
    provider = _provider(
        [
            *_single_object_rows("dir/exact.bin", "exact.bin", b"exact"),
            *_single_object_rows("dir/neighbor.bin", "neighbor.bin", b"neighbor"),
        ],
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
    )

    assert [item.key for item in provider.list_objects_recursive("dir/exact.bin")] == ["dir/exact.bin"]
    assert [
        item.key
        for item in provider.list_objects_recursive(
            "dir/exact.bin", start_after="dir/alpha.bin", end_at="dir/exact.bin"
        )
    ] == ["dir/exact.bin"]
    assert list(provider.list_objects_recursive("dir/exact.bin", start_after="dir/exact.bin")) == []
    assert list(provider.list_objects_recursive("dir/exact.bin", end_at="dir/alpha.bin")) == []


def test_manifest_provider_reads_touched_chunks_concurrently_in_manifest_order() -> None:
    objects = {f"chunk-{index}.bin": bytes([65 + index]) for index in range(4)}
    rows = [
        object_row(
            key="ordered.bin",
            size_bytes=4,
            chunk_index=index,
            chunk_size_bytes=1,
            source_profile="objects",
            source_path=f"chunk-{index}.bin",
            source_offset=0,
        )
        for index in range(4)
    ]
    probe = _ConcurrencyProbe(parties=2)
    source = _RecordingRangeReader(
        objects,
        binding_identity="file:///objects",
        probe=probe,
        synchronization_order={f"chunk-{index}.bin": index for index in range(4)},
    )
    provider = _provider(
        rows,
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
        max_workers=2,
    )

    assert provider.get_object("ordered.bin") == b"ABCD"
    assert probe.maximum == 2
    assert probe.completion_order == [1, 0, 3, 2]
    assert len(source.calls) == 4


def test_manifest_provider_replenishes_bounded_reads_after_any_completion() -> None:
    """A later read starts after another worker finishes even when the first submitted read remains blocked."""

    class HeadOfLineBlockingReader(_RecordingRangeReader):
        def __init__(self) -> None:
            super().__init__(
                {"first.bin": b"A", "second.bin": b"B", "third.bin": b"C"},
                binding_identity="file:///objects",
            )
            self.first_started = threading.Event()
            self.second_completed = threading.Event()
            self.third_started = threading.Event()
            self.release_first = threading.Event()

        def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
            if byte_range is not None and path == "first.bin":
                self.first_started.set()
                assert self.release_first.wait(timeout=5), "first read was never released"
            if byte_range is not None and path == "third.bin":
                self.third_started.set()
            data = super().read(path, byte_range)
            if byte_range is not None and path == "second.bin":
                self.second_completed.set()
            return data

    source = HeadOfLineBlockingReader()
    provider = _provider(
        [
            object_row(
                key="joined.bin",
                size_bytes=3,
                chunk_index=index,
                chunk_size_bytes=1,
                source_profile="objects",
                source_path=source_path,
                source_offset=0,
            )
            for index, source_path in enumerate(("first.bin", "second.bin", "third.bin"))
        ],
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
        max_workers=2,
    )
    result: list[bytes] = []
    errors: list[BaseException] = []

    def read_joined_file() -> None:
        try:
            result.append(provider.get_object("joined.bin"))
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)

    read_thread = threading.Thread(target=read_joined_file)
    read_thread.start()
    assert source.first_started.wait(timeout=5), "first read did not start"
    assert source.second_completed.wait(timeout=5), "second read did not complete"
    try:
        assert source.third_started.wait(timeout=5), "later read waited for the blocked head future"
    finally:
        source.release_first.set()
    read_thread.join(timeout=5)

    assert not read_thread.is_alive()
    assert not errors
    assert result == [b"ABC"]
    provider.close()


def test_manifest_provider_waits_for_running_siblings_before_propagating_a_subread_failure() -> None:
    """A failed logical read leaves no abandoned source request that can overlap an outer retry."""

    class FailingReaderWithBlockedSibling(_RecordingRangeReader):
        def __init__(self) -> None:
            super().__init__(
                {"failing.bin": b"A", "blocked.bin": b"B"},
                binding_identity="file:///objects",
            )
            self.blocked_started = threading.Event()
            self.failure_raised = threading.Event()
            self.release_blocked = threading.Event()

        def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
            if byte_range is not None and path == "blocked.bin":
                self.blocked_started.set()
                assert self.release_blocked.wait(timeout=5), "blocked sibling was never released"
            if byte_range is not None and path == "failing.bin":
                assert self.blocked_started.wait(timeout=5), "sibling read did not start"
                self.failure_raised.set()
                raise OSError("injected subread failure")
            return super().read(path, byte_range)

    source = FailingReaderWithBlockedSibling()
    provider = _provider(
        [
            object_row(
                key="joined.bin",
                size_bytes=2,
                chunk_index=index,
                chunk_size_bytes=1,
                source_profile="objects",
                source_path=source_path,
                source_offset=0,
            )
            for index, source_path in enumerate(("failing.bin", "blocked.bin"))
        ],
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
        max_workers=2,
    )
    errors: list[BaseException] = []

    def read_joined_file() -> None:
        try:
            provider.get_object("joined.bin")
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)

    read_thread = threading.Thread(target=read_joined_file)
    read_thread.start()
    assert source.failure_raised.wait(timeout=5), "failing subread did not run"
    read_thread.join(timeout=0.1)
    try:
        assert read_thread.is_alive(), "logical read returned while a sibling source request was still running"
    finally:
        source.release_blocked.set()
    read_thread.join(timeout=5)

    assert not read_thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], OSError)
    assert str(errors[0]) == "injected subread failure"
    provider.close()


def test_manifest_provider_executes_one_touched_chunk_through_the_shared_read_pool() -> None:
    source = _RecordingRangeReader({"data.bin": b"abc"}, binding_identity="file:///objects")
    provider = _provider(
        _single_object_rows("data.bin", "data.bin", b"abc"),
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
    )

    assert provider._read_executor is None
    assert provider.get_object("data.bin") == b"abc"
    assert provider._read_executor is not None
    provider.close()


def test_manifest_provider_bounds_concurrent_one_chunk_reads_across_callers() -> None:
    class BlockingReader(_RecordingRangeReader):
        def __init__(self) -> None:
            super().__init__({"one.bin": b"1", "two.bin": b"2"}, binding_identity="file:///objects")
            self._active_lock = threading.Lock()
            self.active = 0
            self.maximum_active = 0
            self.started = threading.Event()
            self.multiple_active = threading.Event()
            self.release = threading.Event()

        def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
            if byte_range is not None:
                with self._active_lock:
                    self.active += 1
                    self.maximum_active = max(self.maximum_active, self.active)
                    self.started.set()
                    if self.active > 1:
                        self.multiple_active.set()
                try:
                    assert self.release.wait(timeout=5), "bounded source read was never released"
                finally:
                    with self._active_lock:
                        self.active -= 1
            return super().read(path, byte_range)

    source = BlockingReader()
    provider = _provider(
        [
            *_single_object_rows("one.bin", "one.bin", b"1"),
            *_single_object_rows("two.bin", "two.bin", b"2"),
        ],
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
        max_workers=1,
    )

    with ThreadPoolExecutor(max_workers=2) as callers:
        first = callers.submit(provider.get_object, "one.bin")
        assert source.started.wait(timeout=5), "first source read did not start"
        second = callers.submit(provider.get_object, "two.bin")
        assert not source.multiple_active.wait(timeout=0.2)
        source.release.set()
        assert first.result(timeout=5) == b"1"
        assert second.result(timeout=5) == b"2"

    assert source.maximum_active == 1
    provider.close()


def test_manifest_provider_reuses_one_bounded_read_pool_for_multi_chunk_reads() -> None:
    source = _RecordingRangeReader(
        {"one.bin": b"a", "two.bin": b"b"},
        binding_identity="file:///objects",
    )
    rows = [
        object_row(
            key="joined.bin",
            size_bytes=2,
            chunk_index=0,
            chunk_size_bytes=1,
            source_profile="objects",
            source_path="one.bin",
            source_offset=0,
        ),
        object_row(
            key="joined.bin",
            size_bytes=2,
            chunk_index=1,
            chunk_size_bytes=1,
            source_profile="objects",
            source_path="two.bin",
            source_offset=0,
        ),
    ]
    provider = _provider(rows, source_bindings={"objects": SourceBinding(source, "objects-r1")}, max_workers=2)

    assert provider.get_object("joined.bin") == b"ab"
    executor = provider._read_executor
    assert executor is not None
    assert provider.get_object("joined.bin") == b"ab"
    assert provider._read_executor is executor
    provider.close()
    assert provider._read_executor is None


def test_manifest_provider_close_is_terminal_during_a_bounded_read() -> None:
    """Closing during a read prevents later replenishment and every subsequent read deterministically."""

    class BlockingFirstReadReader(_RecordingRangeReader):
        def __init__(self) -> None:
            super().__init__(
                {"first.bin": b"A", "second.bin": b"B"},
                binding_identity="file:///objects",
            )
            self.first_started = threading.Event()
            self.release_first = threading.Event()

        def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
            if byte_range is not None and path == "first.bin":
                self.first_started.set()
                assert self.release_first.wait(timeout=5), "first read was never released"
            return super().read(path, byte_range)

    source = BlockingFirstReadReader()
    provider = _provider(
        [
            object_row(
                key="joined.bin",
                size_bytes=2,
                chunk_index=index,
                chunk_size_bytes=1,
                source_profile="objects",
                source_path=source_path,
                source_offset=0,
            )
            for index, source_path in enumerate(("first.bin", "second.bin"))
        ],
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
        max_workers=1,
    )
    errors: list[BaseException] = []

    def read_joined_file() -> None:
        try:
            provider.get_object("joined.bin")
        except BaseException as error:  # pragma: no cover - asserted below
            errors.append(error)

    read_thread = threading.Thread(target=read_joined_file)
    read_thread.start()
    assert source.first_started.wait(timeout=5), "first read did not start"

    close_threads = [threading.Thread(target=provider.close) for _ in range(2)]
    for close_thread in close_threads:
        close_thread.start()
    for close_thread in close_threads:
        close_thread.join(timeout=5)
        assert not close_thread.is_alive()

    try:
        source.release_first.set()
        read_thread.join(timeout=5)
        assert not read_thread.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], RuntimeError)
        assert str(errors[0]) == "ManifestStorageProvider is closed."
        with pytest.raises(RuntimeError, match="ManifestStorageProvider is closed"):
            provider.get_object("joined.bin")
    finally:
        source.release_first.set()
        provider.close()


def test_manifest_provider_bounds_queued_reads_to_the_worker_limit() -> None:
    """Large touched-read plans never queue one future per manifest chunk."""
    read_count = 128
    objects = {f"part-{index}.bin": bytes([index]) for index in range(read_count)}
    rows = [
        object_row(
            key="large.bin",
            size_bytes=read_count,
            chunk_index=index,
            chunk_size_bytes=1,
            source_profile="objects",
            source_path=f"part-{index}.bin",
            source_offset=0,
        )
        for index in range(read_count)
    ]
    source = _RecordingRangeReader(objects, binding_identity="file:///objects")
    provider = _provider(rows, source_bindings={"objects": SourceBinding(source, "objects-r1")}, max_workers=4)

    class TrackingExecutor(ThreadPoolExecutor):
        def __init__(self) -> None:
            super().__init__(max_workers=4)
            self._tracking_lock = threading.Lock()
            self.in_flight = 0
            self.maximum_in_flight = 0

        def submit(self, function, *args):
            with self._tracking_lock:
                self.in_flight += 1
                self.maximum_in_flight = max(self.maximum_in_flight, self.in_flight)
            future = super().submit(function, *args)

            def record_completion(_future) -> None:
                with self._tracking_lock:
                    self.in_flight -= 1

            future.add_done_callback(record_completion)
            return future

    executor = TrackingExecutor()
    provider._read_executor = executor  # type: ignore[assignment]

    assert provider.get_object("large.bin") == bytes(range(read_count))
    assert executor.maximum_in_flight <= 4
    provider.close()


def test_manifest_provider_prefix_listing_decodes_only_the_matching_row_group_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _RecordingRangeReader(
        {"target/direct.bin": b"a", "target/nested/leaf.bin": b"b"},
        binding_identity="file:///objects",
    )
    provider = _provider(
        [
            *_single_object_rows("before/00000", "before/00000", b"x"),
            *_single_object_rows("before/00001", "before/00001", b"y"),
            *_single_object_rows("target/direct.bin", "target/direct.bin", b"a"),
            *_single_object_rows("target/nested/leaf.bin", "target/nested/leaf.bin", b"b"),
            *_single_object_rows("z/00000", "z/00000", b"z"),
            *_single_object_rows("z/00001", "z/00001", b"z"),
        ],
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
        row_group_size=1,
    )
    decoded_row_groups: list[int] = []
    original = provider._catalog._iter_row_group_batches

    def track(row_group: int):
        decoded_row_groups.append(row_group)
        yield from original(row_group)

    monkeypatch.setattr(provider._catalog, "_iter_row_group_batches", track)

    entries = list(provider.list_objects("target", include_directories=True))

    assert [(entry.key, entry.type) for entry in entries] == [
        ("target/direct.bin", "file"),
        ("target/nested", "directory"),
    ]
    assert decoded_row_groups == [2, 3, 4]


@pytest.mark.parametrize(
    ("byte_range", "expected", "expected_object_calls", "expected_service_range"),
    [
        pytest.param(
            None,
            b"ABCDEFGHIJKL",
            [("first.bin", Range(10, 4)), ("second.bin", Range(20, 5))],
            Range(0, 3),
            id="whole-file",
        ),
        pytest.param(Range(0, 2), b"AB", [("first.bin", Range(10, 2))], None, id="first-chunk-prefix"),
        pytest.param(
            Range(2, 4),
            b"CDEF",
            [("first.bin", Range(12, 2)), ("second.bin", Range(20, 2))],
            None,
            id="cross-object-chunks",
        ),
        pytest.param(
            Range(3, 8),
            b"DEFGHIJK",
            [("first.bin", Range(13, 1)), ("second.bin", Range(20, 5))],
            Range(0, 2),
            id="cross-object-and-service-chunks",
        ),
        pytest.param(Range(9, 2), b"JK", [], Range(0, 2), id="service-prefix"),
        pytest.param(Range(10, 99), b"KL", [], Range(1, 2), id="clip-at-eof"),
        pytest.param(Range(0, 0), b"", [], None, id="zero-size"),
        pytest.param(Range(12, 1), b"", [], None, id="at-eof"),
        pytest.param(Range(99, 2), b"", [], None, id="beyond-eof"),
    ],
)
def test_manifest_provider_fetches_exact_physical_subranges_for_every_touched_chunk(
    byte_range: Optional[Range],
    expected: bytes,
    expected_object_calls: list[tuple[str, Range]],
    expected_service_range: Optional[Range],
) -> None:
    source = _RecordingRangeReader(
        {
            "first.bin": b"0123456789ABCD",
            "second.bin": b"x" * 20 + b"EFGHI",
        },
        binding_identity="file:///objects",
    )
    service = _RecordingServiceReader(
        {("render/tail", ()): b"JKL"},
        binding_identity="https://renderer.example/v1",
    )
    rows = [
        object_row(
            key="spanning.bin",
            size_bytes=12,
            chunk_index=0,
            chunk_size_bytes=4,
            source_profile="objects",
            source_path="first.bin",
            source_offset=10,
        ),
        object_row(
            key="spanning.bin",
            size_bytes=12,
            chunk_index=1,
            chunk_size_bytes=5,
            source_profile="objects",
            source_path="second.bin",
            source_offset=20,
        ),
        service_row(
            key="spanning.bin",
            size_bytes=12,
            chunk_index=2,
            chunk_size_bytes=3,
            service_id="renderer",
            service_path="render/tail",
            service_query=[],
        ),
    ]
    provider = _provider(
        rows,
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
        service_bindings={"renderer": ServiceBinding(service, "renderer-r1")},
        row_group_size=3,
    )

    assert provider.get_object("spanning.bin", byte_range) == expected
    actual_object_calls = sorted(
        (path, requested_range) for path, requested_range in source.calls if requested_range is not None
    )
    assert actual_object_calls == sorted(expected_object_calls)
    assert [(call[0], call[1], call[2], call[3]) for call in service.calls] == (
        [] if expected_service_range is None else [("render/tail", (), expected_service_range, 3)]
    )


def test_manifest_provider_coalesces_only_contiguous_object_ranges_from_the_same_binding() -> None:
    source = _RecordingRangeReader({"blob.bin": b"abcdefghi"}, binding_identity="file:///objects")
    query = (("variant", "mobile"),)
    service = _RecordingServiceReader(
        {
            ("render/one", query): b"G",
            ("render/two", query): b"H",
        },
        binding_identity="https://renderer.example/v1",
    )
    rows = [
        object_row(
            key="joined.bin",
            size_bytes=7,
            chunk_index=0,
            chunk_size_bytes=2,
            source_profile="objects-a",
            source_path="blob.bin",
            source_offset=0,
        ),
        object_row(
            key="joined.bin",
            size_bytes=7,
            chunk_index=1,
            chunk_size_bytes=2,
            source_profile="objects-a",
            source_path="blob.bin",
            source_offset=2,
        ),
        object_row(
            key="joined.bin",
            size_bytes=7,
            chunk_index=2,
            chunk_size_bytes=1,
            source_profile="objects-a",
            source_path="blob.bin",
            source_offset=8,
        ),
        service_row(
            key="joined.bin",
            size_bytes=7,
            chunk_index=3,
            chunk_size_bytes=1,
            service_id="renderer",
            service_path="render/one",
            service_query=[{"name": "variant", "value": "mobile"}],
        ),
        service_row(
            key="joined.bin",
            size_bytes=7,
            chunk_index=4,
            chunk_size_bytes=1,
            service_id="renderer",
            service_path="render/two",
            service_query=[{"name": "variant", "value": "mobile"}],
        ),
    ]
    provider = _provider(
        rows,
        source_bindings={"objects-a": SourceBinding(source, "objects-r1")},
        service_bindings={"renderer": ServiceBinding(service, "renderer-r1")},
    )

    assert provider.get_object("joined.bin") == b"abcdiGH"
    object_ranges = sorted(
        (call_range.offset, call_range.size) for _, call_range in source.calls if call_range is not None
    )
    assert object_ranges == [(0, 4), (8, 1)]
    assert [(call[0], call[2]) for call in service.calls] == [
        ("render/one", Range(offset=0, size=1)),
        ("render/two", Range(offset=0, size=1)),
    ]


@pytest.mark.parametrize("boundary", ["alias", "path"])
def test_manifest_provider_does_not_coalesce_contiguous_ranges_across_binding_boundaries(boundary: str) -> None:
    if boundary == "alias":
        objects = {"blob.bin": b"abcd"}
        first_alias, second_alias = "objects-a", "objects-b"
        first_path = second_path = "blob.bin"
        first_offset, second_offset = 0, 2
        readers = [_RecordingRangeReader(objects, binding_identity="file:///objects")]
        bindings = {
            "objects-a": SourceBinding(readers[0], "objects-r1"),
            "objects-b": SourceBinding(readers[0], "objects-r1"),
        }
    else:
        objects = {"first.bin": b"ab", "second.bin": b"__cd"}
        first_alias = second_alias = "objects"
        first_path, second_path = "first.bin", "second.bin"
        first_offset, second_offset = 0, 2
        readers = [_RecordingRangeReader(objects, binding_identity="file:///objects")]
        bindings = {"objects": SourceBinding(readers[0], "objects-r1")}

    rows = [
        object_row(
            key="joined.bin",
            size_bytes=4,
            chunk_index=0,
            chunk_size_bytes=2,
            source_profile=first_alias,
            source_path=first_path,
            source_offset=first_offset,
        ),
        object_row(
            key="joined.bin",
            size_bytes=4,
            chunk_index=1,
            chunk_size_bytes=2,
            source_profile=second_alias,
            source_path=second_path,
            source_offset=second_offset,
        ),
    ]
    provider = _provider(rows, source_bindings=bindings)

    assert provider.get_object("joined.bin") == b"abcd"
    calls = [(path, byte_range) for reader in readers for path, byte_range in reader.calls if byte_range is not None]
    assert sorted(calls, key=lambda call: (call[0], call[1].offset)) == sorted(
        [
            (first_path, Range(first_offset, 2)),
            (second_path, Range(second_offset, 2)),
        ],
        key=lambda call: (call[0], call[1].offset),
    )


def test_manifest_provider_rejects_short_object_and_service_results() -> None:
    short_source = _RecordingRangeReader(
        {"short.bin": b"data"},
        binding_identity="file:///objects",
        truncate_ranges=True,
    )
    object_provider = _provider(
        _single_object_rows("object.bin", "short.bin", b"data"),
        source_bindings={"objects": SourceBinding(short_source, "objects-r1")},
    )
    with pytest.raises(IOError, match="expected.*4.*received.*3|3.*expected.*4"):
        object_provider.get_object("object.bin")

    query: tuple[tuple[str, str], ...] = ()
    short_service = _RecordingServiceReader(
        {("render/short", query): b"data"},
        binding_identity="https://renderer.example/v1",
        truncate_ranges=True,
    )
    service_provider = _provider(
        [
            service_row(
                key="service.bin",
                size_bytes=4,
                chunk_size_bytes=4,
                service_id="renderer",
                service_path="render/short",
                service_query=[],
            )
        ],
        service_bindings={"renderer": ServiceBinding(short_service, "renderer-r1")},
    )
    with pytest.raises(IOError, match="expected.*4.*received.*3|3.*expected.*4"):
        service_provider.get_object("service.bin")


@pytest.mark.parametrize("source_kind", ["object", "coalesced-object", "service"])
def test_manifest_provider_rejects_overlong_range_results(source_kind: str) -> None:
    if source_kind == "service":
        service = _RecordingServiceReader(
            {("render/long", ()): b"data"},
            binding_identity="https://renderer.example/v1",
            extend_ranges=True,
        )
        provider = _provider(
            [
                service_row(
                    key="overlong.bin",
                    size_bytes=4,
                    chunk_size_bytes=4,
                    service_id="renderer",
                    service_path="render/long",
                    service_query=[],
                )
            ],
            service_bindings={"renderer": ServiceBinding(service, "renderer-r1")},
        )
        expected_calls = service.calls
    else:
        source = _RecordingRangeReader(
            {"long.bin": b"data"},
            binding_identity="file:///objects",
            extend_ranges=True,
        )
        rows = _single_object_rows("overlong.bin", "long.bin", b"data")
        if source_kind == "coalesced-object":
            rows = [
                object_row(
                    key="overlong.bin",
                    size_bytes=4,
                    chunk_index=0,
                    chunk_size_bytes=2,
                    source_profile="objects",
                    source_path="long.bin",
                    source_offset=0,
                ),
                object_row(
                    key="overlong.bin",
                    size_bytes=4,
                    chunk_index=1,
                    chunk_size_bytes=2,
                    source_profile="objects",
                    source_path="long.bin",
                    source_offset=2,
                ),
            ]
        provider = _provider(
            rows,
            source_bindings={"objects": SourceBinding(source, "objects-r1")},
        )
        expected_calls = source.calls

    with pytest.raises(IOError, match="expected.*4.*received.*5|5.*expected.*4"):
        provider.get_object("overlong.bin")

    assert len(expected_calls) == 1


def test_manifest_provider_defers_a_late_invalid_row_without_source_reads_or_partial_side_effects() -> None:
    source = _RecordingRangeReader(
        {"valid.bin": b"abc"},
        binding_identity="file:///objects",
    )
    service = _RecordingServiceReader(
        {("render/valid", ()): b"xyz"},
        binding_identity="https://renderer.example/v1",
        allowed_path_prefixes=("render/",),
        allowed_query_parameters=("variant",),
    )
    rows = [
        *_single_object_rows("00-valid-object.bin", "valid.bin", b"abc"),
        service_row(
            key="01-valid-service.bin",
            service_id="renderer",
            service_path="render/valid",
            service_query=[],
        ),
        service_row(
            key="99-invalid-service.bin",
            service_id="renderer",
            service_path="admin/secrets",
            service_query=[{"name": "token", "value": "secret"}],
        ),
    ]

    provider = _provider(
        rows,
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
        service_bindings={"renderer": ServiceBinding(service, "renderer-r1")},
    )

    assert service.validations == []
    assert provider.get_object("00-valid-object.bin") == b"abc"
    source.calls.clear()

    with pytest.raises(ManifestValidationError, match="not allowlisted"):
        provider.get_object("99-invalid-service.bin")

    assert service.validations == [("admin/secrets", (("token", "secret"),))]
    assert source.calls == []
    assert service.calls == []


def test_manifest_etag_tracks_used_locations_revisions_and_manifest_content_only() -> None:
    content = b"data"

    def etag(
        *,
        used_identity: str = "file:///objects",
        used_revision: str = "objects-r1",
        unused_revision: str = "unused-r1",
        metadata: Optional[dict[str, str]] = None,
    ) -> str:
        used = _RecordingRangeReader({"data.bin": content}, binding_identity=used_identity)
        unused = _RecordingRangeReader({}, binding_identity="file:///unused")
        rows = _single_object_rows("data.bin", "data.bin", content)
        rows[0]["metadata"] = metadata or {"kind": "first"}
        provider = _provider(
            rows,
            source_bindings={
                "objects": SourceBinding(used, used_revision),
                "unused": SourceBinding(unused, unused_revision),
            },
        )
        value = provider.get_object_metadata("data.bin").etag
        assert value is not None
        return value

    baseline = etag()
    assert etag(unused_revision="unused-r2") == baseline
    assert etag(used_revision="objects-r2") != baseline
    assert etag(used_identity="file:///other-objects") != baseline
    assert etag(metadata={"kind": "second"}) != baseline


def test_manifest_download_streams_eight_mib_windows_and_atomically_replaces_paths(tmp_path: Path) -> None:
    content = os.urandom(2 * _DOWNLOAD_WINDOW_SIZE + 1)
    source = _RecordingRangeReader({"large.bin": content}, binding_identity="file:///objects")
    provider = _provider(
        _single_object_rows("large.bin", "large.bin", content),
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
    )
    destination = tmp_path / "download.bin"
    destination.write_bytes(b"old-content")

    provider.download_file("large.bin", str(destination))

    assert destination.read_bytes() == content
    assert [byte_range for _, byte_range in source.calls if byte_range is not None] == [
        Range(offset=0, size=_DOWNLOAD_WINDOW_SIZE),
        Range(offset=_DOWNLOAD_WINDOW_SIZE, size=_DOWNLOAD_WINDOW_SIZE),
        Range(offset=2 * _DOWNLOAD_WINDOW_SIZE, size=1),
    ]
    assert set(tmp_path.iterdir()) == {destination}


def test_manifest_download_preserves_destination_and_cleans_temp_file_on_failure(tmp_path: Path) -> None:
    content = b"x" * (2 * _DOWNLOAD_WINDOW_SIZE)
    source = _RecordingRangeReader(
        {"large.bin": content},
        binding_identity="file:///objects",
        fail_on_range_call=2,
    )
    provider = _provider(
        _single_object_rows("large.bin", "large.bin", content),
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
    )
    destination = tmp_path / "download.bin"
    destination.write_bytes(b"old-content")

    with pytest.raises(OSError, match="injected range-read failure"):
        provider.download_file("large.bin", str(destination))

    assert destination.read_bytes() == b"old-content"
    assert set(tmp_path.iterdir()) == {destination}


def test_manifest_download_accepts_only_binary_file_like_destinations() -> None:
    content = b"binary-data"
    source = _RecordingRangeReader({"data.bin": content}, binding_identity="file:///objects")
    provider = _provider(
        _single_object_rows("data.bin", "data.bin", content),
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
    )
    destination = io.BytesIO()

    provider.download_file("data.bin", destination)

    assert destination.getvalue() == content
    with pytest.raises(TypeError, match="binary"):
        provider.download_file("data.bin", io.StringIO())


def test_manifest_file_like_download_accepts_a_writer_that_returns_none_after_consuming_a_window() -> None:
    content = b"binary-data"
    source = _RecordingRangeReader({"data.bin": content}, binding_identity="file:///objects")
    provider = _provider(
        _single_object_rows("data.bin", "data.bin", content),
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
    )

    class NoneWritingDestination:
        def __init__(self) -> None:
            self.writes: list[bytes] = []
            self.data = bytearray()

        def write(self, data: bytes) -> None:
            payload = bytes(data)
            self.writes.append(payload)
            self.data.extend(payload)

    destination = NoneWritingDestination()

    provider.download_file("data.bin", cast(Any, destination))

    assert bytes(destination.data) == content
    assert destination.writes == [content]


def test_manifest_file_like_download_rejects_a_writer_that_returns_zero_progress() -> None:
    content = b"binary-data"
    source = _RecordingRangeReader({"data.bin": content}, binding_identity="file:///objects")
    provider = _provider(
        _single_object_rows("data.bin", "data.bin", content),
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
    )

    class ZeroWritingDestination:
        def __init__(self) -> None:
            self.writes: list[bytes] = []

        def write(self, data: bytes) -> int:
            self.writes.append(bytes(data))
            return 0

    destination = ZeroWritingDestination()

    with pytest.raises(OSError, match="no progress"):
        provider.download_file("data.bin", cast(Any, destination))

    assert destination.writes == [content]


def test_manifest_file_like_download_retries_positive_short_writes() -> None:
    content = b"binary-data"
    source = _RecordingRangeReader({"data.bin": content}, binding_identity="file:///objects")
    provider = _provider(
        _single_object_rows("data.bin", "data.bin", content),
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
    )

    class PrefixWritingDestination:
        def __init__(self) -> None:
            self.data = bytearray()

        def write(self, data: bytes) -> int:
            accepted = max(1, len(data) // 2)
            self.data.extend(data[:accepted])
            return accepted

    destination = PrefixWritingDestination()

    provider.download_file("data.bin", cast(Any, destination))

    assert bytes(destination.data) == content


def test_manifest_file_like_download_publishes_once_after_a_single_client_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retry after a later source window cannot leave a duplicated prefix in the caller's writer."""
    content = b"x" * (2 * _DOWNLOAD_WINDOW_SIZE)
    source = _RecordingRangeReader(
        {"large.bin": content},
        binding_identity="file:///objects",
        retryable_fail_on_range_call=2,
    )
    provider = _provider(
        _single_object_rows("large.bin", "large.bin", content),
        source_bindings={"objects": SourceBinding(source, "objects-r1")},
    )
    client = SingleStorageClient.__new__(SingleStorageClient)
    client._metadata_provider = None
    client._replica_manager = None
    client._storage_provider = provider
    client._retry_config = RetryConfig(attempts=2, delay=0, backoff_multiplier=1)
    destination = io.BytesIO()
    monkeypatch.setattr("multistorageclient.retry.sleep_before_retry", lambda retry_config, attempt: None)

    client.download_file("large.bin", destination)

    assert destination.getvalue() == content
    assert [byte_range for _, byte_range in source.calls if byte_range is not None] == [
        Range(offset=0, size=_DOWNLOAD_WINDOW_SIZE),
        Range(offset=_DOWNLOAD_WINDOW_SIZE, size=_DOWNLOAD_WINDOW_SIZE),
        Range(offset=0, size=_DOWNLOAD_WINDOW_SIZE),
        Range(offset=_DOWNLOAD_WINDOW_SIZE, size=_DOWNLOAD_WINDOW_SIZE),
    ]


@pytest.mark.parametrize(
    "operation",
    [
        lambda provider: provider.put_object("new.bin", b"data"),
        lambda provider: provider.copy_object("source.bin", "copy.bin"),
        lambda provider: provider.delete_object("source.bin"),
        lambda provider: provider.delete_objects(["source.bin", "other.bin"]),
        lambda provider: provider.make_symlink("link.bin", "source.bin"),
        lambda provider: provider.upload_file("new.bin", io.BytesIO(b"data")),
        lambda provider: provider.generate_presigned_url("source.bin"),
    ],
    ids=["put", "copy", "delete", "delete-many", "symlink", "upload", "presigned-url"],
)
def test_manifest_provider_rejects_mutations_and_presigned_urls(operation: Any) -> None:
    provider = _provider([])

    assert provider.is_read_only is True
    assert provider.default_read_prefetch is False
    with pytest.raises(NotImplementedError, match="read-only|presigned URL"):
        operation(provider)


def test_legacy_manifest_metadata_provider_does_not_reinterpret_direct_parquet(tmp_path: Path) -> None:
    manifest_path = tmp_path / "catalog.parquet"
    manifest_path.write_bytes(write_manifest([]).getvalue())
    storage = PosixFileStorageProvider(base_path=str(tmp_path))

    with pytest.raises(NotImplementedError, match="Manifest file type parquet is not supported"):
        ManifestMetadataProvider(storage_provider=storage, manifest_path=manifest_path.name)


class _HTTPRangeServer(ThreadingHTTPServer):
    payloads: dict[tuple[str, tuple[tuple[str, str], ...]], bytes]
    requests: list[tuple[str, tuple[tuple[str, str], ...], str, Optional[str]]]
    errors: list[str]
    failures_remaining: int
    failure_lock: threading.Lock


class _SeekRecordingFile:
    def __init__(self, file: Any) -> None:
        self._file = file
        self.seek_calls: list[tuple[int, int]] = []

    def read(self, size: int = -1) -> bytes:
        return self._file.read(size)

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        self.seek_calls.append((offset, whence))
        return self._file.seek(offset, whence)

    def tell(self) -> int:
        return self._file.tell()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._file, name)


class _RangeRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802
        server = self.server
        assert isinstance(server, _HTTPRangeServer)
        parsed = urlsplit(self.path)
        query = tuple(parse_qsl(parsed.query, keep_blank_values=True))
        range_header = self.headers.get("Range")
        server.requests.append((parsed.path, query, range_header or "", self.headers.get("X-Manifest-Test")))

        payload = server.payloads.get((parsed.path, query))
        if payload is None or range_header is None or not range_header.startswith("bytes="):
            server.errors.append(f"invalid request: {self.path} {range_header}")
            self.send_error(400)
            return

        with server.failure_lock:
            if server.failures_remaining:
                server.failures_remaining -= 1
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

        try:
            start_text, end_text = range_header.removeprefix("bytes=").split("-", 1)
            start = int(start_text)
            end = int(end_text)
        except (TypeError, ValueError):
            server.errors.append(f"invalid range: {range_header}")
            self.send_error(400)
            return

        body = payload[start : end + 1]
        self.send_response(206)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return


@contextmanager
def _serve_ranges(
    payloads: Mapping[tuple[str, tuple[tuple[str, str], ...]], bytes],
    *,
    failures_before_success: int = 0,
) -> Iterator[tuple[str, _HTTPRangeServer]]:
    server = _HTTPRangeServer(("127.0.0.1", 0), _RangeRequestHandler)
    server.payloads = dict(payloads)
    server.requests = []
    server.errors = []
    server.failures_remaining = failures_before_success
    server.failure_lock = threading.Lock()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    server_address = server.server_address
    host = cast(str, server_address[0])
    port = cast(int, server_address[1])
    try:
        yield f"http://{host}:{port}", server
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def _manifest_config(
    *,
    manifest_root: Path,
    source_root: Path,
    source_revision: str = "objects-r1",
    service_base_url: Optional[str] = None,
    cache_root: Optional[Path] = None,
) -> dict[str, Any]:
    source_profile: dict[str, Any] = {
        "storage_provider": {"type": "file", "options": {"base_path": str(source_root)}},
        "retry": {"attempts": 2, "delay": 0, "backoff_multiplier": 1},
    }
    manifest_options: dict[str, Any] = {
        "manifest_storage_profile": "manifest-store",
        "manifest_path": "catalog.parquet",
        "max_workers": 3,
        "source_profiles": {
            "objects": {"profile": "object-store", "binding_revision": source_revision},
        },
    }
    if service_base_url is not None:
        manifest_options["services"] = {
            "renderer": {
                "type": "http",
                "options": {
                    "base_url": service_base_url,
                    "binding_revision": "renderer-r1",
                    "allowed_path_prefixes": ["render/"],
                    "allowed_query_parameters": ["variant", "frame"],
                    "headers": {"X-Manifest-Test": "configured"},
                    "allow_insecure_http": True,
                },
            }
        }

    virtual_profile: dict[str, Any] = {
        "storage_provider": {"type": "manifest", "options": manifest_options},
    }
    config: dict[str, Any] = {
        "profiles": {
            "manifest-store": {"storage_provider": {"type": "file", "options": {"base_path": str(manifest_root)}}},
            "object-store": source_profile,
            "virtual": virtual_profile,
        }
    }
    if cache_root is not None:
        virtual_profile["caching_enabled"] = True
        config["cache"] = {
            "size": "16M",
            "cache_line_size": "1M",
            "location": str(cache_root),
            "check_source_version": True,
            "eviction_policy": {"policy": "fifo"},
        }
    return config


def _write_integration_manifest(manifest_root: Path, source_root: Path, service_suffix: bytes) -> dict[str, bytes]:
    mp4_prefix = b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00"
    table_buffer = io.BytesIO()
    pq.write_table(
        pa.table(
            {
                "id": [1, 2],
                "value": ["first", "second"],
                "payload": [b"a" * 80_000, b"b" * 80_000],
            }
        ),
        table_buffer,
        row_group_size=1,
        compression="NONE",
        use_dictionary=False,
    )
    parquet_bytes = table_buffer.getvalue()
    jsonl_bytes = ('{"id":1,"message":"snowman ☃"}\n{"id":2,"message":"café"}\n').encode("utf-8")
    multibyte_start = jsonl_bytes.index("☃".encode("utf-8"))
    jsonl_split = multibyte_start + 1

    source_root.mkdir()
    manifest_root.mkdir()
    (source_root / "clip-prefix.bin").write_bytes(mp4_prefix)
    (source_root / "actual.parquet").write_bytes(parquet_bytes)
    (source_root / "events.jsonl").write_bytes(jsonl_bytes)

    mp4_size = len(mp4_prefix) + len(service_suffix)
    rows = [
        object_row(
            key="events/events.jsonl",
            size_bytes=len(jsonl_bytes),
            content_type="application/x-ndjson",
            metadata={"kind": "events"},
            chunk_index=0,
            chunk_size_bytes=jsonl_split,
            source_profile="objects",
            source_path="events.jsonl",
            source_offset=0,
        ),
        object_row(
            key="events/events.jsonl",
            size_bytes=len(jsonl_bytes),
            content_type="application/x-ndjson",
            metadata={"kind": "events"},
            chunk_index=1,
            chunk_size_bytes=len(jsonl_bytes) - jsonl_split,
            source_profile="objects",
            source_path="events.jsonl",
            source_offset=jsonl_split,
        ),
        object_row(
            key="media/clip.mp4",
            size_bytes=mp4_size,
            content_type="video/mp4",
            metadata={"kind": "video"},
            chunk_index=0,
            chunk_size_bytes=len(mp4_prefix),
            source_profile="objects",
            source_path="clip-prefix.bin",
            source_offset=0,
        ),
        service_row(
            key="media/clip.mp4",
            size_bytes=mp4_size,
            content_type="video/mp4",
            metadata={"kind": "video"},
            chunk_index=1,
            chunk_size_bytes=len(service_suffix),
            service_id="renderer",
            service_path="render/clip",
            service_query=[
                {"name": "variant", "value": "mobile"},
                {"name": "frame", "value": "7"},
            ],
        ),
        *_single_object_rows(
            "tables/actual.parquet",
            "actual.parquet",
            parquet_bytes,
            metadata={"kind": "table"},
            content_type="application/vnd.apache.parquet",
        ),
    ]
    (manifest_root / "catalog.parquet").write_bytes(write_manifest(rows).getvalue())
    return {
        "mp4": mp4_prefix + service_suffix,
        "mp4_prefix": mp4_prefix,
        "parquet": parquet_bytes,
        "jsonl": jsonl_bytes,
        "jsonl_head": jsonl_bytes[:jsonl_split],
    }


def test_manifest_fsspec_distinguishes_an_exact_file_from_its_child_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An exact file wins at its bare path while its trailing-slash prefix remains traversable."""
    source_root = tmp_path / "objects"
    manifest_root = tmp_path / "manifests"
    source_root.mkdir()
    manifest_root.mkdir()
    (source_root / "a.bin").write_bytes(b"one")
    (source_root / "a-b.bin").write_bytes(b"two")
    (manifest_root / "catalog.parquet").write_bytes(
        write_manifest(
            [
                object_row(key="a", source_profile="objects", source_path="a.bin", source_offset=0),
                object_row(key="a/b", source_profile="objects", source_path="a-b.bin", source_offset=0),
            ]
        ).getvalue()
    )
    config_dict = _manifest_config(manifest_root=manifest_root, source_root=source_root)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config_dict), encoding="utf-8")
    monkeypatch.setenv("MSC_CONFIG", str(config_path))
    monkeypatch.setattr(config_module, "read_rclone_config", lambda: ({}, None))
    with shortcuts._STORAGE_CLIENT_CACHE_LOCK:
        shortcuts._STORAGE_CLIENT_CACHE.clear()

    filesystem = fsspec.filesystem("msc", skip_instance_cache=True)

    assert filesystem.info("virtual/a")["type"] == "file"
    assert filesystem.info("virtual/a/")["type"] == "directory"
    assert filesystem.isfile("virtual/a")
    assert filesystem.isdir("virtual/a/")
    assert filesystem.cat_file("virtual/a") == b"one"
    assert filesystem.cat_file("virtual/a/b") == b"two"
    assert filesystem.ls("virtual", detail=False) == ["virtual/a"]
    assert filesystem.ls("virtual/a", detail=False) == ["virtual/a/b"]
    assert filesystem.ls("virtual/a/", detail=False) == ["virtual/a/b"]
    assert filesystem.glob("virtual/a") == ["virtual/a"]
    assert filesystem.glob("virtual/a/*") == ["virtual/a/b"]


def test_manifest_config_defers_unused_row_validation_until_exact_access(tmp_path: Path) -> None:
    """Construction and an unrelated read succeed before an invalid logical file is touched."""
    source_root = tmp_path / "objects"
    manifest_root = tmp_path / "manifests"
    source_root.mkdir()
    manifest_root.mkdir()
    (source_root / "good.bin").write_bytes(b"good")
    rows = [
        object_row(
            key="good.bin",
            size_bytes=4,
            chunk_size_bytes=4,
            source_profile="objects",
            source_path="good.bin",
            source_offset=0,
        ),
        object_row(
            key="unused-invalid.bin",
            source_profile="objects",
            source_path="missing.bin",
            source_offset=-1,
        ),
    ]
    (manifest_root / "catalog.parquet").write_bytes(write_manifest(rows, row_group_size=2).getvalue())

    client = StorageClient(
        StorageClientConfig.from_dict(
            _manifest_config(manifest_root=manifest_root, source_root=source_root),
            profile="virtual",
        )
    )

    assert client.read("good.bin") == b"good"
    with pytest.raises(ManifestValidationError, match="source offset"):
        client.read("unused-invalid.bin")


def test_manifest_config_reconstructs_mixed_virtual_files_with_local_ranges_and_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "objects"
    manifest_root = tmp_path / "manifests"
    service_suffix = b"\x00\x00\x00\x09mdatFRAME"
    expected_query = (("variant", "mobile"), ("frame", "7"))
    payloads = {("/render/clip", expected_query): service_suffix}
    original_import_class = config_module.import_class

    def import_class(
        class_name: str,
        module_name: str,
        package_name: Optional[str] = None,
    ) -> type:
        if class_name == "PosixFileStorageProvider":
            return _InstrumentedPosixStorageProvider
        return original_import_class(class_name, module_name, package_name)

    _InstrumentedPosixStorageProvider.reset_calls()
    monkeypatch.setattr(config_module, "import_class", import_class)

    with _serve_ranges(payloads) as (base_url, server):
        expected = _write_integration_manifest(manifest_root, source_root, service_suffix)
        config_dict = _manifest_config(
            manifest_root=manifest_root,
            source_root=source_root,
            service_base_url=base_url,
        )
        client = StorageClient(StorageClientConfig.from_dict(config_dict, profile="virtual"))
        setup_msc_config(config_dict)

        assert isinstance(client._storage_provider, ManifestStorageProvider)
        assert client.read("media/clip.mp4") == expected["mp4"]
        cross_boundary_offset = len(expected["mp4_prefix"]) - 2
        assert (
            client.read("media/clip.mp4", Range(offset=cross_boundary_offset, size=7))
            == expected["mp4"][cross_boundary_offset : cross_boundary_offset + 7]
        )

        filesystem = fsspec.filesystem("msc", skip_instance_cache=True)
        with filesystem.open(
            "virtual/tables/actual.parquet",
            "rb",
            buffering=31,
            prefetch_file=False,
        ) as parquet_stream:
            seek_recording_stream = _SeekRecordingFile(parquet_stream)
            parquet_table = pq.ParquetFile(seek_recording_stream).read()
        assert parquet_table.select(["id", "value"]).to_pydict() == {
            "id": [1, 2],
            "value": ["first", "second"],
        }
        assert [len(payload) for payload in parquet_table.column("payload").to_pylist()] == [80_000, 80_000]
        assert len(seek_recording_stream.seek_calls) >= 2
        assert any(whence == os.SEEK_END for _, whence in seek_recording_stream.seek_calls)
        parquet_source_path = str(source_root / "actual.parquet")
        parquet_source_ranges = [
            byte_range
            for path, byte_range in _InstrumentedPosixStorageProvider.get_calls
            if path == parquet_source_path
        ]
        assert len(parquet_source_ranges) >= 2
        assert all(
            0 < byte_range.size < len(expected["parquet"])
            for byte_range in parquet_source_ranges
            if byte_range is not None
        )
        source_get_calls = [
            (path, byte_range)
            for path, byte_range in _InstrumentedPosixStorageProvider.get_calls
            if Path(path).is_relative_to(source_root)
        ]
        assert all(byte_range is not None for _, byte_range in source_get_calls)
        assert not [
            path for path in _InstrumentedPosixStorageProvider.download_calls if Path(path).is_relative_to(source_root)
        ]

        with client.open(
            "events/events.jsonl",
            "r",
            encoding="utf-8",
            buffering=1,
            prefetch_file=False,
        ) as jsonl_stream:
            events = [json.loads(line) for line in jsonl_stream]
        assert events == [
            {"id": 1, "message": "snowman ☃"},
            {"id": 2, "message": "café"},
        ]
        jsonl_split = len(expected["jsonl_head"])
        assert expected["jsonl_head"][-1] & 0xC0 == 0xC0
        assert expected["jsonl"][jsonl_split] & 0xC0 == 0x80
        assert client.info("media/clip.mp4").metadata == {"kind": "video"}
        assert [item.key for item in client.list()] == [
            "events/events.jsonl",
            "media/clip.mp4",
            "tables/actual.parquet",
        ]

        reconstructed = pickle.loads(pickle.dumps(client))
        assert reconstructed.read("events/events.jsonl") == expected["jsonl"]

    assert server.errors == []
    assert server.requests
    assert all(request[1] == expected_query for request in server.requests)
    assert all(request[3] == "configured" for request in server.requests)


def test_manifest_http_service_range_retries_a_fail_once_response_end_to_end(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "objects"
    manifest_root = tmp_path / "manifests"
    service_suffix = b"retryable-service-bytes"
    expected_query = (("variant", "mobile"), ("frame", "7"))
    payloads = {("/render/clip", expected_query): service_suffix}
    monkeypatch.setattr("multistorageclient.retry.sleep_before_retry", lambda retry_config, attempt: None)

    with _serve_ranges(payloads, failures_before_success=1) as (base_url, server):
        expected = _write_integration_manifest(manifest_root, source_root, service_suffix)
        config_dict = _manifest_config(
            manifest_root=manifest_root,
            source_root=source_root,
            service_base_url=base_url,
        )
        config_dict["profiles"]["virtual"]["retry"] = {"attempts": 2, "delay": 0, "backoff_multiplier": 1}
        client = StorageClient(StorageClientConfig.from_dict(config_dict, profile="virtual"))

        assert client.read("media/clip.mp4") == expected["mp4"]

    assert server.errors == []
    assert len(server.requests) == 2
    assert all(request[0] == "/render/clip" for request in server.requests)


@pytest.mark.parametrize(
    ("provider_type", "endpoint_url", "rust_options"),
    [
        ("s3", "http://s3.example.test", {"allow_http": True}),
        ("s8k", "https://pdx.s8k.example.test", {}),
    ],
)
def test_manifest_factory_reads_from_a_rust_backed_object_source_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provider_type: str,
    endpoint_url: str,
    rust_options: dict[str, Any],
) -> None:
    manifest_root = tmp_path / "manifests"
    manifest_root.mkdir()
    rows = [
        object_row(
            key="virtual.bin",
            size_bytes=4,
            chunk_size_bytes=4,
            source_profile="objects",
            source_path="payload.bin",
            source_offset=1,
        )
    ]
    (manifest_root / "catalog.parquet").write_bytes(write_manifest(rows).getvalue())
    rust_clients: list[_RustRangeClient] = []
    received_rust_options: list[dict[str, Any]] = []

    monkeypatch.setattr(S3StorageProvider, "_create_s3_client", lambda self, **kwargs: MagicMock())

    def create_rust_client(_provider: S3StorageProvider, options: Optional[dict[str, Any]] = None) -> _RustRangeClient:
        received_rust_options.append(dict(options or {}))
        rust_client = _RustRangeClient({"prefix/payload.bin": b"_rust_"})
        rust_clients.append(rust_client)
        return rust_client

    monkeypatch.setattr(S3StorageProvider, "_create_rust_client", create_rust_client)
    config_dict = {
        "profiles": {
            "manifest-store": {"storage_provider": {"type": "file", "options": {"base_path": str(manifest_root)}}},
            "object-store": {
                "storage_provider": {
                    "type": provider_type,
                    "options": {
                        "base_path": "bucket/prefix",
                        "endpoint_url": endpoint_url,
                        "region_name": "us-west-2",
                        "rust_client": rust_options,
                    },
                },
                "credentials_provider": {
                    "type": "S3Credentials",
                    "options": {"access_key": "test-access", "secret_key": "test-secret"},
                },
            },
            "virtual": {
                "storage_provider": {
                    "type": "manifest",
                    "options": {
                        "manifest_storage_profile": "manifest-store",
                        "manifest_path": "catalog.parquet",
                        "source_profiles": {"objects": {"profile": "object-store", "binding_revision": "objects-r1"}},
                    },
                }
            },
        }
    }

    client = StorageClient(StorageClientConfig.from_dict(config_dict, profile="virtual"))

    assert client.read("virtual.bin") == b"rust"
    assert len(rust_clients) == 1
    assert rust_clients[0].calls == [("prefix/payload.bin", Range(1, 4))]
    assert received_rust_options == [rust_options]


def test_manifest_factory_preserves_credentials_and_retry_for_manifest_and_source_profiles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy_id = str(tmp_path)
    rows = _single_object_rows("virtual.bin", "payload.bin", b"payload")
    _AuthenticatedFailOnceStorageProvider.datasets[(spy_id, "manifest")] = {
        "catalog.parquet": write_manifest(rows).getvalue()
    }
    _AuthenticatedFailOnceStorageProvider.datasets[(spy_id, "source")] = {"payload.bin": b"payload"}
    original_import_class = config_module.import_class

    def import_class(
        class_name: str,
        module_name: str,
        package_name: Optional[str] = None,
    ) -> type:
        if class_name == "PosixFileStorageProvider":
            return _AuthenticatedFailOnceStorageProvider
        return original_import_class(class_name, module_name, package_name)

    monkeypatch.setattr(config_module, "import_class", import_class)
    monkeypatch.setattr("multistorageclient.retry.sleep_before_retry", lambda retry_config, attempt: None)
    credential_type = f"{__name__}._CredentialSpy"

    def config_for(credential_set: str) -> dict[str, Any]:
        return {
            "profiles": {
                "manifest-store": {
                    "storage_provider": {
                        "type": "file",
                        "options": {"base_path": "", "spy_id": spy_id, "role": "manifest"},
                    },
                    "credentials_provider": {
                        "type": credential_type,
                        "options": {
                            "spy_id": spy_id,
                            "role": "manifest",
                            "credential_set": credential_set,
                        },
                    },
                    "retry": {"attempts": 2, "delay": 0, "backoff_multiplier": 1},
                },
                "object-store": {
                    "storage_provider": {
                        "type": "file",
                        "options": {"base_path": "", "spy_id": spy_id, "role": "source"},
                    },
                    "credentials_provider": {
                        "type": credential_type,
                        "options": {
                            "spy_id": spy_id,
                            "role": "source",
                            "credential_set": credential_set,
                        },
                    },
                    "retry": {"attempts": 2, "delay": 0, "backoff_multiplier": 1},
                },
                "virtual": {
                    "storage_provider": {
                        "type": "manifest",
                        "options": {
                            "manifest_storage_profile": "manifest-store",
                            "manifest_path": "catalog.parquet",
                            "source_profiles": {
                                "objects": {
                                    "profile": "object-store",
                                    "binding_revision": "objects-r1",
                                }
                            },
                        },
                    },
                    "retry": {"attempts": 1, "delay": 0, "backoff_multiplier": 1},
                },
            }
        }

    try:
        synthetic_etags: list[str] = []
        for credential_set in ("rotation-a", "rotation-b"):
            client = StorageClient(StorageClientConfig.from_dict(config_for(credential_set), profile="virtual"))
            synthetic_etag = client.info("virtual.bin").etag
            assert synthetic_etag is not None
            synthetic_etags.append(synthetic_etag)
            assert client.read("virtual.bin") == b"payload"

            manifest_provider = _AuthenticatedFailOnceStorageProvider.instances[(spy_id, "manifest")]
            source_provider = _AuthenticatedFailOnceStorageProvider.instances[(spy_id, "source")]
            assert len(manifest_provider.read_attempts) > 2
            assert manifest_provider.read_attempts[0] == manifest_provider.read_attempts[1]
            assert manifest_provider.read_attempts[0][1] is not None
            assert all(
                path == "catalog.parquet" and byte_range is not None
                for path, byte_range in manifest_provider.read_attempts
            )
            assert source_provider.read_attempts == [
                ("payload.bin", Range(0, 7)),
                ("payload.bin", Range(0, 7)),
            ]
            assert manifest_provider.authenticated_access_keys == [f"manifest-{credential_set}-access"] * (
                len(manifest_provider.read_attempts) + 1
            )
            assert source_provider.authenticated_access_keys == [
                f"source-{credential_set}-access",
                f"source-{credential_set}-access",
            ]
            assert _CredentialSpy.instances[(spy_id, "manifest")].get_calls == len(
                manifest_provider.authenticated_access_keys
            )
            assert _CredentialSpy.instances[(spy_id, "source")].get_calls == 2

        assert synthetic_etags[0]
        assert synthetic_etags[0] == synthetic_etags[1]
    finally:
        for role in ("manifest", "source"):
            _AuthenticatedFailOnceStorageProvider.datasets.pop((spy_id, role), None)
            _AuthenticatedFailOnceStorageProvider.instances.pop((spy_id, role), None)
            _CredentialSpy.instances.pop((spy_id, role), None)


def test_manifest_cache_uses_virtual_etag_and_can_intentionally_serve_stale_data(tmp_path: Path) -> None:
    source_root = tmp_path / "objects"
    manifest_root = tmp_path / "manifests"
    cache_root = tmp_path / "cache"
    source_root.mkdir()
    manifest_root.mkdir()
    source_path = source_root / "data.bin"
    source_path.write_bytes(b"old")
    rows = _single_object_rows("data.bin", "data.bin", b"old")
    (manifest_root / "catalog.parquet").write_bytes(write_manifest(rows).getvalue())

    first_config = _manifest_config(
        manifest_root=manifest_root,
        source_root=source_root,
        source_revision="objects-r1",
        cache_root=cache_root,
    )
    first_client = StorageClient(StorageClientConfig.from_dict(first_config, profile="virtual"))
    first_etag = first_client.info("data.bin").etag
    assert first_client.read("data.bin", check_source_version=SourceVersionCheckMode.ENABLE) == b"old"

    source_path.write_bytes(b"new")
    unchanged_binding_client = StorageClient(StorageClientConfig.from_dict(first_config, profile="virtual"))
    assert unchanged_binding_client.info("data.bin").etag == first_etag
    assert unchanged_binding_client.read("data.bin", check_source_version=SourceVersionCheckMode.ENABLE) == b"old"

    second_config = _manifest_config(
        manifest_root=manifest_root,
        source_root=source_root,
        source_revision="objects-r2",
        cache_root=cache_root,
    )
    second_client = StorageClient(StorageClientConfig.from_dict(second_config, profile="virtual"))
    assert second_client.info("data.bin").etag != first_etag
    assert second_client.read("data.bin", check_source_version=SourceVersionCheckMode.DISABLE) == b"old"
    assert second_client.read("data.bin", check_source_version=SourceVersionCheckMode.ENABLE) == b"new"
