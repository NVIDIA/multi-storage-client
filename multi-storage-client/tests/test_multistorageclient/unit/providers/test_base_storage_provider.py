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

import asyncio
import tempfile
import time
from collections.abc import Iterator
from datetime import datetime
from typing import IO, Any, Optional, Union
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from multistorageclient.providers.base import BaseStorageProvider
from multistorageclient.telemetry import Telemetry
from multistorageclient.types import ObjectMetadata, Range, SymlinkHandling


class MockBaseStorageProvider(BaseStorageProvider):
    _rust_client: Any = None

    def _put_object(self, path: str, body: bytes) -> None:
        pass

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        return b""

    def _copy_object(self, src_path: str, dest_path: str) -> None:
        pass

    def _delete_object(self, path: str, etag: Optional[str] = None) -> None:
        pass

    def _make_symlink(self, path: str, target: str) -> None:
        pass

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        if not path.endswith("txt"):
            return ObjectMetadata(key=path, content_length=0, type="directory", last_modified=datetime.now())
        else:
            return ObjectMetadata(key=path, content_length=0, type="file", last_modified=datetime.now())

    def _list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
    ) -> Iterator[ObjectMetadata]:
        return iter([])

    def _upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        pass

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> int:
        return 0


class FailFastStorageProvider(MockBaseStorageProvider):
    @property
    def supports_parallel_listing(self) -> bool:
        return True

    def _list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
    ) -> Iterator[ObjectMetadata]:
        if path.endswith("bad/"):
            raise RuntimeError("should fail fast")
        if include_directories and path.rstrip("/") == "bucket":
            yield ObjectMetadata(
                key="bucket/bad",
                content_length=0,
                type="directory",
                last_modified=datetime.now(),
            )


def test_list_objects_with_base_path():
    mock_objects = [
        ObjectMetadata(key="prefix/dir/file1.txt", content_length=0, type="file", last_modified=datetime.now()),
        ObjectMetadata(key="prefix/dir/file2.txt", content_length=0, type="file", last_modified=datetime.now()),
        ObjectMetadata(key="prefix/dir", content_length=0, type="directory", last_modified=datetime.now()),
    ]
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider._list_objects = MagicMock(return_value=iter(mock_objects))
    response = list(provider.list_objects(path="prefix/dir"))
    assert len(response) == 3

    for m in response:
        assert m.key.startswith("prefix/dir")


def test_list_objects_with_prefix_in_base_path():
    mock_objects = [
        ObjectMetadata(key="bucket/prefix/dir/file1.txt", content_length=0, type="file", last_modified=datetime.now()),
        ObjectMetadata(key="bucket/prefix/dir/file2.txt", content_length=0, type="file", last_modified=datetime.now()),
        ObjectMetadata(key="bucket/prefix/dir", content_length=0, type="directory", last_modified=datetime.now()),
    ]
    provider = MockBaseStorageProvider(base_path="bucket/prefix", provider_name="mock")
    provider._list_objects = MagicMock(return_value=iter(mock_objects))
    response = list(provider.list_objects(path="dir/"))
    assert len(response) == 3

    for m in response:
        assert m.key.startswith("dir")


def test_put_object_converts_provider_attributes_to_strings():
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider._put_object = MagicMock(return_value=4)

    attributes = {
        "name": "model",
        "count": 1,
        "enabled": True,
        "labels": ["train", "eval"],
        "nested": {"source": "test"},
    }

    provider.put_object("file.txt", b"data", attributes=attributes)

    provider._put_object.assert_called_once_with(
        "bucket/file.txt",
        b"data",
        None,
        None,
        {
            "name": "model",
            "count": "1",
            "enabled": "true",
            "labels": '["train","eval"]',
            "nested": '{"source":"test"}',
        },
    )
    assert attributes["count"] == 1
    assert attributes["labels"] == ["train", "eval"]


def test_upload_file_converts_provider_attributes_to_strings():
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider._upload_file = MagicMock(return_value=4)

    provider.upload_file("file.txt", "/tmp/file.txt", attributes={"count": 1})

    provider._upload_file.assert_called_once_with("bucket/file.txt", "/tmp/file.txt", {"count": "1"})


def test_provider_attribute_conversion_leaves_validation_to_provider_hook():
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider._put_object = MagicMock(return_value=4)

    provider.put_object("file.txt", b"data", attributes={"values": ["x" * 128]})

    provider._put_object.assert_called_once_with("bucket/file.txt", b"data", None, None, {"values": f'["{"x" * 128}"]'})


def test_list_objects_with_empty_base_path():
    """Test that list_objects raises ValueError when base_path is empty."""
    provider = MockBaseStorageProvider(base_path="", provider_name="mock")

    with pytest.raises(ValueError, match="The base_path cannot be empty when calling list_objects"):
        list(provider.list_objects(path=""))


def test_async_metrics_disabled_by_default():
    """Test that async metrics are disabled when not specified in config."""
    config = {
        "opentelemetry": {
            "metrics": {
                "exporter": {"type": "console"},
                "reader": {
                    "options": {},
                },
            }
        }
    }

    mock_telemetry = Mock(spec=Telemetry)
    mock_telemetry.gauge = Mock(return_value=Mock())
    mock_telemetry.counter = Mock(return_value=Mock())

    provider = MockBaseStorageProvider(
        base_path="bucket",
        provider_name="mock",
        config_dict=config,
        telemetry_provider=lambda: mock_telemetry,
    )

    provider._init_metrics()

    # Verify async mode is disabled
    assert provider._async_metrics_enabled is False
    assert provider._metrics_queue is None
    assert provider._metrics_worker is None


def test_async_metrics_queuing():
    """Test that metrics are queued in async mode instead of recorded immediately."""
    config = {
        "opentelemetry": {
            "metrics": {
                "exporter": {"type": "console"},
                "reader": {
                    "async": True,
                    "options": {},
                },
            }
        }
    }

    mock_gauge = Mock()
    mock_counter = Mock()
    mock_telemetry = Mock(spec=Telemetry)
    mock_telemetry.gauge = Mock(return_value=mock_gauge)
    mock_telemetry.counter = Mock(return_value=mock_counter)

    provider = MockBaseStorageProvider(
        base_path="bucket",
        provider_name="mock",
        config_dict=config,
        telemetry_provider=lambda: mock_telemetry,
    )

    provider._init_metrics()

    # Perform an operation that should trigger metrics
    result = provider._emit_metrics(BaseStorageProvider._Operation.READ, lambda: b"test_data")

    assert result == b"test_data"

    # Wait a moment for the worker thread to process
    time.sleep(0.1)

    # Verify metrics were eventually recorded
    assert mock_gauge.set.call_count > 0
    assert mock_counter.add.call_count > 0

    # Cleanup
    provider._shutdown_async_telemetry()


def test_async_metrics_queue_full_drops_metrics():
    """Test that metrics are dropped when queue is full and counter is incremented."""
    config = {
        "opentelemetry": {
            "metrics": {
                "exporter": {"type": "console"},
                "reader": {
                    "async": True,
                    "options": {},
                },
            }
        }
    }

    mock_telemetry = Mock(spec=Telemetry)
    mock_telemetry.gauge = Mock(return_value=Mock())
    mock_telemetry.counter = Mock(return_value=Mock())

    provider = MockBaseStorageProvider(
        base_path="bucket",
        provider_name="mock",
        config_dict=config,
        telemetry_provider=lambda: mock_telemetry,
    )

    provider._init_metrics()

    # Stop worker and fill the queue
    if provider._metrics_worker is not None:
        provider._metrics_worker_shutdown.set()
        provider._metrics_worker.join(timeout=1.0)

    initial_dropped = provider._metrics_dropped_count

    # Try to emit more metrics than queue can hold (default queue size is 100,000)
    # We'll fill it by directly accessing the queue
    if provider._metrics_queue is not None:
        # Fill queue to capacity
        for _ in range(provider._metrics_queue.maxsize + 10):
            try:
                provider._emit_metrics(BaseStorageProvider._Operation.READ, lambda: b"data")
            except Exception:
                pass

        # Verify some metrics were dropped
        assert provider._metrics_dropped_count > initial_dropped


def test_async_metrics_worker_processes_queue():
    """Test that the worker thread correctly processes queued metrics."""
    config = {
        "opentelemetry": {
            "metrics": {
                "exporter": {"type": "console"},
                "reader": {
                    "async": True,
                    "options": {},
                },
            }
        }
    }

    mock_gauge = Mock()
    mock_counter = Mock()
    mock_telemetry = Mock(spec=Telemetry)
    mock_telemetry.gauge = Mock(return_value=mock_gauge)
    mock_telemetry.counter = Mock(return_value=mock_counter)

    provider = MockBaseStorageProvider(
        base_path="bucket",
        provider_name="mock",
        config_dict=config,
        telemetry_provider=lambda: mock_telemetry,
    )

    provider._init_metrics()

    # Emit multiple metrics
    num_operations = 5
    for _ in range(num_operations):
        provider._emit_metrics(BaseStorageProvider._Operation.READ, lambda: b"test_data")

    # Wait for worker to process all metrics
    time.sleep(0.2)

    # Verify all metrics were processed
    assert mock_gauge.set.call_count >= num_operations
    assert mock_counter.add.call_count >= num_operations

    # Cleanup
    provider._shutdown_async_telemetry()


def test_async_metrics_graceful_shutdown():
    """Test that async metrics shutdown gracefully without errors."""
    config = {
        "opentelemetry": {
            "metrics": {
                "exporter": {"type": "console"},
                "reader": {
                    "async": True,
                    "options": {},
                },
            }
        }
    }

    mock_telemetry = Mock(spec=Telemetry)
    mock_telemetry.gauge = Mock(return_value=Mock())
    mock_telemetry.counter = Mock(return_value=Mock())

    provider = MockBaseStorageProvider(
        base_path="bucket",
        provider_name="mock",
        config_dict=config,
        telemetry_provider=lambda: mock_telemetry,
    )

    provider._init_metrics()

    # Queue some metrics
    for _ in range(3):
        provider._emit_metrics(BaseStorageProvider._Operation.READ, lambda: b"test")

    # Shutdown should complete without hanging or errors
    provider._shutdown_async_telemetry()

    # Worker thread should be stopped
    assert provider._metrics_worker is not None
    assert not provider._metrics_worker.is_alive()


def test_async_metrics_handles_errors_in_worker():
    """Test that errors in the worker thread don't crash the application."""
    config = {
        "opentelemetry": {
            "metrics": {
                "exporter": {"type": "console"},
                "reader": {
                    "async": True,
                    "options": {},
                },
            }
        }
    }

    mock_gauge = Mock()
    mock_gauge.set.side_effect = Exception("Test error")  # Simulate error
    mock_counter = Mock()
    mock_telemetry = Mock(spec=Telemetry)
    mock_telemetry.gauge = Mock(return_value=mock_gauge)
    mock_telemetry.counter = Mock(return_value=mock_counter)

    provider = MockBaseStorageProvider(
        base_path="bucket",
        provider_name="mock",
        config_dict=config,
        telemetry_provider=lambda: mock_telemetry,
    )

    provider._init_metrics()

    # Emit metrics despite error in worker
    result = provider._emit_metrics(BaseStorageProvider._Operation.READ, lambda: b"test_data")

    # Operation should still succeed
    assert result == b"test_data"

    # Worker thread should still be alive
    time.sleep(0.1)
    if provider._metrics_worker is not None:
        assert provider._metrics_worker.is_alive()

    # Cleanup
    provider._shutdown_async_telemetry()


def test_parallel_listing_error_propagation():
    """Errors from background prefix expansion must propagate to the caller."""
    provider = FailFastStorageProvider(base_path="bucket", provider_name="fail-fast")
    with pytest.raises(RuntimeError, match="should fail fast"):
        list(provider.list_objects_recursive(path=""))


class MockParallelListingProvider(MockBaseStorageProvider):
    """Mock provider with a configurable prefix tree for testing the heap algorithm."""

    def __init__(self, tree: dict[str, list[ObjectMetadata]], **kwargs: Any):
        super().__init__(**kwargs)
        self._tree = tree

    @property
    def supports_parallel_listing(self) -> bool:
        return True

    def _list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
    ) -> Iterator[ObjectMetadata]:
        for obj in self._tree.get(path, []):
            yield obj


def _obj(key: str, obj_type: str = "file") -> ObjectMetadata:
    return ObjectMetadata(key=key, content_length=0, type=obj_type, last_modified=datetime.now())


class TestParallelListingHeap:
    """Unit tests for the heap-based parallel listing algorithm."""

    def test_flat_prefix_no_heap(self):
        tree = {"bucket/": [_obj("bucket/a.txt"), _obj("bucket/b.txt")]}
        provider = MockParallelListingProvider(tree=tree, base_path="bucket", provider_name="mock")
        keys = [o.key for o in provider.list_objects_recursive()]
        assert keys == ["a.txt", "b.txt"]

    def test_mixed_objects_and_prefixes(self):
        tree = {
            "bucket/": [
                _obj("bucket/00-readme.txt"),
                _obj("bucket/a", "directory"),
                _obj("bucket/z-final.txt"),
            ],
            "bucket/a/": [_obj("bucket/a/file.txt")],
        }
        provider = MockParallelListingProvider(tree=tree, base_path="bucket", provider_name="mock")
        keys = [o.key for o in provider.list_objects_recursive()]
        assert keys == ["00-readme.txt", "a/file.txt", "z-final.txt"]

    def test_deep_nesting(self):
        tree = {
            "bucket/": [_obj("bucket/a", "directory")],
            "bucket/a/": [_obj("bucket/a/b", "directory")],
            "bucket/a/b/": [_obj("bucket/a/b/c", "directory")],
            "bucket/a/b/c/": [_obj("bucket/a/b/c/file.txt")],
        }
        provider = MockParallelListingProvider(tree=tree, base_path="bucket", provider_name="mock")
        keys = [o.key for o in provider.list_objects_recursive()]
        assert keys == ["a/b/c/file.txt"]

    def test_start_after_end_at_filtering(self):
        tree = {
            "bucket/": [
                _obj("bucket/a", "directory"),
                _obj("bucket/b", "directory"),
                _obj("bucket/c", "directory"),
            ],
            "bucket/a/": [_obj("bucket/a/f.txt")],
            "bucket/b/": [_obj("bucket/b/f.txt")],
            "bucket/c/": [_obj("bucket/c/f.txt")],
        }
        provider = MockParallelListingProvider(tree=tree, base_path="bucket", provider_name="mock")
        keys = [o.key for o in provider.list_objects_recursive(start_after="a/f.txt", end_at="b/f.txt")]
        assert keys == ["b/f.txt"]

    def test_non_leaf_objects_interleave_correctly(self):
        tree = {
            "bucket/": [
                _obj("bucket/a", "directory"),
            ],
            "bucket/a/": [
                _obj("bucket/a/file.txt"),
                _obj("bucket/a/sub", "directory"),
            ],
            "bucket/a/sub/": [_obj("bucket/a/sub/deep.txt")],
        }
        provider = MockParallelListingProvider(tree=tree, base_path="bucket", provider_name="mock")
        keys = [o.key for o in provider.list_objects_recursive()]
        assert keys == ["a/file.txt", "a/sub/deep.txt"]

    def test_self_marker_directory_does_not_hang(self):
        """A directory marker pointing back to its own prefix must be skipped, not re-expanded."""
        tree = {
            "bucket/": [_obj("bucket/a", "directory")],
            "bucket/a/": [
                _obj("bucket/a", "directory"),
                _obj("bucket/a/file.txt"),
            ],
        }
        provider = MockParallelListingProvider(tree=tree, base_path="bucket", provider_name="mock")
        keys = [o.key for o in provider.list_objects_recursive()]
        assert keys == ["a/file.txt"]


def test_download_files_threaded():
    """Threaded path: multiple files, empty list, and validation."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider.download_file = MagicMock()

    with pytest.raises(ValueError, match="same length"):
        provider.download_files(["a.txt", "b.txt"], ["/tmp/a.txt"])

    with pytest.raises(ValueError, match="at least 1"):
        provider.download_files(["a.txt"], ["/tmp/a.txt"], max_workers=0)

    with pytest.raises(ValueError, match="metadata must have the same length"):
        provider.download_files(["a.txt"], ["/tmp/a.txt"], metadata=[])

    provider.download_files([], [])
    provider.download_file.assert_not_called()

    remote_paths = ["file1.txt", "file2.txt", "file3.txt"]
    local_paths = ["/tmp/file1.txt", "/tmp/file2.txt", "/tmp/file3.txt"]
    provider.download_files(remote_paths, local_paths, max_workers=4)

    assert provider.download_file.call_count == 3
    called_args = sorted([call.args for call in provider.download_file.call_args_list], key=lambda x: x[0])
    for args in called_args:
        assert isinstance(args[2], ObjectMetadata)


def test_download_files_threaded_with_metadata():
    """Threaded path: per-file metadata is forwarded; missing entries are resolved."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider.download_file = MagicMock()

    remote_paths = ["file1.txt", "file2.txt", "file3.txt"]
    local_paths = ["/tmp/file1.txt", "/tmp/file2.txt", "/tmp/file3.txt"]
    meta = [
        ObjectMetadata(key="file1.txt", content_length=100, last_modified=datetime.now()),
        None,
        ObjectMetadata(key="file3.txt", content_length=999999999, last_modified=datetime.now()),
    ]
    provider.download_files(remote_paths, local_paths, metadata=meta, max_workers=4)

    assert provider.download_file.call_count == 3
    called_args = sorted([call.args for call in provider.download_file.call_args_list], key=lambda x: x[0])
    assert called_args[0] == ("file1.txt", "/tmp/file1.txt", meta[0])
    assert isinstance(called_args[1][2], ObjectMetadata)
    assert called_args[2] == ("file3.txt", "/tmp/file3.txt", meta[2])


def test_download_files_async():
    """Async/Rust path: without metadata, fetches metadata and uses threshold to decide."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")

    mock_rust_client = MagicMock()
    mock_rust_client.download = AsyncMock(return_value=200)
    mock_rust_client.download_multipart_to_file = AsyncMock(return_value=200)
    provider._rust_client = mock_rust_client

    remote_paths = ["small1.txt", "small2.txt", "large1.txt"]
    local_paths = ["/tmp/small1.txt", "/tmp/small2.txt", "/tmp/large1.txt"]

    with patch("multistorageclient.providers.base.safe_makedirs"):
        provider.download_files(remote_paths, local_paths, max_workers=4)

    assert mock_rust_client.download.await_count == 3
    called_args = {call.args for call in mock_rust_client.download.call_args_list}
    assert called_args == {
        ("small1.txt", "/tmp/small1.txt"),
        ("small2.txt", "/tmp/small2.txt"),
        ("large1.txt", "/tmp/large1.txt"),
    }


def test_download_files_async_with_metadata():
    """Async/Rust path: metadata drives multipart vs regular download based on threshold."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider._multipart_threshold = 1000

    mock_rust_client = MagicMock()
    mock_rust_client.download = AsyncMock(return_value=100)
    mock_rust_client.download_multipart_to_file = AsyncMock(return_value=5000)
    provider._rust_client = mock_rust_client

    remote_paths = ["small.txt", "large.txt", "unknown.txt"]
    local_paths = ["/tmp/small.txt", "/tmp/large.txt", "/tmp/unknown.txt"]
    meta = [
        ObjectMetadata(key="small.txt", content_length=500, last_modified=datetime.now()),
        ObjectMetadata(key="large.txt", content_length=5000, last_modified=datetime.now()),
        None,
    ]

    with patch("multistorageclient.providers.base.safe_makedirs"):
        provider.download_files(remote_paths, local_paths, metadata=meta, max_workers=4)

    assert mock_rust_client.download.await_count == 2
    download_args = {call.args for call in mock_rust_client.download.call_args_list}
    assert download_args == {
        ("small.txt", "/tmp/small.txt"),
        ("unknown.txt", "/tmp/unknown.txt"),
    }

    assert mock_rust_client.download_multipart_to_file.await_count == 1
    mock_rust_client.download_multipart_to_file.assert_awaited_with("large.txt", "/tmp/large.txt")


@pytest.mark.asyncio
async def test_download_files_async_inside_running_loop():
    """Async/Rust batch downloads should work when the caller already has a running event loop."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")

    mock_rust_client = MagicMock()
    mock_rust_client.download = AsyncMock(return_value=50)
    provider._rust_client = mock_rust_client

    with patch("multistorageclient.providers.base.safe_makedirs"):
        provider.download_files(["small1.txt"], ["/tmp/small1.txt"], max_workers=1)

    mock_rust_client.download.assert_awaited_once_with("small1.txt", "/tmp/small1.txt")


def test_upload_files_threaded():
    """Threaded path: multiple files, empty list, and validation."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider.upload_file = MagicMock()

    with pytest.raises(ValueError, match="same length"):
        provider.upload_files(["/tmp/a.txt", "/tmp/b.txt"], ["a.txt"])

    with pytest.raises(ValueError, match="at least 1"):
        provider.upload_files(["/tmp/a.txt"], ["a.txt"], max_workers=0)

    provider.upload_files([], [])
    provider.upload_file.assert_not_called()

    local_paths = ["/tmp/file1.txt", "/tmp/file2.txt", "/tmp/file3.txt"]
    remote_paths = ["file1.txt", "file2.txt", "file3.txt"]
    provider.upload_files(local_paths, remote_paths, max_workers=4)

    assert provider.upload_file.call_count == 3
    called_args = {call.args for call in provider.upload_file.call_args_list}
    assert called_args == {
        ("file1.txt", "/tmp/file1.txt", None),
        ("file2.txt", "/tmp/file2.txt", None),
        ("file3.txt", "/tmp/file3.txt", None),
    }


def test_upload_files_threaded_with_attributes():
    """Threaded path: per-file attributes are forwarded to each upload_file call."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider.upload_file = MagicMock()

    local_paths = ["/tmp/file1.txt", "/tmp/file2.txt", "/tmp/file3.txt"]
    remote_paths = ["file1.txt", "file2.txt", "file3.txt"]
    attrs = [{"tag": "a"}, None, {"tag": "c"}]
    provider.upload_files(local_paths, remote_paths, attributes=attrs, max_workers=4)

    assert provider.upload_file.call_count == 3
    called_args = sorted([call.args for call in provider.upload_file.call_args_list], key=lambda x: x[0])
    assert called_args == [
        ("file1.txt", "/tmp/file1.txt", {"tag": "a"}),
        ("file2.txt", "/tmp/file2.txt", None),
        ("file3.txt", "/tmp/file3.txt", {"tag": "c"}),
    ]


def test_upload_files_rejects_mismatched_attributes_length():
    """Attributes list must match local_paths/remote_paths length."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")

    with pytest.raises(ValueError, match="attributes must have the same length"):
        provider.upload_files(["/tmp/a.txt", "/tmp/b.txt"], ["a.txt", "b.txt"], attributes=[{"k": "v"}])


def test_upload_files_with_attributes_skips_rust_path():
    """When any file has attributes, the Rust async path is bypassed in favour of threaded."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider.upload_file = MagicMock()

    mock_rust_client = MagicMock()
    provider._rust_client = mock_rust_client

    local_paths = ["/tmp/file1.txt"]
    remote_paths = ["file1.txt"]
    attrs = [{"key": "val"}]
    provider.upload_files(local_paths, remote_paths, attributes=attrs, max_workers=2)

    provider.upload_file.assert_called_once_with("file1.txt", "/tmp/file1.txt", {"key": "val"})
    mock_rust_client.upload.assert_not_called()
    mock_rust_client.upload_multipart_from_file.assert_not_called()


def test_upload_files_with_all_none_attributes_uses_rust_path():
    """When attributes list exists but all entries are None, Rust path is still used."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider._multipart_threshold = 100

    mock_rust_client = MagicMock()
    mock_rust_client.upload = AsyncMock(return_value=50)
    provider._rust_client = mock_rust_client

    local_paths = ["/tmp/file1.txt"]
    remote_paths = ["file1.txt"]

    def fake_getsize(path: str) -> int:
        return 50

    with patch("multistorageclient.providers.base.os.path.getsize", side_effect=fake_getsize):
        provider.upload_files(local_paths, remote_paths, attributes=[None], max_workers=2)

    mock_rust_client.upload.assert_awaited_once()


def test_upload_files_async():
    """Async/Rust path: uses upload for small files, upload_multipart_from_file for large."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider._multipart_threshold = 100

    mock_rust_client = MagicMock()
    mock_rust_client.upload = AsyncMock(return_value=50)
    mock_rust_client.upload_multipart_from_file = AsyncMock(return_value=200)
    provider._rust_client = mock_rust_client

    local_paths = ["/tmp/small1.txt", "/tmp/small2.txt", "/tmp/large1.txt"]
    remote_paths = ["small1.txt", "small2.txt", "large1.txt"]

    def fake_getsize(path: str) -> int:
        return 200 if "large" in path else 50

    with patch("multistorageclient.providers.base.os.path.getsize", side_effect=fake_getsize):
        provider.upload_files(local_paths, remote_paths, max_workers=4)

    assert mock_rust_client.upload.await_count == 2
    assert mock_rust_client.upload_multipart_from_file.await_count == 1
    assert mock_rust_client.upload_multipart_from_file.call_args.args == ("/tmp/large1.txt", "large1.txt")


def test_upload_files_async_concurrency():
    """Rust upload concurrency should be capped by max_workers."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")
    provider._multipart_threshold = 10_000_000

    num_files = 10
    max_workers = 4
    active_uploads = 0
    peak_uploads = 0

    async def slow_upload(local_path, key):
        nonlocal active_uploads, peak_uploads
        active_uploads += 1
        peak_uploads = max(peak_uploads, active_uploads)
        try:
            await asyncio.sleep(0.1)
            return 100
        finally:
            active_uploads -= 1

    mock_rust_client = MagicMock()
    mock_rust_client.upload = AsyncMock(side_effect=slow_upload)
    provider._rust_client = mock_rust_client

    local_paths = [f"/tmp/file{i}.txt" for i in range(num_files)]
    remote_paths = [f"file{i}.txt" for i in range(num_files)]

    with patch("multistorageclient.providers.base.os.path.getsize", return_value=100):
        provider.upload_files(local_paths, remote_paths, max_workers=max_workers)

    assert mock_rust_client.upload.await_count == num_files
    assert peak_uploads == max_workers


def test_download_files_async_concurrency():
    """Rust download concurrency should be capped by max_workers."""
    provider = MockBaseStorageProvider(base_path="bucket", provider_name="mock")

    num_files = 10
    max_workers = 4
    active_downloads = 0
    peak_downloads = 0

    async def slow_download(key, local_path):
        nonlocal active_downloads, peak_downloads
        active_downloads += 1
        peak_downloads = max(peak_downloads, active_downloads)
        try:
            await asyncio.sleep(0.1)
            return 100
        finally:
            active_downloads -= 1

    mock_rust_client = MagicMock()
    mock_rust_client.download = AsyncMock(side_effect=slow_download)
    provider._rust_client = mock_rust_client

    remote_paths = [f"file{i}.txt" for i in range(num_files)]
    local_paths = [f"/tmp/file{i}.txt" for i in range(num_files)]

    with patch("multistorageclient.providers.base.safe_makedirs"):
        provider.download_files(remote_paths, local_paths, max_workers=max_workers)

    assert mock_rust_client.download.await_count == num_files
    assert peak_downloads == max_workers


def test_object_metadata_symlink_target_field():
    metadata = ObjectMetadata(
        key="link.txt",
        content_length=0,
        last_modified=datetime.now(),
        symlink_target="dir/target.txt",
    )
    assert metadata.symlink_target == "dir/target.txt"

    data = metadata.to_dict()
    assert data["symlink_target"] == "dir/target.txt"

    restored = ObjectMetadata.from_dict(data)
    assert restored.symlink_target == "dir/target.txt"


def test_object_metadata_symlink_target_none_by_default():
    metadata = ObjectMetadata(
        key="file.txt",
        content_length=100,
        last_modified=datetime.now(),
    )
    assert metadata.symlink_target is None

    data = metadata.to_dict()
    assert "symlink_target" not in data

    restored = ObjectMetadata.from_dict(data)
    assert restored.symlink_target is None


class SymlinkMockProvider(MockBaseStorageProvider):
    """A mock that stores objects and metadata for symlink resolution tests."""

    def __init__(self, objects: dict[str, bytes], metadata: dict[str, ObjectMetadata]):
        super().__init__(base_path="", provider_name="mock")
        self._objects = objects
        self._metadata = metadata

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        return self._objects.get(path, b"")

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        if path in self._metadata:
            return self._metadata[path]
        return ObjectMetadata(key=path, content_length=0, last_modified=datetime.now())

    def _download_file(self, remote_path: str, f, metadata=None) -> int:
        data = self._get_object(remote_path)
        if isinstance(f, str):
            import os

            os.makedirs(os.path.dirname(f) or ".", exist_ok=True)
            with open(f, "wb") as fh:
                fh.write(data)
        else:
            f.write(data)
        return len(data)


def test_get_object_follows_symlink():
    now = datetime.now()
    objects = {"target.txt": b"real content"}
    metadata = {
        "link.txt": ObjectMetadata(key="link.txt", content_length=0, last_modified=now, symlink_target="target.txt"),
        "target.txt": ObjectMetadata(key="target.txt", content_length=12, last_modified=now),
    }
    provider = SymlinkMockProvider(objects=objects, metadata=metadata)
    assert provider.get_object("link.txt") == b"real content"


def test_get_object_symlink_chain():
    now = datetime.now()
    objects = {"final.txt": b"chain result"}
    metadata = {
        "link_a": ObjectMetadata(key="link_a", content_length=0, last_modified=now, symlink_target="link_b"),
        "link_b": ObjectMetadata(key="link_b", content_length=0, last_modified=now, symlink_target="final.txt"),
        "final.txt": ObjectMetadata(key="final.txt", content_length=12, last_modified=now),
    }
    provider = SymlinkMockProvider(objects=objects, metadata=metadata)
    assert provider.get_object("link_a") == b"chain result"


def test_get_object_symlink_cycle_raises():
    now = datetime.now()
    objects: dict[str, bytes] = {}
    metadata = {
        "a": ObjectMetadata(key="a", content_length=0, last_modified=now, symlink_target="b"),
        "b": ObjectMetadata(key="b", content_length=0, last_modified=now, symlink_target="a"),
    }
    provider = SymlinkMockProvider(objects=objects, metadata=metadata)
    with pytest.raises(ValueError, match="cycle"):
        provider.get_object("a")


def test_get_object_symlink_depth_limit():
    now = datetime.now()
    objects = {"target.txt": b"deep"}
    metadata: dict[str, ObjectMetadata] = {}
    for i in range(50):
        metadata[f"link_{i}"] = ObjectMetadata(
            key=f"link_{i}", content_length=0, last_modified=now, symlink_target=f"link_{i + 1}"
        )
    metadata["link_50"] = ObjectMetadata(
        key="link_50", content_length=0, last_modified=now, symlink_target="target.txt"
    )
    metadata["target.txt"] = ObjectMetadata(key="target.txt", content_length=4, last_modified=now)
    provider = SymlinkMockProvider(objects=objects, metadata=metadata)
    with pytest.raises(ValueError, match="Too many levels"):
        provider.get_object("link_0")


def test_get_object_empty_file_not_symlink():
    now = datetime.now()
    objects = {"empty.txt": b""}
    metadata = {
        "empty.txt": ObjectMetadata(key="empty.txt", content_length=0, last_modified=now),
    }
    provider = SymlinkMockProvider(objects=objects, metadata=metadata)
    assert provider.get_object("empty.txt") == b""


def test_download_file_follows_symlink():
    now = datetime.now()
    objects = {"target.txt": b"real content"}
    metadata = {
        "link.txt": ObjectMetadata(key="link.txt", content_length=0, last_modified=now, symlink_target="target.txt"),
        "target.txt": ObjectMetadata(key="target.txt", content_length=12, last_modified=now),
    }
    provider = SymlinkMockProvider(objects=objects, metadata=metadata)
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
    try:
        provider.download_file("link.txt", tmp_path)
        with open(tmp_path, "rb") as f:
            assert f.read() == b"real content"
    finally:
        import os

        os.unlink(tmp_path)
