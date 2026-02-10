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

import time
from collections.abc import Iterator
from datetime import datetime
from typing import IO, Any, Optional, Union
from unittest.mock import MagicMock, Mock

import pytest

from multistorageclient.providers.base import BaseStorageProvider
from multistorageclient.telemetry import Telemetry
from multistorageclient.types import ObjectMetadata, Range


class MockBaseStorageProvider(BaseStorageProvider):
    def _put_object(self, path: str, body: bytes) -> None:
        pass

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        return b""

    def _copy_object(self, src_path: str, dest_path: str) -> None:
        pass

    def _delete_object(self, path: str, etag: Optional[str] = None) -> None:
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
        follow_symlinks: bool = True,
    ) -> Iterator[ObjectMetadata]:
        return iter([])

    def _upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        pass

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        pass


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
        follow_symlinks: bool = True,
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
        follow_symlinks: bool = True,
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
