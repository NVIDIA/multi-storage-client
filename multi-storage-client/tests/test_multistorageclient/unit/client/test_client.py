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

import pickle
import threading
from datetime import datetime, timezone
from unittest.mock import ANY, MagicMock

import pytest

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.client.composite import CompositeStorageClient
from multistorageclient.client.single import SingleStorageClient
from multistorageclient.types import (
    BatchTransferError,
    BatchTransferFailure,
    ObjectMetadata,
    ResolvedPath,
    ResolvedPathState,
    RetryableError,
    SymlinkHandling,
)


@pytest.fixture
def single_backend_config():
    """Fixture for single-backend configuration."""
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


@pytest.fixture(scope="function")
def multi_backend_config():
    """Fixture for multi-backend configuration."""
    import uuid

    # Use unique profile names per test to avoid conflicts during serialization
    profile_name = f"test-multi-{uuid.uuid4().hex[:8]}"
    return StorageClientConfig.from_yaml(
        f"""
        profiles:
          {profile_name}:
            provider_bundle:
              type: test_multistorageclient.unit.utils.mocks.TestProviderBundleV2MultiBackend
        """,
        profile=profile_name,
    )


def test_single_backend_facade(single_backend_config):
    """Test that single-backend config delegates to SingleStorageClient and forwards all properties/methods."""
    client = StorageClient(single_backend_config)

    # Verify delegation
    assert isinstance(client._delegate, SingleStorageClient)
    assert client.delegate is client._delegate

    # Verify basic properties
    assert client.profile == "test-single"
    assert client._storage_provider is not None
    assert client._config is not None

    # Verify backward compatibility properties
    assert client._credentials_provider is None
    assert client._retry_config is not None
    assert client._metadata_provider is None
    assert client._metadata_provider_lock is None
    assert client._cache_manager is None

    # Verify utility methods
    assert isinstance(client._is_rust_client_enabled(), bool)
    assert client._is_posix_file_storage_provider() is True
    assert client.is_default_profile() is False

    # get_posix_path returns the physical path for POSIX providers (returns path with base_path prepended)
    posix_path = client.get_posix_path("test/path")
    assert posix_path is not None
    assert posix_path.startswith("/tmp/test")


def test_multi_backend_facade(multi_backend_config):
    """Test that multi-backend config delegates to CompositeStorageClient."""
    client = StorageClient(multi_backend_config)

    # Verify delegation
    assert isinstance(client._delegate, CompositeStorageClient)
    assert client.delegate is client._delegate

    # CompositeStorageClient doesn't have a single storage provider
    assert client._storage_provider is None
    assert client.profile.startswith("test-multi")  # Profile name has UUID suffix

    # Verify child_configs is set on config
    assert multi_backend_config.child_configs is not None
    assert len(multi_backend_config.child_configs) == 3  # loc1, loc2, loc2-backup

    # Verify child clients are created with credentials from child_configs
    composite = client._delegate
    assert "loc1" in composite._child_clients
    assert "loc2" in composite._child_clients
    assert "loc2-backup" in composite._child_clients
    assert composite._child_clients["loc1"]._credentials_provider is not None

    # Verify child clients receive replicas from child_configs
    loc2_client = composite._child_clients["loc2"]
    assert len(loc2_client.replicas) == 1
    assert loc2_client.replicas[0].profile == "loc2-backup"


@pytest.mark.parametrize(
    "operation,args",
    [
        ("write", ("/test/path", b"data")),
        ("upload_file", ("/remote/path", "/local/path")),
        ("upload_files", (["/remote/path"], ["/local/path"])),
        ("delete", ("/test/path",)),
        ("copy", ("/src/path", "/dest/path")),
    ],
)
def test_composite_client_blocks_write_operations(multi_backend_config, operation, args):
    """Test that CompositeStorageClient raises NotImplementedError for all write operations."""
    client = StorageClient(multi_backend_config)

    with pytest.raises(NotImplementedError, match="read-only"):
        getattr(client, operation)(*args)


@pytest.mark.parametrize("mode", ["w", "wb", "a", "ab"])
def test_composite_client_blocks_write_modes(multi_backend_config, mode):
    """Test that CompositeStorageClient raises NotImplementedError for write modes in open()."""
    client = StorageClient(multi_backend_config)

    with pytest.raises(NotImplementedError, match="read mode"):
        client.open("/test/path", mode=mode)


@pytest.mark.parametrize(
    "client_class,config_fixture,expected_error",
    [
        (SingleStorageClient, "multi_backend_config", "SingleStorageClient requires storage_provider"),
        (CompositeStorageClient, "single_backend_config", "CompositeStorageClient requires storage_provider_profiles"),
    ],
)
def test_client_rejects_wrong_config(client_class, config_fixture, expected_error, request):
    """Test that client classes reject incompatible configurations."""
    config = request.getfixturevalue(config_fixture)

    with pytest.raises(ValueError, match=expected_error):
        client_class(config)


def test_serialization_single_backend(single_backend_config):
    """Test that StorageClient can be pickled and unpickled (single backend)."""
    client = StorageClient(single_backend_config)

    # Pickle and unpickle
    serialized = pickle.dumps(client)
    restored = pickle.loads(serialized)

    # Verify restoration
    assert isinstance(restored._delegate, SingleStorageClient)
    assert restored.profile == "test-single"
    assert restored._storage_provider is not None


def test_serialization_multi_backend(multi_backend_config):
    """Test that StorageClient with CompositeStorageClient can be pickled and unpickled."""
    client = StorageClient(multi_backend_config)
    expected_profile = client.profile

    # Pickle and unpickle
    serialized = pickle.dumps(client)
    restored = pickle.loads(serialized)

    # Verify restoration
    assert isinstance(restored._delegate, CompositeStorageClient)
    assert restored.profile == expected_profile
    assert restored._storage_provider is None  # Composite doesn't have single provider


def test_composite_client_is_rust_client_enabled(multi_backend_config):
    """Test CompositeStorageClient._is_rust_client_enabled() aggregates child client results."""
    client = StorageClient(multi_backend_config)
    composite = client._delegate
    assert isinstance(composite, CompositeStorageClient)

    child_clients = list(composite._child_clients.values())

    # should returns True if all child clients are Rust-enabled
    for child in child_clients:
        child._is_rust_client_enabled = MagicMock(return_value=True)
    assert composite._is_rust_client_enabled() is True

    # should returns False if any child client is not Rust-enabled
    child_clients[1]._is_rust_client_enabled = MagicMock(return_value=False)
    assert composite._is_rust_client_enabled() is False


def test_single_client_is_rust_client_enabled(single_backend_config):
    """Test SingleStorageClient._is_rust_client_enabled() returns correct value based on _rust_client attribute."""
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    # File provider doesn't have _rust_client attribute - should return False
    assert single._is_rust_client_enabled() is False

    # Mock provider with _rust_client = None (disabled) - should return False
    setattr(single._storage_provider, "_rust_client", None)
    assert single._is_rust_client_enabled() is False

    # Mock provider with _rust_client set to a value - should return True
    setattr(single._storage_provider, "_rust_client", MagicMock())
    assert single._is_rust_client_enabled() is True


def test_list_recursive_delegates_to_single_client(single_backend_config):
    client = StorageClient(single_backend_config)
    expected = iter([])
    client._delegate.list_recursive = MagicMock(return_value=expected)

    result = client.list_recursive(
        path="/tmp/data",
        max_workers=8,
        look_ahead=4,
        include_url_prefix=True,
        follow_symlinks=False,
    )

    assert result is expected
    client._delegate.list_recursive.assert_called_once_with(
        path="/tmp/data",
        start_after=None,
        end_at=None,
        max_workers=8,
        look_ahead=4,
        include_url_prefix=True,
        follow_symlinks=False,
        patterns=None,
        symlink_handling=SymlinkHandling.FOLLOW,
    )


def test_list_recursive_delegates_to_composite_client(multi_backend_config):
    client = StorageClient(multi_backend_config)
    expected = iter([])
    client._delegate.list_recursive = MagicMock(return_value=expected)

    result = client.list_recursive(path="datasets/")

    assert result is expected
    client._delegate.list_recursive.assert_called_once_with(
        path="datasets/",
        start_after=None,
        end_at=None,
        max_workers=32,
        look_ahead=2,
        include_url_prefix=False,
        follow_symlinks=None,
        patterns=None,
        symlink_handling=SymlinkHandling.FOLLOW,
    )


def test_single_list_recursive_uses_provider_recursive_listing(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    expected_obj = ObjectMetadata(key="data/file.bin", content_length=1, type="file", last_modified=datetime.now())
    single._storage_provider.list_objects_recursive = MagicMock(return_value=iter([expected_obj]))

    results = list(client.list_recursive(path="data/"))

    assert [obj.key for obj in results] == ["data/file.bin"]
    single._storage_provider.list_objects_recursive.assert_called_once_with(
        "data/",
        start_after=None,
        end_at=None,
        max_workers=32,
        look_ahead=2,
        symlink_handling=SymlinkHandling.FOLLOW,
    )


def test_single_list_recursive_uses_metadata_provider_when_configured(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    expected_obj = ObjectMetadata(key="data/file.bin", content_length=1, type="file", last_modified=datetime.now())
    metadata_provider = MagicMock()
    metadata_provider.realpath.return_value = MagicMock(exists=False)
    metadata_provider.list_objects.return_value = iter([expected_obj])
    client._metadata_provider = metadata_provider

    single._storage_provider.list_objects_recursive = MagicMock(
        side_effect=AssertionError("provider path should be skipped")
    )

    results = list(client.list_recursive(path="data/", max_workers=4, look_ahead=1))

    assert [obj.key for obj in results] == ["data/file.bin"]
    metadata_provider.list_objects.assert_called_once_with(
        "data/",
        start_after=None,
        end_at=None,
        include_directories=False,
    )


def test_single_list_recursive_file_short_circuit_applies_key_bounds(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    single.is_file = MagicMock(return_value=True)  # type: ignore[method-assign]
    single.info = MagicMock(
        return_value=ObjectMetadata(key="data/file.bin", content_length=1, type="file", last_modified=datetime.now())
    )  # type: ignore[method-assign]
    single._storage_provider.list_objects_recursive = MagicMock(
        side_effect=AssertionError("should short-circuit on file path")
    )

    filtered_by_start_after = list(client.list_recursive(path="data/file.bin", start_after="z"))
    filtered_by_end_at = list(client.list_recursive(path="data/file.bin", end_at="a"))
    included = list(client.list_recursive(path="data/file.bin", start_after="a", end_at="z"))

    assert filtered_by_start_after == []
    assert filtered_by_end_at == []
    assert [obj.key for obj in included] == ["data/file.bin"]


def test_single_write_with_metadata_provider_accepts_non_string_attributes(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.return_value = ResolvedPath(
        physical_path="logical/file.bin", state=ResolvedPathState.UNTRACKED
    )
    metadata_provider.generate_physical_path.return_value = ResolvedPath(
        physical_path="physical/file.bin", state=ResolvedPathState.UNTRACKED
    )
    metadata_provider.allow_overwrites.return_value = False

    physical_metadata = ObjectMetadata(
        key="physical/file.bin",
        content_length=4,
        last_modified=datetime.now(tz=timezone.utc),
        metadata={"existing": "value"},
    )
    single._storage_provider.put_object = MagicMock()
    single._storage_provider.get_object_metadata = MagicMock(return_value=physical_metadata)
    single._metadata_provider = metadata_provider
    single._metadata_provider_lock = None

    attributes = {
        "count": 1,
        "enabled": True,
        "labels": ["train", "eval"],
        "nested": {"source": "test"},
    }

    client.write("logical/file.bin", b"data", attributes=attributes)

    single._storage_provider.put_object.assert_called_once_with("physical/file.bin", b"data", attributes=attributes)
    metadata_provider.add_file.assert_called_once()
    add_file_path, add_file_metadata = metadata_provider.add_file.call_args.args
    assert add_file_path == "logical/file.bin"
    assert add_file_metadata.metadata == {"existing": "value", **attributes}


# --- Batch download_files / upload_files tests ---


def test_download_files_rejects_mismatched_lengths_single(single_backend_config):
    client = StorageClient(single_backend_config)
    with pytest.raises(ValueError, match="same length"):
        client.download_files(["/a", "/b"], ["/local_a"])


def test_single_download_files_rejects_mismatched_metadata_length_with_replica_manager(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)
    single._replica_manager = MagicMock()

    with pytest.raises(ValueError, match="metadata must have the same length"):
        client.download_files(["/a"], ["/local_a"], metadata=[])


def test_download_files_rejects_mismatched_lengths_composite(multi_backend_config):
    client = StorageClient(multi_backend_config)
    with pytest.raises(ValueError, match="same length"):
        client.download_files(["/a"], ["/la", "/lb"])


def test_upload_files_rejects_mismatched_lengths(single_backend_config):
    client = StorageClient(single_backend_config)
    with pytest.raises(ValueError, match="same length"):
        client.upload_files(["/a", "/b"], ["/local_a"])


def test_download_files_delegates_to_single(single_backend_config):
    client = StorageClient(single_backend_config)
    client._delegate.download_files = MagicMock()

    client.download_files(["/remote/a"], ["/local/a"], max_workers=8)

    client._delegate.download_files.assert_called_once_with(["/remote/a"], ["/local/a"], None, 8)


def test_download_files_delegates_metadata_to_single(single_backend_config):
    client = StorageClient(single_backend_config)
    client._delegate.download_files = MagicMock()

    meta = [ObjectMetadata(key="/remote/a", content_length=42, last_modified=datetime.now())]
    client.download_files(["/remote/a"], ["/local/a"], metadata=meta, max_workers=8)

    client._delegate.download_files.assert_called_once_with(["/remote/a"], ["/local/a"], meta, 8)


def test_download_files_delegates_to_composite(multi_backend_config):
    client = StorageClient(multi_backend_config)
    client._delegate.download_files = MagicMock()

    client.download_files(["/remote/a"], ["/local/a"])

    client._delegate.download_files.assert_called_once_with(["/remote/a"], ["/local/a"], None, 16)


def test_upload_files_delegates_to_single(single_backend_config):
    client = StorageClient(single_backend_config)
    client._delegate.upload_files = MagicMock()

    client.upload_files(["/remote/a"], ["/local/a"], max_workers=4)

    client._delegate.upload_files.assert_called_once_with(["/remote/a"], ["/local/a"], None, 4)


def test_upload_files_delegates_attributes_to_single(single_backend_config):
    client = StorageClient(single_backend_config)
    client._delegate.upload_files = MagicMock()

    attrs = [{"key": "val"}]
    client.upload_files(["/remote/a"], ["/local/a"], attributes=attrs, max_workers=4)

    client._delegate.upload_files.assert_called_once_with(["/remote/a"], ["/local/a"], attrs, 4)


def test_single_download_files_without_metadata_delegates_to_provider(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)
    assert single._metadata_provider is None

    single._storage_provider.download_files = MagicMock()

    client.download_files(["/a", "/b"], ["/la", "/lb"], max_workers=4)

    single._storage_provider.download_files.assert_called_once_with(["/a", "/b"], ["/la", "/lb"], None, 4)


def test_single_download_files_with_metadata_resolves_paths(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.side_effect = [
        ResolvedPath(physical_path="physical/a", state=ResolvedPathState.EXISTS),
        ResolvedPath(physical_path="physical/b", state=ResolvedPathState.EXISTS),
    ]
    single._metadata_provider = metadata_provider
    single._storage_provider.download_files = MagicMock()

    client.download_files(["logical/a", "logical/b"], ["/la", "/lb"], max_workers=8)

    assert metadata_provider.realpath.call_count == 2
    single._storage_provider.download_files.assert_called_once_with(
        ["physical/a", "physical/b"], ["/la", "/lb"], None, 8
    )


def test_single_download_files_with_metadata_reports_missing_file_without_data_transfer(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.side_effect = [
        ResolvedPath(physical_path="physical/a", state=ResolvedPathState.EXISTS),
        ResolvedPath(physical_path="logical/missing", state=ResolvedPathState.UNTRACKED),
    ]
    single._metadata_provider = metadata_provider
    single._storage_provider.download_files = MagicMock()

    with pytest.raises(FileNotFoundError):
        client.download_files(["logical/a", "logical/missing"], ["/la", "/lb"])

    single._storage_provider.download_files.assert_not_called()


def test_single_download_files_with_metadata_does_not_retry_metadata_provider_failures(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.side_effect = RetryableError("metadata provider unavailable")
    single._metadata_provider = metadata_provider
    single._storage_provider.download_files = MagicMock()

    with pytest.raises(RetryableError):
        client.download_files(["logical/a"], ["/la"])

    assert metadata_provider.realpath.call_count == 1
    single._storage_provider.download_files.assert_not_called()


def test_single_download_files_with_metadata_reports_provider_failures_for_physical_paths(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.return_value = ResolvedPath(physical_path="physical/a", state=ResolvedPathState.EXISTS)
    single._metadata_provider = metadata_provider
    single._storage_provider.download_files = MagicMock(
        side_effect=BatchTransferError(
            [
                BatchTransferFailure(
                    index=0,
                    source_path="physical/a",
                    destination_path="/la",
                    error=FileNotFoundError("missing"),
                )
            ]
        )
    )

    with pytest.raises(BatchTransferError) as exc_info:
        client.download_files(["logical/a"], ["/la"])

    assert exc_info.value.failures[0].index == 0
    assert exc_info.value.failures[0].source_path == "physical/a"
    assert exc_info.value.failures[0].destination_path == "/la"


def test_single_download_files_empty_lists(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    single._storage_provider.download_files = MagicMock()
    client.download_files([], [])
    single._storage_provider.download_files.assert_called_once_with([], [], None, 16)


def test_single_upload_files_without_metadata_delegates_to_provider(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)
    assert single._metadata_provider is None

    single._storage_provider.upload_files = MagicMock()

    client.upload_files(["/remote/a", "/remote/b"], ["/la", "/lb"], max_workers=4)

    single._storage_provider.upload_files.assert_called_once_with(["/la", "/lb"], ["/remote/a", "/remote/b"], None, 4)


def test_single_upload_files_without_metadata_forwards_attributes(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)
    assert single._metadata_provider is None

    single._storage_provider.upload_files = MagicMock()

    attrs = [{"k1": "v1"}, {"k2": "v2"}]
    client.upload_files(["/remote/a", "/remote/b"], ["/la", "/lb"], attributes=attrs, max_workers=4)

    single._storage_provider.upload_files.assert_called_once_with(["/la", "/lb"], ["/remote/a", "/remote/b"], attrs, 4)


def test_single_upload_files_with_metadata_resolves_and_registers(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.side_effect = [
        ResolvedPath(physical_path="logical/a", state=ResolvedPathState.UNTRACKED),
        ResolvedPath(physical_path="logical/b", state=ResolvedPathState.UNTRACKED),
    ]
    metadata_provider.generate_physical_path.side_effect = [
        ResolvedPath(physical_path="physical/a", state=ResolvedPathState.UNTRACKED),
        ResolvedPath(physical_path="physical/b", state=ResolvedPathState.UNTRACKED),
    ]
    metadata_provider.allow_overwrites.return_value = False

    obj_meta_a = ObjectMetadata(key="physical/a", content_length=100, last_modified=datetime.now(tz=timezone.utc))
    obj_meta_b = ObjectMetadata(key="physical/b", content_length=200, last_modified=datetime.now(tz=timezone.utc))
    single._storage_provider.get_object_metadata = MagicMock(side_effect=[obj_meta_a, obj_meta_b])
    single._storage_provider.upload_files = MagicMock()

    single._metadata_provider = metadata_provider
    single._metadata_provider_lock = None

    client.upload_files(["logical/a", "logical/b"], ["/la", "/lb"], max_workers=8)

    single._storage_provider.upload_files.assert_called_once_with(["/la", "/lb"], ["physical/a", "physical/b"], None, 8)
    metadata_provider.generate_physical_path.assert_any_call("logical/a", for_overwrite=False)
    metadata_provider.generate_physical_path.assert_any_call("logical/b", for_overwrite=False)
    assert metadata_provider.add_file.call_count == 2
    metadata_provider.add_file.assert_any_call("logical/a", obj_meta_a)
    metadata_provider.add_file.assert_any_call("logical/b", obj_meta_b)


def test_single_upload_files_with_metadata_and_attributes(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.side_effect = [
        ResolvedPath(physical_path="logical/a", state=ResolvedPathState.UNTRACKED),
        ResolvedPath(physical_path="logical/b", state=ResolvedPathState.UNTRACKED),
    ]
    metadata_provider.generate_physical_path.side_effect = [
        ResolvedPath(physical_path="physical/a", state=ResolvedPathState.UNTRACKED),
        ResolvedPath(physical_path="physical/b", state=ResolvedPathState.UNTRACKED),
    ]
    metadata_provider.allow_overwrites.return_value = False

    obj_meta_a = ObjectMetadata(key="physical/a", content_length=100, last_modified=datetime.now(tz=timezone.utc))
    obj_meta_b = ObjectMetadata(
        key="physical/b", content_length=200, last_modified=datetime.now(tz=timezone.utc), metadata={"existing": "val"}
    )
    single._storage_provider.get_object_metadata = MagicMock(side_effect=[obj_meta_a, obj_meta_b])
    single._storage_provider.upload_files = MagicMock()

    single._metadata_provider = metadata_provider
    single._metadata_provider_lock = None

    attrs = [{"tag": "first", "count": 1}, {"tag": "second", "nested": {"key": "value"}}]
    client.upload_files(["logical/a", "logical/b"], ["/la", "/lb"], attributes=attrs, max_workers=8)

    single._storage_provider.upload_files.assert_called_once_with(
        ["/la", "/lb"], ["physical/a", "physical/b"], attrs, 8
    )
    assert metadata_provider.add_file.call_count == 2
    added_meta_a = metadata_provider.add_file.call_args_list[0][0][1]
    assert added_meta_a.metadata == {"tag": "first", "count": 1}
    added_meta_b = metadata_provider.add_file.call_args_list[1][0][1]
    assert added_meta_b.metadata == {"existing": "val", "tag": "second", "nested": {"key": "value"}}


def test_single_upload_files_registers_metadata_in_parallel(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.side_effect = [
        ResolvedPath(physical_path="logical/a", state=ResolvedPathState.UNTRACKED),
        ResolvedPath(physical_path="logical/b", state=ResolvedPathState.UNTRACKED),
    ]
    metadata_provider.generate_physical_path.side_effect = [
        ResolvedPath(physical_path="physical/a", state=ResolvedPathState.UNTRACKED),
        ResolvedPath(physical_path="physical/b", state=ResolvedPathState.UNTRACKED),
    ]
    metadata_provider.allow_overwrites.return_value = False
    single._metadata_provider = metadata_provider
    single._metadata_provider_lock = threading.Lock()
    single._storage_provider.upload_files = MagicMock()

    started_count = 0
    started_lock = threading.Lock()
    both_started = threading.Event()

    def get_object_metadata(path: str, strict: bool = True):
        nonlocal started_count
        with started_lock:
            started_count += 1
            if started_count == 2:
                both_started.set()

        if not both_started.wait(timeout=1):
            raise AssertionError("metadata registration did not run in parallel")

        return ObjectMetadata(key=path, content_length=100, last_modified=datetime.now(tz=timezone.utc))

    single._storage_provider.get_object_metadata = get_object_metadata

    client.upload_files(["logical/a", "logical/b"], ["/la", "/lb"], max_workers=2)

    assert metadata_provider.add_file.call_count == 2
    metadata_provider.add_file.assert_any_call(
        "logical/a", ObjectMetadata(key="physical/a", content_length=100, last_modified=ANY)
    )
    metadata_provider.add_file.assert_any_call(
        "logical/b", ObjectMetadata(key="physical/b", content_length=100, last_modified=ANY)
    )


def test_single_upload_files_with_metadata_rejects_overwrite(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.return_value = ResolvedPath(physical_path="existing", state=ResolvedPathState.EXISTS)
    metadata_provider.allow_overwrites.return_value = False
    single._metadata_provider = metadata_provider
    single._storage_provider.upload_files = MagicMock()

    with pytest.raises(FileExistsError):
        client.upload_files(["existing"], ["/la"])

    single._storage_provider.upload_files.assert_not_called()


def test_single_upload_files_with_metadata_does_not_upload_when_overwrite_fails(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.side_effect = [
        ResolvedPath(physical_path="logical/a", state=ResolvedPathState.UNTRACKED),
        ResolvedPath(physical_path="existing", state=ResolvedPathState.EXISTS),
    ]
    metadata_provider.generate_physical_path.return_value = ResolvedPath(
        physical_path="physical/a", state=ResolvedPathState.UNTRACKED
    )
    metadata_provider.allow_overwrites.return_value = False
    obj_meta = ObjectMetadata(key="physical/a", content_length=100, last_modified=datetime.now(tz=timezone.utc))
    single._storage_provider.get_object_metadata = MagicMock(return_value=obj_meta)
    single._storage_provider.upload_files = MagicMock()
    single._metadata_provider = metadata_provider
    single._metadata_provider_lock = None

    with pytest.raises(FileExistsError):
        client.upload_files(["logical/a", "existing"], ["/la", "/lb"])

    single._storage_provider.upload_files.assert_not_called()
    metadata_provider.add_file.assert_not_called()


def test_single_upload_files_with_metadata_does_not_register_partial_provider_success(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.side_effect = [
        ResolvedPath(physical_path="logical/a", state=ResolvedPathState.UNTRACKED),
        ResolvedPath(physical_path="logical/b", state=ResolvedPathState.UNTRACKED),
    ]
    metadata_provider.generate_physical_path.side_effect = [
        ResolvedPath(physical_path="physical/a", state=ResolvedPathState.UNTRACKED),
        ResolvedPath(physical_path="physical/b", state=ResolvedPathState.UNTRACKED),
    ]
    metadata_provider.allow_overwrites.return_value = False
    single._metadata_provider = metadata_provider
    single._metadata_provider_lock = None
    single._storage_provider.get_object_metadata = MagicMock(
        return_value=ObjectMetadata(key="physical/a", content_length=100, last_modified=datetime.now(tz=timezone.utc))
    )
    single._storage_provider.upload_files = MagicMock(
        side_effect=BatchTransferError(
            [
                BatchTransferFailure(
                    index=1,
                    source_path="/lb",
                    destination_path="physical/b",
                    error=RuntimeError("permanent upload failure"),
                )
            ]
        )
    )

    with pytest.raises(BatchTransferError):
        client.upload_files(["logical/a", "logical/b"], ["/la", "/lb"])

    metadata_provider.add_file.assert_not_called()


def test_single_upload_files_with_metadata_does_not_retry_metadata_provider_failures(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.side_effect = RetryableError("metadata provider unavailable")
    single._metadata_provider = metadata_provider
    single._metadata_provider_lock = None
    single._storage_provider.upload_files = MagicMock()

    with pytest.raises(RetryableError):
        client.upload_files(["logical/a"], ["/la"])

    assert metadata_provider.realpath.call_count == 1
    single._storage_provider.upload_files.assert_not_called()


def test_single_upload_files_with_metadata_reports_provider_failures_for_physical_paths(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.return_value = ResolvedPath(physical_path="logical/a", state=ResolvedPathState.UNTRACKED)
    metadata_provider.generate_physical_path.return_value = ResolvedPath(
        physical_path="physical/a", state=ResolvedPathState.UNTRACKED
    )
    metadata_provider.allow_overwrites.return_value = False
    single._metadata_provider = metadata_provider
    single._metadata_provider_lock = None
    single._storage_provider.upload_files = MagicMock(
        side_effect=BatchTransferError(
            [
                BatchTransferFailure(
                    index=0,
                    source_path="/la",
                    destination_path="physical/a",
                    error=FileNotFoundError("missing local"),
                )
            ]
        )
    )

    with pytest.raises(BatchTransferError) as exc_info:
        client.upload_files(["logical/a"], ["/la"])

    assert exc_info.value.failures[0].index == 0
    assert exc_info.value.failures[0].source_path == "/la"
    assert exc_info.value.failures[0].destination_path == "physical/a"


def test_single_upload_files_with_metadata_allows_overwrite(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    metadata_provider.realpath.return_value = ResolvedPath(physical_path="existing", state=ResolvedPathState.EXISTS)
    metadata_provider.allow_overwrites.return_value = True
    metadata_provider.generate_physical_path.return_value = ResolvedPath(
        physical_path="physical/existing_v2", state=ResolvedPathState.EXISTS
    )
    obj_meta = ObjectMetadata(
        key="physical/existing_v2", content_length=100, last_modified=datetime.now(tz=timezone.utc)
    )
    single._storage_provider.get_object_metadata = MagicMock(return_value=obj_meta)
    single._storage_provider.upload_files = MagicMock()
    single._metadata_provider = metadata_provider
    single._metadata_provider_lock = None

    client.upload_files(["existing"], ["/la"])

    metadata_provider.generate_physical_path.assert_called_once_with("existing", for_overwrite=True)
    single._storage_provider.upload_files.assert_called_once_with(["/la"], ["physical/existing_v2"], None, 16)
    metadata_provider.add_file.assert_called_once_with("existing", obj_meta)


def test_upload_files_rejects_mismatched_attributes_length(single_backend_config):
    client = StorageClient(single_backend_config)
    with pytest.raises(ValueError, match="attributes must have the same length"):
        client.upload_files(["/a", "/b"], ["/la", "/lb"], attributes=[{"k": "v"}])


def test_composite_download_files_groups_by_profile(multi_backend_config):
    client = StorageClient(multi_backend_config)
    composite = client._delegate
    assert isinstance(composite, CompositeStorageClient)

    composite._metadata_provider = MagicMock()
    composite._metadata_provider.realpath.side_effect = [
        ResolvedPath(physical_path="phys/a", state=ResolvedPathState.EXISTS, profile="loc1"),
        ResolvedPath(physical_path="phys/b", state=ResolvedPathState.EXISTS, profile="loc2"),
        ResolvedPath(physical_path="phys/c", state=ResolvedPathState.EXISTS, profile="loc1"),
    ]

    loc1_client = MagicMock()
    loc2_client = MagicMock()
    composite._child_clients["loc1"] = loc1_client
    composite._child_clients["loc2"] = loc2_client

    client.download_files(
        ["logical/a", "logical/b", "logical/c"],
        ["/la", "/lb", "/lc"],
        max_workers=4,
    )

    loc1_client.download_files.assert_called_once_with(["phys/a", "phys/c"], ["/la", "/lc"], None, 4)
    loc2_client.download_files.assert_called_once_with(["phys/b"], ["/lb"], None, 4)


def test_composite_download_files_raises_on_missing_file(multi_backend_config):
    client = StorageClient(multi_backend_config)
    composite = client._delegate
    assert isinstance(composite, CompositeStorageClient)

    composite._metadata_provider = MagicMock()
    composite._metadata_provider.realpath.return_value = ResolvedPath(
        physical_path="missing", state=ResolvedPathState.UNTRACKED, profile="loc1"
    )

    with pytest.raises(FileNotFoundError, match="not found"):
        client.download_files(["missing"], ["/la"])


def test_composite_download_files_raises_on_none_profile(multi_backend_config):
    client = StorageClient(multi_backend_config)
    composite = client._delegate
    assert isinstance(composite, CompositeStorageClient)

    composite._metadata_provider = MagicMock()
    composite._metadata_provider.realpath.return_value = ResolvedPath(
        physical_path="phys/a", state=ResolvedPathState.EXISTS, profile=None
    )

    with pytest.raises(ValueError, match="requires profile"):
        client.download_files(["logical/a"], ["/la"])


def test_make_symlink_delegates_to_single(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)
    assert single._metadata_provider is None

    single._storage_provider.make_symlink = MagicMock()

    client.make_symlink("link.txt", "target.txt")

    single._storage_provider.make_symlink.assert_called_once_with("link.txt", "target.txt")


def test_make_symlink_with_metadata_provider(single_backend_config):
    client = StorageClient(single_backend_config)
    single = client._delegate
    assert isinstance(single, SingleStorageClient)

    metadata_provider = MagicMock()
    single._metadata_provider = metadata_provider
    single._metadata_provider_lock = MagicMock()
    single._storage_provider.make_symlink = MagicMock()

    client.make_symlink("link.txt", "target.txt")

    single._storage_provider.make_symlink.assert_not_called()
    metadata_provider.add_file.assert_called_once()
    call_args = metadata_provider.add_file.call_args
    assert call_args[0][0] == "link.txt"
    obj_metadata = call_args[0][1]
    assert obj_metadata.key == "link.txt"
    assert obj_metadata.symlink_target == "target.txt"
    assert obj_metadata.content_length == 0


def test_composite_make_symlink_raises(multi_backend_config):
    client = StorageClient(multi_backend_config)
    composite = client._delegate
    assert isinstance(composite, CompositeStorageClient)

    with pytest.raises(NotImplementedError):
        client.make_symlink("link.txt", "target.txt")
