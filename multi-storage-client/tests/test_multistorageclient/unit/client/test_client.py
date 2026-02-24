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
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.client.composite import CompositeStorageClient
from multistorageclient.client.single import SingleStorageClient
from multistorageclient.types import ObjectMetadata


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
        follow_symlinks=True,
        patterns=None,
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
        follow_symlinks=True,
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
