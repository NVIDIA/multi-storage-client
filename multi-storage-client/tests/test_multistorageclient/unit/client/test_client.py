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

import pytest

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.client.composite import CompositeStorageClient
from multistorageclient.client.single import SingleStorageClient


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
