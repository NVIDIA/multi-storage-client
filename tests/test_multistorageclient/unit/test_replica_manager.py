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

import uuid

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.types import ExecutionMode
from test_multistorageclient.unit.utils import tempdatastore


def test_replica_read_from_replica_after_sync():
    """Test that data written to origin store can be read from replica after sync_replicas."""
    origin_profile = "origin"
    replica_profile = "replica"
    origin_with_replica_profile = "origin_with_replica"

    with (
        tempdatastore.TemporaryPOSIXDirectory() as origin_store,
        tempdatastore.TemporaryPOSIXDirectory() as replica_store,
    ):
        # Create configuration with origin and replica profiles
        config = {
            "profiles": {
                origin_profile: origin_store.profile_config_dict(),
                replica_profile: replica_store.profile_config_dict(),
                origin_with_replica_profile: origin_store.profile_config_dict()
                | {"replicas": [{"replica_profile": replica_profile, "read_priority": 1}]},
            }
        }

        # Create origin client for writing data
        origin_client = StorageClient(config=StorageClientConfig.from_dict(config, profile=origin_profile))

        # Create client with replica configuration for reading
        origin_with_replica_client = StorageClient(
            config=StorageClientConfig.from_dict(config, profile=origin_with_replica_profile)
        )

        # Test data
        test_file_path = f"test-data-{uuid.uuid4()}/testfile.bin"
        test_content = b"This is test content for replica testing"

        # Step 1: Write data to origin store
        origin_client.write(test_file_path, test_content)

        # Verify file exists in origin storage
        assert origin_client.is_file(test_file_path), f"File {test_file_path} should exist in origin storage"

        # Step 2: Sync replicas
        origin_with_replica_client.sync_replicas("", execution_mode=ExecutionMode.LOCAL)

        # Step 3: Verify replicas are configured
        assert hasattr(origin_with_replica_client, "replicas"), "Client should have replicas attribute"
        assert origin_with_replica_client.replicas is not None, "Replicas should not be None"
        assert len(origin_with_replica_client.replicas) > 0, "At least one replica should be configured"

        # Step 4: Verify file exists in replica
        for replica in origin_with_replica_client.replicas:
            assert replica.is_file(test_file_path), f"File {test_file_path} should exist in replica {replica.profile}"

        # Step 5: Read from replica (this should trigger replica-aware reading)
        content_from_replica = origin_with_replica_client.read(test_file_path)

        # Step 6: Verify content from replica matches original content
        assert content_from_replica == test_content, (
            f"Content from replica mismatch: expected {test_content}, got {content_from_replica}"
        )


def test_replica_read_with_multiple_replicas():
    """Test replica reading with multiple replicas configured."""
    origin_profile = "origin"
    replica1_profile = "replica1"
    replica2_profile = "replica2"
    origin_with_replicas_profile = "origin_with_replicas"

    with (
        tempdatastore.TemporaryPOSIXDirectory() as origin_store,
        tempdatastore.TemporaryPOSIXDirectory() as replica1_store,
        tempdatastore.TemporaryPOSIXDirectory() as replica2_store,
    ):
        # Create configuration with origin and multiple replica profiles
        config = {
            "profiles": {
                origin_profile: origin_store.profile_config_dict(),
                replica1_profile: replica1_store.profile_config_dict(),
                replica2_profile: replica2_store.profile_config_dict(),
                origin_with_replicas_profile: origin_store.profile_config_dict()
                | {
                    "replicas": [
                        {"replica_profile": replica1_profile, "read_priority": 1},
                        {"replica_profile": replica2_profile, "read_priority": 2},
                    ]
                },
            }
        }

        # Create origin client for writing data
        origin_client = StorageClient(config=StorageClientConfig.from_dict(config, profile=origin_profile))

        # Create client with multiple replica configuration for reading
        origin_with_replicas_client = StorageClient(
            config=StorageClientConfig.from_dict(config, profile=origin_with_replicas_profile)
        )

        # Test data
        test_file_path = f"test-data-{uuid.uuid4()}/testfile.bin"
        test_content = b"This is test content for multiple replica testing"

        # Step 1: Write data to origin store
        origin_client.write(test_file_path, test_content)

        # Verify file exists in origin storage
        assert origin_client.is_file(test_file_path), f"File {test_file_path} should exist in origin storage"

        # Step 2: Sync replicas
        origin_with_replicas_client.sync_replicas("", execution_mode=ExecutionMode.LOCAL)

        # Step 3: Verify multiple replicas are configured
        assert len(origin_with_replicas_client.replicas) == 2, "Should have exactly 2 replicas configured"

        # Step 4: Verify file exists in all replicas with the same content
        for replica in origin_with_replicas_client.replicas:
            assert replica.is_file(test_file_path), f"File {test_file_path} should exist in replica {replica.profile}"
            # Verify that all replicas have the same content after syncing
            replica_content = replica.read(test_file_path)
            assert replica_content == test_content, (
                f"Replica {replica.profile} should have the same content as origin: expected {test_content}, got {replica_content}"
            )

        # Step 5: Alter the file on the lower-priority replica (replica2)
        # This will test that read_priority is respected when replicas have different content
        altered_content = b"This is altered content in replica2"
        replica2_client = StorageClient(config=StorageClientConfig.from_dict(config, profile=replica2_profile))
        replica2_client.write(test_file_path, altered_content)

        # Verify replica2 now has different content
        assert replica2_client.read(test_file_path) == altered_content, "Replica2 should have altered content"

        # Step 6: Read from replicas (should use replica with highest priority first)
        # The replica manager should respect read_priority and read from replica1 (priority 1) first
        content_from_replica = origin_with_replicas_client.read(test_file_path)

        # Step 7: Verify content from replica matches the original content from replica1 (highest priority)
        # Since replica1 has the original content and replica2 has altered content,
        # and replica1 has higher priority (1 vs 2), we should get the original content
        assert content_from_replica == test_content, (
            f"Content from replica should match original content from highest-priority replica: expected {test_content}, got {content_from_replica}"
        )


def test_replica_read_with_cache():
    """Test replica reading with cache enabled."""
    origin_profile = "origin"
    replica_profile = "replica"
    origin_with_replica_profile = "origin_with_replica"

    with (
        tempdatastore.TemporaryPOSIXDirectory() as origin_store,
        tempdatastore.TemporaryPOSIXDirectory() as replica_store,
    ):
        import tempfile

        # Create configuration with origin, replica, and cache
        config = {
            "profiles": {
                origin_profile: origin_store.profile_config_dict(),
                replica_profile: replica_store.profile_config_dict(),
                origin_with_replica_profile: origin_store.profile_config_dict()
                | {"replicas": [{"replica_profile": replica_profile, "read_priority": 1}]},
            },
            "cache": {
                "size": "10M",
                "use_etag": True,
                "location": tempfile.mkdtemp(),
                "eviction_policy": {
                    "policy": "random",
                },
            },
        }

        # Create origin client for writing data
        origin_client = StorageClient(config=StorageClientConfig.from_dict(config, profile=origin_profile))

        # Create client with replica and cache configuration for reading
        origin_with_replica_client = StorageClient(
            config=StorageClientConfig.from_dict(config, profile=origin_with_replica_profile)
        )

        # Test data
        test_file_path = f"test-data-{uuid.uuid4()}/testfile.bin"
        test_content = b"This is test content for replica testing with cache"

        # Step 1: Write data to origin store
        origin_client.write(test_file_path, test_content)

        # Verify file exists in origin storage
        assert origin_client.is_file(test_file_path), f"File {test_file_path} should exist in origin storage"

        # Step 2: Sync replicas
        origin_with_replica_client.sync_replicas("", execution_mode=ExecutionMode.LOCAL)

        # Step 3: Verify cache is configured
        assert origin_with_replica_client._cache_manager is not None, "Cache manager should be configured"

        # Step 4: Read from replica (should use cache if available, then replica)
        content_from_replica = origin_with_replica_client.read(test_file_path)

        # Step 5: Verify content from replica matches original content
        assert content_from_replica == test_content, (
            f"Content from replica mismatch: expected {test_content}, got {content_from_replica}"
        )


def test_replica_read_fallback_to_origin():
    """Test that reading falls back to origin when file is not in any replica."""
    origin_profile = "origin"
    replica_profile = "replica"
    origin_with_replica_profile = "origin_with_replica"

    with (
        tempdatastore.TemporaryPOSIXDirectory() as origin_store,
        tempdatastore.TemporaryPOSIXDirectory() as replica_store,
    ):
        # Create configuration with origin and replica profiles
        config = {
            "profiles": {
                origin_profile: origin_store.profile_config_dict(),
                replica_profile: replica_store.profile_config_dict(),
                origin_with_replica_profile: origin_store.profile_config_dict()
                | {"replicas": [{"replica_profile": replica_profile, "read_priority": 1}]},
            }
        }

        # Create origin client for writing data
        origin_client = StorageClient(config=StorageClientConfig.from_dict(config, profile=origin_profile))

        # Create client with replica configuration for reading
        origin_with_replica_client = StorageClient(
            config=StorageClientConfig.from_dict(config, profile=origin_with_replica_profile)
        )

        # Test data
        test_file_path = f"test-data-{uuid.uuid4()}/testfile.bin"
        test_content = b"This is test content for fallback testing"

        # Step 1: Write data to origin store only (don't sync to replica)
        origin_client.write(test_file_path, test_content)

        # Verify file exists in origin storage
        assert origin_client.is_file(test_file_path), f"File {test_file_path} should exist in origin storage"

        # Step 2: Verify file does NOT exist in replica
        replica_client = StorageClient(config=StorageClientConfig.from_dict(config, profile=replica_profile))
        assert not replica_client.is_file(test_file_path), f"File {test_file_path} should NOT exist in replica"

        # Step 3: Read from client with replica configuration
        # This should fall back to origin since the file doesn't exist in replica
        content_from_fallback = origin_with_replica_client.read(test_file_path)

        # Step 4: Verify content from fallback matches original content from origin
        assert content_from_fallback == test_content, (
            f"Content from fallback should match original content from origin: expected {test_content}, got {content_from_fallback}"
        )


def test_storage_client_copy_with_replicas():
    """Test copy functionality with replicas configured."""
    origin_profile = "origin"
    replica_profile = "replica"
    origin_with_replica_profile = "origin_with_replica"

    with (
        tempdatastore.TemporaryPOSIXDirectory() as origin_store,
        tempdatastore.TemporaryPOSIXDirectory() as replica_store,
    ):
        # Create configuration with origin and replica profiles
        config = {
            "profiles": {
                origin_profile: origin_store.profile_config_dict(),
                replica_profile: replica_store.profile_config_dict(),
                origin_with_replica_profile: origin_store.profile_config_dict()
                | {"replicas": [{"replica_profile": replica_profile, "read_priority": 1}]},
            }
        }

        # Create origin client for writing data
        origin_client = StorageClient(config=StorageClientConfig.from_dict(config, profile=origin_profile))

        # Create client with replica configuration for copying
        origin_with_replica_client = StorageClient(
            config=StorageClientConfig.from_dict(config, profile=origin_with_replica_profile)
        )

        # Test data
        src_path = f"test-data-{uuid.uuid4()}/source.txt"
        dest_path = f"new_path/{src_path}"  # Copy to a new location
        test_content = b"This is test content for copy testing with replicas"

        # Step 1: Write source file to origin
        origin_client.write(src_path, test_content)

        # Verify source file exists in origin
        assert origin_client.is_file(src_path), f"Source file {src_path} should exist in origin"

        # Step 2: Sync source file to replicas so it's accessible in all storage locations
        origin_with_replica_client.sync_replicas("", execution_mode=ExecutionMode.LOCAL)

        # Verify source file exists in replica
        replica_client = StorageClient(config=StorageClientConfig.from_dict(config, profile=replica_profile))
        assert replica_client.is_file(src_path), f"Source file {src_path} should exist in replica after sync"

        # Step 3: Copy file using origin client (which has replica configuration)
        # Now that src_path exists in both origin and replica, the copy should work
        origin_with_replica_client.copy(src_path, dest_path)

        # Step 3: Verify destination file exists in origin
        assert origin_client.is_file(dest_path), f"Destination file {dest_path} should exist in origin"
        assert origin_client.read(dest_path) == test_content, "Destination file in origin should have correct content"

        # Step 4: Verify destination file exists in replica (should be copied via replica manager)
        assert replica_client.is_file(dest_path), f"Destination file {dest_path} should exist in replica"
        assert replica_client.read(dest_path) == test_content, "Destination file in replica should have correct content"
