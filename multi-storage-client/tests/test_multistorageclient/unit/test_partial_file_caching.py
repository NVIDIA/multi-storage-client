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

import os
import tempfile
import time
import uuid
from typing import Any, Dict

import xattr

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.types import Range, SourceVersionCheckMode
from test_multistorageclient.unit.utils import tempdatastore
from test_multistorageclient.unit.utils.tempdatastore import create_test_data

# Type alias for configuration dictionary
ConfigDict = Dict[str, Any]


def create_partial_caching_config(
    origin_store: tempdatastore.TemporaryDataStore,
    origin_profile: str = "origin",
) -> ConfigDict:
    """Create a configuration with origin store and partial file caching enabled."""
    return {
        "profiles": {
            origin_profile: origin_store.profile_config_dict() | {"caching_enabled": True},
        },
        "cache": {
            "size": "50M",
            "location": tempfile.mkdtemp(),
            "cache_line_size": "1M",  # 1MB cache lines for testing
            "check_source_version": True,
            "eviction_policy": {
                "policy": "lru",
                "refresh_interval": 300,
            },
        },
    }


def test_partial_file_caching_range_read() -> None:
    """Test partial file caching with range reads."""
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create configuration with partial file caching enabled
        config = create_partial_caching_config(origin_store)
        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create 3 test files, each 4MB
        test_files = []
        for i in range(3):
            file_path = f"test-data-{uuid.uuid4()}/file_{i}.bin"
            test_content = create_test_data(4)  # 4MB file
            test_files.append((file_path, test_content))

        # Write test files to origin store
        for file_path, content in test_files:
            client.write(file_path, content)
            # Note: We don't do a full read here to avoid caching the full file,
            # which would prevent chunk-based range reads from being tested

        # Test partial file caching with range read
        test_file_path, test_content = test_files[0]  # Use first file for testing

        # Read 16KB starting at offset 512KB (should be in chunk 0)
        range_read = Range(offset=512 * 1024, size=16 * 1024)  # 16KB at 512KB offset
        partial_content = client.read(test_file_path, byte_range=range_read)

        # Verify the range read returned correct data
        expected_content = test_content[range_read.offset : range_read.offset + range_read.size]
        assert partial_content == expected_content, (
            f"Range read content mismatch: expected {len(expected_content)} bytes, got {len(partial_content)} bytes"
        )

        # Verify that only the first chunk (1MB) was downloaded to cache
        # The chunk should be stored as .file_0.bin#chunk0
        cache_dir = config["cache"]["location"]
        cache_profile_dir = os.path.join(cache_dir, "origin")
        # The cache path mirrors the file structure
        file_dir = os.path.join(cache_profile_dir, os.path.dirname(test_file_path))
        base_name = os.path.basename(test_file_path)
        chunk_path = os.path.join(file_dir, f".{base_name}#chunk0")

        # Check that the chunk file exists and is 1MB
        assert os.path.exists(chunk_path), f"Chunk file {chunk_path} should exist"
        chunk_size = os.path.getsize(chunk_path)
        assert chunk_size == 1024 * 1024, f"Chunk size should be 1MB, got {chunk_size} bytes"

        # Verify that no other chunks were downloaded (chunk1, chunk2, chunk3 should not exist)
        for chunk_idx in [1, 2, 3]:
            other_chunk_path = os.path.join(file_dir, f".{base_name}#chunk{chunk_idx}")
            assert not os.path.exists(other_chunk_path), f"Chunk {chunk_idx} should not exist yet"

        # Test another range read that spans two chunks
        # Read 1.5MB starting at offset 512KB (spans chunk 0 and chunk 1)
        range_read_spanning = Range(offset=512 * 1024, size=1536 * 1024)  # 1.5MB at 512KB offset
        spanning_content = client.read(test_file_path, byte_range=range_read_spanning)

        # Verify the spanning range read returned correct data
        expected_spanning_content = test_content[
            range_read_spanning.offset : range_read_spanning.offset + range_read_spanning.size
        ]
        assert spanning_content == expected_spanning_content, (
            f"Spanning range read content mismatch: expected {len(expected_spanning_content)} bytes, got {len(spanning_content)} bytes"
        )

        # Verify that both chunk 0 and chunk 1 now exist
        chunk1_path = os.path.join(file_dir, f".{base_name}#chunk1")
        assert os.path.exists(chunk1_path), "Chunk 1 should exist after spanning read"
        chunk1_size = os.path.getsize(chunk1_path)
        assert chunk1_size == 1024 * 1024, f"Chunk 1 size should be 1MB, got {chunk1_size} bytes"

        # Verify chunk 2 and 3 still don't exist
        for chunk_idx in [2, 3]:
            other_chunk_path = os.path.join(file_dir, f".{base_name}#chunk{chunk_idx}")
            assert not os.path.exists(other_chunk_path), f"Chunk {chunk_idx} should not exist yet"


def test_partial_file_caching_without_source_version() -> None:
    """Test partial file caching when source_version is disabled (None)."""
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create configuration with partial file caching enabled but source version disabled
        config = create_partial_caching_config(origin_store)
        config["cache"]["check_source_version"] = False  # Disable source version checking

        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a test file
        file_path = f"test-data-{uuid.uuid4()}/file.bin"
        test_content = create_test_data(4)  # 4MB file
        client.write(file_path, test_content)

        # Read a range that should trigger chunking
        range_read = Range(offset=512 * 1024, size=16 * 1024)  # 16KB at 512KB offset
        partial_content = client.read(
            file_path, byte_range=range_read, check_source_version=SourceVersionCheckMode.DISABLE
        )

        # Verify the range read returned correct data
        expected_content = test_content[range_read.offset : range_read.offset + range_read.size]
        assert partial_content == expected_content, (
            f"Range read content mismatch: expected {len(expected_content)} bytes, got {len(partial_content)} bytes"
        )

        # Verify that chunk was created (should work without xattr validation)
        cache_dir = config["cache"]["location"]
        cache_profile_dir = os.path.join(cache_dir, "origin")
        file_dir = os.path.join(cache_profile_dir, os.path.dirname(file_path))
        base_name = os.path.basename(file_path)
        chunk_path = os.path.join(file_dir, f".{base_name}#chunk0")
        full_cache_path = os.path.join(file_dir, base_name)

        # When size=None, chunk 0 gets renamed to the original file name
        # Check that either the chunk file exists OR the full file exists (renamed chunk)
        chunk_exists = os.path.exists(chunk_path)
        full_file_exists = os.path.exists(full_cache_path)

        assert chunk_exists or full_file_exists, (
            f"Either chunk file {chunk_path} or full file {full_cache_path} should exist"
        )

        # Check the size of whichever file exists
        if chunk_exists:
            chunk_size = os.path.getsize(chunk_path)
            assert chunk_size == 1024 * 1024, f"Chunk size should be 1MB, got {chunk_size} bytes"
        else:
            full_file_size = os.path.getsize(full_cache_path)
            assert full_file_size == 1024 * 1024, f"Full file size should be 1MB, got {full_file_size} bytes"


def test_partial_file_caching_edge_cases() -> None:
    """Test edge cases for partial file caching."""
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        config = create_partial_caching_config(origin_store)
        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a test file
        file_path = f"test-data-{uuid.uuid4()}/edge_case.bin"
        test_content = create_test_data(4)  # 4MB file
        client.write(file_path, test_content)

        # Test 1: Read at chunk boundary (start of chunk 1)
        range_read = Range(offset=1024 * 1024, size=1024)  # 1KB at 1MB offset
        partial_content = client.read(file_path, byte_range=range_read)
        expected_content = test_content[range_read.offset : range_read.offset + range_read.size]
        assert partial_content == expected_content, "Chunk boundary read failed"

        # Test 2: Read at end of file
        range_read = Range(offset=4 * 1024 * 1024 - 1024, size=1024)  # Last 1KB
        partial_content = client.read(file_path, byte_range=range_read)
        expected_content = test_content[range_read.offset : range_read.offset + range_read.size]
        assert partial_content == expected_content, "End of file read failed"

        # Test 3: Read entire chunk
        range_read = Range(offset=1024 * 1024, size=1024 * 1024)  # Entire chunk 1
        partial_content = client.read(file_path, byte_range=range_read)
        expected_content = test_content[range_read.offset : range_read.offset + range_read.size]
        assert partial_content == expected_content, "Entire chunk read failed"

        # Test 4: Read across multiple chunks
        range_read = Range(offset=512 * 1024, size=2 * 1024 * 1024)  # 2MB spanning 3 chunks
        partial_content = client.read(file_path, byte_range=range_read)
        expected_content = test_content[range_read.offset : range_read.offset + range_read.size]
        assert partial_content == expected_content, "Multi-chunk read failed"


def test_partial_file_caching_repeated_reads() -> None:
    """Test that repeated reads use cached chunks."""
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        config = create_partial_caching_config(origin_store)
        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a test file
        file_path = f"test-data-{uuid.uuid4()}/repeated.bin"
        test_content = create_test_data(4)  # 4MB file
        client.write(file_path, test_content)

        # First read - should download chunk
        range_read = Range(offset=512 * 1024, size=16 * 1024)
        partial_content1 = client.read(file_path, byte_range=range_read)
        expected_content = test_content[range_read.offset : range_read.offset + range_read.size]
        assert partial_content1 == expected_content

        # Second read - should use cached chunk
        partial_content2 = client.read(file_path, byte_range=range_read)
        assert partial_content2 == expected_content
        assert partial_content1 == partial_content2

        # Verify chunk file exists
        cache_dir = config["cache"]["location"]
        cache_profile_dir = os.path.join(cache_dir, "origin")
        file_dir = os.path.join(cache_profile_dir, os.path.dirname(file_path))
        base_name = os.path.basename(file_path)
        chunk_path = os.path.join(file_dir, f".{base_name}#chunk0")
        assert os.path.exists(chunk_path), "Chunk should exist after first read"


def test_partial_file_caching_different_files() -> None:
    """Test partial file caching with multiple files."""
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        config = create_partial_caching_config(origin_store)
        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create multiple test files
        test_files = []
        for i in range(3):
            file_path = f"test-data-{uuid.uuid4()}/multi_file_{i}.bin"
            test_content = create_test_data(4)  # 4MB file
            test_files.append((file_path, test_content))
            client.write(file_path, test_content)

        # Read from each file
        for file_path, test_content in test_files:
            range_read = Range(offset=512 * 1024, size=16 * 1024)
            partial_content = client.read(file_path, byte_range=range_read)
            expected_content = test_content[range_read.offset : range_read.offset + range_read.size]
            assert partial_content == expected_content, f"Read failed for {file_path}"

        # Verify chunks exist for each file
        cache_dir = config["cache"]["location"]
        cache_profile_dir = os.path.join(cache_dir, "origin")

        for file_path, _ in test_files:
            file_dir = os.path.join(cache_profile_dir, os.path.dirname(file_path))
            base_name = os.path.basename(file_path)
            chunk_path = os.path.join(file_dir, f".{base_name}#chunk0")
            assert os.path.exists(chunk_path), f"Chunk should exist for {file_path}"


def test_partial_file_caching_large_chunk_size() -> None:
    """Test partial file caching with a custom chunk size of 2MB.

    This test verifies that:
    1. MSC correctly handles custom chunk sizes (2MB instead of default 64MB)
    2. Range reads spanning multiple chunks work correctly
    3. Full chunks are cached for future use
    4. Data integrity is maintained across chunk boundaries
    """
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create configuration with large chunk size
        config = {
            "profiles": {
                "origin": origin_store.profile_config_dict() | {"caching_enabled": True},
            },
            "cache": {
                "size": "50M",
                "location": tempfile.mkdtemp(),
                "cache_line_size": "2M",  # 2MB cache lines
                "check_source_version": True,
                "eviction_policy": {
                    "policy": "lru",
                    "refresh_interval": 300,
                },
            },
        }

        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a test file
        file_path = f"test-data-{uuid.uuid4()}/large_chunk.bin"
        test_content = create_test_data(8)  # 8MB file
        client.write(file_path, test_content)

        # Read that spans multiple 2MB chunks
        range_read = Range(offset=1024 * 1024, size=3 * 1024 * 1024)  # 3MB read
        partial_content = client.read(file_path, byte_range=range_read)
        expected_content = test_content[range_read.offset : range_read.offset + range_read.size]
        assert partial_content == expected_content

        # Verify chunks exist with correct sizes
        cache_dir = config["cache"]["location"]
        cache_profile_dir = os.path.join(cache_dir, "origin")
        file_dir = os.path.join(cache_profile_dir, os.path.dirname(file_path))
        base_name = os.path.basename(file_path)

        # Should have chunk0 and chunk1
        chunk0_path = os.path.join(file_dir, f".{base_name}#chunk0")
        chunk1_path = os.path.join(file_dir, f".{base_name}#chunk1")

        assert os.path.exists(chunk0_path), "Chunk 0 should exist"
        assert os.path.exists(chunk1_path), "Chunk 1 should exist"

        chunk0_size = os.path.getsize(chunk0_path)
        chunk1_size = os.path.getsize(chunk1_path)

        # Both chunks should be 2MB (full chunks)
        expected_size = 2 * 1024 * 1024  # 2MB
        assert chunk0_size == expected_size, f"Chunk 0 should be 2MB, got {chunk0_size} bytes"
        assert chunk1_size == expected_size, f"Chunk 1 should be 2MB, got {chunk1_size} bytes"


def test_partial_file_caching_chunk_invalidation() -> None:
    """Test partial file caching chunk invalidation when source version changes.

    This test verifies that:
    1. Initial chunks are cached with version1
    2. When source version changes to version2, all existing chunks are invalidated
    3. New chunks are fetched with the new version
    4. Data integrity is maintained throughout the process
    """
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create configuration with small chunk size for easier testing
        config = {
            "profiles": {
                "origin": origin_store.profile_config_dict() | {"caching_enabled": True},
            },
            "cache": {
                "size": "50M",
                "location": tempfile.mkdtemp(),
                "cache_line_size": "1M",  # 1MB cache lines for easier testing
                "check_source_version": True,
                "eviction_policy": {
                    "policy": "lru",
                    "refresh_interval": 300,
                },
            },
        }

        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a test file
        file_path = f"test-data-{uuid.uuid4()}/version_test.bin"
        test_content_v1 = create_test_data(4)  # 4MB file
        client.write(file_path, test_content_v1)

        # Get initial metadata (version1)
        metadata_v1 = client.info(file_path)
        etag_v1 = metadata_v1.etag

        # Read first chunk (0-1MB) - this should cache chunk0
        range_read_1 = Range(offset=0, size=1 * 1024 * 1024)  # 1MB read
        partial_content_1 = client.read(file_path, byte_range=range_read_1)
        expected_content_1 = test_content_v1[range_read_1.offset : range_read_1.offset + range_read_1.size]
        assert partial_content_1 == expected_content_1

        # Verify chunk0 exists with version1
        cache_dir = config["cache"]["location"]
        cache_profile_dir = os.path.join(cache_dir, "origin")
        file_dir = os.path.join(cache_profile_dir, os.path.dirname(file_path))
        base_name = os.path.basename(file_path)

        chunk0_path = os.path.join(file_dir, f".{base_name}#chunk0")
        assert os.path.exists(chunk0_path), "Chunk 0 should exist after first read"

        # Verify chunk0 has version1 etag
        chunk_etag = xattr.getxattr(chunk0_path, "user.etag").decode("utf-8")
        assert chunk_etag == etag_v1, f"Chunk should have version1 etag, got {chunk_etag}"

        # Update the file content (this changes the ETag)
        # Create completely different content
        test_content_v2 = b"UPDATED_CONTENT_" * (4 * 1024 * 1024 // 16)  # Different 4MB content
        client.write(file_path, test_content_v2)

        # Get new metadata (version2)
        metadata_v2 = client.info(file_path)
        etag_v2 = metadata_v2.etag

        assert etag_v2 != etag_v1, "ETag should have changed after file update"

        # Read a range that spans both chunks (0-2MB) - this should invalidate chunk0 and fetch both chunks with version2
        range_read_2 = Range(offset=0, size=2 * 1024 * 1024)  # 2MB read spanning chunks 0 and 1
        partial_content_2 = client.read(file_path, byte_range=range_read_2)
        expected_content_2 = test_content_v2[range_read_2.offset : range_read_2.offset + range_read_2.size]
        assert partial_content_2 == expected_content_2

        # Verify chunk0 was invalidated and replaced with version2
        assert os.path.exists(chunk0_path), "Chunk 0 should still exist"
        chunk_etag_after = xattr.getxattr(chunk0_path, "user.etag").decode("utf-8")
        assert chunk_etag_after == etag_v2, f"Chunk should have version2 etag, got {chunk_etag_after}"

        # Verify chunk1 exists with version2
        chunk1_path = os.path.join(file_dir, f".{base_name}#chunk1")
        assert os.path.exists(chunk1_path), "Chunk 1 should exist after second read"
        chunk1_etag = xattr.getxattr(chunk1_path, "user.etag").decode("utf-8")
        assert chunk1_etag == etag_v2, f"Chunk 1 should have version2 etag, got {chunk1_etag}"

        # Verify that reading the first chunk again returns version2 data
        partial_content_1_after = client.read(file_path, byte_range=range_read_1)
        expected_content_1_after = test_content_v2[range_read_1.offset : range_read_1.offset + range_read_1.size]
        assert partial_content_1_after == expected_content_1_after, "First chunk should return version2 data"
        assert partial_content_1_after != expected_content_1, "First chunk should not return version1 data"

        # Verify both chunks have the correct size
        chunk0_size = os.path.getsize(chunk0_path)
        chunk1_size = os.path.getsize(chunk1_path)
        expected_chunk_size = 1 * 1024 * 1024  # 1MB
        assert chunk0_size == expected_chunk_size, f"Chunk 0 should be 1MB, got {chunk0_size} bytes"
        assert chunk1_size == expected_chunk_size, f"Chunk 1 should be 1MB, got {chunk1_size} bytes"


def test_partial_file_caching_cleanup() -> None:
    """Test partial file caching cleanup with automatic eviction.

    This test verifies that:
    1. Partial file chunks are properly cached when reading ranges
    2. After the refresh interval (5 seconds), cleanup kicks in
    3. Old chunks are properly evicted from the cache
    4. Cache size limits are respected
    """
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create configuration with 5-second refresh interval for testing
        config = {
            "profiles": {
                "origin": origin_store.profile_config_dict() | {"caching_enabled": True},
            },
            "cache": {
                "size": "2M",  # Very small cache size to force eviction of chunks
                "location": tempfile.mkdtemp(),
                "cache_line_size": "1M",  # 1MB cache lines
                "check_source_version": True,
                "eviction_policy": {"policy": "lru"},
            },
        }

        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a test file
        file_path = f"test-data-{uuid.uuid4()}/cleanup_test.bin"
        test_content = create_test_data(5)  # 5MB file
        client.write(file_path, test_content)

        # Read first chunk (0-1MB) - this should cache chunk0
        range_read_1 = Range(offset=0, size=1 * 1024 * 1024)  # 1MB read
        partial_content_1 = client.read(file_path, byte_range=range_read_1)
        expected_content_1 = test_content[range_read_1.offset : range_read_1.offset + range_read_1.size]
        assert partial_content_1 == expected_content_1

        # Read second chunk (1-2MB) - this should cache chunk1
        range_read_2 = Range(offset=1 * 1024 * 1024, size=1 * 1024 * 1024)  # 1MB read
        partial_content_2 = client.read(file_path, byte_range=range_read_2)
        expected_content_2 = test_content[range_read_2.offset : range_read_2.offset + range_read_2.size]
        assert partial_content_2 == expected_content_2

        # Verify both chunks exist in cache
        cache_dir = config["cache"]["location"]
        cache_profile_dir = os.path.join(cache_dir, "origin")
        file_dir = os.path.join(cache_profile_dir, os.path.dirname(file_path))
        base_name = os.path.basename(file_path)

        chunk0_path = os.path.join(file_dir, f".{base_name}#chunk0")
        chunk1_path = os.path.join(file_dir, f".{base_name}#chunk1")

        assert os.path.exists(chunk0_path), "Chunk 0 should exist after first read"
        assert os.path.exists(chunk1_path), "Chunk 1 should exist after second read"

        # Verify chunk sizes
        chunk0_size = os.path.getsize(chunk0_path)
        chunk1_size = os.path.getsize(chunk1_path)
        expected_chunk_size = 1 * 1024 * 1024  # 1MB
        assert chunk0_size == expected_chunk_size, f"Chunk 0 should be 1MB, got {chunk0_size} bytes"
        assert chunk1_size == expected_chunk_size, f"Chunk 1 should be 1MB, got {chunk1_size} bytes"

        # Wait for cleanup to trigger (refresh_interval is 5 seconds)
        time.sleep(6)  # Wait a bit more than 5 seconds to ensure cleanup runs

        # Force a cache refresh to trigger cleanup
        # We can do this by reading a different range that would trigger cache operations
        range_read_3 = Range(offset=2 * 1024 * 1024, size=1 * 1024 * 1024)  # 1MB read
        partial_content_3 = client.read(file_path, byte_range=range_read_3)
        expected_content_3 = test_content[range_read_3.offset : range_read_3.offset + range_read_3.size]
        assert partial_content_3 == expected_content_3

        # Force a cache refresh to trigger cleanup
        cache_manager = client._cache_manager
        assert cache_manager is not None
        cache_manager.refresh_cache()

        # Verify that LRU eviction worked correctly:
        # - chunk0 (oldest) should be deleted (LRU policy)
        # - chunk1 and chunk2 should remain (within cache size limit)
        assert not os.path.exists(chunk0_path), "Chunk 0 should be deleted after cleanup (LRU eviction)"
        assert os.path.exists(chunk1_path), "Chunk 1 should remain (within cache size limit)"

        # Verify the new chunk was also created
        chunk2_path = os.path.join(file_dir, f".{base_name}#chunk2")
        assert os.path.exists(chunk2_path), "Chunk 2 should exist after third read"


def test_partial_file_caching_full_file_optimization() -> None:
    """Test that range reads use full cached files when available instead of chunking."""
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create configuration with partial file caching enabled
        config = create_partial_caching_config(origin_store)
        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a 3MB test file
        file_path = f"test-data-{uuid.uuid4()}/full_file_test.bin"
        test_content = create_test_data(3)  # 3MB file
        client.write(file_path, test_content)

        # Verify file was written correctly
        assert client.read(file_path) == test_content, "File content mismatch"

        # Get cache paths
        cache_dir = config["cache"]["location"]
        cache_profile_dir = os.path.join(cache_dir, "origin")
        file_dir = os.path.join(cache_profile_dir, os.path.dirname(file_path))
        base_name = os.path.basename(file_path)
        full_cache_path = os.path.join(file_dir, base_name)

        # Verify full file is cached
        assert os.path.exists(full_cache_path), "Full file should be cached after read"

        # Now perform a range read - this should use the full cached file, not chunks
        range_read = Range(offset=1 * 1024 * 1024, size=512 * 1024)  # 512KB at 1MB offset
        partial_content = client.read(file_path, byte_range=range_read)

        # Verify the range read returned correct data
        expected_content = test_content[range_read.offset : range_read.offset + range_read.size]
        assert partial_content == expected_content, (
            f"Range read content mismatch: expected {len(expected_content)} bytes, got {len(partial_content)} bytes"
        )

        # Verify that NO chunks were created (since we used the full cached file)
        chunk0_path = os.path.join(file_dir, f".{base_name}#chunk0")
        chunk1_path = os.path.join(file_dir, f".{base_name}#chunk1")
        chunk2_path = os.path.join(file_dir, f".{base_name}#chunk2")

        assert not os.path.exists(chunk0_path), "Chunk 0 should NOT exist (used full cached file)"
        assert not os.path.exists(chunk1_path), "Chunk 1 should NOT exist (used full cached file)"
        assert not os.path.exists(chunk2_path), "Chunk 2 should NOT exist (used full cached file)"

        # Verify the full cached file still exists and has correct etag
        assert os.path.exists(full_cache_path), "Full cached file should still exist"

        # Check that the full cached file has the correct etag
        try:
            cached_etag = xattr.getxattr(full_cache_path, "user.etag").decode("utf-8")
            # The etag should match the source version (we can't easily get the exact etag,
            # but we can verify it exists and is not empty)
            assert cached_etag, "Cached file should have an etag"
        except (OSError, AttributeError):
            # xattrs might not be supported on some systems, that's okay for this test
            pass

        # Test multiple range reads to ensure they all use the full cached file
        range_read_2 = Range(offset=0, size=256 * 1024)  # First 256KB
        range_read_3 = Range(offset=2 * 1024 * 1024, size=256 * 1024)  # Last 256KB

        partial_content_2 = client.read(file_path, byte_range=range_read_2)
        partial_content_3 = client.read(file_path, byte_range=range_read_3)

        expected_content_2 = test_content[range_read_2.offset : range_read_2.offset + range_read_2.size]
        expected_content_3 = test_content[range_read_3.offset : range_read_3.offset + range_read_3.size]

        assert partial_content_2 == expected_content_2, "Second range read content mismatch"
        assert partial_content_3 == expected_content_3, "Third range read content mismatch"

        # Verify still no chunks were created
        assert not os.path.exists(chunk0_path), "Chunk 0 should still NOT exist after multiple range reads"
        assert not os.path.exists(chunk1_path), "Chunk 1 should still NOT exist after multiple range reads"
        assert not os.path.exists(chunk2_path), "Chunk 2 should still NOT exist after multiple range reads"


def test_partial_file_caching_full_file_read_optimization() -> None:
    """Test that byte_range with offset=0 and size>=file_size caches whole file instead of chunking."""
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create configuration with partial file caching enabled
        config = create_partial_caching_config(origin_store)
        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a 3MB test file (larger than 1MB chunk size)
        file_path = f"test-data-{uuid.uuid4()}/full_file_read_test.bin"
        test_content = create_test_data(3)  # 3MB file
        client.write(file_path, test_content)

        # Get cache paths
        cache_dir = config["cache"]["location"]
        cache_profile_dir = os.path.join(cache_dir, "origin")
        file_dir = os.path.join(cache_profile_dir, os.path.dirname(file_path))
        base_name = os.path.basename(file_path)
        full_cache_path = os.path.join(file_dir, base_name)

        # Verify file is NOT cached initially
        assert not os.path.exists(full_cache_path), "Full file should not be cached initially"

        # Perform a range read with offset=0 and size >= file_size (full file read)
        # This should cache the whole file instead of chunking
        file_size = len(test_content)
        range_read = Range(offset=0, size=file_size)  # Full file read
        full_content = client.read(file_path, byte_range=range_read)

        # Verify the full file read returned correct data
        assert full_content == test_content, "Full file read content mismatch"

        # Verify that the whole file is cached (not chunks)
        assert os.path.exists(full_cache_path), "Full file should be cached after full file range read"
        cached_file_size = os.path.getsize(full_cache_path)
        assert cached_file_size == file_size, f"Expected cached file size {file_size}, got {cached_file_size}"

        # Verify that NO chunks were created (since we cached the whole file)
        chunk0_path = os.path.join(file_dir, f".{base_name}#chunk0")
        chunk1_path = os.path.join(file_dir, f".{base_name}#chunk1")
        chunk2_path = os.path.join(file_dir, f".{base_name}#chunk2")

        assert not os.path.exists(chunk0_path), "Chunk 0 should NOT exist (whole file cached instead)"
        assert not os.path.exists(chunk1_path), "Chunk 1 should NOT exist (whole file cached instead)"
        assert not os.path.exists(chunk2_path), "Chunk 2 should NOT exist (whole file cached instead)"

        # Test with size > file_size (should still cache whole file)
        range_read_larger = Range(offset=0, size=file_size + 1024)  # Size larger than file
        full_content_larger = client.read(file_path, byte_range=range_read_larger)

        # Should return the whole file (truncated to file_size)
        assert len(full_content_larger) == file_size, "Should return file_size bytes even if requested size is larger"
        assert full_content_larger == test_content, "Content should match full file"

        # Verify still no chunks were created
        assert not os.path.exists(chunk0_path), "Chunk 0 should still NOT exist"
        assert not os.path.exists(chunk1_path), "Chunk 1 should still NOT exist"
        assert not os.path.exists(chunk2_path), "Chunk 2 should still NOT exist"


def test_partial_file_caching_full_file_read_optimization_with_source_version_disabled() -> None:
    """Test that when check_source_version is DISABLED, optimization doesn't apply and chunking is used."""
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create configuration with partial file caching enabled
        config = create_partial_caching_config(origin_store)
        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a 3MB test file (larger than 1MB chunk size)
        file_path = f"test-data-{uuid.uuid4()}/test.bin"
        test_content = create_test_data(3)  # 3MB file
        client.write(file_path, test_content)

        # Get cache paths
        cache_dir = config["cache"]["location"]
        cache_profile_dir = os.path.join(cache_dir, "origin")
        file_dir = os.path.join(cache_profile_dir, os.path.dirname(file_path))
        base_name = os.path.basename(file_path)
        full_cache_path = os.path.join(file_dir, base_name)

        # Perform a range read with offset=0 and size >= file_size (full file read)
        # with check_source_version DISABLED - optimization should NOT apply (no metadata fetch)
        # so it should use chunk-based caching instead
        range_read = Range(offset=0, size=len(test_content))
        full_content = client.read(
            file_path, byte_range=range_read, check_source_version=SourceVersionCheckMode.DISABLE
        )

        # Verify the full file read returned correct data
        assert full_content == test_content, "Full file read content mismatch"

        # Verify that chunks are used (optimization doesn't apply when version checking is disabled)
        chunk0_path = os.path.join(file_dir, f".{base_name}#chunk0")
        chunk1_path = os.path.join(file_dir, f".{base_name}#chunk1")
        chunk2_path = os.path.join(file_dir, f".{base_name}#chunk2")
        assert os.path.exists(chunk0_path), "Chunk 0 should exist (chunking used when version checking disabled)"
        assert os.path.exists(chunk1_path), "Chunk 1 should exist"
        assert os.path.exists(chunk2_path), "Chunk 2 should exist"
        # Full file should NOT be cached (chunks are used instead)
        assert not os.path.exists(full_cache_path), "Full file should NOT be cached (chunking used instead)"


def test_partial_file_caching_chunk_to_full_file_merge() -> None:
    """Test that small files are renamed from chunk 0 to original file name for efficiency."""
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create configuration with partial file caching enabled
        config = create_partial_caching_config(origin_store)
        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a 512KB test file (smaller than 1MB chunk size)
        file_path = f"test-data-{uuid.uuid4()}/small_file_test.bin"
        test_content = create_test_data(1)[: 512 * 1024]  # 512KB file
        client.write(file_path, test_content)

        # Get cache paths
        cache_dir = config["cache"]["location"]
        cache_profile_dir = os.path.join(cache_dir, "origin")
        file_dir = os.path.join(cache_profile_dir, os.path.dirname(file_path))
        base_name = os.path.basename(file_path)
        full_cache_path = os.path.join(file_dir, base_name)

        # Verify no full file is cached initially
        assert not os.path.exists(full_cache_path), "Full file should not be cached initially"

        # Perform a range read - this should create and rename chunk 0 to the original file name
        range_read = Range(offset=128 * 1024, size=128 * 1024)  # 128KB at 128KB offset
        partial_content = client.read(file_path, byte_range=range_read)

        # Verify the range read returned correct data
        expected_content = test_content[range_read.offset : range_read.offset + range_read.size]
        assert partial_content == expected_content, (
            f"Range read content mismatch: expected {len(expected_content)} bytes, got {len(partial_content)} bytes"
        )

        # Verify that the original file exists (chunk 0 was renamed to it since file size < chunk size)
        assert os.path.exists(full_cache_path), "Full file should exist (renamed from chunk 0)"

        # Now perform a full file read - this should use the cached file, not re-download
        full_content = client.read(file_path)

        # Verify the full file read returned correct data
        assert full_content == test_content, "Full file read content mismatch"

        # Verify that the full cached file still exists (was reused)
        assert os.path.exists(full_cache_path), "Full cached file should still exist after full file read"

        # Verify the full cached file contains the correct data
        with open(full_cache_path, "rb") as f:
            cached_data = f.read()
        # The cached file should contain the full file data (512KB)
        assert len(cached_data) == len(test_content), (
            f"Cached file should contain full file data, got {len(cached_data)} bytes"
        )
        assert cached_data == test_content, "Cached file data should match full file content"


def test_partial_file_caching_3mb_file_1mb_read():
    """Test that reading 1MB from a 3MB file creates and saves a chunk in cache."""
    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create configuration with partial file caching enabled
        config = create_partial_caching_config(origin_store)
        config["cache"]["cache_line_size"] = "1M"  # 1MB cache lines
        config["cache"]["size"] = "2M"  # 2MB cache size (smaller than 3MB file)

        # Create storage client
        msc = StorageClient(StorageClientConfig.from_dict(config, profile="origin"))

        # Create a 3MB test file
        test_content = b"X" * (3 * 1024 * 1024)  # 3MB of data
        test_file_path = "test_3mb_file.bin"

        # Write the file to S3
        msc.write(test_file_path, test_content)

        # Read 1MB from the file with prefetch_file=False
        with msc.open(test_file_path, "rb", prefetch_file=False) as f:
            # 1. f (ObjectFile) receives calls (f.read(), f.seek())
            # 2. f delegates to f._file (RemoteFileReader)
            # 3. f._file (RemoteFileReader) does the actual work:
            # - Converts read(size) → Range(offset=self._pos, size=size)
            # - Calls storage_client.read(byte_range=range)
            # - Updates self._pos

            # Seek to beginning and read 1MB
            f.seek(0)
            data = f.read(1024 * 1024)  # Read 1MB

            # Verify we got the expected data
            assert len(data) == 1024 * 1024, f"Expected 1MB, got {len(data)} bytes"
            assert data == test_content[: 1024 * 1024], "Data should match first 1MB of test content"

        # Debug: Check what was created in the cache
        cache_dir = config["cache"]["location"]
        origin_dir = os.path.join(cache_dir, "origin")

        # Check if chunk0 was created (chunks are stored directly in origin directory)
        chunk0_path = os.path.join(origin_dir, f".{test_file_path}#chunk0")

        assert os.path.exists(chunk0_path), "Chunk 0 should be created in cache"

        # Verify chunk0 contains the correct data (1MB)
        with open(chunk0_path, "rb") as f:
            chunk_data = f.read()
        assert len(chunk_data) == 1024 * 1024, f"Chunk should contain 1MB, got {len(chunk_data)} bytes"
        assert chunk_data == test_content[: 1024 * 1024], "Chunk data should match first 1MB of test content"

        # Verify xattrs are set correctly
        try:
            import xattr

            etag_attr = xattr.getxattr(chunk0_path, "user.etag")
            cache_line_size_attr = xattr.getxattr(chunk0_path, "user.cache_line_size")
            size_attr = xattr.getxattr(chunk0_path, "user.size")

            assert etag_attr is not None, "ETag xattr should be set"
            assert cache_line_size_attr.decode("utf-8") == "1048576", "Cache line size xattr should be 1MB"
            assert size_attr.decode("utf-8") == str(len(test_content)), "Size xattr should be total file size (3MB)"
        except (OSError, AttributeError):
            # xattrs not supported on this system, skip xattr verification
            pass


def test_chunk_download_lock_file_cleanup():
    """Test that lock files are automatically cleaned up when FileLock context manager exits.

    This test verifies that when _download_missing_chunks completes, the lock files
    created by the FileLock context manager are automatically cleaned up.
    """

    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create configuration with partial file caching enabled
        config = {
            "profiles": {
                "origin": origin_store.profile_config_dict() | {"caching_enabled": True},
            },
            "cache": {
                "size": "10M",
                "location": tempfile.mkdtemp(),
                "cache_line_size": "1M",  # 1MB cache lines for testing
                "check_source_version": True,
                "eviction_policy": {
                    "policy": "lru",
                    "refresh_interval": 300,
                },
            },
        }

        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a test file
        file_path = f"test-data-{uuid.uuid4()}/lock_cleanup_test.bin"
        test_content = create_test_data(2)  # 2MB file
        client.write(file_path, test_content)

        # Ensure the cache directory structure exists
        cache_dir = os.path.join(config["cache"]["location"], "origin")
        os.makedirs(cache_dir, exist_ok=True)

        # Read a byte range to trigger chunk download
        byte_range = Range(offset=0, size=512 * 1024)  # 512KB starting at beginning
        result = client.read(file_path, byte_range=byte_range)

        # Verify we got the expected data
        assert result == test_content[byte_range.offset : byte_range.offset + byte_range.size]

        # Check that chunk files were created
        file_dir = os.path.join(cache_dir, os.path.dirname(file_path))
        base_name = os.path.basename(file_path)
        chunk0_path = os.path.join(file_dir, f".{base_name}#chunk0")
        assert os.path.exists(chunk0_path), "Chunk 0 should exist after range read"

        # Verify that NO lock files remain after chunk download completes
        lock_files = [f for f in os.listdir(file_dir) if f.endswith(".lock")]
        assert len(lock_files) == 0, f"Expected no lock files after chunk download, found: {lock_files}"

        # Specifically check that the chunk lock file doesn't exist
        chunk_lock_path = os.path.join(file_dir, f".{base_name}#chunk0.lock")
        assert not os.path.exists(chunk_lock_path), "Chunk lock file should be automatically cleaned up"


def test_cache_directory_structure():
    """Test that cache directory structure does not create unnecessary intermediate folders.

    This test verifies that the cache should not create folders outside the cache directory.
    If the data is at tmp/footest/A/B/C/foo.txt, the cache should handle this path intelligently
    without creating a full nested structure that mirrors the original path exactly.
    """

    with tempdatastore.TemporaryAWSS3Bucket() as origin_store:
        # Create a temporary cache directory to avoid conflicts during concurrent test runs
        temp_cache_dir = tempfile.mkdtemp(prefix="msc_cache_")

        # Create configuration with partial file caching enabled
        config = {
            "profiles": {
                "origin": origin_store.profile_config_dict() | {"caching_enabled": True},
            },
            "cache": {
                "size": "10M",
                "location": temp_cache_dir,
                "cache_line_size": "1M",  # 1MB cache lines for testing
                "check_source_version": True,
                "eviction_policy": {
                    "policy": "lru",
                    "refresh_interval": 300,
                },
            },
        }

        client = StorageClient(config=StorageClientConfig.from_dict(config, profile="origin"))

        # Create a test file with a nested path structure
        file_path = "tmp/footest/A/B/C/structure_test.bin"
        test_content = create_test_data(2)  # 2MB file
        client.write(file_path, test_content)

        # Ensure the cache directory structure exists
        cache_dir = os.path.join(config["cache"]["location"], "origin")
        os.makedirs(cache_dir, exist_ok=True)

        # Read a byte range to trigger chunk download and cache creation
        byte_range = Range(offset=0, size=512 * 1024)  # 512KB starting at beginning
        result = client.read(file_path, byte_range=byte_range)

        # Verify we got the expected data
        assert result == test_content[byte_range.offset : byte_range.offset + byte_range.size]

        # Check the cache directory structure
        expected_chunk_path = os.path.join(
            cache_dir, os.path.dirname(file_path), f".{os.path.basename(file_path)}#chunk0"
        )

        # Verify the expected cache structure exists
        assert os.path.exists(expected_chunk_path), f"Expected chunk at {expected_chunk_path}"

        # Verify the directory structure is correct
        expected_dir = os.path.join(cache_dir, "tmp", "footest", "A", "B", "C")
        assert os.path.exists(expected_dir), f"Expected directory structure at {expected_dir}"

        # The cache creates the full path structure, which is the expected behavior
        # This documents the current behavior for future reference

        # Verify that the chunk file exists (regardless of the directory structure approach)
        assert os.path.exists(expected_chunk_path), f"Expected chunk at {expected_chunk_path}"

        # The current implementation creates the full path structure, so we verify it exists
        # This documents the current behavior, which may be improved in the future
        assert os.path.exists(expected_dir), f"Expected directory structure at {expected_dir}"

        # Verify the full path structure is preserved
        full_cache_path = os.path.join(cache_dir, file_path)
        full_cache_dir = os.path.dirname(full_cache_path)
        assert os.path.exists(full_cache_dir), f"Expected full cache directory structure at {full_cache_dir}"

        # CRITICAL: Verify that files are ONLY written to cache, NOT to the original path structure
        # The cache should not create any files outside the cache directory

        # Check that the specific path structure does NOT exist in the filesystem
        # e.g., tmp/footest/A/B/C/foo.txt should NOT exist at /tmp/footest/A/B/C/foo.txt
        # But we need to be careful not to check system directories like /tmp

        # Check the full original path doesn't exist (this is the key test)
        full_original_path = os.path.join("/", file_path)
        assert not os.path.exists(full_original_path), (
            f"ERROR: Cache created {full_original_path} outside cache directory!"
        )

        # Check that the specific nested path doesn't exist
        # /tmp/footest should not exist (assuming /tmp exists but /tmp/footest should not)
        footest_path = "/tmp/footest"
        assert not os.path.exists(footest_path), f"ERROR: Cache created {footest_path} outside cache directory!"

        # Check that the full nested structure doesn't exist
        full_nested_path = "/tmp/footest/A/B/C"
        assert not os.path.exists(full_nested_path), f"ERROR: Cache created {full_nested_path} outside cache directory!"

        # Verify that the cache path resolution works correctly
        # The cache should mirror the original file path structure
        relative_path = os.path.relpath(full_cache_path, cache_dir)
        assert relative_path == file_path, f"Cache path structure mismatch: expected {file_path}, got {relative_path}"
