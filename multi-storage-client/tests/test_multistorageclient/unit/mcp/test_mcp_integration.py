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


"""Unit tests for MCP server using temporary fake data stores."""

import json
import sys
import tempfile
import uuid
from datetime import datetime
from typing import Any, Dict

import pytest

# Add this decorator to all test classes and fixtures
import multistorageclient as msc
import multistorageclient.telemetry as telemetry
from test_multistorageclient.unit.utils import config, tempdatastore
from test_multistorageclient.unit.utils.telemetry.metrics.export import InMemoryMetricExporter

MCP_AVAILABLE = sys.version_info >= (3, 10)

pytestmark = pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP requires Python >= 3.10 and fastmcp")


@pytest.fixture(
    params=[
        (tempdatastore.TemporaryPOSIXDirectory, False),
        (tempdatastore.TemporaryPOSIXDirectory, True),
    ]
)
def mcp_server_parametrized(request):
    """Fixture that creates MCP server with specified temp store."""

    if not MCP_AVAILABLE:
        pytest.skip("MCP requires Python >= 3.10 and fastmcp")

    temp_data_store_type, with_cache = request.param

    with temp_data_store_type() as temp_data_store:
        msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

        profile = "data"
        config_dict = {
            "profiles": {profile: temp_data_store.profile_config_dict()},
            "opentelemetry": {
                "metrics": {
                    "attributes": [
                        {"type": "static", "options": {"attributes": {"cluster": "local"}}},
                        {"type": "host", "options": {"attributes": {"node": "name"}}},
                        {"type": "process", "options": {"attributes": {"process": "pid"}}},
                    ],
                    "exporter": {"type": telemetry._fully_qualified_name(InMemoryMetricExporter)},
                },
            },
        }
        if with_cache:
            config_dict["cache"] = {
                "size": "10M",
                "use_etag": True,
                "location": tempfile.mkdtemp(),
                "eviction_policy": {
                    "policy": "random",
                },
            }

        config.setup_msc_config(config_dict)

        from multistorageclient.mcp.server import mcp  # pyright: ignore[reportAttributeAccessIssue]

        yield mcp, profile

        msc.shortcuts._STORAGE_CLIENT_CACHE.clear()


def create_test_file(profile_name: str, file_path: str, content: bytes) -> str:
    """Helper to create a test file and return its URL."""
    url = f"msc://{profile_name}/{file_path}"
    msc.write(url, content)
    return url


@pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP requires Python >= 3.10 and fastmcp")
class TestMCPServerBasicOperations:
    """Test basic MCP server operations against different storage backends."""

    @pytest.mark.asyncio
    async def test_msc_list(self, mcp_server_parametrized):
        """Test that msc_list tool returns files in a directory."""

        from fastmcp import Client  # pyright: ignore[reportMissingImports]
        from mcp.types import TextContent  # pyright: ignore[reportMissingImports]

        mcp_server, profile_name = mcp_server_parametrized

        # Create nested structure
        test_prefix = f"dir-test-{uuid.uuid4()}"
        create_test_file(profile_name, f"{test_prefix}/subdir/file.txt", b"nested file")
        create_test_file(profile_name, f"{test_prefix}/file.txt", b"root file")

        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "msc_list", {"url": f"msc://{profile_name}/{test_prefix}/", "include_directories": False}
            )

            assert isinstance(result.content[0], TextContent)
            textContent: TextContent = result.content[0]
            response: Dict[str, Any] = json.loads(textContent.text)

            assert isinstance(response, dict), "Response should be a JSON object"
            assert "success" in response, "Response should have success field"
            assert "count" in response, "Response should have count field"
            assert "objects" in response, "Response should have objects field"
            assert "url" in response, "Response should have url field"

            assert response["success"] is True, "Operation should succeed"
            assert response["url"] == f"msc://{profile_name}/{test_prefix}/", "URL should match request"
            assert isinstance(response["count"], int), "Count should be an integer"
            assert isinstance(response["objects"], list), "Objects should be a list"
            assert response["count"] == len(response["objects"]), "Count should match objects length"

            assert response["count"] == 2, f"Should find 2 files, but found {response['count']}"

            for i, obj in enumerate(response["objects"]):
                assert isinstance(obj, dict), f"Object {i} should be a dictionary"

                required_fields = ["key", "content_length", "last_modified", "type"]
                for field in required_fields:
                    assert field in obj, f"Object {i} should have {field} field"

            returned_keys = [obj["key"] for obj in response["objects"]]
            expected_files = ["file.txt", "subdir/file.txt"]

            for expected_file in expected_files:
                found = any(expected_file in key for key in returned_keys)
                assert found, f"Should find {expected_file} in returned keys: {returned_keys}"

    @pytest.mark.asyncio
    async def test_mcp_info_tool(self, mcp_server_parametrized):
        """Test that msc_info tool returns file metadata."""
        from fastmcp import Client  # pyright: ignore[reportMissingImports]
        from mcp.types import TextContent  # pyright: ignore[reportMissingImports]

        mcp_server, profile_name = mcp_server_parametrized

        test_prefix = f"info-test-{uuid.uuid4()}"
        test_content = b"This is test content for msc_info validation"
        test_file_path = f"{test_prefix}/test-file.txt"
        create_test_file(profile_name, test_file_path, test_content)

        async with Client(mcp_server) as client:
            result = await client.call_tool("msc_info", {"url": f"msc://{profile_name}/{test_file_path}"})

            assert isinstance(result.content[0], TextContent)
            textContent: TextContent = result.content[0]
            response: Dict[str, Any] = json.loads(textContent.text)

            # Basic response structure validation
            assert isinstance(response, dict), "Response should be a JSON object"
            assert "success" in response, "Response should have success field"
            assert "url" in response, "Response should have url field"
            assert "metadata" in response, "Response should have metadata field"

            # Validate response values
            assert response["success"] is True, "Operation should succeed"
            assert response["url"] == f"msc://{profile_name}/{test_file_path}", "URL should match request"
            assert isinstance(response["metadata"], dict), "Metadata should be a dictionary"

            # Extract metadata for detailed validation
            metadata = response["metadata"]

            # Validate metadata structure - required fields
            required_metadata_fields = ["key", "content_length", "last_modified", "type"]
            for field in required_metadata_fields:
                assert field in metadata, f"Metadata should have {field} field"

            # Validate metadata field types
            assert isinstance(metadata["key"], str), "Metadata key should be string"
            assert isinstance(metadata["content_length"], int), "Metadata content_length should be integer"
            assert isinstance(metadata["type"], str), "Metadata type should be string"
            assert isinstance(metadata["last_modified"], str), "Metadata last_modified should be string"

            # Validate metadata field values
            assert metadata["type"] == "file", "Object should be a file"
            assert metadata["content_length"] == len(test_content), (
                f"Content length should be {len(test_content)}, got {metadata['content_length']}"
            )
            assert metadata["content_length"] > 0, "File should have positive size"
            assert len(metadata["key"]) > 0, "Key should be non-empty"
            assert test_file_path in metadata["key"], f"Key should contain file path {test_file_path}"

            # Validate last_modified timestamp exists and has valid format
            assert metadata["last_modified"] is not None, "last_modified should not be None"
            assert isinstance(metadata["last_modified"], str), "last_modified should be a string"
            assert len(metadata["last_modified"]) > 0, "last_modified should not be empty"

            # Validate timestamp format can be parsed (ISO format expected)
            try:
                datetime.fromisoformat(metadata["last_modified"].replace("Z", "+00:00"))
            except ValueError:
                pytest.fail(f"Invalid timestamp format: {metadata['last_modified']}")

            error_fields = ["error"]
            for field in error_fields:
                assert field not in response, f"Response should not contain error field: {response.get(field)}"

    @pytest.mark.asyncio
    async def test_mcp_upload_download_file(self, mcp_server_parametrized):
        """Test that msc_upload_file and msc_download_file tools work correctly."""
        from fastmcp import Client  # pyright: ignore[reportMissingImports]
        from mcp.types import TextContent  # pyright: ignore[reportMissingImports]

        mcp_server, profile_name = mcp_server_parametrized

        test_prefix = f"upload-test-{uuid.uuid4()}"
        test_content = b"This is test content for upload/download validation"
        test_file_path = f"{test_prefix}/uploaded-file.txt"

        import tempfile

        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".txt") as tmp_upload:
            tmp_upload.write(test_content)
            upload_path = tmp_upload.name

        try:
            async with Client(mcp_server) as client:
                upload_result = await client.call_tool(
                    "msc_upload_file",
                    {"url": f"msc://{profile_name}/{test_file_path}", "local_path": upload_path},
                )

                assert isinstance(upload_result.content[0], TextContent)
                upload_response: Dict[str, Any] = json.loads(upload_result.content[0].text)

                assert upload_response["success"] is True
                assert upload_response["url"] == f"msc://{profile_name}/{test_file_path}"
                assert upload_response["local_path"] == upload_path
                assert "uploaded_metadata" in upload_response
                assert upload_response["uploaded_metadata"]["content_length"] == len(test_content)

                with tempfile.NamedTemporaryFile(mode="rb", delete=False, suffix=".txt") as tmp_download:
                    download_path = tmp_download.name

                download_result = await client.call_tool(
                    "msc_download_file",
                    {"url": f"msc://{profile_name}/{test_file_path}", "local_path": download_path},
                )

                assert isinstance(download_result.content[0], TextContent)
                download_response: Dict[str, Any] = json.loads(download_result.content[0].text)

                assert download_response["success"] is True
                assert download_response["url"] == f"msc://{profile_name}/{test_file_path}"
                assert download_response["local_path"] == download_path

                with open(download_path, "rb") as f:
                    downloaded_content = f.read()
                    assert downloaded_content == test_content

        finally:
            import os

            if os.path.exists(upload_path):
                os.unlink(upload_path)
            if "download_path" in locals() and os.path.exists(download_path):
                os.unlink(download_path)

    @pytest.mark.asyncio
    async def test_mcp_copy(self, mcp_server_parametrized):
        """Test that msc_copy tool copies files correctly."""
        from fastmcp import Client  # pyright: ignore[reportMissingImports]
        from mcp.types import TextContent  # pyright: ignore[reportMissingImports]

        mcp_server, profile_name = mcp_server_parametrized

        test_prefix = f"copy-test-{uuid.uuid4()}"
        test_content = b"This is test content for copy validation"
        source_file_path = f"{test_prefix}/source-file.txt"
        target_file_path = f"{test_prefix}/target-file.txt"

        create_test_file(profile_name, source_file_path, test_content)

        async with Client(mcp_server) as client:
            result = await client.call_tool(
                "msc_copy",
                {
                    "source_url": f"msc://{profile_name}/{source_file_path}",
                    "target_url": f"msc://{profile_name}/{target_file_path}",
                },
            )

            assert isinstance(result.content[0], TextContent)
            textContent: TextContent = result.content[0]
            response: Dict[str, Any] = json.loads(textContent.text)

            assert response["success"] is True
            assert response["source_url"] == f"msc://{profile_name}/{source_file_path}"
            assert response["target_url"] == f"msc://{profile_name}/{target_file_path}"
            assert "source_metadata" in response
            assert "target_metadata" in response
            assert response["source_metadata"]["content_length"] == len(test_content)
            assert response["target_metadata"]["content_length"] == len(test_content)

            info_result = await client.call_tool("msc_info", {"url": f"msc://{profile_name}/{target_file_path}"})
            assert isinstance(info_result.content[0], TextContent)
            textContent = info_result.content[0]
            info_response: Dict[str, Any] = json.loads(textContent.text)
            assert info_response["success"] is True
            assert info_response["metadata"]["content_length"] == len(test_content)

    @pytest.mark.asyncio
    async def test_mcp_delete_single_file(self, mcp_server_parametrized):
        """Test that msc_delete tool deletes a single file."""
        from fastmcp import Client  # pyright: ignore[reportMissingImports]
        from mcp.types import TextContent  # pyright: ignore[reportMissingImports]

        mcp_server, profile_name = mcp_server_parametrized

        test_prefix = f"delete-test-{uuid.uuid4()}"
        test_content = b"This file will be deleted"
        test_file_path = f"{test_prefix}/file-to-delete.txt"

        create_test_file(profile_name, test_file_path, test_content)

        async with Client(mcp_server) as client:
            info_before = await client.call_tool("msc_info", {"url": f"msc://{profile_name}/{test_file_path}"})
            assert isinstance(info_before.content[0], TextContent)
            textContent: TextContent = info_before.content[0]
            info_response: Dict[str, Any] = json.loads(textContent.text)
            assert info_response["success"] is True

            delete_result = await client.call_tool(
                "msc_delete", {"url": f"msc://{profile_name}/{test_file_path}", "recursive": False}
            )

            assert isinstance(delete_result.content[0], TextContent)
            textContent: TextContent = delete_result.content[0]
            delete_response: Dict[str, Any] = json.loads(textContent.text)

            assert delete_response["success"] is True
            assert delete_response["url"] == f"msc://{profile_name}/{test_file_path}"
            assert delete_response["recursive"] is False

            list_result = await client.call_tool("msc_list", {"url": f"msc://{profile_name}/{test_prefix}/"})
            assert isinstance(list_result.content[0], TextContent)
            textContent = list_result.content[0]
            list_response: Dict[str, Any] = json.loads(textContent.text)
            assert list_response["count"] == 0

    @pytest.mark.asyncio
    async def test_mcp_delete_recursive(self, mcp_server_parametrized):
        """Test that msc_delete tool recursively deletes directories with nested files."""
        from fastmcp import Client  # pyright: ignore[reportMissingImports]
        from mcp.types import TextContent  # pyright: ignore[reportMissingImports]

        mcp_server, profile_name = mcp_server_parametrized

        test_prefix = f"recursive-delete-test-{uuid.uuid4()}"

        create_test_file(profile_name, f"{test_prefix}/file1.txt", b"file 1")
        create_test_file(profile_name, f"{test_prefix}/file2.txt", b"file 2")
        create_test_file(profile_name, f"{test_prefix}/subdir/nested1.txt", b"nested 1")
        create_test_file(profile_name, f"{test_prefix}/subdir/nested2.txt", b"nested 2")
        create_test_file(profile_name, f"{test_prefix}/subdir/deep/nested3.txt", b"nested 3")

        async with Client(mcp_server) as client:
            list_before = await client.call_tool("msc_list", {"url": f"msc://{profile_name}/{test_prefix}/"})
            assert isinstance(list_before.content[0], TextContent)
            textContent: TextContent = list_before.content[0]
            list_before_response: Dict[str, Any] = json.loads(textContent.text)
            assert list_before_response["count"] == 5

            delete_result = await client.call_tool(
                "msc_delete", {"url": f"msc://{profile_name}/{test_prefix}/", "recursive": True}
            )

            assert isinstance(delete_result.content[0], TextContent)
            textContent = delete_result.content[0]
            delete_response: Dict[str, Any] = json.loads(textContent.text)

            assert delete_response["success"] is True
            assert delete_response["url"] == f"msc://{profile_name}/{test_prefix}/"
            assert delete_response["recursive"] is True

            list_after = await client.call_tool("msc_list", {"url": f"msc://{profile_name}/{test_prefix}/"})
            assert isinstance(list_after.content[0], TextContent)
            textContent = list_after.content[0]
            list_after_response: Dict[str, Any] = json.loads(textContent.text)
            assert list_after_response["count"] == 0

    @pytest.mark.asyncio
    async def test_mcp_is_file(self, mcp_server_parametrized):
        """Test that msc_is_file tool correctly identifies files vs directories."""
        from fastmcp import Client  # pyright: ignore[reportMissingImports]
        from mcp.types import TextContent  # pyright: ignore[reportMissingImports]

        mcp_server, profile_name = mcp_server_parametrized

        test_prefix = f"is-file-test-{uuid.uuid4()}"
        test_file_path = f"{test_prefix}/test-file.txt"

        create_test_file(profile_name, test_file_path, b"test content")
        create_test_file(profile_name, f"{test_prefix}/subdir/nested.txt", b"nested")

        async with Client(mcp_server) as client:
            file_result = await client.call_tool("msc_is_file", {"url": f"msc://{profile_name}/{test_file_path}"})

            assert isinstance(file_result.content[0], TextContent)
            textContent: TextContent = file_result.content[0]
            file_response: Dict[str, Any] = json.loads(textContent.text)

            assert file_response["success"] is True
            assert file_response["is_file"] is True
            assert file_response["url"] == f"msc://{profile_name}/{test_file_path}"

            dir_result = await client.call_tool("msc_is_file", {"url": f"msc://{profile_name}/{test_prefix}/"})
            assert isinstance(dir_result.content[0], TextContent)
            textContent = dir_result.content[0]
            dir_response: Dict[str, Any] = json.loads(textContent.text)

            assert dir_response["success"] is True
            assert dir_response["is_file"] is False

    @pytest.mark.asyncio
    async def test_mcp_is_empty(self, mcp_server_parametrized):
        """Test that msc_is_empty tool correctly identifies empty directories."""
        from fastmcp import Client  # pyright: ignore[reportMissingImports]
        from mcp.types import TextContent  # pyright: ignore[reportMissingImports]

        mcp_server, profile_name = mcp_server_parametrized

        test_prefix = f"is-empty-test-{uuid.uuid4()}"
        empty_prefix = f"{test_prefix}/empty-dir/"
        non_empty_prefix = f"{test_prefix}/non-empty-dir/"

        create_test_file(profile_name, f"{non_empty_prefix}/file.txt", b"content")

        async with Client(mcp_server) as client:
            empty_result = await client.call_tool("msc_is_empty", {"url": f"msc://{profile_name}/{empty_prefix}"})

            assert isinstance(empty_result.content[0], TextContent)
            textContent: TextContent = empty_result.content[0]
            empty_response: Dict[str, Any] = json.loads(textContent.text)

            assert empty_response["success"] is True
            assert empty_response["is_empty"] is True
            assert empty_response["url"] == f"msc://{profile_name}/{empty_prefix}"

            non_empty_result = await client.call_tool(
                "msc_is_empty", {"url": f"msc://{profile_name}/{non_empty_prefix}"}
            )
            assert isinstance(non_empty_result.content[0], TextContent)
            textContent = non_empty_result.content[0]
            non_empty_response: Dict[str, Any] = json.loads(textContent.text)

            assert non_empty_response["success"] is True
            assert non_empty_response["is_empty"] is False

    @pytest.mark.asyncio
    async def test_mcp_sync(self, mcp_server_parametrized):
        """Test that msc_sync tool synchronizes directories correctly."""
        from fastmcp import Client  # pyright: ignore[reportMissingImports]
        from mcp.types import TextContent  # pyright: ignore[reportMissingImports]

        mcp_server, profile_name = mcp_server_parametrized

        test_prefix = f"sync-test-{uuid.uuid4()}"
        source_prefix = f"{test_prefix}/source/"
        target_prefix = f"{test_prefix}/target/"

        create_test_file(profile_name, f"{source_prefix}file1.txt", b"file 1 content")
        create_test_file(profile_name, f"{source_prefix}file2.txt", b"file 2 content")
        create_test_file(profile_name, f"{source_prefix}subdir/nested.txt", b"nested content")

        async with Client(mcp_server) as client:
            list_before = await client.call_tool("msc_list", {"url": f"msc://{profile_name}/{target_prefix}"})
            assert isinstance(list_before.content[0], TextContent)
            textContent: TextContent = list_before.content[0]
            list_before_response: Dict[str, Any] = json.loads(textContent.text)
            assert list_before_response["count"] == 0

            sync_result = await client.call_tool(
                "msc_sync",
                {
                    "source_url": f"msc://{profile_name}/{source_prefix}",
                    "target_url": f"msc://{profile_name}/{target_prefix}",
                    "delete_unmatched_files": False,
                    "preserve_source_attributes": False,
                },
            )

            assert isinstance(sync_result.content[0], TextContent)
            textContent = sync_result.content[0]
            sync_response: Dict[str, Any] = json.loads(textContent.text)

            assert sync_response["success"] is True
            assert sync_response["source_url"] == f"msc://{profile_name}/{source_prefix}"
            assert sync_response["target_url"] == f"msc://{profile_name}/{target_prefix}"
            assert sync_response["delete_unmatched_files"] is False
            assert sync_response["preserve_source_attributes"] is False

            list_after = await client.call_tool("msc_list", {"url": f"msc://{profile_name}/{target_prefix}"})
            assert isinstance(list_after.content[0], TextContent)
            textContent = list_after.content[0]
            list_after_response: Dict[str, Any] = json.loads(textContent.text)
            assert list_after_response["count"] == 3

            returned_keys = [obj["key"] for obj in list_after_response["objects"]]
            expected_files = ["file1.txt", "file2.txt", "subdir/nested.txt"]
            for expected_file in expected_files:
                found = any(expected_file in key for key in returned_keys)
                assert found, f"Should find {expected_file} in synced target: {returned_keys}"

    @pytest.mark.asyncio
    async def test_mcp_sync_with_delete_unmatched(self, mcp_server_parametrized):
        """Test that msc_sync tool with delete_unmatched_files removes extra files in target."""
        from fastmcp import Client  # pyright: ignore[reportMissingImports]
        from mcp.types import TextContent  # pyright: ignore[reportMissingImports]

        mcp_server, profile_name = mcp_server_parametrized

        test_prefix = f"sync-delete-test-{uuid.uuid4()}"
        source_prefix = f"{test_prefix}/source/"
        target_prefix = f"{test_prefix}/target/"

        create_test_file(profile_name, f"{source_prefix}file1.txt", b"source file 1")
        create_test_file(profile_name, f"{source_prefix}file2.txt", b"source file 2")

        create_test_file(profile_name, f"{target_prefix}file1.txt", b"old target file 1")
        create_test_file(profile_name, f"{target_prefix}extra.txt", b"extra file to be deleted")

        async with Client(mcp_server) as client:
            list_before = await client.call_tool("msc_list", {"url": f"msc://{profile_name}/{target_prefix}"})
            assert isinstance(list_before.content[0], TextContent)
            textContent: TextContent = list_before.content[0]
            list_before_response: Dict[str, Any] = json.loads(textContent.text)
            assert list_before_response["count"] == 2

            sync_result = await client.call_tool(
                "msc_sync",
                {
                    "source_url": f"msc://{profile_name}/{source_prefix}",
                    "target_url": f"msc://{profile_name}/{target_prefix}",
                    "delete_unmatched_files": True,
                    "preserve_source_attributes": False,
                },
            )

            assert isinstance(sync_result.content[0], TextContent)
            textContent = sync_result.content[0]
            sync_response: Dict[str, Any] = json.loads(textContent.text)

            assert sync_response["success"] is True
            assert sync_response["delete_unmatched_files"] is True

            list_after = await client.call_tool("msc_list", {"url": f"msc://{profile_name}/{target_prefix}"})
            assert isinstance(list_after.content[0], TextContent)
            textContent = list_after.content[0]
            list_after_response: Dict[str, Any] = json.loads(textContent.text)
            assert list_after_response["count"] == 2

            returned_keys = [obj["key"] for obj in list_after_response["objects"]]
            assert any("file1.txt" in key for key in returned_keys), "file1.txt should exist"
            assert any("file2.txt" in key for key in returned_keys), "file2.txt should exist"
            assert not any("extra.txt" in key for key in returned_keys), "extra.txt should be deleted"
