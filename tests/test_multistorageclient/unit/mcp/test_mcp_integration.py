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

            assert isinstance(result[0], TextContent)
            textContent: TextContent = result[0]
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

            assert isinstance(result[0], TextContent)
            textContent: TextContent = result[0]
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

            # Validate last_modified timestamp
            try:
                parsed_time = datetime.fromisoformat(metadata["last_modified"].replace("Z", "+00:00"))
                # Ensure timestamp is reasonable (not too old or in future)
                current_time = datetime.now().timestamp()
                parsed_timestamp = parsed_time.timestamp()
                assert abs(current_time - parsed_timestamp) < 3600, "Timestamp should be within last hour"
            except ValueError:
                pytest.fail(f"Invalid timestamp format: {metadata['last_modified']}")

            error_fields = ["error"]
            for field in error_fields:
                assert field not in response, f"Response should not contain error field: {response.get(field)}"
