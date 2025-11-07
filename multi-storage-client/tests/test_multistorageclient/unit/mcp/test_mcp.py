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


"""Most basic MCP server tests - just verify it can be imported and initialized."""

import asyncio
import sys

import pytest

MCP_AVAILABLE = sys.version_info >= (3, 10)

pytestmark = pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP requires Python >= 3.10 and fastmcp")


class TestMCP:
    """Basic MCP server import and initialization tests."""

    def test_can_import_mcp_server(self):
        """Test that we can import the MCP server module without errors."""
        try:
            from multistorageclient.mcp.server import mcp  # pyright: ignore[reportAttributeAccessIssue]

            assert mcp is not None
        except ImportError as e:
            pytest.fail(f"Failed to import MCP server: {e}")

    def test_mcp_tools_are_registered(self):
        """Test that MCP tools are registered with the server."""
        from multistorageclient.mcp.server import mcp  # pyright: ignore[reportAttributeAccessIssue]

        async def check_tools():
            tools = await mcp.get_tools()
            tool_names = list(tools.keys())

            expected_tools = [
                "msc_list",
                "msc_info",
                "msc_upload_file",
                "msc_download_file",
                "msc_delete",
                "msc_copy",
                "msc_is_file",
                "msc_is_empty",
                "msc_sync",
                "msc_sync_replicas",
            ]

            for expected_tool in expected_tools:
                assert expected_tool in tool_names, f"Expected tool {expected_tool} not found in {tool_names}"

            return tool_names

        tool_names = asyncio.run(check_tools())
        assert len(tool_names) == 10, f"Expected 10 tools, found {len(tool_names)}: {tool_names}"

    def test_mcp_prompts_are_registered(self):
        """Test that MCP prompts are registered with the server."""
        from multistorageclient.mcp.server import mcp  # pyright: ignore[reportAttributeAccessIssue]

        async def check_prompts():
            prompts = await mcp.get_prompts()
            prompt_names = list(prompts.keys())

            assert "msc_help" in prompt_names
            return prompt_names

        prompt_names = asyncio.run(check_prompts())
        assert len(prompt_names) == 1
