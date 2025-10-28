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

import sys

if sys.version_info >= (3, 10):
    """FastMCP server instance and main entry point for Multi-Storage Client."""

    import logging
    from pathlib import Path
    from typing import Optional

    import fastmcp

    class MCPServerWrapper:
        """Enhanced wrapper around FastMCP server with configuration management."""

        def __init__(
            self,
            config_path: Optional[Path] = None,
            server_name: str = "Multi-Storage Client MCP Server",
        ):
            """Initialize the MCP server wrapper.

            Args:
                config_path: Path to the MSC configuration file
                server_name: Human-readable name for the MCP server
            """
            self.config_path = config_path
            self.server_name = server_name

            self._server = fastmcp.FastMCP(self.server_name)
            self._logger = logging.getLogger(__name__)

        @property
        def server(self) -> fastmcp.FastMCP:
            """Access the underlying FastMCP server instance."""
            return self._server

        @property
        def logger(self) -> logging.Logger:
            """Access the logger instance for this MCP server."""
            return self._logger

        def update_config_path(self, config_path: Path) -> None:
            """Update the configuration path for this server instance.

            Args:
                config_path: New path to the MSC configuration file.
            """
            self.config_path = config_path
            self._logger.info(f"Updated MSC configuration file: {config_path}")

        def run(self, *args, **kwargs):
            """Run the MCP server."""
            return self._server.run(*args, **kwargs)

    mcp_wrapper = MCPServerWrapper()
    mcp = mcp_wrapper.server

    def register_handlers():
        """Register all MCP handlers after import to avoid circular dependencies."""
        from . import prompts, tools

        # Ensure modules are loaded for their side effects of tool registration
        assert prompts and tools

    logger = logging.getLogger(__name__)
    register_handlers()

    if __name__ == "__main__":
        mcp.run()
