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

"""CLI action for starting the MCP server."""

import argparse
import logging
import sys

from .action import Action


class MCPServerAction(Action):
    """Action for starting the Multi-Storage Client MCP server."""

    def name(self) -> str:
        """Return the name of this CLI action."""
        return "mcp-server"

    def help(self) -> str:
        """Return the help text for this CLI action."""
        return "Start the Multi-Storage Client MCP (Model Context Protocol) server"

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        """Set up the argument parser for the MCP server command.

        Args:
            parser: The argument parser to configure
        """
        subparsers = parser.add_subparsers(dest="mcp_command", help="MCP server commands")

        # Start command
        start_parser = subparsers.add_parser("start", help="Start the MCP server")
        start_parser.add_argument(
            "--config", type=str, help="Path to MSC configuration file (defaults to standard MSC config discovery)"
        )

    def run(self, args: argparse.Namespace) -> int:
        """Execute the MCP server action.

        Args:
            args: Parsed command line arguments

        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        if not hasattr(args, "mcp_command") or args.mcp_command is None:
            print("Error: No MCP command specified. Use 'msc mcp-server start' to start the server.")
            return 1

        if args.mcp_command == "start":
            return self._start_server(args)
        else:
            print(f"Error: Unknown MCP command: {args.mcp_command}")
            return 1

    def _start_server(self, args: argparse.Namespace) -> int:
        """Start the MCP server with the specified configuration."""
        try:
            from multistorageclient.mcp.server import mcp_wrapper  # pyright: ignore[reportAttributeAccessIssue]

            if hasattr(args, "config") and args.config:
                mcp_wrapper.logger.info(f"Using MSC configuration file: {args.config}")
                mcp_wrapper.update_config_path(args.config)

            logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

            mcp_wrapper.logger.info("Starting Multi-Storage Client MCP server")
            mcp_wrapper.logger.info("Server running on stdio transport")

            # Run the server
            mcp_wrapper.run()

            return 0

        except KeyboardInterrupt:
            if "mcp_wrapper" in locals():
                mcp_wrapper.logger.info("MCP server stopped by user")
            else:
                print("MCP server stopped by user")
            return 0
        except ImportError as e:
            print(f"Error: Failed to import MCP server components: {e}", file=sys.stderr)
            return 1
        except (AttributeError, RuntimeError, OSError) as e:
            if "mcp_wrapper" in locals():
                mcp_wrapper.logger.error(f"Error starting MCP server: {e}")
            print(f"Error: Failed to start MCP server: {e}", file=sys.stderr)
            return 1
        except Exception as e:
            if "mcp_wrapper" in locals():
                mcp_wrapper.logger.error(f"Unexpected error starting MCP server: {e}")
            print(f"Error: Unexpected failure starting MCP server: {e}", file=sys.stderr)
            return 1
