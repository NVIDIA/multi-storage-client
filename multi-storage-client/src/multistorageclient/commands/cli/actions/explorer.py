# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import argparse
import sys

from multistorageclient.constants import DEFAULT_EXPLORER_HOST, DEFAULT_EXPLORER_PORT

from .action import Action


class ExplorerAction(Action):
    """Action for starting the MSC Explorer application."""

    def name(self) -> str:
        """Return the name of this CLI action."""
        return "explorer"

    def help(self) -> str:
        """Return the help text for this CLI action."""
        return "Start the MSC Explorer application for browsing files"

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        """Set up the argument parser for the explorer command.

        Args:
            parser: The argument parser to configure
        """

    def run(self, args: argparse.Namespace) -> int:
        """Execute the explorer action.

        Args:
            args: Parsed command line arguments

        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        return self._start_server(args)

    def _start_server(self, args: argparse.Namespace) -> int:
        """Start the MSC Explorer web server with the specified configuration."""
        try:
            import uvicorn
        except ImportError:
            print(
                "Error: uvicorn is not installed. "
                "Install the explorer dependencies with: pip install multi-storage-client[explorer]",
                file=sys.stderr,
            )
            return 1

        try:
            from multistorageclient.explorer.api.server import _static_assets_available, app
        except ImportError as e:
            print(
                f"Error: Failed to import MSC Explorer components: {e}\n"
                "Install the explorer dependencies with: pip install multi-storage-client[explorer]",
                file=sys.stderr,
            )
            return 1

        print(f"\nStarting MSC Explorer on http://{DEFAULT_EXPLORER_HOST}:{DEFAULT_EXPLORER_PORT}")
        print()
        if _static_assets_available:
            print("✓ Frontend assets found - UI will be served at /")
        else:
            print("⚠ Frontend not built - run 'just multi-storage-explorer'")
            print("  API is still accessible at /api/* and /docs")
        print()

        try:
            uvicorn.run(
                app,
                host=DEFAULT_EXPLORER_HOST,
                port=DEFAULT_EXPLORER_PORT,
            )
            return 0
        except KeyboardInterrupt:
            print("\nMSC Explorer stopped by user")
            return 0
        except Exception as e:
            print(f"Error: Failed to start MSC Explorer: {e}", file=sys.stderr)
            return 1
