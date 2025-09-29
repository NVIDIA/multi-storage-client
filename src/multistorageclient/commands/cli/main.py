# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import multistorageclient as msc

from .actions import (
    ActionRegistry,
    ConfigAction,
    GlobAction,
    HelpAction,
    LsAction,
    MSCArgumentParser,
    RmAction,
    SyncAction,
)


def create_parser() -> MSCArgumentParser:
    """
    Create the main argument parser for the MSC CLI.
    """
    parser = MSCArgumentParser(
        description="Multi-Storage Client command line interface", usage="msc [options] <command> [parameters]"
    )

    parser.add_argument("--version", action="store_true", help="Display the version of this tool")

    # Add a required command argument
    parser.add_argument("command", help="Command to run", nargs="?", default="help")

    # Add remaining arguments
    parser.add_argument("args", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)

    return parser


def main() -> int:
    """
    Main entry point for the MSC CLI.
    """
    # Create action registry and register all actions
    registry = ActionRegistry()

    # Register commands with instances
    registry.register_action(HelpAction(registry))
    registry.register_action(ConfigAction())
    registry.register_action(SyncAction())
    registry.register_action(GlobAction())
    registry.register_action(LsAction())
    registry.register_action(RmAction())

    # Parse command line arguments
    parser = create_parser()
    args, _ = parser.parse_known_args()

    # Display version if requested
    if args.version:
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        print()
        print(f"msc-cli/{msc.__version__} Python/{python_version}")
        print()
        return 0

    # Get the action for the command
    action = registry.get_action(args.command)
    if action is None:
        print()
        print(f"Unknown command: {args.command}")
        print("Run 'msc help' to see available commands.")
        print()
        return 1

    # Parse command-specific arguments
    cmd_parser = MSCArgumentParser(prog=f"msc {args.command}", description=action.help())
    action.setup_parser(cmd_parser)

    # Check if --help is requested for the parent command (not a subcommand)
    if args.args == ["--help"] or args.args == ["-h"]:
        cmd_parser.print_help()
        return 0

    try:
        cmd_args = cmd_parser.parse_args(args.args)
        return action.run(cmd_args)
    except Exception as e:
        print()
        print(f"msc: error: {str(e)}", file=sys.stderr)
        print()
        return 1


if __name__ == "__main__":
    sys.exit(main())
