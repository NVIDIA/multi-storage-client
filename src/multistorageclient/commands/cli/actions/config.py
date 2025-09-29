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
import json
import sys

import yaml

from multistorageclient.config import StorageClientConfig

from .action import Action


class ConfigAction(Action):
    """
    Action for configuration management commands.
    """

    def name(self) -> str:
        return "config"

    def help(self) -> str:
        return "Configuration management commands"

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        # Create subparsers for config subcommands
        subparsers = parser.add_subparsers(
            title="available commands",
            dest="subcommand",
            required=True,
        )

        # Add validate subcommand
        validate_parser = subparsers.add_parser(
            "validate", help="Validate and print configuration used by MSC", add_help=True
        )

        # Add options specific to validate subcommand
        validate_parser.add_argument(
            "--format",
            choices=["json", "yaml"],
            default="yaml",
            help="Output format (default: yaml)",
        )

        validate_parser.add_argument(
            "--config-file",
            help="Path to a specific config file (overrides default search paths)",
            default=None,
            metavar="CONFIG_FILE_PATH",
        )

        validate_parser.epilog = """examples:
  # Validate and print resolved MSC configuration based on default search path
  msc config validate

  # Validate and print resolved MSC configuration based on specific config file
  msc config validate --config-file /path/to/config.yaml
"""

    def run(self, args: argparse.Namespace) -> int:
        """Run the config action with parsed arguments."""
        if args.subcommand == "validate":
            return self._run_validate(args)
        else:
            print(f"Unknown subcommand: {args.subcommand}")
            return 1

    def _run_validate(self, args: argparse.Namespace) -> int:
        """Handle the 'config validate' subcommand."""
        try:
            config_file_paths = [args.config_file] if args.config_file else None

            # Get the merged and validated config
            config_obj = StorageClientConfig.from_file(config_file_paths=config_file_paths)
            config_dict = config_obj._config_dict

            # Output in requested format
            if args.format == "json":
                print(json.dumps(config_dict, indent=2))
            else:  # yaml
                print(yaml.dump(config_dict, default_flow_style=False, sort_keys=False))
            return 0

        except Exception as e:
            print(f"Error: {str(e)}", file=sys.stderr)
            return 1
