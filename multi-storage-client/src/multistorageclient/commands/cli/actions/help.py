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

from .action import Action, MSCArgumentParser


class HelpAction(Action):
    """
    Action for displaying help messages.
    """

    def __init__(self, action_registry):
        self.action_registry = action_registry

    def name(self) -> str:
        return "help"

    def help(self) -> str:
        return "Display help for commands"

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("command", nargs="?", help="Command to get help for")

    def run(self, args: argparse.Namespace) -> int:
        if not args.command:
            # Print general help
            self.action_registry.print_main_help()
            return 0

        # Print help for specific command
        if args.command in self.action_registry.actions:
            action = self.action_registry.actions[args.command]
            parser = MSCArgumentParser(prog=f"msc {args.command}", description=action.help())
            action.setup_parser(parser)
            print()
            parser.print_help()
            print()
        else:
            print(f"Unknown command: {args.command}")
            return 1

        return 0
