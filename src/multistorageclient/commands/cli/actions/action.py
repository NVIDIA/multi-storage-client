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
from abc import ABC, abstractmethod
from typing import Optional


class MSCHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """
    Help message formatter for MSC CLI.
    """

    def _format_usage(self, usage, actions, groups, prefix):
        if prefix is None:
            prefix = "usage: "
        return super()._format_usage(usage, actions, groups, prefix)


class MSCArgumentParser(argparse.ArgumentParser):
    """
    Custom argument parser for MSC CLI.
    """

    def __init__(self, **kwargs):
        kwargs["formatter_class"] = MSCHelpFormatter
        kwargs.setdefault("add_help", False)
        super().__init__(**kwargs)

    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(2, f"msc: error: {message}\n")


class Action(ABC):
    """
    Base class for all MSC CLI actions.
    """

    @abstractmethod
    def name(self) -> str:
        """Return the name of the action."""
        pass

    @abstractmethod
    def help(self) -> str:
        """Return the help text for the action."""
        pass

    @abstractmethod
    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        """Set up the argument parser for this action."""
        pass

    @abstractmethod
    def run(self, args: argparse.Namespace) -> int:
        """Run the action with the parsed arguments."""
        pass


class ActionRegistry:
    """
    Registry for all actions in the MSC CLI.
    """

    def __init__(self):
        self.actions: dict[str, Action] = {}

    def register_action(self, action: Action) -> None:
        """Register an action."""
        self.actions[action.name()] = action

    def get_action(self, name: str) -> Optional[Action]:
        """Get an action by name."""
        return self.actions.get(name)

    def print_main_help(self) -> None:
        """Print the main help message."""
        print()  # Blank line at the start
        print("usage: msc <command> [options] [parameters]")
        print("To see help text, you can run:")
        print()
        print("  msc help")
        print("  msc help <command>")
        print()
        print("commands:")

        # Dynamically list all registered commands
        for name, action in sorted(self.actions.items()):
            # Format the command name and help text
            print(f"  {name:<8} {action.help()}")

        print()
