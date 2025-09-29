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

from .action import Action


class GlobAction(Action):
    """Action for finding files using glob patterns with optional attribute filtering."""

    def name(self) -> str:
        return "glob"

    def help(self) -> str:
        return "Find files using Unix-style wildcard patterns with optional attribute filtering"

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--attribute-filter-expression",
            "-e",
            help="Filter by attributes using a filter expression (e.g., 'model_name = \"gpt\" AND version > 1.0')",
        )
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Enable debug output with file details",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="Limit the number of results to display",
        )

        parser.add_argument("pattern", help="The glob pattern to search for (POSIX path or msc:// URL)")

        # Add examples as description
        parser.description = """Find files using Unix-style wildcard patterns. Supports:
  1. Basic wildcards: *, ?
  2. Recursive patterns: **
  3. Attribute filtering
  4. Limited results
"""

        # Add examples as epilog (appears after argument help)
        parser.epilog = """examples:
  # Basic glob patterns
  msc glob "msc://profile/data/*.txt"
  msc glob "msc://profile/models/**/*.bin"
  msc glob "/path/to/files/**/model_*.json"

  # Glob with attribute filtering
  msc glob "msc://profile/models/**/*.bin" --attribute-filter-expression 'model_name = \"gpt\"'
  msc glob "msc://profile/data/**/*" --attribute-filter-expression 'version >= 1.0 AND environment != \"test\"'

  # Limited results
  msc glob "msc://profile/data/**/*" --limit 10

  # Debug output
  msc glob "msc://profile/models/*.bin" --debug
"""

    def run(self, args: argparse.Namespace) -> int:
        if args.debug:
            print("Arguments:", vars(args))

        try:
            # Execute glob search
            results = msc.glob(args.pattern, attribute_filter_expression=args.attribute_filter_expression)

            if not results:
                if args.debug:
                    print("No files found matching the specified pattern and filters.")
                return 0

            # Apply limit if specified
            if args.limit:
                results = results[: args.limit]

            # Display results
            for result in results:
                print(result)

            if args.debug:
                print(f"\nFound {len(results)} file(s) matching the pattern and filters.")
                if args.limit and len(results) == args.limit:
                    print(f"(Output limited to {args.limit} results)")

            return 0

        except ValueError as e:
            print(f"Error in command arguments: {str(e)}", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"Error during glob search: {str(e)}", file=sys.stderr)
            return 1
