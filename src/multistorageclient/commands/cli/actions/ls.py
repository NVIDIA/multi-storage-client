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

from prettytable import PrettyTable

import multistorageclient as msc

from .action import Action


class LsAction(Action):
    """Action for listing files and directories with optional attribute filtering."""

    def name(self) -> str:
        return "ls"

    def help(self) -> str:
        return "List files and directories with optional attribute filtering"

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--attribute-filter-expression",
            "-e",
            help="Filter by attributes using a filter expression (e.g., 'model_name = \"gpt\" AND version > 1.0')",
        )
        parser.add_argument(
            "--recursive",
            action="store_true",
            help="List contents recursively (default: list only first level)",
        )
        parser.add_argument(
            "--human-readable",
            action="store_true",
            help="Displays file sizes in human readable format",
        )
        parser.add_argument(
            "--summarize",
            action="store_true",
            help="Displays summary information (number of objects, total size)",
        )
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Enable debug output",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="Limit the number of results to display",
        )
        parser.add_argument(
            "--show-attributes",
            action="store_true",
            help="Display metadata attributes dictionary as an additional column",
        )
        parser.add_argument("path", help="The path to list (POSIX path or msc:// URL)")

        # Add examples as description
        parser.description = """List files and directories at the specified path. Supports:
  1. Simple directory listings
  2. Attribute filtering
  3. Human readable sizes
  4. Summary information
  5. Metadata attributes display
"""

        # Add examples as epilog (appears after argument help)
        parser.epilog = """examples:
  # Basic directory listing
  msc ls "msc://profile/data/"
  msc ls "/path/to/files/"

  # Human readable sizes
  msc ls "msc://profile/models/" --human-readable

  # Show summary information
  msc ls "msc://profile/data/" --summarize

  # List with attribute filtering
  msc ls "msc://profile/models/" --attribute-filter-expression 'model_name = \"gpt\"'
  msc ls "msc://profile/data/" --attribute-filter-expression 'version >= 1.0 AND environment != \"test\"'

  # Limited results
  msc ls "msc://profile/data/" --limit 10

  # List contents recursively
  msc ls "msc://profile/data/" --recursive

  # Show metadata attributes
  msc ls "msc://profile/models/" --show-attributes
  msc ls "msc://profile/data/" --show-attributes --human-readable
"""

    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format."""
        float_size_bytes = float(size_bytes)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if float_size_bytes < 1024:
                return f"{float_size_bytes:.1f}{unit}"
            float_size_bytes = float_size_bytes / 1024
        return f"{float_size_bytes:.1f}PB"

    def _format_listing(self, metadata, human_readable: bool = False, show_attributes: bool = False) -> list:
        """Format file information in listing format."""
        if not metadata:
            return ["-", "-", "-"] if not show_attributes else ["-", "-", "-", "-"]

        size_str = (
            self._format_size(metadata.content_length or 0) if human_readable else str(metadata.content_length or 0)
        )
        date_str = metadata.last_modified.strftime("%Y-%m-%d %H:%M:%S") if metadata.last_modified else "-"

        if show_attributes:
            # Format attributes dictionary as JSON string
            try:
                attributes_str = json.dumps(metadata.metadata) if metadata.metadata else ""
            except TypeError:
                # the dict can have None values, which can't be serialized to JSON, so we just convert to string
                attributes_str = str(metadata.metadata) if metadata.metadata else ""
            return [date_str, size_str, metadata.key, attributes_str]
        else:
            return [date_str, size_str, metadata.key]

    def run(self, args: argparse.Namespace) -> int:
        try:
            if args.debug:
                print("Arguments:", vars(args))

            # Use msc.list with proper parameters
            results_iter = msc.list(
                url=args.path,
                start_after=None,  # Could be added as CLI argument in future
                end_at=None,  # Could be added as CLI argument in future
                include_directories=not args.recursive,
                attribute_filter_expression=args.attribute_filter_expression,
                show_attributes=args.show_attributes,
            )

            # Collect results
            table_data = []
            count = 0
            total_size = 0
            for obj_metadata in results_iter:
                row = self._format_listing(obj_metadata, args.human_readable, args.show_attributes)
                table_data.append(row)
                count += 1
                total_size += obj_metadata.content_length or 0
                if args.limit and count == args.limit:
                    break

            # Pretty print results
            if table_data:
                if args.show_attributes:
                    field_names = ["Last Modified", "Size", "Name", "Attributes"]
                    table = PrettyTable(field_names=field_names, max_width=100, valign="t")
                    for field_name, alignment in zip(field_names, ["l", "r", "l", "l"]):
                        table.align[field_name] = alignment
                    table.add_rows(table_data)
                    print(table)
                else:
                    field_names = ["Last Modified", "Size", "Name"]
                    table = PrettyTable(field_names=field_names, max_width=100, valign="t")
                    for field_name, alignment in zip(field_names, ["l", "r", "l"]):
                        table.align[field_name] = alignment
                    table.add_rows(table_data)
                    print(table)

            if args.limit and count == args.limit:
                print(f"\n(Output limited to {args.limit} results)")
            elif count == 0:
                print("No files found matching the specified criteria.")
                return 0

            # Show summary if requested
            if args.summarize:
                print("\nSummary:")
                print(f"Total Objects: {count}")
                if args.human_readable:
                    print(f"Total Size: {self._format_size(total_size)}")
                else:
                    print(f"Total Size: {total_size} bytes")

            return 0

        except ValueError as e:
            print(f"Error in command arguments: {str(e)}", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"Error during file listing: {str(e)}", file=sys.stderr)
            return 1
