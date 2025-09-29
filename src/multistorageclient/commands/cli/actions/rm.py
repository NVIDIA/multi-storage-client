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


class RmAction(Action):
    """Action for deletion of files or directories."""

    def name(self) -> str:
        return "rm"

    def help(self) -> str:
        return "Delete files or directories"

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Enable debug output with deletion details",
        )
        parser.add_argument(
            "--dryrun",
            action="store_true",
            help="Show what would be deleted without actually deleting",
        )
        parser.add_argument(
            "--quiet",
            action="store_true",
            help="Suppress output of operations performed",
        )
        parser.add_argument(
            "-r",
            "--recursive",
            action="store_true",
            help="Delete directories and their contents recursively (This option is needed to delete directories)",
        )
        parser.add_argument(
            "--only-show-errors",
            action="store_true",
            help="Only errors and warnings are displayed. All other output is suppressed",
        )
        parser.add_argument(
            "-y",
            "--yes",
            action="store_true",
            help="Skip confirmation prompt and proceed with deletion",
        )

        parser.add_argument("path", help="The file or directory path to delete (either POSIX path or MSC URL)")

        # Add examples as description
        parser.description = """Delete files or directories."""

        # Add examples as epilog (appears after argument help)
        parser.epilog = """examples:
  # Delete a specific file
  msc rm "msc://profile/foo/file.txt"
  msc rm "/path/to/files/specific_file.bin"

  # Delete a directory and its contents recursively
  msc rm "msc://profile/foo/old_dir/" -r
  msc rm "/path/to/directory/" --recursive

  # Skip confirmation prompt
  msc rm "msc://profile/foo/file.txt" -y

  # Dry run to see what would be deleted
  msc rm "msc://profile/temp/" --recursive --dryrun

  # Debug output
  msc rm "msc://profile/old/" --recursive --debug

  # Quiet mode
  msc rm "msc://profile/old/" --recursive --quiet

  # Only show errors
  msc rm "msc://profile/old/" --recursive --only-show-errors
"""

    def run(self, args: argparse.Namespace) -> int:
        if args.debug and not args.quiet and not args.only_show_errors:
            print("Arguments:", vars(args))

        try:
            if args.dryrun:
                # For dryrun, we need to list first to show what would be deleted
                if args.recursive:
                    results = msc.list(
                        url=args.path,
                        include_directories=False,  # list all the files that match the path
                    )
                else:
                    results = msc.list(
                        url=args.path,
                        include_directories=True,  # list only first level of files and directories
                    )

                if not args.quiet and not args.only_show_errors:
                    print("\nFiles that would be deleted:")
                    count = 0
                    for result in results:
                        count += 1
                        print(f"  {result.key}")
                    print(f"\nTotal: {count} file(s)")
                return 0

            # Check if user confirmation is required
            if not args.yes:
                # Show confirmation prompt
                if args.recursive:
                    print(f"This will delete everything under the path: {args.path} (recursively)")
                else:
                    print(f"This will delete the file: {args.path}")
                response = input("Are you sure you want to continue? (y/N): ").strip().lower()

                if response not in ["y", "yes"]:
                    print("Deletion cancelled.")
                    return 0

            if not args.quiet and not args.only_show_errors:
                print(f"Deleting: {args.path}")
            # Perform actual deletion with recursive flag from args
            msc.delete(args.path, recursive=args.recursive)

            if not args.quiet and not args.only_show_errors:
                print(f"Successfully deleted: {args.path}")
            return 0

        except ValueError as e:
            print(f"Error in command arguments: {str(e)}", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"Error during deletion: {str(e)}", file=sys.stderr)
            return 1
