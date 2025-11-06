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
from typing import Optional

import multistorageclient as msc
from multistorageclient.types import ExecutionMode, PatternList, PatternType

from .action import Action


class OrderedPatternAction(argparse.Action):
    """Custom action to capture the order of include/exclude patterns."""

    def __init__(self, option_strings, dest, pattern_type, **kwargs):
        self.pattern_type = pattern_type
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if not hasattr(namespace, "ordered_patterns"):
            namespace.ordered_patterns = []
        namespace.ordered_patterns.append((self.pattern_type, values))


class SyncAction(Action):
    """Action for synchronizing files to a storage location."""

    def name(self) -> str:
        return "sync"

    def help(self) -> str:
        return "Synchronize files from the source storage to the target storage"

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--delete-unmatched-files",
            action="store_true",
            help="Delete files at the target that are not present at the source",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose logging",
        )
        parser.add_argument(
            "--ray-cluster",
            help="Ray cluster address (e.g. 'ray://<ip>:<port>')",
        )
        parser.add_argument(
            "--target-url",
            help="The path or URL for the target storage (POSIX path or msc:// URL). If not provided, will sync to all the replicas if configured.",
        )
        parser.add_argument(
            "--replica-indices",
            help="The indices (comma separated list) of the replicas to sync to. If not provided, will sync to all the replicas. Index starts from 0.",
        )
        parser.add_argument(
            "--include",
            action=OrderedPatternAction,
            pattern_type=PatternType.INCLUDE,
            help="Include only files that match the specified pattern. Can be used multiple times. Supports AWS S3 compatible glob patterns (*, ?, [sequence], [!sequence]).",
        )
        parser.add_argument(
            "--exclude",
            action=OrderedPatternAction,
            pattern_type=PatternType.EXCLUDE,
            help="Exclude files that match the specified pattern. Can be used multiple times. Supports AWS S3 compatible glob patterns (*, ?, [sequence], [!sequence]).",
        )
        parser.add_argument(
            "--no-ignore-hidden",
            action="store_true",
            help="Do not ignore hidden files and directories (those starting with a dot). By default, hidden files are ignored.",
        )
        parser.add_argument("source_url", help="The path or URL for the source storage (POSIX path or msc:// URL)")

        # Add examples as description
        parser.description = """Synchronize files between storage locations. Can be used to:
  1. Upload files from filesystem to object stores
  2. Download files from object stores to filesystem
  3. Transfer files between different object stores
  4. Synchronize files to all the replicas
"""

        # Add examples as epilog (appears after argument help)
        parser.epilog = """examples:
  # Upload: filesystem to object store
  msc sync /path/to/dataset --target-url msc://profile/prefix

  # Download: object store to filesystem
  msc sync msc://profile/prefix --target-url /path/to/dataset

  # Transfer: between object stores
  msc sync msc://profile1/prefix --target-url msc://profile2/prefix

  # Sync with cleanup (removes files in target not in source)
  msc sync msc://source-profile/data --delete-unmatched-files --target-url msc://target-profile/data

  # Sync with Ray (requires Ray to be installed)
  msc sync msc://source-profile/data --ray-cluster 127.0.0.1:6379 --target-url msc://target-profile/data

  # Sync to all the replicas
  msc sync msc://source-profile/data

  # Sync to specific replicas, index starts from 0
  msc sync msc://source-profile/data --replica-indices "0,1"

  # Sync only specific file types
  msc sync msc://source-profile/data --include "*.jpg" --include "*.png" --target-url msc://target-profile/data

  # Sync all files except certain types
  msc sync msc://source-profile/data --exclude "*.tmp" --exclude "*.log" --target-url msc://target-profile/data

  # Sync with complex patterns (exclude all, then include specific types)
  msc sync msc://source-profile/data --exclude "*" --include "*.jpg" --include "*.png" --target-url msc://target-profile/data

  # Order matters! Later patterns override earlier ones
  msc sync msc://source-profile/data --exclude "*" --include "*.txt" --target-url msc://target-profile/data  # Only .txt files
  msc sync msc://source-profile/data --include "*.txt" --exclude "*" --target-url msc://target-profile/data  # No files (exclude overrides include)

  # Sync with directory patterns (AWS S3 compatible)
  msc sync msc://source-profile/data --include "images/*.jpg" --exclude "temp/*" --target-url msc://target-profile/data

  # Include hidden files in sync (by default they are ignored)
  msc sync /path/to/dataset --no-ignore-hidden --target-url msc://profile/prefix
"""

    def run(self, args: argparse.Namespace) -> int:
        if args.verbose:
            if args.target_url:
                print(f"Synchronizing files from {args.source_url} to {args.target_url} ...")
            else:
                print(f"Synchronizing files from {args.source_url} to replicas ...")

        if args.ray_cluster:
            try:
                import ray

                ray.init(address=args.ray_cluster)
                execution_mode = ExecutionMode.RAY
            except ImportError:
                print("Ray is not installed. Please install it with 'pip install ray'.")
                return 1
        else:
            execution_mode = ExecutionMode.LOCAL

        try:
            # Create ordered pattern list if patterns are provided
            ordered_patterns = getattr(args, "ordered_patterns", [])
            patterns: Optional[PatternList] = ordered_patterns if ordered_patterns else None

            # Convert --no-ignore-hidden flag to ignore_hidden parameter (inverted logic)
            ignore_hidden = not args.no_ignore_hidden

            if args.target_url:
                msc.sync(
                    args.source_url,
                    args.target_url,
                    args.delete_unmatched_files,
                    execution_mode=execution_mode,
                    patterns=patterns,
                    ignore_hidden=ignore_hidden,
                )
            else:
                try:
                    replica_indices = (
                        [int(i) for i in args.replica_indices.split(",")] if args.replica_indices else None
                    )
                except ValueError:
                    print(
                        'Invalid --replica-indices value. Use a comma-separated list of integers, e.g. "0,2".',
                        file=sys.stderr,
                    )
                    return 1
                msc.sync_replicas(
                    args.source_url,
                    replica_indices=replica_indices,
                    delete_unmatched_files=args.delete_unmatched_files,
                    execution_mode=execution_mode,
                    patterns=patterns,
                    ignore_hidden=ignore_hidden,
                )
            if args.verbose:
                print("Synchronization completed successfully")
            return 0
        except Exception as e:
            print(f"Error during synchronization: {str(e)}", file=sys.stderr)
            return 1
        finally:
            if args.ray_cluster:
                try:
                    import ray

                    ray.shutdown()
                except Exception as e:
                    print(f"Error shutting down Ray: {str(e)}", file=sys.stderr)
