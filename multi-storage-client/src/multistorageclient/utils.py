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

import fnmatch
import importlib
import logging
import math
import multiprocessing
import os
import re
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

from lark import Lark, Transformer
from wcmatch import glob as wcmatch_glob

from .types import ExecutionMode, ObjectMetadata, PatternList, PatternType

if TYPE_CHECKING:
    from .client.types import AbstractStorageClient

logger = logging.getLogger(__name__)


def split_path(path: str) -> tuple[str, str]:
    """
    Splits the given path into components: bucket, key

    :param path: The path to split.
    :return: A tuple containing the bucket and key.
    """
    parts = path.lstrip("/").split("/", 1)
    if len(parts) == 2:
        bucket, key = parts
    else:
        bucket = parts[0]
        key = ""
    return bucket, key


def glob(keys: list[str], pattern: str) -> list[str]:
    """
    Matches a list of keys against a Unix-style wildcard pattern, including recursive ``**``.

    :param keys: A list of keys to match against the pattern.
    :param pattern: A Unix-style wildcard pattern (e.g., ``*.txt``, ``**/*.log``).

    :return: A list of keys that match the pattern.
    """
    return [key for key in keys if wcmatch_glob.globmatch(key, [pattern], flags=wcmatch_glob.GLOBSTAR)]


def insert_directories(keys: list[str]) -> list[str]:
    """
    Inserts implied directory paths into a list of object keys.

    Object stores typically don't return directory entries, only file/object keys.
    This function extracts all unique directory paths from the given keys and
    adds them to create a complete list for glob pattern matching.

    Example:
        Input: [
            "folder1/file1.txt",
            "folder1/subfolder/file2.txt",
            "folder2/file3.txt"
        ]
        Output: [
            "folder1",
            "folder1/file1.txt",
            "folder1/subfolder",
            "folder1/subfolder/file2.txt",
            "folder2",
            "folder2/file3.txt",
        ]

    :param keys: A list of object keys.
    :return: A list containing both the original keys and all implied directory paths in ascending order.
    """
    expanded_keys = set()

    for key in keys:
        parts = key.split("/")
        for i in range(len(parts)):
            expanded_keys.add("/".join(parts[: i + 1]))

    return sorted(expanded_keys)


def import_class(class_name: str, module_name: str, package_name: Optional[str] = None) -> Any:
    """
    Dynamically imports a class from a given module and package.

    Example:
        cls = import_class('MyClass', 'my_module', 'my_package')
        obj = cls()

    :param class_name: The name of the class to import.
    :param module_name: The name of the module containing the class.
    :param package_name: The package name to resolve relative imports (optional).

    :return: The imported class object.

    :raises AttributeError: If the specified class is not found in the module.
    :raises ImportError: If the specified module cannot be imported.
    """
    if package_name:
        module = importlib.import_module(module_name, package_name)
    else:
        module = importlib.import_module(module_name)

    cls = getattr(module, class_name)
    return cls


def join_paths(base: str, path: str) -> str:
    """
    Joins two path components, ensuring that no redundant slashes are added.

    This function works for both filesystem paths and custom scheme paths like ``msc://``.
    It removes any trailing slashes from the base and leading slashes from the path before joining them together.
    """
    return os.path.join(base.rstrip("/"), path.lstrip("/"))


def expand_env_vars(data: Any) -> Any:
    """
    Recursively expands environment variables within strings in a data structure.

    This function traverses through a nested data structure (which can be a combination
    of dictionaries, lists, and strings) and replaces any environment variable references
    within strings using the current environment variables.

    Environment variable references can be in the form of:
    - ${VAR_NAME}
    - $VAR_NAME

    If an environment variable is not set, it raises a ValueError indicating the unresolved variables.

    Args:
        data (dict, list, str, or any): The data structure containing strings with possible
            environment variable references.

    Returns:
        The data structure with all environment variables expanded in the strings.

    Raises:
        ValueError: If there are any unresolved environment variables in the strings after expansion.
    """
    if isinstance(data, dict):
        return {key: expand_env_vars(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [expand_env_vars(element) for element in data]
    elif isinstance(data, str):
        expanded = os.path.expandvars(data)
        unresolved_vars = re.findall(r"\$\{[^}]+\}|\$[A-Za-z_][A-Za-z0-9_]*", expanded)
        if unresolved_vars:
            raise ValueError(f"Unresolved environment variables {unresolved_vars} in '{data}'")
        return expanded
    else:
        return data


def extract_prefix_from_glob(s: str) -> str:
    parts = s.split("/")
    prefix_parts = []

    for part in parts:
        # Check if the part contains any glob special characters
        if any(c in part for c in "*?[]{}"):
            break  # Stop if a glob character is found
        prefix_parts.append(part)

    prefix = "/".join(prefix_parts)
    return prefix


def merge_dictionaries_no_overwrite(
    dict1: Optional[dict[str, Any]] = None,
    dict2: Optional[dict[str, Any]] = None,
    conflicted_keys: Optional[list[str]] = None,
    allow_idempotent: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    """
    Recursively merges two dictionaries without overwriting existing keys.

    :param dict1: first dictionary to be merged
    :param dict2: second dictionary to be merged
    :param conflicted_keys: a list that collects any keys for which there is a collision.
        If not provided, a new list is created.
    :param allow_idempotent: If True, keys with identical values are not considered conflicts (idempotent).
        If False (default), any duplicate key is a conflict regardless of value.

    :return: A tuple of:
            - The merged dictionary from dict1 and dict2 with no overwritten keys.
            - The list of keys that caused conflicts (if any).
    """
    if dict1 is None:
        dict1 = {}

    if dict2 is None:
        dict2 = {}

    if conflicted_keys is None:
        conflicted_keys = []

    for key, value2 in dict2.items():
        if key not in dict1:
            # If the key doesn't exist in dict1, set it.
            dict1[key] = value2
        else:
            # Potential collision
            value1 = dict1[key]

            # If both values are dicts, recurse to check nested fields
            if isinstance(value1, dict) and isinstance(value2, dict):
                merge_dictionaries_no_overwrite(value1, value2, conflicted_keys, allow_idempotent)
            else:
                if allow_idempotent and value1 == value2:
                    pass
                else:
                    conflicted_keys.append(key)

    return dict1, conflicted_keys


def find_executable_path(executable_name: str) -> Optional[Path]:
    """
    Find the path of an executable in the PATH environment variable.

    :param executable_name: Name of the executable to look for in PATH
    :return: A Path object representing the full path of the executable, or None if not found
    """
    executable_path = shutil.which(executable_name)
    if executable_path:
        return Path(executable_path)
    return None


def _get_cgroup_cpu_limit() -> Optional[int]:
    """
    Try to read CPU limit from cgroup v2 and v1 filesystems.

    :return: CPU limit if found, None otherwise
    """
    # Try cgroup v2 first
    cgroup_v2_path = "/sys/fs/cgroup/cpu.max"
    if os.path.exists(cgroup_v2_path):
        try:
            with open(cgroup_v2_path, "r") as f:
                content = f.read().strip()
                if content == "max":
                    return None  # No limit set
                # Format: "quota period" (e.g., "200000 100000" = 2 CPUs)
                parts = content.split()
                if len(parts) == 2:
                    quota, period = int(parts[0]), int(parts[1])
                    if period > 0:
                        return math.ceil(quota / period)
        except (OSError, ValueError, IndexError):
            pass

    # Try cgroup v1 as fallback
    cgroup_v1_path = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
    if os.path.exists(cgroup_v1_path):
        try:
            with open(cgroup_v1_path, "r") as f:
                quota = int(f.read().strip())
                if quota == -1:
                    return None  # No limit set

            # Read the period
            period_path = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"
            if os.path.exists(period_path):
                with open(period_path, "r") as f:
                    period = int(f.read().strip())
                    if period > 0:
                        return math.ceil(quota / period)
        except (OSError, ValueError):
            pass

    return None


def get_available_cpu_count() -> int:
    """
    Get the available CPU count, accounting for job scheduler environments.

    This function detects the execution environment and returns the appropriate
    CPU count based on the job scheduler's resource allocation:

    - **Slurm jobs**: Uses SLURM_CPUS_PER_TASK environment variable
    - **Containerized environments** (including Kubernetes): Uses cgroup filesystem (/sys/fs/cgroup/cpu.max or cpu.cfs_quota_us)
    - **Local execution**: Falls back to multiprocessing.cpu_count()

    :return: Number of available CPUs for the current job/process
    """
    # Check if running in a Slurm job
    if "SLURM_JOB_ID" in os.environ:
        if "SLURM_CPUS_PER_TASK" in os.environ:
            try:
                return int(os.environ["SLURM_CPUS_PER_TASK"])
            except (ValueError, TypeError):
                pass

    # Try reading from cgroup filesystem (works in containers/K8s pods)
    try:
        cgroup_cpu = _get_cgroup_cpu_limit()
        if cgroup_cpu is not None:
            return cgroup_cpu
    except (OSError, ValueError, TypeError):
        pass

    # Fallback to system CPU count for local execution
    return multiprocessing.cpu_count()


def calculate_worker_processes_and_threads(
    num_worker_processes: Optional[int] = None,
    execution_mode: ExecutionMode = ExecutionMode.LOCAL,
    source_client: Optional["AbstractStorageClient"] = None,
    target_client: Optional["AbstractStorageClient"] = None,
):
    """
    Calculate the number of worker processes and threads based on CPU count and environment variables.

    :param num_worker_processes: The number of worker processes to use. If not provided, the number of processes will be
        calculated based on the CPU count and the MSC_NUM_PROCESSES environment variable.
    :param execution_mode: The execution mode to use.
    :param source_client: The source client to use. If provided, the calculation will be updated if the Rust client is enabled.
    :param target_client: The target client to use. If provided, the calculation will be updated if the Rust client is enabled.

    :return: Tuple of (num_worker_processes, num_worker_threads)
    """
    cpu_count = get_available_cpu_count()
    default_processes = "8" if cpu_count > 8 else str(cpu_count)
    if num_worker_processes is None:
        num_worker_processes = int(os.getenv("MSC_NUM_PROCESSES", default_processes))
    num_worker_threads = int(os.getenv("MSC_NUM_THREADS_PER_PROCESS", max(cpu_count // num_worker_processes, 16)))

    # Under the following conditions, multiprocessing is not needed for the local execution mode.
    # 1. Both source and target clients are using the Rust client or POSIX file storage provider.
    # 2. One of the clients is using the Rust client and the other is using the POSIX file storage provider.
    if execution_mode == ExecutionMode.LOCAL:
        if source_client is not None and target_client is not None:
            if all(
                client._is_rust_client_enabled() or client._is_posix_file_storage_provider()
                for client in (source_client, target_client)
            ):
                num_worker_processes = 1
                num_worker_threads = cpu_count

    return num_worker_processes, num_worker_threads


def validate_attributes(attributes: Optional[dict[str, str]]) -> Optional[dict[str, str]]:
    """
    Validates key/value lengths.

    :param attributes: Dictionary of attributes to parse
    :raises ValueError: If key or value exceeds maximum length limits
    :return: same attributes dictionary or None if attributes is None

    Limits:
    - Maximum Key Length: 32 Unicode characters
    - Maximum Value Length: 128 Unicode characters
    """
    if not attributes:
        return None

    for key, value in attributes.items():
        # Validate key length
        if len(key) > 32:
            raise ValueError(f"Attribute key '{key}' exceeds maximum length of 32 characters (actual: {len(key)})")

        # Validate value length
        if len(value) > 128:
            raise ValueError(
                f"Attribute value for key '{key}' exceeds maximum length of 128 characters (actual: {len(value)})"
            )

    return attributes


# Grammar for attribute filter expressions
ATTRIBUTE_FILTER_GRAMMAR = r"""
    ?start: expr

    ?expr: expr "OR" expr   -> or_expr
         | expr "AND" expr  -> and_expr
         | "(" expr ")"     -> parens
         | comparison

    ?comparison: CNAME OP value -> compare

    OP: "=" | "!=" | ">" | ">=" | "<" | "<="

    value: ESCAPED_STRING   -> string
         | SIGNED_NUMBER    -> number

    %import common.CNAME
    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
"""


class AttributeFilterEvaluator(Transformer):
    """Evaluator for attribute filter expressions."""

    def _compare_values(self, actual: str, operator: str, expected: str) -> bool:
        """
        Helper function to compare values based on operator.

        For ordering operators (>, >=, <, <=), attempts numeric comparison first,
        falls back to string comparison if conversion fails.
        """
        if operator == "=":
            return actual == expected
        elif operator == "!=":
            return actual != expected
        elif operator in [">", ">=", "<", "<="]:
            # Try numeric comparison first for ordering operators
            try:
                actual_num = float(actual)
                expected_num = float(expected)
                if operator == ">":
                    return actual_num > expected_num
                elif operator == ">=":
                    return actual_num >= expected_num
                elif operator == "<":
                    return actual_num < expected_num
                elif operator == "<=":
                    return actual_num <= expected_num
            except ValueError:
                # Fall back to lexicographic string comparison
                if operator == ">":
                    return actual > expected
                elif operator == ">=":
                    return actual >= expected
                elif operator == "<":
                    return actual < expected
                elif operator == "<=":
                    return actual <= expected
        else:
            raise ValueError(f"Unsupported operator: {operator}")

        return False

    def compare(self, items):
        """Handle comparison expressions."""
        key, op_token, val = items
        key = str(key)
        op = str(op_token)
        return lambda metadata: (key in metadata and self._compare_values(str(metadata[key]), op, val))

    def and_expr(self, items):
        """Handle AND expressions."""
        return lambda metadata: items[0](metadata) and items[1](metadata)

    def or_expr(self, items):
        """Handle OR expressions."""
        return lambda metadata: items[0](metadata) or items[1](metadata)

    def not_expr(self, items):
        """Handle NOT expressions."""
        return lambda metadata: not items[0](metadata)

    def start(self, items):
        """Start rule."""
        return items[0]

    def string(self, s):
        """Handle string literals."""
        return str(s[0])[1:-1]  # strip quotes

    def number(self, n):
        """Handle numeric literals."""
        return float(n[0])

    def parens(self, items):
        """Handle parenthesized expressions."""
        return items[0]


# Global parser instance
attribute_filter_parser = Lark(ATTRIBUTE_FILTER_GRAMMAR, parser="lalr", start="start")


def create_attribute_filter_evaluator(attribute_filter_expression: str) -> Callable[[dict], bool]:
    """
    Create a evaluator for the given attribute filter expression.

    :param attribute_filter_expression: Filter expression string
                    Example: "model_name = my-test-model AND version != 0.5"
    :return: A callable that takes metadata dict and returns bool
    :raises ValueError: If the expression is invalid
    """
    if not attribute_filter_expression:
        return lambda metadata: True

    try:
        # Parse the expression and create an evaluator
        tree = attribute_filter_parser.parse(attribute_filter_expression)
        return AttributeFilterEvaluator().transform(tree)
    except Exception as e:
        raise ValueError(f"Invalid attribute filter expression: {attribute_filter_expression}. Error: {str(e)}")


def matches_attribute_filter_expression(
    obj_metadata: ObjectMetadata, evaluator: Optional[Callable[[dict], bool]]
) -> bool:
    """
    Check if an object's metadata matches the given attribute filter expression.

    :param obj_metadata: The object metadata to check
    :param evaluator: The evaluator function from create_attribute_filter_parser
    :return: True if the expression evaluates to true, False otherwise
    """
    if not evaluator:
        return True

    if not obj_metadata.metadata:
        return False

    return evaluator(obj_metadata.metadata)


class NullStorageClient:
    """
    Null implementation of StorageClient, where any call to list returns an empty list.
    """

    def list(self, **kwargs: Any) -> Iterator[ObjectMetadata]:
        return iter([])

    def commit_metadata(self, prefix: Optional[str] = None) -> None:
        pass

    def _is_rust_client_enabled(self) -> bool:
        return False

    def _is_posix_file_storage_provider(self) -> bool:
        return True


class PatternMatcher:
    """
    A pattern matcher that implements AWS S3 sync-style include/exclude filtering.

    This class provides functionality to filter file paths based on glob patterns, following the same logic as AWS S3 sync:
    1. All files are included by default
    2. Exclude patterns are applied first
    3. Include patterns are applied after exclusions to re-include specific files

    The patterns support AWS S3 compatible glob syntax:
    - * matches everything
    - ? matches any single character
    - [sequence] matches any character in sequence
    - [!sequence] matches any character not in sequence
    """

    patterns: PatternList

    def __init__(self, ordered_patterns: PatternList):
        """
        Create a PatternMatcher from an ordered list of pattern operations.

        :param ordered_patterns: List of (PatternType, pattern) tuples in order
        :return: PatternMatcher instance
        """
        self.patterns = ordered_patterns.copy()

    def should_include_file(self, file_path: str) -> bool:
        """
        Determine if a file should be included based on the include/exclude patterns.

        The logic follows AWS S3 sync behavior with proper ordering:
        1. Determine initial state based on pattern types
        2. Apply patterns in order - later patterns override earlier ones
        3. Each pattern either includes or excludes the file if it matches

        :param file_path: The file path to check (relative to the sync root)
        :return: True if the file should be included, False otherwise
        """
        # Determine initial state based on pattern types
        has_exclude_patterns = any(pt == PatternType.EXCLUDE for pt, _ in self.patterns)
        has_include_patterns = any(pt == PatternType.INCLUDE for pt, _ in self.patterns)

        if has_include_patterns and not has_exclude_patterns:
            # If we have ONLY include patterns (no exclude patterns),
            # start with all files excluded and only include those that match
            included = False
        else:
            # If we have exclude patterns OR no patterns at all,
            # start with all files included and exclude those that match exclude patterns
            included = True

        # Apply patterns in order - later patterns override earlier ones
        for pattern_type, pattern in self.patterns:
            if self._matches_pattern(file_path, pattern):
                if pattern_type == PatternType.EXCLUDE:
                    logger.debug(f"File {file_path} excluded by pattern: {pattern}")
                    included = False
                elif pattern_type == PatternType.INCLUDE:
                    logger.debug(f"File {file_path} included by pattern: {pattern}")
                    included = True

        logger.debug(f"File {file_path} final decision: {'included' if included else 'excluded'}")
        return included

    def _matches_pattern(self, file_path: str, pattern: str) -> bool:
        """
        Check if a file path matches a glob pattern.

        This method uses standard fnmatch patterns compatible with AWS S3 sync.

        :param file_path: The file path to check
        :param pattern: The glob pattern to match against
        :return: True if the path matches the pattern, False otherwise
        """
        # Use standard fnmatch for AWS S3 compatible patterns
        return fnmatch.fnmatch(file_path, pattern)

    def has_patterns(self) -> bool:
        """
        Check if any include or exclude patterns are configured.

        :return: True if any patterns are configured, False otherwise
        """
        return bool(self.patterns)

    def __repr__(self) -> str:
        """
        String representation of the pattern matcher.
        """
        return f"PatternMatcher(patterns={self.patterns})"
