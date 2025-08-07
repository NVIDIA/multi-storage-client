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

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

import multistorageclient as msc


@pytest.fixture
def run_cli():
    """
    Run the CLI as a subprocess with the given arguments.
    """

    def _run_cli(*args, expected_return_code=0):
        cmd = [sys.executable, "-m", "multistorageclient.commands.cli.main"] + list(args)

        # Pass through existing environment variables to the subprocess
        env = os.environ.copy()

        result = subprocess.run(cmd, capture_output=True, text=True, env=env)

        # Print output if return code doesn't match expected
        if result.returncode != expected_return_code:
            print(f"Expected return code {expected_return_code}, got {result.returncode}")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

        assert result.returncode == expected_return_code
        return result.stdout, result.stderr

    return _run_cli


def test_version_command(run_cli):
    stdout, stderr = run_cli("--version")
    assert f"msc-cli/{msc.__version__}" in stdout
    assert "Python" in stdout


def test_unknown_command(run_cli):
    stdout, stderr = run_cli("unknown_command", expected_return_code=1)
    assert "Unknown command: unknown_command" in stdout
    assert "Run 'msc help'" in stdout


def test_help_command(run_cli):
    stdout, stderr = run_cli("help")
    assert "commands:" in stdout
    assert "help" in stdout


def test_subcommand_help_without_required_args_error(run_cli):
    """Test that --help for subcommands works without showing required argument errors."""

    # Test ls --help
    _, stderr = run_cli("ls", "--help")
    # Ensure no error about required arguments
    assert "error: the following arguments are required" not in stderr

    # Test sync --help
    _, stderr = run_cli("sync", "--help")
    # Ensure no error about required arguments
    assert "error: the following arguments are required" not in stderr


def test_sync_help_command(run_cli):
    stdout, stderr = run_cli("help", "sync")
    assert "Synchronize files" in stdout
    assert "--delete-unmatched-files" in stdout
    assert "--verbose" in stdout
    assert "source_url" in stdout
    assert "--target-url" in stdout
    assert "--replica-indices" in stdout
    assert "--ray-cluster" in stdout


def test_sync_without_replicas(run_cli):
    stdout, stderr = run_cli("sync", "msc://default/data")
    assert "No replicas found in profile 'default'" in stderr


def test_sync_command_with_real_files(run_cli):
    with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as target_dir:
        source_file = Path(source_dir) / "test.txt"
        source_file.write_text("Test content")

        # Run the sync command
        stdout, stderr = run_cli("sync", "--verbose", source_dir, "--target-url", target_dir)

        # Verify that the file was copied
        target_file = Path(target_dir) / "test.txt"
        assert target_file.exists()
        assert target_file.read_text() == "Test content"

        assert "Synchronizing files from" in stdout
        assert "Synchronization completed successfully" in stdout


def test_glob_command_without_attribute_filter_expression(run_cli):
    with tempfile.TemporaryDirectory() as test_dir:
        # Create test files with different extensions
        test_files = [
            Path(test_dir) / "model1.bin",
            Path(test_dir) / "model2.bin",
            Path(test_dir) / "data.txt",
            Path(test_dir) / "config.json",
        ]

        for file_path in test_files:
            file_path.write_text(f"Content of {file_path.name}")

        # Test basic glob pattern
        stdout, _ = run_cli("glob", f"{test_dir}/*.bin")

        # Should find both .bin files
        assert "model1.bin" in stdout
        assert "model2.bin" in stdout
        assert "data.txt" not in stdout
        assert "config.json" not in stdout

        # Test debug output
        stdout, _ = run_cli("glob", "--debug", f"{test_dir}/*")

        # Should show all files with debug info
        assert "model1.bin" in stdout
        assert "model2.bin" in stdout
        assert "data.txt" in stdout
        assert "config.json" in stdout


def test_glob_command_with_attribute_filter_expression(run_cli):
    with tempfile.TemporaryDirectory() as test_dir:
        # Create test files with different attributes using msc.open
        files_with_attrs = [
            ("model1.bin", {"model_name": "gpt", "version": "1.0", "priority": "10"}),
            ("model2.bin", {"model_name": "bert", "version": "2.0", "priority": "5"}),
            ("data.txt", {"model_name": "gpt", "version": "0.5", "priority": "15"}),
            ("config.json", {"type": "config", "version": "1.5", "priority": "20"}),
        ]

        for filename, attributes in files_with_attrs:
            file_path = f"{test_dir}/{filename}"
            with msc.open(file_path, "w", attributes=attributes) as f:
                f.write(f"Content of {filename}")

        # Test equality operator - should find files with model_name = gpt
        stdout, _ = run_cli("glob", "--attribute-filter-expression", 'model_name = "gpt"', f"{test_dir}/*")
        assert "model1.bin" in stdout  # has model_name = gpt
        assert "data.txt" in stdout  # has model_name = gpt
        assert "model2.bin" not in stdout  # has model_name = bert
        assert "config.json" not in stdout  # no model_name attribute

        # Test multiple filters (AND logic) - model_name = gpt AND version >= 1.0
        stdout, _ = run_cli(
            "glob", "--attribute-filter-expression", 'model_name = "gpt" AND version >= 1.0', f"{test_dir}/*"
        )
        assert "model1.bin" in stdout  # model_name = gpt AND version = 1.0
        assert "data.txt" not in stdout  # model_name = gpt BUT version = 0.5
        assert "model2.bin" not in stdout  # version = 2.0 BUT model_name = bert


def test_ls_command_without_attribute_filter_expression(run_cli):
    with tempfile.TemporaryDirectory() as test_dir:
        # Create test files and subdirectory
        test_files = [
            Path(test_dir) / "file1.txt",
            Path(test_dir) / "file2.bin",
        ]

        for file_path in test_files:
            file_path.write_text(f"Content of {file_path.name}")

        # Test basic ls command
        stdout, _ = run_cli("ls", test_dir)

        assert "file1.txt" in stdout
        assert "file2.bin" in stdout

        # Test recursive ls command
        stdout, _ = run_cli("ls", "--recursive", os.path.dirname(test_dir))

        assert "file1.txt" in stdout
        assert "file2.bin" in stdout

        # Test human readable format
        stdout, _ = run_cli("ls", "--human-readable", "--recursive", test_dir)

        # Should show file details with human readable sizes
        assert "file1.txt" in stdout
        assert "file2.bin" in stdout
        assert "B" in stdout  # Should show bytes unit

        # Test summarize
        stdout, _ = run_cli("ls", "--summarize", test_dir)
        assert "Total Objects:" in stdout
        assert "Total Size:" in stdout


def test_ls_command_with_attribute_filter_expression(run_cli):
    with tempfile.TemporaryDirectory() as test_dir:
        # Create test files with different attributes
        files_with_attrs = [
            ("dataset1.bin", {"type": "dataset", "version": "1.5", "priority": "8"}),
            ("dataset2.bin", {"type": "dataset", "version": "0.8", "priority": "12"}),
            ("model.bin", {"type": "model", "version": "2.0", "priority": "5"}),
            ("config.txt", {"type": "config", "version": "1.0", "priority": "15"}),
        ]

        for filename, attributes in files_with_attrs:
            file_path = f"{test_dir}/{filename}"
            with msc.open(file_path, "w", attributes=attributes) as f:
                f.write(f"Content of {filename}")

        # Test comparison operator - should find files with version >= 1.0
        stdout, _ = run_cli("ls", "--recursive", "--attribute-filter-expression", "version >= 1.0", test_dir)
        assert "dataset1.bin" in stdout  # version = 1.5
        assert "model.bin" in stdout  # version = 2.0
        assert "config.txt" in stdout  # version = 1.0
        assert "dataset2.bin" not in stdout  # version = 0.8

        # Test multiple filters - type = dataset AND version <= 2.0
        stdout, _ = run_cli(
            "ls", "--recursive", "--attribute-filter-expression", 'type = "dataset" AND version <= 2.0', test_dir
        )
        assert "dataset1.bin" in stdout  # type = dataset AND version = 1.5
        assert "dataset2.bin" in stdout  # type = dataset AND version = 0.8
        assert "model.bin" not in stdout  # version = 2.0 BUT type = model
        assert "config.txt" not in stdout  # version = 1.0 BUT type = config

        # Test with human readable and summarize
        stdout, _ = run_cli(
            "ls",
            "--recursive",
            "--human-readable",
            "--summarize",
            "--attribute-filter-expression",
            'type = "dataset"',
            test_dir,
        )
        assert "dataset1.bin" in stdout
        assert "dataset2.bin" in stdout
        assert "Total Objects:" in stdout
        assert "Total Size:" in stdout
        assert "B" in stdout  # Should show bytes unit


def test_ls_command_with_show_attributes(run_cli):
    with tempfile.TemporaryDirectory() as test_dir:
        # Create test files with different attributes
        files_with_attrs = [
            ("dataset1.bin", {"type": "dataset", "version": "1.5", "priority": "8"}),
            ("dataset2.bin", {"type": "dataset", "version": "0.8", "priority": "12"}),
            ("model.bin", {"type": "model", "version": "2.0", "priority": "5"}),
            ("config.txt", {"type": "config", "version": "1.0", "priority": "15"}),
        ]

        for filename, attributes in files_with_attrs:
            file_path = f"{test_dir}/{filename}"
            with msc.open(file_path, "w", attributes=attributes) as f:
                f.write(f"Content of {filename}")

        # Show attributes without filters
        stdout, _ = run_cli(
            "ls",
            "--recursive",
            "--show-attributes",
            test_dir,
        )
        assert "dataset1.bin" in stdout
        assert "dataset2.bin" in stdout
        assert "model.bin" in stdout
        assert "config.txt" in stdout

        # Check that JSON attributes are displayed for dataset files
        assert '{"type": "dataset", "version": "1.5", "priority": "8"}' in stdout
        assert '{"type": "dataset", "version": "0.8", "priority": "12"}' in stdout
        assert '{"type": "model", "version": "2.0", "priority": "5"}' in stdout
        assert '{"type": "config", "version": "1.0", "priority": "15"}' in stdout

        # Show attributes with filters
        stdout, _ = run_cli(
            "ls",
            "--recursive",
            "--show-attributes",
            "--attribute-filter-expression",
            'type = "dataset"',
            test_dir,
        )
        assert "dataset1.bin" in stdout
        assert "dataset2.bin" in stdout

        # Check that JSON attributes are displayed for dataset files
        assert '{"type": "dataset", "version": "1.5", "priority": "8"}' in stdout
        assert '{"type": "dataset", "version": "0.8", "priority": "12"}' in stdout


def test_attribute_filter_expression_parsing_errors(run_cli):
    with tempfile.TemporaryDirectory() as test_dir:
        test_file = Path(test_dir) / "test.txt"
        test_file.write_text("Test content")

        # Test invalid operator
        _, stderr = run_cli(
            "glob", "--attribute-filter-expression", "version ~= 1.0", f"{test_dir}/*", expected_return_code=1
        )
        assert "Invalid attribute filter expression" in stderr

        # Test invalid format (missing value)
        _, stderr = run_cli("ls", "--attribute-filter-expression", "version >=", test_dir, expected_return_code=1)
        assert "Invalid attribute filter expression" in stderr


def test_rm_command(run_cli):
    with tempfile.TemporaryDirectory() as test_dir:
        # Create test files with different prefixes
        test_files = [
            Path(test_dir) / "old_file1.txt",
            Path(test_dir) / "old_file2.bin",
            Path(test_dir) / "new_file1.txt",
            Path(test_dir) / "new_file2.bin",
        ]

        for file_path in test_files:
            file_path.write_text(f"Content of {file_path.name}")

        # Test dryrun
        stdout, _ = run_cli("rm", "--dryrun", f"{test_dir}/")
        assert "Files that would be deleted:" in stdout
        assert "old_file1.txt" in stdout
        assert "old_file2.bin" in stdout
        assert "new_file1.txt" in stdout
        assert "new_file2.bin" in stdout

        # Partial path is not supported
        stdout, _ = run_cli("rm", "--dryrun", f"{test_dir}/old_")
        assert "old_file1.txt" not in stdout
        assert "old_file2.bin" not in stdout

        # Test debug output
        stdout, _ = run_cli("rm", "--dryrun", "--debug", f"{test_dir}/")
        assert "Arguments:" in stdout

        # Test quiet mode
        stdout, _ = run_cli("rm", "--dryrun", "--quiet", f"{test_dir}/")
        assert "Arguments:" not in stdout

        # Test only-show-errors
        stdout, _ = run_cli("rm", "--dryrun", "--only-show-errors", f"{test_dir}/")
        assert "Successfully deleted files with prefix" not in stdout

        # Test actual deletion without recursive (file by file)
        stdout, _ = run_cli("rm", f"{test_dir}/old_file1.txt")
        assert f"Successfully deleted files with prefix: {test_dir}/old_file1.txt" in stdout
        assert not (Path(test_dir) / "old_file1.txt").exists()
        assert (Path(test_dir) / "old_file2.bin").exists()
        assert (Path(test_dir) / "new_file1.txt").exists()
        assert (Path(test_dir) / "new_file2.bin").exists()

        stdout, _ = run_cli("rm", f"{test_dir}/old_file2.bin")
        assert f"Successfully deleted files with prefix: {test_dir}/old_file2.bin" in stdout
        assert not (Path(test_dir) / "old_file2.bin").exists()
        assert (Path(test_dir) / "new_file1.txt").exists()
        assert (Path(test_dir) / "new_file2.bin").exists()

        # Test recursive deletion
        subdir = Path(test_dir) / "subdir"
        subdir.mkdir()
        (subdir / "old_file3.txt").write_text("Content")
        stdout, _ = run_cli("rm", "--recursive", f"{test_dir}")
        # Verify files were actually deleted
        assert not (Path(test_dir) / "new_file1.txt").exists()
        assert not (Path(test_dir) / "new_file2.bin").exists()
        assert not (Path(test_dir) / "subdir" / "old_file3.txt").exists()
