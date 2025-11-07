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

import json
import os
import random
import string
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pytest
import yaml

import multistorageclient as msc
from multistorageclient.commands.cli.actions.ls import LsAction
from multistorageclient.types import ObjectMetadata


def create_test_files_with_random_structure(
    source_dir: str, num_files: int = 200, file_extensions: Optional[List[str]] = None
) -> List[Tuple[str, str, str, str]]:
    """
    Create test files with random 2-level directory structure.

    Args:
        source_dir: Base directory to create files in
        num_files: Number of files to create
        file_extensions: List of file extensions to use. If None, uses .txt

    Returns:
        List of tuples containing (level1, level2, filename, content) for each created file
    """
    if file_extensions is None:
        file_extensions = ["txt"]

    created_files = []

    for i in range(num_files):
        # Generate random 2-level directory structure
        level1 = "".join(random.choices(string.ascii_lowercase, k=5))
        level2 = "".join(random.choices(string.ascii_lowercase, k=5))

        # Create directory path
        dir_path = Path(source_dir) / level1 / level2
        dir_path.mkdir(parents=True, exist_ok=True)

        # Create file with random content
        extension = random.choice(file_extensions)
        filename = f"file_{i:03d}.{extension}"
        file_path = dir_path / filename
        content = f"Content for file {i} in {level1}/{level2}\n" + "".join(
            random.choices(string.ascii_letters + string.digits, k=100)
        )
        file_path.write_text(content)

        # Store for verification
        created_files.append((level1, level2, filename, content))

    return created_files


@pytest.fixture
def run_cli():
    """
    Run the CLI as a subprocess with the given arguments.
    """

    def _run_cli(*args, expected_return_code=0):
        cmd = [sys.executable, "-m", "multistorageclient.commands.cli.main"] + list(args)

        # Pass through existing environment variables to the subprocess
        env = os.environ.copy()

        result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=120)

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
        # Create about 200 files in random 2-level directories
        created_files = create_test_files_with_random_structure(source_dir, num_files=200)

        # Run the sync command
        stdout, stderr = run_cli("sync", "--verbose", source_dir, "--target-url", target_dir)

        # Verify that files were copied (check a few random files)
        for i in range(0, 200, 20):  # Check every 20th file
            level1, level2, filename, content = created_files[i]
            target_file = Path(target_dir) / level1 / level2 / filename
            assert target_file.exists(), f"Target file {target_file} does not exist"
            assert target_file.read_text() == content, f"Content mismatch for {target_file}"

        assert "200/200" in stderr
        assert "Synchronizing files from" in stdout
        assert "Synchronization completed successfully" in stdout


def test_sync_command_with_real_files_and_patterns(run_cli):
    with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as target_dir:
        # Create about 200 files in random 2-level directories with mixed extensions
        created_files = create_test_files_with_random_structure(
            source_dir, num_files=200, file_extensions=["txt", "bin"]
        )

        # Run the sync command
        stdout, stderr = run_cli("sync", "--verbose", source_dir, "--target-url", target_dir, "--include", "*.txt")

        # verify all the files that are txt are copied
        total_txt_files = 0
        for level1, level2, filename, content in created_files:
            target_file = Path(target_dir) / level1 / level2 / filename
            if filename.endswith(".txt"):
                total_txt_files += 1
                assert target_file.exists(), f"Target file {target_file} does not exist"
                assert target_file.read_text() == content, f"Content mismatch for {target_file}"
            else:
                assert not target_file.exists(), f"Target file {target_file} should not exist"

        assert f"{total_txt_files}/{total_txt_files}" in stderr
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
            Path(test_dir) / "dir0" / "file1.txt",
            Path(test_dir) / "file2.bin",
        ]

        for file_path in test_files:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(f"Content of {file_path.name}")

        # Test basic ls command
        stdout, _ = run_cli("ls", test_dir)

        assert "file1.txt" not in stdout
        assert "file2.bin" in stdout

        # Test recursive ls command
        stdout, _ = run_cli("ls", "--recursive", test_dir)

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
        # Create test files with different paths
        test_files = [
            Path(test_dir) / "old_file1.txt",
            Path(test_dir) / "old_file2.bin",
            Path(test_dir) / "new_file1.txt",
            Path(test_dir) / "new_file2.bin",
            Path(test_dir) / "subdir" / "old_file3.txt",
        ]

        for file_path in test_files:
            file_path.parent.mkdir(parents=True, exist_ok=True)
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

        # Test case 2: debug output
        stdout, _ = run_cli("rm", "--dryrun", "--debug", f"{test_dir}/")
        assert "Arguments:" in stdout

        # Test case 3: quiet mode
        stdout, _ = run_cli("rm", "--dryrun", "--quiet", f"{test_dir}/")
        assert "Arguments:" not in stdout

        # Test case 4: only-show-errors
        stdout, _ = run_cli("rm", "--dryrun", "--only-show-errors", f"{test_dir}/")
        assert "Successfully deleted files with path" not in stdout

        # Test case 5: delete directory without recursive will fail
        with pytest.raises(AssertionError):
            stdout, stderr = run_cli("rm", "-y", f"{test_dir}")
            assert stderr is not None
        # validate that the files still exist
        assert (Path(test_dir) / "old_file1.txt").exists()
        assert (Path(test_dir) / "old_file2.bin").exists()

        # Test case 6: delete a file with recursive can work
        stdout, _ = run_cli("rm", "-y", f"{test_dir}/old_file1.txt")
        assert not (Path(test_dir) / "old_file1.txt").exists()
        assert (Path(test_dir) / "old_file2.bin").exists()
        assert (Path(test_dir) / "new_file1.txt").exists()
        assert (Path(test_dir) / "new_file2.bin").exists()

        # Test case 7: delete a file without recursive can also work
        stdout, bc = run_cli("rm", "-y", "--recursive", f"{test_dir}/old_file2.bin")
        assert not (Path(test_dir) / "old_file2.bin").exists()
        assert (Path(test_dir) / "new_file1.txt").exists()
        assert (Path(test_dir) / "new_file2.bin").exists()

        # Test case 8: delete a directory with recursive can work
        stdout, _ = run_cli("rm", "-y", "--recursive", f"{test_dir}")
        # Verify files were actually deleted
        assert not (Path(test_dir) / "new_file1.txt").exists()
        assert not (Path(test_dir) / "new_file2.bin").exists()
        assert not (Path(test_dir) / "subdir" / "old_file3.txt").exists()


def test_rm_command_with_progress(run_cli):
    with tempfile.TemporaryDirectory() as test_dir:
        # Create test files with different paths
        test_files = [
            Path(test_dir) / "old_file1.txt",
            Path(test_dir) / "old_file2.bin",
            Path(test_dir) / "new_file1.txt",
            Path(test_dir) / "new_file2.bin",
            Path(test_dir) / "subdir" / "old_file3.txt",
        ]

        for file_path in test_files:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(f"Content of {file_path.name}")

        # Test with progress
        _, stderr = run_cli("rm", "-y", "--recursive", f"{test_dir}")
        assert "5/5" in stderr


def test_config_validate_command(run_cli):
    """Test config validate command with YAML output."""
    test_config = {
        "profiles": {"test-profile": {"storage_provider": {"type": "file", "options": {"base_path": "/tmp/test"}}}}
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as f:
        yaml.dump(test_config, f, default_flow_style=False)

        stdout, stderr = run_cli("config", "validate", "--config-file", f.name)

        config = yaml.safe_load(stdout)
        assert "profiles" in config
        assert "test-profile" in config["profiles"]
        assert config["profiles"]["test-profile"] == test_config["profiles"]["test-profile"]


def test_config_validate_command_json_format(run_cli):
    """Test config validate command with JSON output format."""
    test_config = {
        "profiles": {"json-test": {"storage_provider": {"type": "s3", "options": {"base_path": "test-bucket"}}}}
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as f:
        yaml.dump(test_config, f, default_flow_style=False)

        stdout, stderr = run_cli("config", "validate", "--config-file", f.name, "--format", "json")

        config = json.loads(stdout)
        assert "profiles" in config
        assert "json-test" in config["profiles"]
        assert config["profiles"]["json-test"] == test_config["profiles"]["json-test"]


def test_ls_command_with_path_prefix():
    action = LsAction()

    result = action._remove_path_prefix(
        "dir1/dir2",
        ObjectMetadata(key="dir1/dir2/file1.txt", type="file", last_modified=datetime.now(), content_length=100),
    )
    assert result == "file1.txt"

    result = action._remove_path_prefix(
        "dir1/",
        ObjectMetadata(key="dir1/dir2/file1.txt", type="file", last_modified=datetime.now(), content_length=100),
    )
    assert result == "dir2/file1.txt"

    result = action._remove_path_prefix(
        "/etc", ObjectMetadata(key="etc/hosts", type="file", last_modified=datetime.now(), content_length=100)
    )
    assert result == "hosts"

    result = action._remove_path_prefix(
        "/etc/hosts", ObjectMetadata(key="etc/hosts", type="file", last_modified=datetime.now(), content_length=100)
    )
    assert result == "/etc/hosts"

    result = action._remove_path_prefix(
        "dir1/dir2/file1.txt",
        ObjectMetadata(key="dir1/dir2/file1.txt", type="file", last_modified=datetime.now(), content_length=100),
    )
    assert result == "dir1/dir2/file1.txt"
