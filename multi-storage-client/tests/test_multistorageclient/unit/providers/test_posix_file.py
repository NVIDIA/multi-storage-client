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


import glob
import os
import tempfile

from multistorageclient.providers.posix_file import PosixFileStorageProvider


def test_list_objects_with_ascending_order():
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a directory structure with gaps (skipping some directories) to test edge cases
        # This tests how the BFS walk handles missing directories and mixed depths

        # Create directories with gaps - only create some at each level
        # Level 1: create dirs 0, 2, 4, 6, 8 (skip 1, 3, 5, 7, 9)
        # Level 2: create dirs 1, 3, 5, 7, 9 (skip 0, 2, 4, 6, 8)
        # Level 3: create dirs 0, 2, 4, 6, 8 (skip 1, 3, 5, 7, 9)

        file_count = 0
        for level1 in [0, 2, 4, 6, 8]:  # Skip odd numbers
            level1_path = os.path.join(temp_dir, f"level1_{level1:02d}")
            os.makedirs(level1_path, exist_ok=True)

            for level2 in [1, 3, 5, 7, 9]:  # Skip even numbers
                level2_path = os.path.join(level1_path, f"level2_{level2:02d}")
                os.makedirs(level2_path, exist_ok=True)

                for level3 in [0, 2, 4, 6, 8]:  # Skip odd numbers
                    file_path = os.path.join(level2_path, f"file_{level3:02d}.txt")
                    with open(file_path, "w") as f:
                        f.write(f"content_{level1:02d}_{level2:02d}_{level3:02d}")
                    file_count += 1

        # Add some edge case files at different levels
        edge_case_files = [
            ("root_file.txt", "root content"),
            ("level1_00/standalone_file.txt", "standalone content"),
            ("level1_02/level2_01/extra_file.jpg", "extra content"),
            ("level1_04/level2_03/level3_02/special_file.dat", "special content"),
        ]

        for file_path, content in edge_case_files:
            full_path = os.path.join(temp_dir, file_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w") as f:
                f.write(content)
            file_count += 1

        provider = PosixFileStorageProvider(base_path=temp_dir)
        objects = list(provider.list_objects(path=""))

        # Verify we have the expected number of files
        expected_file_count = file_count
        assert len(objects) == expected_file_count, f"Expected {expected_file_count} files, got {len(objects)}"

        # Verify files are in ascending order
        file_keys = [o.key for o in objects]
        glob_file_keys = [
            f.removeprefix(temp_dir + "/")
            for f in glob.glob(os.path.join(temp_dir, "**", "*"), recursive=True)
            if os.path.isfile(f)
        ]
        glob_file_keys.sort()
        assert file_keys == glob_file_keys, "Files not sorted"


def test_list_objects_with_paths():
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a test directory structure
        test_files = [
            "root_file.txt",
            "docs/readme.md",
            "docs/guide.md",
            "data/2024/report.json",
            "data/archive/old_data.csv",
        ]

        # Create all test files
        for file_path in test_files:
            full_path = os.path.join(temp_dir, file_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w") as f:
                f.write(f"content for {file_path}")

        provider = PosixFileStorageProvider(base_path=temp_dir)

        # test case 1: empty path
        results = list(provider.list_objects(path=""))
        assert len(results) == len(test_files), "Empty path should list all files"
        result_keys = [obj.key for obj in results]
        for expected_file in test_files:
            assert expected_file in result_keys, f"Should find {expected_file} with empty path"

        # test case 2: root directory path
        results = list(provider.list_objects(path="docs/"))
        result_keys = [obj.key for obj in results]
        expected_docs_files = ["docs/readme.md", "docs/guide.md"]
        assert len(result_keys) == len(expected_docs_files), "Root directory path should list directory contents"
        for expected_file in expected_docs_files:
            assert expected_file in result_keys, f"Should find {expected_file} in docs directory"

        # test case 3: full file path
        results = list(provider.list_objects(path="docs/readme.md"))
        result_keys = [obj.key for obj in results]
        assert len(result_keys) == 1, "Full file path should return single file"
        assert "docs/readme.md" in result_keys, "Should find the specific file"

        # test case 4: partial path (should fail)
        # Test with a partial path that doesn't correspond to a complete directory or file
        results = list(provider.list_objects(path="doc"))
        assert len(results) == 0, "Partial path that doesn't exist should return empty results"
