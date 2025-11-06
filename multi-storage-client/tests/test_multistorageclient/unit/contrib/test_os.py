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
import pathlib
import tempfile

import multistorageclient as msc

PATH_CLASSES = [str, pathlib.Path, msc.Path]


def test_os_path_exist(file_storage_config):
    for path_class in PATH_CLASSES:
        with tempfile.TemporaryDirectory() as tempdir:
            temp_file_path = os.path.join(tempdir, "existent_file.bin")
            with open(temp_file_path, "wb") as temp_file:
                temp_file.write(b"Some test data")

            assert msc.os.path.exists(path_class(temp_file_path))

            non_existent_path = os.path.join(tempdir, "nonexistent_file.bin")
            assert not msc.os.path.exists(path_class(non_existent_path))


def test_os_path_isfile(file_storage_config):
    for path_class in PATH_CLASSES:
        with tempfile.TemporaryDirectory() as tempdir:
            temp_file_path = os.path.join(tempdir, "existent_file.bin")
            with open(temp_file_path, "wb") as temp_file:
                temp_file.write(b"Some test data")

            assert msc.os.path.isfile(path_class(temp_file_path))
            assert msc.os.path.isdir(path_class(temp_file_path)) is False


def test_os_path_isdir(file_storage_config):
    for path_class in PATH_CLASSES:
        with tempfile.TemporaryDirectory() as tempdir:
            path = os.path.join(tempdir, "test-dir")
            assert msc.os.path.isdir(path_class(tempdir))
            assert msc.os.path.isdir(path_class(path)) is False
            assert msc.os.path.isdir(path_class(path)) == msc.os.path.isdir(path_class(path), strict=False)
            assert msc.os.path.isfile(path_class(path)) is False


def test_makedirs(file_storage_config):
    for path_class in PATH_CLASSES:
        # Verify local path
        with tempfile.TemporaryDirectory() as tempdir:
            path = os.path.join(tempdir, "test-dir")
            assert msc.os.path.isdir(path_class(path)) is False
            msc.os.makedirs(path_class(path))
            assert msc.os.path.isdir(path_class(path))
