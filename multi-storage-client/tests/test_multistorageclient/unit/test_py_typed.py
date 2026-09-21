# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import importlib.resources


def test_py_typed_marker_present():
    # PEP 561 marker; without it type checkers ignore the package's inline annotations.
    assert importlib.resources.files("multistorageclient").joinpath("py.typed").is_file()


def test_rust_py_typed_marker_present():
    assert importlib.resources.files("multistorageclient_rust").joinpath("py.typed").is_file()
