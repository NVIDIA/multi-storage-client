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

import sys
from unittest.mock import patch

import pytest


def test_missing_numpy_dependency():
    # Mock the import system to raise ImportError when numpy is imported
    with patch.dict(sys.modules, {"numpy": None}):
        with pytest.raises(ImportError):
            import numpy  # noqa

        # This should not raise an error due to lazy import
        import multistorageclient as msc  # noqa
