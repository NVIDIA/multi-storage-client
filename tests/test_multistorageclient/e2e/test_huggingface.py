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


import pytest

import test_multistorageclient.e2e.common as common


@pytest.mark.parametrize("profile_name", ["test-hf-private-dataset"])
def test_hf_shortcuts(profile_name):
    """Test HuggingFace using common shortcuts test pattern."""
    common.test_shortcuts(profile_name)


@pytest.mark.parametrize("profile_name", ["test-hf-private-dataset"])
def test_hf_storage_client(profile_name):
    """Test HuggingFace using common storage client test pattern."""
    # Commented out until HF rate limiting issue is resolved.
    # common.test_storage_client(profile_name)
