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
import tempfile
from typing import Any


def setup_msc_config(config_dict: dict[str, Any]) -> None:
    """
    Setup the multi-storage client configuration.
    """
    config_file = tempfile.NamedTemporaryFile(suffix=".json", delete=False)

    with open(config_file.name, "w") as f:
        json.dump(config_dict, f)

    os.environ["MSC_CONFIG"] = config_file.name
