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
from typing import Union

from ...pathlib import MultiStoragePath as Path
from .path import *  # noqa: F403


def makedirs(name: Union[str, os.PathLike], mode: int = 0o777, exist_ok: bool = False) -> None:
    """
    Create a directory and all its parents.

    Args:
        name: The path to the directory to create.
        mode: The mode to set for the directory.
        exist_ok: If True, do not raise an error if the directory already exists.
    """
    return Path(name).mkdir(mode=mode, parents=True, exist_ok=exist_ok)
