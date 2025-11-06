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

import logging
import os
from typing import Union

from ...pathlib import MultiStoragePath as Path

logger = logging.getLogger(__name__)


def exists(path: Union[str, os.PathLike]) -> bool:
    """
    Check if a given path exists.

    :param path: The path to check. It can be a local filesystem path or a path prefixed with a custom protocol (msc://).
    :return: True if the path exists, False otherwise.
    """
    return Path(path).exists()


def isdir(path: Union[str, os.PathLike], strict: bool = True) -> bool:
    """
    Check if a given path is a directory.

    :param path: The path to check. It can be a local filesystem path or a path prefixed with a custom protocol (msc://).
    :param strict: If True, performs additional validation to ensure the directory exists by issuing extra LIST operations
                   on object stores. This can help detect cases where a directory-like path exists but may incur
                   additional latency due to extra API calls. Defaults to True.
    :return: True if the path is a directory, False otherwise.
    """
    return Path(path).is_dir(strict=strict)


def isfile(path: Union[str, os.PathLike]) -> bool:
    """
    Check if a given path is a file.

    :param path: The path to check. It can be a local filesystem path or a path prefixed with a custom protocol (msc://).
    :return: True if the path is a file, False otherwise.
    """
    return Path(path).is_file(strict=False)
