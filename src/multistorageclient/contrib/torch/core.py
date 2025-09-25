# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
from typing import IO, Any, Union

import torch as _torch

from ...pathlib import MultiStoragePath
from ...shortcuts import open as msc_open


def load(f: Union[str, os.PathLike[str], IO[bytes]], *args: Any, **kwargs: Any) -> Any:
    """
    Adapt ``torch.load``.
    """
    if isinstance(f, str):
        with msc_open(f, "rb", prefetch_file=True) as fp:
            return _torch.load(fp, *args, **kwargs)
    elif isinstance(f, MultiStoragePath):
        with f.open("rb") as fp:
            return _torch.load(fp, *args, **kwargs)
    else:
        return _torch.load(f, *args, **kwargs)


def save(obj: object, f: Union[str, os.PathLike[str], IO[bytes]], *args: Any, **kwargs: Any) -> Any:
    """
    Adapt ``torch.save``.
    """
    if isinstance(f, str):
        with msc_open(f, "wb") as fp:
            return _torch.save(obj, fp, *args, **kwargs)
    elif isinstance(f, MultiStoragePath):
        with f.open("wb") as fp:
            return _torch.save(obj, fp, *args, **kwargs)
    else:
        return _torch.save(obj, f, *args, **kwargs)
