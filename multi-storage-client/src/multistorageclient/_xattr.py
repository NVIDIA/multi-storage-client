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

"""
Typed wrappers around :py:mod:`xattr`.

``xattr>=1.3.0`` gave ``xattr.xattr.get`` an implicit ``default=_SENTINEL_MISSING``, where
``_SENTINEL_MISSING = object()``. The sentinel is never returned to a caller: it only marks
"no default supplied", and ``get`` re-raises ``OSError`` for a missing attribute in that case.
Its presence in the signature is nonetheless enough to widen the inferred return type of the
module-level ``xattr.getxattr`` from ``bytes`` to ``object``, so every ``.decode(...)`` on the
result fails type checking. These wrappers restore the ``bytes`` contract in one place instead
of casting at each call site.
"""

import os
from typing import cast

import xattr


def getxattr(path: str | os.PathLike, name: str) -> bytes:
    """
    Read the extended attribute ``name`` from ``path``.

    :param path: The file to read the extended attribute from.
    :param name: The extended attribute's name.

    :return: The extended attribute's value.

    :raises OSError: If the attribute is missing or extended attributes are unsupported.
    """
    # No default is passed, so xattr either returns bytes or raises.
    return cast(bytes, xattr.getxattr(path, name))
