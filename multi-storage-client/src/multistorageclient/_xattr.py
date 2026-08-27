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
"no default supplied", and ``xattr.xattr.get`` re-raises ``OSError`` for a missing attribute
in that case. The module-level ``xattr.getxattr`` takes no ``default`` at all — it delegates
to ``xattr.xattr.get`` without one — so it still raises ``OSError`` for a missing attribute.

The sentinel's presence is nonetheless enough to widen the inferred return type. ``xattr`` is
unannotated and ships no ``py.typed``, so pyright infers ``get`` from its body: the
``return default`` branch contributes ``object`` (the sentinel's type) and the ``return
self._call(...)`` branch contributes ``Unknown`` (it dispatches into a C extension). The
result is ``Unknown | object``, which propagates to ``xattr.getxattr`` and fails every
``.decode(...)`` on the result.

Pyright cannot narrow this away on its own. Proving the ``object`` branch unreachable from
``xattr.getxattr`` needs call-site-sensitive reasoning about an unpassed defaulted parameter
combined with identity-comparison reachability, which no mainstream type checker models. And
even if that branch vanished, what remains is ``Unknown`` rather than ``bytes``, so
``.decode(...)`` would type check without actually being checked.

So this module is the one place the untyped boundary gets validated. The check is a runtime
``isinstance`` rather than a ``cast`` on purpose: a ``cast`` would be silently wrong if the
contract ever changes again, surfacing later as a cryptic ``AttributeError`` on ``object``.
"""

import os
from typing import Any

import xattr


def getxattr(path: str | os.PathLike, name: str) -> bytes:
    """
    Read the extended attribute ``name`` from ``path``.

    :param path: The file to read the extended attribute from.
    :param name: The extended attribute's name.

    :return: The extended attribute's value.

    :raises OSError: If the attribute is missing or extended attributes are unsupported.
    :raises TypeError: If :py:mod:`xattr` returns a non-``bytes`` value, which it should
        never do — ``xattr.getxattr`` passes no ``default``, so it either returns the raw
        attribute value or raises.
    """
    value: Any = xattr.getxattr(path, name)
    if not isinstance(value, bytes):
        raise TypeError(
            f"Reading extended attribute {name!r} from {os.fspath(path)!r} returned "
            f"{type(value).__name__}, expected bytes. This means the installed xattr "
            f"{getattr(xattr, '__version__', '(unknown version)')} no longer matches the "
            f"contract multistorageclient._xattr relies on."
        )
    return value
