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

"""Contracts for integrations that resolve catalog URLs to read-only storage profiles."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True)
class ParsedReference:
    """A locally parsed dataset root, decoded key, selector kind and authorization scope.

    Set access_scope to None if the effective caller cannot be identified locally.
    Only immutable identifiers and concrete revisions qualify as canonical roots.
    """

    root_url: str
    relative_path: str
    is_canonical: bool
    access_scope: str | None


@dataclass(frozen=True)
class BoundProfile:
    """An authorized JSON-compatible MSC profile and its pinned root.

    reuse_until is the UTC deadline for local reuse, or None to disable reuse.
    """

    profile_config: dict[str, Any]
    canonical_root_url: str
    reuse_until: datetime | None


class URLResolver(Protocol):
    """An explicitly configured URI scheme integration."""

    def parse(self, url: str) -> ParsedReference:
        """Parse a URI and identify the current caller scope without performing I/O."""
        ...

    def bind(self, reference: ParsedReference) -> BoundProfile:
        """Authorize the captured scope and return the pinned storage profile."""
        ...
