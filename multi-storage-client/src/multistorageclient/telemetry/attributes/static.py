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

import copy

import opentelemetry.util.types as api_types

from .base import AttributesProvider


class StaticAttributesProvider(AttributesProvider):
    """
    Provides :py:type:`opentelemetry.util.types.Attributes` from static attributes.
    """

    #: Static attributes.
    _attributes: api_types.Attributes

    def __init__(self, attributes: api_types.Attributes):
        """
        :param attributes: Map of attribute key to static attribute value.
        """
        self._attributes = attributes

    def attributes(self) -> api_types.Attributes:
        return copy.deepcopy(self._attributes)
