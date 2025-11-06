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

import enum
import socket
from collections.abc import Mapping

import opentelemetry.util.types as api_types

from .base import AttributesProvider


class HostAttributesProvider(AttributesProvider):
    """
    Provides :py:type:`opentelemetry.util.types.Attributes` from host information.
    """

    class HostAttribute(enum.Enum):
        """
        Host attribute.

        Use the enum value in the attributes dictionary values.
        """

        #: Hostname.
        NAME = "name"

    #: Attribute key to host attribute map.
    _attributes: Mapping[str, HostAttribute]

    def __init__(self, attributes: Mapping[str, str]):
        """
        :param attributes: Map of attribute key to host attribute.
        """

        self._attributes = {
            attribute_key: HostAttributesProvider.HostAttribute(host_attribute)
            for attribute_key, host_attribute in attributes.items()
        }

    def attributes(self) -> api_types.Attributes:
        return {
            attribute_key: self._host_attribute_value(host_attribute=host_attribute)
            for attribute_key, host_attribute in self._attributes.items()
        }

    def _host_attribute_value(self, host_attribute: HostAttribute) -> api_types.AttributeValue:
        if host_attribute == HostAttributesProvider.HostAttribute.NAME:
            return socket.gethostname()
