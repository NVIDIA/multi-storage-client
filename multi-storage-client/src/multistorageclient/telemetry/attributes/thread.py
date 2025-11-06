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
import threading
from collections.abc import Mapping

import opentelemetry.util.types as api_types

from .base import AttributesProvider


class ThreadAttributesProvider(AttributesProvider):
    """
    Provides :py:type:`opentelemetry.util.types.Attributes` from current thread information.
    """

    class ThreadAttribute(enum.Enum):
        """
        Thread attribute.

        Use the enum value in the attributes dictionary values.
        """

        #: Python thread ID.
        IDENT = "ident"
        #: OS thread ID.
        NATIVE_ID = "native_id"

    #: Attribute key to thread attribute key map.
    _attributes: Mapping[str, ThreadAttribute]

    def __init__(self, attributes: Mapping[str, str]):
        """
        :param attributes: Map of attribute key to thread attribute.
        """

        self._attributes = {
            attribute_key: ThreadAttributesProvider.ThreadAttribute(thread_attribute)
            for attribute_key, thread_attribute in attributes.items()
        }

    def attributes(self) -> api_types.Attributes:
        thread = threading.current_thread()
        return {
            attribute_key: getattr(thread, thread_attribute.value)
            for attribute_key, thread_attribute in self._attributes.items()
            if hasattr(thread, thread_attribute.value)
        }
