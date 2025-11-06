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
import multiprocessing
from collections.abc import Mapping

import opentelemetry.util.types as api_types

from .base import AttributesProvider


class ProcessAttributesProvider(AttributesProvider):
    """
    Provides :py:type:`opentelemetry.util.types.Attributes` from current process information.
    """

    class ProcessAttribute(enum.Enum):
        """
        Process attribute.

        Use the enum value in the attributes dictionary values.
        """

        #: Process ID.
        PID = "pid"

    #: Attribute key to process attribute map.
    _attributes: Mapping[str, ProcessAttribute]

    def __init__(self, attributes: Mapping[str, str]):
        """
        :param attributes: Map of attribute key to process attribute.
        """

        self._attributes = {
            attribute_key: ProcessAttributesProvider.ProcessAttribute(process_attribute)
            for attribute_key, process_attribute in attributes.items()
        }

    def attributes(self) -> api_types.Attributes:
        process = multiprocessing.current_process()
        return {
            attribute_key: getattr(process, process_attribute.value)
            for attribute_key, process_attribute in self._attributes.items()
            if hasattr(process, process_attribute.value)
        }
