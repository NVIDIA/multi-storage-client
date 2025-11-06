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
import hashlib
from collections.abc import Mapping
from typing import Any, TypedDict

import jmespath
import jmespath.functions as jmespath_functions
import opentelemetry.util.types as api_types

from .base import AttributesProvider


class _MSCConfigJMESPathFunctions(jmespath_functions.Functions):
    """
    Additional JMESPath functions.
    """

    @jmespath_functions.signature({"types": ["string"]}, {"types": ["string"]})
    def _func_hash(self, algorithm: str, value: str) -> str:
        """
        Return the hexadecimal hash digest of a string.

        :param algorithm: Hash algorithm.
        :param value: Hash value.
        :return: Hexadecimal hash digest.
        """
        value_hash = hashlib.new(algorithm)
        value_hash.update(value.encode())
        return value_hash.hexdigest()


class MSCConfigAttributesProvider(AttributesProvider):
    """
    Provides :py:type:`opentelemetry.util.types.Attributes` from a multi-storage client configuration.
    """

    class AttributeValueOptions(TypedDict):
        """
        MSC configuration attribute value options.
        """

        #: JMESPath expression.
        #:
        #: Additional JMESPath functions:
        #:
        #: - ``hash(algorithm: str, value: str)``
        #:    - Calculate the hash digest of a value using a specific hash algorithm (e.g. ``sha3-256``).
        #:    - See :py:meth:`hashlib.new` for algorithms.
        expression: str

    #: Static attributes.
    _attributes: api_types.Attributes

    def __init__(self, attributes: Mapping[str, AttributeValueOptions], config_dict: Mapping[str, Any]):
        """
        :param attributes: Map of attribute key to map of attribute value options.
        """

        self._attributes = {
            attribute_key: jmespath.search(
                attribute_value_options["expression"],
                config_dict,
                options=jmespath.Options(custom_functions=_MSCConfigJMESPathFunctions()),
            )
            for attribute_key, attribute_value_options in attributes.items()
        }

    def attributes(self) -> api_types.Attributes:
        return copy.deepcopy(self._attributes)
