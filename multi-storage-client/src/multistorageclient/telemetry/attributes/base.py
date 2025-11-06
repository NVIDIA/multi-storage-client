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

from abc import ABC, abstractmethod
from collections.abc import Sequence

import opentelemetry.util.types as api_types


class AttributesProvider(ABC):
    """
    Provides :py:type:`opentelemetry.util.types.Attributes`.
    """

    @abstractmethod
    def attributes(self) -> api_types.Attributes:
        """
        Collect attributes.
        """
        pass


def collect_attributes(attributes_providers: Sequence[AttributesProvider]) -> api_types.Attributes:
    """
    Collect and merge attributes from a sequence of attribute providers.

    If multiple attributes providers return an attribute with the same key, the value from the latest attribute provider is kept.

    :param attributes_providers: Attributes providers to collect attributes from.
    :return: Merged attributes.
    """
    merged_attributes: api_types.Attributes = {}

    for attributes in [attributes_provider.attributes() for attributes_provider in attributes_providers]:
        if attributes is not None:
            merged_attributes.update(attributes)

    return merged_attributes
