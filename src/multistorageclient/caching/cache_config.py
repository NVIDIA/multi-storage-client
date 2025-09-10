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

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class EvictionPolicyConfig:
    """
    Configuration for cache eviction policy.

    This class defines the configuration parameters for cache eviction policies,
    including the policy type and refresh interval.
    """

    #: The eviction policy type (LRU, FIFO, RANDOM)
    policy: str
    #: Cache refresh interval in seconds. Default is 300 (5 minutes)
    refresh_interval: int = 300


def default_eviction_policy() -> EvictionPolicyConfig:
    """
    Create a default eviction policy configuration. Default is FIFO because it is supported by both backends.

    :return: An EvictionPolicyConfig instance with default values.
    """
    return EvictionPolicyConfig(policy="fifo", refresh_interval=300)


@dataclass
class CacheConfig:
    """
    Configuration for the CacheManager.

    This class defines the complete configuration for the cache system,
    including size limits, check_source_version usage, eviction policy, and location.
    """

    #: The maximum size of the cache in megabytes.
    size: str
    #: Cache line size for range caching. Can be string (e.g., '1M'), defaults to 64MB.
    cache_line_size: str
    #: Use check_source_version(e.g. etag) to update the cached files. Default is True.
    check_source_version: bool = True
    #: The location of the cache. Default is tempdir/msc-cache.
    location: Optional[str] = None
    #: Cache eviction policy configuration. Default is LRU with 300s refresh.
    eviction_policy: EvictionPolicyConfig = field(default_factory=default_eviction_policy)

    def size_bytes(self) -> int:
        """
        Convert cache size to bytes.

        :return: The cache size in bytes.
        """
        return self._convert_to_bytes(self.size)

    def cache_line_size_bytes(self) -> int:
        """
        Convert cache line size to bytes.

        :return: The cache line size in bytes.
        """
        return self._convert_to_bytes(self.cache_line_size)

    def get_eviction_policy(self) -> str:
        """
        Get the eviction policy.

        :return: The current eviction policy type.
        """
        return self.eviction_policy.policy

    def _convert_to_bytes(self, size_str: str) -> int:
        """
        Convert size string with unit suffix to bytes.

        :param size_str: Size string with unit suffix (e.g., '200G', '500M', '1T').
        :return: Size in bytes as an integer.
        :raises ValueError: If the size string has an invalid format or unit.

        Examples:
            >>> _convert_to_bytes("200K")  # Returns 204800
            >>> _convert_to_bytes("1.5G")  # Returns 1610612736
        """
        # Extract numeric part and unit
        unit = size_str[-1].upper()
        try:
            numeric_part = size_str[:-1]
            size = float(numeric_part) if "." in numeric_part else int(numeric_part)
        except ValueError:
            raise ValueError(f"Invalid numeric format in size string: {size_str}")

        # Convert to bytes
        conversion_factors = {"M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5, "E": 1024**6}

        if unit not in conversion_factors:
            raise ValueError(f"Invalid size unit: {unit}. Must be one of: M, G, T, P, E")

        return int(size * conversion_factors[unit])
