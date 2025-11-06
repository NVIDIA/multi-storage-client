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

import random
from abc import ABC, abstractmethod

from .cache_item import CacheItem

# Valid eviction policy types
LRU = "lru"  # Least Recently Used
MRU = "mru"  # Most Recently Used
FIFO = "fifo"  # First In First Out
RANDOM = "random"  # Random Eviction
NO_EVICTION = "no_eviction"  # No Eviction

# Use a set for faster lookups
VALID_EVICTION_POLICIES = {LRU, MRU, FIFO, RANDOM, NO_EVICTION}


class EvictionPolicy(ABC):
    """Base class for cache eviction policies.

    This abstract base class defines the interface for all cache eviction policies.
    Each policy must implement methods for sorting items and updating access times.
    """

    @abstractmethod
    def sort_items(self, cache_items: list[CacheItem]) -> list[CacheItem]:
        """Sort cache items according to the eviction policy.

        :param cache_items: List of cache items to sort.
        :return: Sorted list of cache items according to the policy.
        """
        pass


class LRUEvictionPolicy(EvictionPolicy):
    """Least Recently Used eviction policy.

    This policy evicts the least recently used items first, based on file access times.
    """

    def sort_items(self, cache_items: list[CacheItem]) -> list[CacheItem]:
        """Sort items by access time (oldest first).

        :param cache_items: List of cache items to sort.
        :return: Items sorted by access time, oldest first.
        """
        cache_items.sort(key=lambda item: item.atime)
        return cache_items


class MRUEvictionPolicy(EvictionPolicy):
    """Most Recently Used eviction policy.

    This policy evicts the most recently used items first, based on file access times.
    """

    def sort_items(self, cache_items: list[CacheItem]) -> list[CacheItem]:
        """Sort items by access time (newest first, for MRU eviction).

        :param cache_items: List of cache items to sort.
        :return: Items sorted by access time, most recently accessed first (to be evicted).
        """
        cache_items.sort(key=lambda item: item.atime, reverse=True)
        return cache_items


class FIFOEvictionPolicy(EvictionPolicy):
    """First In First Out eviction policy.

    This policy evicts items in the order they were added to the cache,
    based on file modification times.
    """

    def sort_items(self, cache_items: list[CacheItem]) -> list[CacheItem]:
        """Sort items by modification time (oldest first).

        :param cache_items: List of cache items to sort.
        :return: Items sorted by modification time, oldest first.
        """
        cache_items.sort(key=lambda item: item.mtime)
        return cache_items


class RandomEvictionPolicy(EvictionPolicy):
    """Random eviction policy.

    This policy randomly selects items for eviction, but preserves the most recently
    added file to prevent immediate eviction of newly cached items.
    """

    def sort_items(self, cache_items: list[CacheItem]) -> list[CacheItem]:
        """Randomly shuffle items, but ensure newest file is preserved.

        For the random policy, we want to ensure that the most recently added file
        (identified by the newest mtime) is preserved, while other files are randomly
        selected for eviction.

        :param cache_items: List of cache items to sort.
        :return: Items with oldest files randomly shuffled, newest file preserved at the end.
        """
        if not cache_items:
            return cache_items

        # First, sort by modification time (newest last)
        cache_items.sort(key=lambda item: item.mtime)

        # The last item is now the newest file
        if len(cache_items) > 1:
            # Randomly shuffle all but the newest file
            oldest_files = cache_items[:-1]
            newest_file = cache_items[-1]

            # Shuffle the oldest files
            random.shuffle(oldest_files)

            # Return with oldest files first (to be evicted) and newest file last (to be preserved)
            return oldest_files + [newest_file]

        return cache_items


class NoEvictionPolicy(EvictionPolicy):
    """No eviction policy.

    This policy prevents any eviction from occurring, effectively making the cache
    grow without bounds until manually cleared.
    """

    def sort_items(self, cache_items: list[CacheItem]) -> list[CacheItem]:
        """Return items in their original order without any sorting.

        Since no eviction should occur, we return the items in their original order.

        :param cache_items: List of cache items to sort.
        :return: Items in their original order.
        """
        return cache_items


class EvictionPolicyFactory:
    """Factory class for creating eviction policy instances.

    This factory creates instances of different eviction policies based on the policy type.
    """

    _policy_map: dict[str, type[EvictionPolicy]] = {
        LRU: LRUEvictionPolicy,
        MRU: MRUEvictionPolicy,
        FIFO: FIFOEvictionPolicy,
        RANDOM: RandomEvictionPolicy,
        NO_EVICTION: NoEvictionPolicy,
    }

    @staticmethod
    def create(policy_type: str) -> EvictionPolicy:
        """Create an eviction policy instance based on the policy type.

        :param policy_type: The type of eviction policy to create.
        :return: An instance of the requested eviction policy.
        :raises ValueError: If the policy type is not supported.
        """
        # Convert to lowercase for consistent handling
        policy_type_lower = policy_type.lower()

        # Get the policy class directly from the map
        policy_class = EvictionPolicyFactory._policy_map.get(policy_type_lower)

        if not policy_class:
            raise ValueError(
                f"Unsupported eviction policy: {policy_type}. Must be one of: {list(VALID_EVICTION_POLICIES)}"
            )

        return policy_class()
