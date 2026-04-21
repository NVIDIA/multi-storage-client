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

import time
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from multistorageclient import StorageClient

T = TypeVar("T")


def wait(
    waitable: Callable[[], T],
    should_wait: Callable[[T], bool],
    max_attempts: int = 60,
    attempt_interval_seconds: int = 1,
) -> T:
    """
    Wait for the return value of a function ``waitable`` to satisfy a wait condition.

    Defaults to 60 attempts at 1 second intervals.

    For handling storage services with eventually consistent operations.
    """
    assert max_attempts >= 1
    assert attempt_interval_seconds >= 0

    for attempt in range(max_attempts):
        value = waitable()
        if should_wait(value):
            if attempt < max_attempts - 1:
                time.sleep(attempt_interval_seconds)
        else:
            return value

    raise AssertionError(f"Waitable didn't return a desired value within {max_attempts} attempt(s)!")


def len_should_wait(expected_len: int) -> Callable[[Iterable], bool]:
    """
    Returns a wait condition on the length of an iterable return value.

    For list and glob operations.
    """
    return lambda value: len(list(value)) != expected_len


def wait_for_is_file(storage_client: "StorageClient", path: str, is_file: bool) -> None:
    """
    Wait for :py:meth:`StorageClient.is_file` to return the expected value. For eventually consistent data stores.

    :param storage_client: Storage client to use.
    :param path: Path to check.
    :param is_file: Expected value.
    """
    wait(
        waitable=lambda: storage_client.is_file(path=path),
        should_wait=lambda actual_is_file: actual_is_file != is_file,
        max_attempts=5,
        attempt_interval_seconds=1,
    )
