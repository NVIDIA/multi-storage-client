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
from collections.abc import Callable
from typing import TypeVar

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
