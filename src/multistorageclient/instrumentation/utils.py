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

from collections.abc import Callable
from functools import wraps
from typing import Any


def file_metrics(operation):
    """
    Decorator to emit metrics for PosixFile I/O operations.

    This decorator wraps file I/O methods to emit metrics through the storage provider's
    _emit_metrics infrastructure. It tracks latency, data size, data rate, and request/response
    counts for file operations.

    :param operation: The operation type (BaseStorageProvider._Operation.READ or WRITE)
    :return: Decorated function that emits metrics

    Usage:
        @file_metrics(operation=BaseStorageProvider._Operation.READ)
        def read(self, size: int = -1) -> Any:
            return self._file.read(size)
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args: Any, **kwargs: Any) -> Any:
            storage_provider = self._storage_client._storage_provider
            return storage_provider._emit_metrics(operation=operation, f=lambda: func(self, *args, **kwargs))

        return wrapper

    return decorator
