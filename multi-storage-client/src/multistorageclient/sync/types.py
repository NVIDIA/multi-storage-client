# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from ..types import ObjectMetadata

QueueLike = Any  # queue.Queue | multiprocessing.Queue | SharedQueue
EventLike = Any  # threading.Event | multiprocessing.Event | SharedEvent


@dataclass
class ErrorInfo:
    """Information about an error that occurred during sync operation.

    This dataclass encapsulates all relevant information about an exception
    that occurred in a worker thread, including context about what file was
    being processed and which operation failed.
    """

    worker_id: str
    exception_type: str
    exception_message: str
    traceback_str: str
    file_key: Optional[str]
    operation: str


class OperationType(Enum):
    """Enumeration of sync operations that can be performed on files.

    This enum defines the different types of operations that can be queued
    during a synchronization process between source and target storage locations.
    """

    ADD = "add"
    DELETE = "delete"
    STOP = "stop"  # Signal to stop the thread.


@dataclass
class OperationBatch:
    """A batch of sync operations of the same type.

    Each item is a ``(primary_metadata, target_metadata_or_None)`` pair:

    * **ADD** – ``(source_metadata, target_metadata | None)``.
      *target_metadata* is the existing object at the target when the file
      needs updating, or ``None`` when the file is new.
    * **DELETE** – ``(target_metadata, None)``.
    * **STOP** – empty list (sentinel).

    Operations are batched by type (ADD or DELETE) and flushed when:
    - The batch reaches the configured batch_size
    - The operation type changes
    - The producer completes iteration
    """

    operation: OperationType
    items: list[tuple["ObjectMetadata", Optional["ObjectMetadata"]]]
