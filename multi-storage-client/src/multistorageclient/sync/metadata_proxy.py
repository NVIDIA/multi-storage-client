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

from collections.abc import Iterator
from typing import Optional

from ..types import MetadataProvider, ObjectMetadata, ResolvedPath
from .types import OperationType, QueueLike


class QueueBackedMetadataProvider(MetadataProvider):
    """
    Proxy that intercepts ``add_file`` and sends it through a result queue
    so the main-process ``ResultMonitorThread`` can replay it on the real
    :py:class:`MetadataProvider`.

    Read-only operations (``realpath``, ``list_objects``, etc.) are
    delegated to the wrapped provider so the worker can still resolve
    paths and check metadata locally.

    In multi-threading mode the delegate IS the real (shared) provider,
    so the delegate call is the authoritative update and the queued
    message is a harmless, idempotent replay.  In multi-processing /
    Ray mode the delegate is a pickled copy; the queued message is the
    only path that reaches the real provider.

    ``remove_file`` is intentionally NOT queued here because it does not
    carry :py:class:`ObjectMetadata` (needed by the monitor for byte
    counting).  DELETE propagation is handled by the sync worker which
    constructs the metadata and queues the result directly.
    """

    def __init__(self, delegate: MetadataProvider, result_queue: QueueLike):
        self._delegate = delegate
        self._result_queue = result_queue

    # ── write operations: update local + queue for main process ──

    def add_file(self, path: str, metadata: ObjectMetadata) -> None:
        meta_dict = metadata.metadata if metadata.metadata else None
        queued_metadata = ObjectMetadata(
            key=metadata.key,
            content_length=metadata.content_length,
            last_modified=metadata.last_modified,
            type=getattr(metadata, "type", "file"),
            content_type=getattr(metadata, "content_type"),
            etag=getattr(metadata, "etag"),
            storage_class=getattr(metadata, "storage_class"),
            metadata=dict(meta_dict) if meta_dict else None,
            symlink_target=getattr(metadata, "symlink_target", None),
        )
        self._delegate.add_file(path, metadata)
        self._result_queue.put((OperationType.ADD, path, queued_metadata))

    def remove_file(self, path: str) -> None:
        self._delegate.remove_file(path)

    def commit_updates(self) -> None:
        self._delegate.commit_updates()

    # ── read operations: delegate to local copy ──

    def list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        attribute_filter_expression: Optional[str] = None,
        show_attributes: bool = False,
    ) -> Iterator[ObjectMetadata]:
        return self._delegate.list_objects(
            path, start_after, end_at, include_directories, attribute_filter_expression, show_attributes
        )

    def get_object_metadata(self, path: str, include_pending: bool = False) -> ObjectMetadata:
        return self._delegate.get_object_metadata(path, include_pending)

    def glob(self, pattern: str, attribute_filter_expression: Optional[str] = None) -> list[str]:
        return self._delegate.glob(pattern, attribute_filter_expression)

    def realpath(self, logical_path: str) -> ResolvedPath:
        return self._delegate.realpath(logical_path)

    def generate_physical_path(self, logical_path: str, for_overwrite: bool = False) -> ResolvedPath:
        return self._delegate.generate_physical_path(logical_path, for_overwrite)

    def is_writable(self) -> bool:
        return self._delegate.is_writable()

    def allow_overwrites(self) -> bool:
        return self._delegate.allow_overwrites()

    def should_use_soft_delete(self) -> bool:
        return self._delegate.should_use_soft_delete()
