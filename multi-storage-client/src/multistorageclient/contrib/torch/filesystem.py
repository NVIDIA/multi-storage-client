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
# See the License for the specific language overning permissions and
# limitations under the License.

import io
import os
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Union, cast

from torch.distributed.checkpoint.filesystem import FileSystemBase, FileSystemReader, FileSystemWriter
from torch.distributed.checkpoint.planner import (
    LoadPlan,
    LoadPlanner,
    ReadItem,
)
from torch.futures import Future

from ...pathlib import MultiStoragePath


class MultiStorageFileSystem(FileSystemBase):
    """
    A filesystem implementation that uses the MultiStoragePath class to handle paths.
    """

    @contextmanager
    def create_stream(self, path: Union[str, os.PathLike], mode: str) -> Generator[io.IOBase, None, None]:
        # always download the file here (used for checkpointing)
        with MultiStoragePath(path).open(mode=mode, prefetch_file=True) as fp:
            yield fp

    def concat_path(self, path: Union[str, os.PathLike], suffix: str) -> Union[str, os.PathLike]:
        return MultiStoragePath(path) / suffix

    def rename(self, path: Union[str, os.PathLike], new_path: Union[str, os.PathLike]) -> None:
        MultiStoragePath(path).rename(new_path)

    def init_path(self, path: Union[str, os.PathLike]) -> Union[str, os.PathLike]:
        return MultiStoragePath(path)

    def mkdir(self, path: Union[str, os.PathLike]) -> None:
        MultiStoragePath(path).mkdir(parents=True, exist_ok=True)

    @classmethod
    def validate_checkpoint_id(cls, checkpoint_id: Union[str, os.PathLike]) -> bool:
        try:
            MultiStoragePath(checkpoint_id)
        except ValueError:
            return False

        return True

    def exists(self, path: Union[str, os.PathLike]) -> bool:
        return MultiStoragePath(path).exists()

    def rm_file(self, path: Union[str, os.PathLike]) -> None:
        MultiStoragePath(path).unlink()

    def ls(self, path: Union[str, os.PathLike]) -> list[str]:
        return [str(p) for p in MultiStoragePath(path).iterdir()]


def _prefetch_objects(fs: MultiStorageFileSystem, urls: list[MultiStoragePath], thread_count: int) -> None:
    """
    Efficiently pre-downloads files from object storage using parallel threads, storing them in cache when enabled for optimized subsequent access.
    """

    def _prefetch(url: MultiStoragePath) -> None:
        with fs.create_stream(url, "rb") as _:
            pass

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [executor.submit(_prefetch, url) for url in urls]
        for future in futures:
            future.result()


class MultiStorageFileSystemReader(FileSystemReader):
    """
    A reader implementation that uses the MultiStorageFileSystem class to handle file system operations.
    """

    def __init__(self, path: Union[str, os.PathLike], thread_count: int = 1) -> None:
        """
        Initialize the MultiStorageFileSystemReader with the MultiStorageFileSystem.

        :param path: The path to the checkpoint.
        :param thread_count: The number of threads to use for prefetching.
        """
        super().__init__(path)
        self.fs = MultiStorageFileSystem()
        self.path = self.fs.init_path(path)
        self.thread_count = thread_count

    def read_data(self, plan: LoadPlan, planner: LoadPlanner) -> Future[None]:
        """
        Override the method to prefetch objects from object storage.
        """
        if self.thread_count > 1:
            # group requests by file
            per_file: dict[str, list[ReadItem]] = {}
            for read_item in plan.items:
                item_md = self.storage_data[read_item.storage_index]
                path = item_md.relative_path
                per_file.setdefault(path, []).append(read_item)

            # prefetch objects
            urls = [cast(MultiStoragePath, self.path) / rel_path for rel_path, _ in per_file.items()]
            _prefetch_objects(self.fs, urls, self.thread_count)

        return super().read_data(plan, planner)

    @classmethod
    def validate_checkpoint_id(cls, checkpoint_id: Union[str, os.PathLike]) -> bool:
        return MultiStorageFileSystem.validate_checkpoint_id(checkpoint_id)


class MultiStorageFileSystemWriter(FileSystemWriter):
    """
    A writer implementation that uses the MultiStorageFileSystem class to handle file system operations.
    """

    def __init__(
        self,
        path: Union[str, os.PathLike],
        single_file_per_rank: bool = True,
        sync_files: bool = True,
        thread_count: int = 1,
        per_thread_copy_ahead: int = 10_000_000,
        cache_staged_state_dict: bool = False,
        overwrite: bool = True,
    ) -> None:
        """
        Initialize the MultiStorageFileSystemWriter with the MultiStorageFileSystem.
        """
        super().__init__(
            path,
            single_file_per_rank,
            sync_files,
            thread_count,
            per_thread_copy_ahead,
            cache_staged_state_dict,
            overwrite=overwrite,
        )
        self.fs = MultiStorageFileSystem()
        self.path = self.fs.init_path(path)

    @classmethod
    def validate_checkpoint_id(cls, checkpoint_id: Union[str, os.PathLike]) -> bool:
        return MultiStorageFileSystem.validate_checkpoint_id(checkpoint_id)
