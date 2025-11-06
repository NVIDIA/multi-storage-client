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

import os
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, Optional

import numpy as np
import zarr as _zarr
from zarr.storage import BaseStore

from ..shortcuts import resolve_storage_client
from ..types import MSC_PROTOCOL

if TYPE_CHECKING:
    from ..client import StorageClient


def open_consolidated(*args: Any, **kwargs: Any) -> _zarr.Group:
    """
    Adapt ``zarr.open_consolidated`` to use :py:class:`LazyZarrStore` when path matches the ``msc`` protocol.

    If the path starts with the MSC protocol, it uses :py:class:`LazyZarrStore` with a resolved
    storage client and prefix, passing ``msc_max_workers`` if provided. Otherwise, it
    directly calls ``zarr.open_consolidated``.
    """
    args_list = list(args)
    path = args_list[0] if args_list else kwargs.get("store")
    msc_max_workers = kwargs.pop("msc_max_workers", None)
    if isinstance(path, str) and path.startswith(MSC_PROTOCOL):
        storage_client, prefix = resolve_storage_client(path)
        zarr_store = LazyZarrStore(storage_client, prefix=prefix, msc_max_workers=msc_max_workers)
        if path == args_list[0]:
            args_list[0] = zarr_store
        else:
            kwargs["store"] = zarr_store
        return _zarr.open_consolidated(*args_list, **kwargs)  # pyright: ignore [reportReturnType]
    return _zarr.open_consolidated(*args, **kwargs)  # pyright: ignore [reportReturnType]


# pyright: reportIncompatibleMethodOverride=false
class LazyZarrStore(BaseStore):
    def __init__(
        self, storage_client: "StorageClient", prefix: str = "", msc_max_workers: Optional[int] = None
    ) -> None:
        self.storage_client = storage_client
        self.prefix = prefix
        self.max_workers = msc_max_workers or int(os.getenv("MSC_MAX_WORKERS", "8"))

    def __getitem__(self, key: str) -> Any:
        full_key = self.prefix + key
        return self.storage_client.read(full_key)

    def getitems(self, keys: Sequence[str], *, contexts: Any) -> Mapping[str, Any]:
        def get_item(key: str) -> tuple[str, Any]:
            return key, self.__getitem__(key)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(get_item, key): key for key in keys}
            results = {}
            for future in as_completed(futures):
                key, value = future.result()
                results[key] = value
        return results

    def __setitem__(self, key: str, value: Any) -> None:
        full_key = self.prefix + key
        if isinstance(value, np.ndarray):
            value = value.tobytes()
        self.storage_client.write(full_key, value)

    def __delitem__(self, key: str) -> None:
        full_key = self.prefix + key
        self.storage_client.delete(full_key)

    def __contains__(self, key: str) -> bool:
        full_key = self.prefix + key
        try:
            self.storage_client.info(full_key)
            return True
        except Exception:
            return False

    def keys(self) -> Iterator[str]:
        for object in self.storage_client.list(self.prefix):
            yield object.key.removeprefix(self.prefix)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return sum(1 for _ in self.keys())
