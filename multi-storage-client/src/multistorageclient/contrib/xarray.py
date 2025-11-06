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

from typing import Any

import xarray as _xarray

from ..shortcuts import resolve_storage_client
from ..types import MSC_PROTOCOL
from .zarr import LazyZarrStore


def open_zarr(*args: Any, **kwargs: Any) -> _xarray.Dataset:
    """
    Adapt ``xarray.open_zarr`` to use :py:class:`multistorageclient.contrib.zarr.LazyZarrStore`
    when path matches the ``msc`` protocol.

    If the path starts with the MSC protocol, it uses :py:class:`multistorageclient.contrib.zarr.LazyZarrStore`
    with a resolved storage client and prefix, passing ``msc_max_workers`` if provided. Otherwise, it
    directly calls ``xarray.open_zarr``.
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
        return _xarray.open_zarr(*args_list, **kwargs)
    return _xarray.open_zarr(*args, **kwargs)
