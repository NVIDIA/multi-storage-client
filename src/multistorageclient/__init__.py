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

import importlib
from importlib.metadata import version

from .cache import CacheConfig
from .client import StorageClient, StorageClientConfig
from .pathlib import MultiStoragePath as Path
from .shortcuts import (
    commit_metadata,
    delete,
    download_file,
    get_telemetry,
    glob,
    info,
    is_empty,
    is_file,
    list,
    open,
    resolve_storage_client,
    set_telemetry,
    sync,
    sync_replicas,
    upload_file,
    write,
)

__version__ = version("multi-storage-client")

__all__ = [
    # Classes
    "StorageClient",
    "StorageClientConfig",
    "CacheConfig",
    "Path",
    # Shortcuts
    "commit_metadata",
    "download_file",
    "get_telemetry",
    "glob",
    "is_empty",
    "is_file",
    "open",
    "resolve_storage_client",
    "set_telemetry",
    "sync",
    "sync_replicas",
    "upload_file",
    "list",
    "write",
    "delete",
    "info",
]


def __getattr__(name: str):
    if name in ["numpy", "pickle", "os", "zarr", "async_fs", "xarray", "torch", "ray", "hydra"]:
        module = importlib.import_module(f"{__package__}.contrib.{name}")
        globals()[name] = module  # Cache for subsequent access
        return module
    raise AttributeError(f"module {__name__} has no attribute {name}")
