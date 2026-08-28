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
from typing import TYPE_CHECKING

from .cache import CacheConfig
from .client import StorageClient
from .config import StorageClientConfig
from .pathlib import MultiStoragePath as Path
from .shortcuts import (
    commit_metadata,
    delete,
    download_file,
    generate_presigned_url,
    get_telemetry_provider,
    glob,
    info,
    is_empty,
    is_file,
    list,
    list_recursive,
    make_symlink,
    open,
    resolve_storage_client,
    set_telemetry_provider,
    sync,
    sync_replicas,
    upload_file,
    write,
)
from .types import (
    DryrunResult,
    ProviderBundleV2,
    ResolvedPath,
    ResolvedPathState,
    SignerType,
    StorageBackend,
    SymlinkHandling,
    SyncResult,
)

__version__ = version("multi-storage-client")

__all__ = [
    "CacheConfig",
    "DryrunResult",
    "Path",
    "ProviderBundleV2",
    "ResolvedPath",
    "ResolvedPathState",
    "SignerType",
    "StorageBackend",
    "StorageClient",
    "StorageClientConfig",
    "SymlinkHandling",
    "SyncResult",
    "commit_metadata",
    "delete",
    "download_file",
    "generate_presigned_url",
    "get_telemetry_provider",
    "glob",
    "info",
    "is_empty",
    "is_file",
    "list",
    "list_recursive",
    "make_symlink",
    "open",
    "resolve_storage_client",
    "set_telemetry_provider",
    "sync",
    "sync_replicas",
    "upload_file",
    "write",
]


if TYPE_CHECKING:
    # Declared for type checkers only; imported lazily at runtime by the __getattr__ below. A
    # module-level __getattr__ that a type checker can see makes every attribute resolve to Any,
    # which would silently disable type checking for anything reached through this module.
    from .contrib import async_fs, hydra, numpy, os, pickle, ray, torch  # noqa: F401
else:

    def __getattr__(name: str):
        if name in ["numpy", "pickle", "os", "async_fs", "torch", "ray", "hydra"]:
            module = importlib.import_module(f"{__package__}.contrib.{name}")
            globals()[name] = module  # Cache for subsequent access
            return module
        raise AttributeError(f"module {__name__} has no attribute {name}")
