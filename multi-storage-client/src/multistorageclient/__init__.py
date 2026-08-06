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
import sys
from importlib.metadata import PackageNotFoundError, version

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


_CONTRIB_MODULES = frozenset({"numpy", "pickle", "os", "zarr", "async_fs", "xarray", "torch", "ray", "hydra"})

# Contrib modules built on Zarr. ``multistorageclient.contrib.xarray`` imports
# ``multistorageclient.contrib.zarr``, so both fail whenever Zarr is unusable.
_ZARR_CONTRIB_MODULES = frozenset({"zarr", "xarray"})

# Third-party top-level module -> extra that provides it. Keyed by the missing module
# rather than the contrib module because one contrib module can need several (e.g.
# ``multistorageclient.xarray`` needs both ``xarray`` and ``zarr``).
_CONTRIB_EXTRAS = {
    "fsspec": "fsspec",
    "hydra": "hydra-core",
    "numpy": "numpy",
    "omegaconf": "hydra-core",
    "ray": "ray",
    "torch": "torch",
    "xarray": "xarray",
    "zarr": "zarr",
}


def _unusable_zarr_hint(attribute: str) -> str | None:
    """
    Explain an unusable Zarr, or return ``None`` when Zarr looks fine.

    MSC's Zarr integration targets Zarr 2.x. Neither of the cases below can be recognized from
    the raised error: on Python 3.14 ``multistorageclient.contrib.zarr`` fails on its ``numpy``
    import before it ever reaches ``zarr``, and under Zarr 3 it fails on a missing *name*
    (``zarr.storage.BaseStore``) rather than a missing module. So we key off the attribute being
    imported and inspect the environment ourselves.
    """
    if sys.version_info >= (3, 14):
        return (
            f"multistorageclient.{attribute} requires Zarr, which is unavailable on Python 3.14. "
            "Zarr 2.x cannot run on Python 3.14 and Zarr 3.x isn't supported yet, so the 'zarr' "
            "extra installs nothing here. Use Python 3.13 or older, or read Zarr data through the "
            "fsspec integration ('msc://' URLs) with a separately installed Zarr 3.x."
        )
    try:
        zarr_version = version("zarr")
    except PackageNotFoundError:
        # Zarr simply isn't installed. The extras hint below says so more precisely.
        return None
    major, _, _ = zarr_version.partition(".")
    if major.isdigit() and int(major) >= 3:
        return (
            f"multistorageclient.{attribute} requires Zarr 2.x, but Zarr {zarr_version} is "
            "installed. MSC's Zarr integration builds on zarr.storage.BaseStore, which Zarr 3 "
            "removed. Install a supported version with: pip install 'multi-storage-client[zarr]', "
            "or read Zarr data through the fsspec integration ('msc://' URLs) with Zarr 3.x."
        )
    return None


def _missing_dependency_hint(attribute: str, error: ImportError) -> str | None:
    """
    Build an actionable message for a contrib module whose third-party dependency is unusable.

    Returns ``None`` when we have nothing more useful to say than the original error, so that
    error propagates untouched instead of masking an unrelated import failure.
    """
    if attribute in _ZARR_CONTRIB_MODULES:
        hint = _unusable_zarr_hint(attribute)
        if hint is not None:
            return hint
    missing = (error.name or "").split(".")[0]
    extra = _CONTRIB_EXTRAS.get(missing)
    if extra is None:
        return None
    return (
        f"multistorageclient.{attribute} requires the '{missing}' package. "
        f"Install it with: pip install 'multi-storage-client[{extra}]'"
    )


def __getattr__(name: str):
    if name not in _CONTRIB_MODULES:
        raise AttributeError(f"module {__name__} has no attribute {name}")
    try:
        module = importlib.import_module(f"{__package__}.contrib.{name}")
    except ImportError as e:
        # Not just ModuleNotFoundError: under Zarr 3 the contrib module fails on a missing name
        # rather than a missing module, which raises plain ImportError.
        hint = _missing_dependency_hint(name, e)
        if hint is None:
            raise
        # ``name`` is deliberately left unset. The hint often concerns a different package than
        # the one that actually failed to import, so copying ``e.name`` onto our own exception
        # would misdirect tooling that auto-installs from it. ``e`` stays chained regardless.
        raise ImportError(hint) from e
    globals()[name] = module  # Cache for subsequent access
    return module
