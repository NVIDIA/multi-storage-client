# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Public contracts for single-Parquet virtual manifests."""

from .bindings import RangeReader, ServiceBinding, ServiceRangeReader, SizedRangeReader, SourceBinding
from .models import QueryParameter
from .schema import (
    MANIFEST_KIND,
    MANIFEST_KIND_METADATA_KEY,
    MANIFEST_VERSION,
    MANIFEST_VERSION_METADATA_KEY,
    virtual_manifest_v2_schema,
)


class ManifestValidationError(ValueError):
    """Raised when a virtual manifest violates the v2 contract."""


__all__ = [
    "MANIFEST_KIND",
    "MANIFEST_KIND_METADATA_KEY",
    "MANIFEST_VERSION",
    "MANIFEST_VERSION_METADATA_KEY",
    "ManifestValidationError",
    "QueryParameter",
    "RangeReader",
    "ServiceBinding",
    "ServiceRangeReader",
    "SizedRangeReader",
    "SourceBinding",
    "virtual_manifest_v2_schema",
]
