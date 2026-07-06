# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Injected byte-range source contracts for virtual manifests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Optional, Protocol

from ..retry import retry
from ..types import ObjectMetadata, Range, RetryConfig, StorageProvider
from .models import QueryParameter


class RangeReader(Protocol):
    """Narrow random-access contract for a configured storage profile."""

    @property
    def binding_identity(self) -> str:
        """Return the non-secret physical binding identity."""
        ...

    def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        """Read a complete object or an exact byte range."""
        ...

    def info(self, path: str) -> ObjectMetadata:
        """Return metadata for an object."""
        ...


class ServiceRangeReader(Protocol):
    """Random-access contract for deterministic service output."""

    @property
    def binding_identity(self) -> str:
        """Return the non-secret service binding identity."""
        ...

    def validate(self, path: str, query: Sequence[QueryParameter]) -> None:
        """Validate one manifest-controlled path and query without issuing I/O."""
        ...

    def read(
        self,
        path: str,
        query: Sequence[QueryParameter],
        byte_range: Range,
        total_size: int,
    ) -> bytes:
        """Read an exact byte range from a deterministic service response."""
        ...


@dataclass(frozen=True, slots=True)
class SourceBinding:
    """Configured object-range source available to a manifest."""

    reader: RangeReader
    binding_revision: str


@dataclass(frozen=True, slots=True)
class ServiceBinding:
    """Configured service-range source available to a manifest."""

    reader: ServiceRangeReader
    binding_revision: str


SourceBindings = Mapping[str, SourceBinding]
ServiceBindings = Mapping[str, ServiceBinding]


class StorageProviderRangeReader:
    """Adapt one direct storage provider to the narrow range-reader contract."""

    def __init__(
        self,
        provider: StorageProvider,
        binding_identity: str,
        retry_config: Optional[RetryConfig] = None,
    ) -> None:
        self._provider = provider
        self._binding_identity = binding_identity
        self._retry_config = retry_config

    @property
    def binding_identity(self) -> str:
        """Return the non-secret physical binding identity."""
        return self._binding_identity

    @retry
    def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        """Read a complete object or exact byte range."""
        data: Any = self._provider.get_object(path, byte_range)
        if hasattr(data, "to_bytes"):
            data = data.to_bytes()
        return bytes(data)

    @retry
    def info(self, path: str) -> ObjectMetadata:
        """Return metadata for an object."""
        return self._provider.get_object_metadata(path)
