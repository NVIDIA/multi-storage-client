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


class SizedRangeReader(RangeReader, Protocol):
    """Random-access reader that can also report an object's size."""

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


def reader_operation_is_callable(reader: object, operation: str) -> bool:
    """Return whether an injected reader exposes one callable operation."""
    try:
        return callable(getattr(reader, operation, None))
    except Exception:
        return False


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


def validate_manifest_bindings(source_bindings: SourceBindings, service_bindings: ServiceBindings) -> None:
    """Validate all injected manifest bindings without reading manifest or source data."""
    _validate_binding_group(source_bindings, SourceBinding, binding_kind="source", requires_validate=False)
    _validate_binding_group(service_bindings, ServiceBinding, binding_kind="service", requires_validate=True)


def _validate_binding_group(
    bindings: Mapping[str, SourceBinding] | Mapping[str, ServiceBinding],
    expected_type: type[SourceBinding] | type[ServiceBinding],
    *,
    binding_kind: str,
    requires_validate: bool,
) -> None:
    """Validate one source or service binding mapping without performing I/O."""
    if not isinstance(bindings, Mapping):
        raise ValueError(f"{binding_kind} bindings must be a mapping.")
    for alias, binding in bindings.items():
        if not isinstance(alias, str) or not alias:
            raise ValueError("binding aliases must be non-empty strings.")
        if not isinstance(binding, expected_type):
            raise ValueError(f"binding {alias!r} has an invalid type.")
        if not isinstance(binding.binding_revision, str) or not binding.binding_revision:
            raise ValueError(f"binding {alias!r} has an invalid revision.")
        if not reader_operation_is_callable(binding.reader, "read"):
            raise ValueError(f"{binding_kind} binding {alias!r} has no callable read method.")
        if requires_validate and not reader_operation_is_callable(binding.reader, "validate"):
            raise ValueError(f"service binding {alias!r} has no callable validate method.")
        try:
            identity = binding.reader.binding_identity
        except Exception as exc:
            raise ValueError(f"binding {alias!r} has an invalid identity.") from exc
        if not isinstance(identity, str) or not identity:
            raise ValueError(f"binding {alias!r} has an invalid identity.")


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
