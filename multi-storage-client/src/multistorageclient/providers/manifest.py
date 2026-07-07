# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read-only virtual files reconstructed from a Parquet manifest."""

from __future__ import annotations

import io
import os
import tempfile
import threading
from bisect import bisect_left
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import IO, Any, Optional, Union, cast

from .._io import write_all as _write_all
from ..file import RemoteFileReader
from ..manifest.bindings import (
    ServiceBinding,
    ServiceBindings,
    SizedRangeReader,
    SourceBinding,
    SourceBindings,
    reader_operation_is_callable,
    validate_manifest_bindings,
)
from ..manifest.models import ManifestFile, PlannedChunkRead, ServiceChunk
from ..manifest.parquet import load_virtual_manifest
from ..manifest.planner import plan_download
from ..telemetry import Telemetry
from ..types import ObjectMetadata, Range, SymlinkHandling
from .base import BaseStorageProvider

_DOWNLOAD_WINDOW_SIZE = 8 * 1024 * 1024


def _validate_manifest_path(manifest_path: str) -> str:
    """Validate the direct-provider manifest path before it reaches a storage reader."""
    if not isinstance(manifest_path, str) or not manifest_path:
        raise ValueError("manifest_path must be a non-empty normalized relative POSIX .parquet path.")
    if manifest_path.startswith("/") or "\\" in manifest_path:
        raise ValueError("manifest_path must be a normalized relative POSIX .parquet path.")
    if any(ord(character) < 32 or ord(character) == 127 for character in manifest_path):
        raise ValueError("manifest_path must be a normalized relative POSIX .parquet path.")
    if not manifest_path.endswith(".parquet") or any(
        segment in ("", ".", "..") for segment in manifest_path.split("/")
    ):
        raise ValueError("manifest_path must be a normalized relative POSIX .parquet path.")
    return manifest_path


@dataclass(frozen=True, slots=True)
class _ObjectRead:
    binding_alias: str
    binding: SourceBinding
    path: str
    physical_offset: int
    output_offset: int
    size_bytes: int


@dataclass(frozen=True, slots=True)
class _ServiceRead:
    binding: ServiceBinding
    chunk: ServiceChunk
    chunk_offset: int
    output_offset: int
    size_bytes: int


_ExecutionRead = Union[_ObjectRead, _ServiceRead]


class ManifestStorageProvider(BaseStorageProvider):
    """Read-only storage provider backed by a single virtual manifest v2."""

    def __init__(
        self,
        manifest_path: str,
        manifest_reader: SizedRangeReader,
        source_bindings: SourceBindings,
        service_bindings: ServiceBindings,
        max_workers: int = 8,
        config_dict: Optional[dict[str, Any]] = None,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
    ) -> None:
        """Create a virtual manifest provider from already resolved bindings.

        :param manifest_path: Normalized relative path of the single Parquet manifest.
        :param manifest_reader: Range-capable reader for the manifest storage profile.
        :param source_bindings: Allowlisted object source aliases and revisions.
        :param service_bindings: Allowlisted deterministic service aliases and revisions.
        :param max_workers: Maximum number of touched chunks fetched concurrently.
        :param config_dict: Provider configuration used by telemetry and diagnostics.
        :param telemetry_provider: Optional telemetry factory.
        """
        manifest_path = _validate_manifest_path(manifest_path)
        if not isinstance(max_workers, int) or isinstance(max_workers, bool) or max_workers < 1:
            raise ValueError("max_workers must be a positive integer.")
        for operation in ("info", "read"):
            if not reader_operation_is_callable(manifest_reader, operation):
                raise ValueError(f"manifest_reader must provide a callable {operation} method.")

        validate_manifest_bindings(source_bindings, service_bindings)
        resolved_source_bindings = MappingProxyType(dict(source_bindings))
        resolved_service_bindings = MappingProxyType(dict(service_bindings))
        manifest_metadata = manifest_reader.info(manifest_path)
        manifest_size = manifest_metadata.content_length
        if not isinstance(manifest_size, int) or isinstance(manifest_size, bool) or manifest_size < 0:
            raise ValueError("Manifest content length must be a non-negative integer.")

        def read_manifest_range(offset: int, size: int) -> bytes:
            data = manifest_reader.read(manifest_path, Range(offset=offset, size=size))
            if len(data) != size:
                raise IOError(f"Manifest range expected {size} bytes but received {len(data)} bytes.")
            return data

        with RemoteFileReader(
            manifest_path,
            manifest_size,
            read_range=read_manifest_range,
        ) as stream:
            files = load_virtual_manifest(
                cast(IO[bytes], stream),
                resolved_source_bindings,
                resolved_service_bindings,
            )

        super().__init__(
            base_path="/",
            provider_name="manifest",
            config_dict=config_dict,
            telemetry_provider=telemetry_provider,
        )
        self._manifest_path = manifest_path
        self._manifest_reader = manifest_reader
        self._source_bindings = resolved_source_bindings
        self._service_bindings = resolved_service_bindings
        self._max_workers = max_workers
        self._read_executor: Optional[ThreadPoolExecutor] = None
        self._read_executor_lock = threading.Lock()
        self._closed = False
        self._files = MappingProxyType(dict(files))
        self._sorted_keys = tuple(sorted(files))
        directory_last_modified: dict[str, datetime] = {}
        for file in files.values():
            segments = file.key.split("/")
            for depth in range(1, len(segments)):
                directory = "/".join(segments[:depth])
                previous = directory_last_modified.get(directory)
                if previous is None or file.last_modified > previous:
                    directory_last_modified[directory] = file.last_modified
        self._directory_last_modified = MappingProxyType(directory_last_modified)

    @property
    def default_read_prefetch(self) -> Optional[bool]:
        """Stream virtual files unless a caller explicitly requests prefetch."""
        return False

    @property
    def is_read_only(self) -> bool:
        """Return ``True`` because manifests expose immutable virtual files."""
        return True

    def _put_object(
        self,
        path: str,
        body: bytes,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
        attributes: Optional[dict[str, str]] = None,
    ) -> int:
        raise NotImplementedError("ManifestStorageProvider is read-only.")

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        self._ensure_open()
        file = self._file(path)
        plan = plan_download(file, byte_range)
        if not plan.reads:
            return b""

        reads = self._execution_reads(plan.reads)
        output = bytearray(plan.size_bytes)
        if len(reads) == 1:
            read = reads[0]
            data = self._execute_read(read)
            output[read.output_offset : read.output_offset + read.size_bytes] = data
            return bytes(output)

        pending: dict[Future[bytes], _ExecutionRead] = {}
        read_iterator = iter(reads)
        for _ in range(min(self._max_workers, len(reads))):
            read = next(read_iterator)
            pending[self._submit_read(read)] = read

        try:
            while pending:
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    read = pending.pop(future)
                    data = future.result()
                    output[read.output_offset : read.output_offset + read.size_bytes] = data
                for _ in completed:
                    try:
                        next_read = next(read_iterator)
                    except StopIteration:
                        break
                    pending[self._submit_read(next_read)] = next_read
        except BaseException:
            running = [future for future in pending if not future.cancel()]
            if running:
                wait(running)
            raise
        return bytes(output)

    def _ensure_open(self) -> None:
        """Raise predictably when a caller tries to read after terminal close."""
        with self._read_executor_lock:
            self._ensure_open_locked()

    def _ensure_open_locked(self) -> None:
        """Raise predictably when the executor coordinator is terminally closed."""
        if self._closed:
            raise RuntimeError("ManifestStorageProvider is closed.")

    def _get_read_executor(self) -> ThreadPoolExecutor:
        with self._read_executor_lock:
            return self._get_read_executor_locked()

    def _get_read_executor_locked(self) -> ThreadPoolExecutor:
        """Return the read executor while its lifecycle coordinator is held."""
        self._ensure_open_locked()
        if self._read_executor is None:
            self._read_executor = ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="msc-manifest-read",
            )
        return self._read_executor

    def _submit_read(self, read: _ExecutionRead) -> Future[bytes]:
        """Acquire the executor and submit one read as one close-synchronized transition."""
        with self._read_executor_lock:
            return self._get_read_executor_locked().submit(self._execute_read, read)

    def close(self) -> None:
        """Terminally close the provider and cancel virtual-file reads that have not started."""
        lock = getattr(self, "_read_executor_lock", None)
        if lock is None:
            return
        with lock:
            if getattr(self, "_closed", False):
                return
            self._closed = True
            executor = self._read_executor
            self._read_executor = None
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)

    def __del__(self) -> None:
        try:
            self.close()
        finally:
            super().__del__()

    def _copy_object(self, src_path: str, dest_path: str) -> int:
        raise NotImplementedError("ManifestStorageProvider is read-only.")

    def _delete_object(self, path: str, if_match: Optional[str] = None) -> None:
        raise NotImplementedError("ManifestStorageProvider is read-only.")

    def _make_symlink(self, path: str, target: str) -> None:
        raise NotImplementedError("ManifestStorageProvider is read-only.")

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        key = self._logical_path(path)
        file = self._files.get(key)
        if file is not None:
            return self._metadata(file)

        directory_last_modified = self._directory_last_modified.get(key.rstrip("/"))
        if directory_last_modified is not None and key:
            return self._directory_metadata(key.rstrip("/"), directory_last_modified)
        raise FileNotFoundError(key)

    def _list_objects(
        self,
        path: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
        symlink_handling: SymlinkHandling = SymlinkHandling.FOLLOW,
    ) -> Iterator[ObjectMetadata]:
        logical_path = self._logical_path(path)
        prefix_key = logical_path.rstrip("/")
        prefix = f"{prefix_key}/" if prefix_key else ""
        start_key = self._logical_path(start_after) if start_after is not None else None
        end_key = self._logical_path(end_at) if end_at is not None else None

        if not logical_path.endswith("/"):
            exact_file = self._files.get(prefix_key)
            if exact_file is not None:
                if (start_key is None or prefix_key > start_key) and (end_key is None or prefix_key <= end_key):
                    yield self._metadata(exact_file)
                return

        if not include_directories:
            index = bisect_left(self._sorted_keys, prefix)
            while index < len(self._sorted_keys):
                key = self._sorted_keys[index]
                if not key.startswith(prefix):
                    break
                if start_key is not None and key <= start_key:
                    index += 1
                    continue
                if end_key is not None and key > end_key:
                    break
                yield self._metadata(self._files[key])
                index += 1
            return

        emitted_entries: set[str] = set()
        index = bisect_left(self._sorted_keys, prefix)
        while index < len(self._sorted_keys):
            key = self._sorted_keys[index]
            if not key.startswith(prefix):
                break
            remainder = key[len(prefix) :]
            if "/" not in remainder:
                entry_key = key
                entry = self._metadata(self._files[key])
                index += 1
            else:
                entry_key = prefix + remainder.split("/", 1)[0]
                entry = self._directory_metadata(
                    entry_key,
                    self._directory_last_modified[entry_key],
                )
                index = bisect_left(self._sorted_keys, self._prefix_successor(f"{entry_key}/"), index + 1)

            if entry_key in emitted_entries:
                continue
            emitted_entries.add(entry_key)
            if start_key is not None and entry_key <= start_key:
                continue
            if end_key is not None and entry_key > end_key:
                break
            yield entry

    @staticmethod
    def _prefix_successor(prefix: str) -> str:
        """Return the first lexical string strictly after every key beginning with ``prefix``."""
        for index in range(len(prefix) - 1, -1, -1):
            codepoint = ord(prefix[index])
            if codepoint < 0x10FFFF:
                return prefix[:index] + chr(codepoint + 1)
        return prefix + "\x00"

    def _upload_file(
        self,
        remote_path: str,
        f: Union[str, IO],
        attributes: Optional[dict[str, str]] = None,
    ) -> int:
        raise NotImplementedError("ManifestStorageProvider is read-only.")

    def _download_file(
        self,
        remote_path: str,
        f: Union[str, IO],
        metadata: Optional[ObjectMetadata] = None,
    ) -> int:
        file = self._file(remote_path)
        if isinstance(f, str):
            destination = os.path.abspath(f)
            parent = os.path.dirname(destination)
            os.makedirs(parent, exist_ok=True)
            temporary_path: Optional[str] = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    dir=parent,
                    prefix=f".{os.path.basename(destination)}.",
                    delete=False,
                ) as temporary:
                    temporary_path = temporary.name
                    self._stream_file(file, temporary)
                    temporary.flush()
                    os.fsync(temporary.fileno())
                os.replace(temporary_path, destination)
                temporary_path = None
            finally:
                if temporary_path is not None:
                    try:
                        os.unlink(temporary_path)
                    except FileNotFoundError:
                        pass
            return file.size_bytes

        self._require_binary_writer(f)
        with tempfile.SpooledTemporaryFile(max_size=_DOWNLOAD_WINDOW_SIZE, mode="w+b") as temporary:
            self._stream_file(file, temporary)
            temporary.seek(0)
            self._copy_staged_file(temporary, f)
        return file.size_bytes

    @staticmethod
    def _logical_path(path: str) -> str:
        return path.lstrip("/")

    def _file(self, path: str) -> ManifestFile:
        key = self._logical_path(path)
        try:
            return self._files[key]
        except KeyError as exc:
            raise FileNotFoundError(key) from exc

    @staticmethod
    def _metadata(file: ManifestFile) -> ObjectMetadata:
        return ObjectMetadata(
            key=f"/{file.key}",
            content_length=file.size_bytes,
            last_modified=file.last_modified,
            content_type=file.content_type,
            etag=file.etag,
            storage_class=file.storage_class,
            metadata=dict(file.metadata) if file.metadata is not None else None,
        )

    @staticmethod
    def _directory_metadata(key: str, last_modified: datetime) -> ObjectMetadata:
        return ObjectMetadata(
            key=f"/{key}",
            content_length=0,
            last_modified=last_modified,
            type="directory",
        )

    def _execution_reads(self, planned_reads: tuple[PlannedChunkRead, ...]) -> tuple[_ExecutionRead, ...]:
        reads: list[_ExecutionRead] = []
        for planned in planned_reads:
            chunk = planned.chunk
            if isinstance(chunk, ServiceChunk):
                reads.append(
                    _ServiceRead(
                        binding=self._service_bindings[chunk.service_id],
                        chunk=chunk,
                        chunk_offset=planned.chunk_offset,
                        output_offset=planned.output_offset,
                        size_bytes=planned.size_bytes,
                    )
                )
                continue

            binding = self._source_bindings[chunk.source_profile]
            physical_offset = chunk.source_offset + planned.chunk_offset
            current = _ObjectRead(
                binding_alias=chunk.source_profile,
                binding=binding,
                path=chunk.source_path,
                physical_offset=physical_offset,
                output_offset=planned.output_offset,
                size_bytes=planned.size_bytes,
            )
            if reads and isinstance(reads[-1], _ObjectRead):
                previous = reads[-1]
                if (
                    previous.binding_alias == current.binding_alias
                    and previous.binding.binding_revision == current.binding.binding_revision
                    and previous.path == current.path
                    and previous.physical_offset + previous.size_bytes == current.physical_offset
                    and previous.output_offset + previous.size_bytes == current.output_offset
                ):
                    reads[-1] = _ObjectRead(
                        binding_alias=previous.binding_alias,
                        binding=previous.binding,
                        path=previous.path,
                        physical_offset=previous.physical_offset,
                        output_offset=previous.output_offset,
                        size_bytes=previous.size_bytes + current.size_bytes,
                    )
                    continue
            reads.append(current)
        return tuple(reads)

    @staticmethod
    def _execute_read(read: _ExecutionRead) -> bytes:
        if isinstance(read, _ObjectRead):
            data = read.binding.reader.read(
                read.path,
                Range(offset=read.physical_offset, size=read.size_bytes),
            )
            description = f"Object range {read.binding_alias}:{read.path}"
        else:
            data = read.binding.reader.read(
                read.chunk.service_path,
                read.chunk.service_query,
                Range(offset=read.chunk_offset, size=read.size_bytes),
                read.chunk.size_bytes,
            )
            description = f"Service range {read.chunk.service_id}:{read.chunk.service_path}"

        if len(data) != read.size_bytes:
            raise IOError(f"{description} expected {read.size_bytes} bytes but received {len(data)} bytes.")
        return data

    def _stream_file(self, file: ManifestFile, writer: IO) -> None:
        offset = 0
        while offset < file.size_bytes:
            size = min(_DOWNLOAD_WINDOW_SIZE, file.size_bytes - offset)
            data = self._get_object(file.key, Range(offset=offset, size=size))
            _write_all(writer, data)
            offset += size

    @staticmethod
    def _copy_staged_file(source: IO[bytes], writer: IO) -> None:
        """Publish a reconstructed virtual file to a caller-owned binary writer."""
        while data := source.read(_DOWNLOAD_WINDOW_SIZE):
            _write_all(writer, data)

    @staticmethod
    def _require_binary_writer(writer: IO) -> None:
        if isinstance(writer, io.TextIOBase):
            raise TypeError("Manifest downloads require a binary destination.")
        mode = getattr(writer, "mode", None)
        if isinstance(mode, str) and "b" not in mode:
            raise TypeError("Manifest downloads require a binary destination.")
        if not callable(getattr(writer, "write", None)):
            raise TypeError("Manifest downloads require a writable binary destination.")
