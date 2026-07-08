# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read-only virtual files reconstructed from a Parquet manifest."""

from __future__ import annotations

import io
import os
import tempfile
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
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
from ..manifest.constants import DEFAULT_ROW_GROUP_CACHE_SIZE_BYTES, MAX_ROW_GROUP_CACHE_SIZE_BYTES
from ..manifest.index import PyArrowManifestCatalog
from ..manifest.models import ManifestFile, PlannedChunkRead, ServiceChunk
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
        manifest_row_group_cache_size_bytes: int = DEFAULT_ROW_GROUP_CACHE_SIZE_BYTES,
    ) -> None:
        """Create a virtual manifest provider from already resolved bindings.

        :param manifest_path: Normalized relative path of the single Parquet manifest.
        :param manifest_reader: Range-capable reader for the manifest storage profile.
        :param source_bindings: Allowlisted object source aliases and revisions.
        :param service_bindings: Allowlisted deterministic service aliases and revisions.
        :param max_workers: Maximum number of touched chunks fetched concurrently.
        :param config_dict: Provider configuration used by telemetry and diagnostics.
        :param telemetry_provider: Optional telemetry factory.
        :param manifest_row_group_cache_size_bytes: Maximum retained decoded Parquet row-group bytes. Defaults to
            64 MiB; zero disables row-group retention.
        """
        manifest_path = _validate_manifest_path(manifest_path)
        if not isinstance(max_workers, int) or isinstance(max_workers, bool) or max_workers < 1:
            raise ValueError("max_workers must be a positive integer.")
        if (
            not isinstance(manifest_row_group_cache_size_bytes, int)
            or isinstance(manifest_row_group_cache_size_bytes, bool)
            or manifest_row_group_cache_size_bytes < 0
            or manifest_row_group_cache_size_bytes > MAX_ROW_GROUP_CACHE_SIZE_BYTES
        ):
            raise ValueError(
                "manifest_row_group_cache_size_bytes must be an integer from 0 through "
                f"{MAX_ROW_GROUP_CACHE_SIZE_BYTES}."
            )
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

        @contextmanager
        def open_manifest_stream() -> Iterator[IO[bytes]]:
            with RemoteFileReader(
                manifest_path,
                manifest_size,
                read_range=read_manifest_range,
            ) as stream:
                yield cast(IO[bytes], stream)

        catalog = PyArrowManifestCatalog(
            open_manifest_stream,
            resolved_source_bindings,
            resolved_service_bindings,
            manifest_row_group_cache_size_bytes,
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
        self._manifest_row_group_cache_size_bytes = manifest_row_group_cache_size_bytes
        self._read_executor: Optional[ThreadPoolExecutor] = None
        self._read_executor_lock = threading.Lock()
        self._closed = False
        self._catalog = catalog

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
        return self._read_file(file, byte_range)

    def _read_file(self, file: ManifestFile, byte_range: Optional[Range] = None) -> bytes:
        plan = plan_download(file, byte_range)
        if not plan.reads:
            return b""

        reads = self._execution_reads(plan.reads)
        output = bytearray(plan.size_bytes)
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
        catalog = getattr(self, "_catalog", None)
        if catalog is not None:
            catalog.close()

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
        if key and not key.endswith("/"):
            try:
                return self._metadata(self._catalog.get_file(key))
            except FileNotFoundError:
                pass

        directory_key = key.rstrip("/")
        directory_last_modified = self._catalog.directory_last_modified(directory_key)
        if directory_last_modified is None or not directory_key:
            raise FileNotFoundError(key)
        return self._directory_metadata(directory_key, directory_last_modified)

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
            try:
                exact_file = self._catalog.get_file(prefix_key)
            except FileNotFoundError:
                exact_file = None
            if exact_file is not None:
                if (start_key is None or prefix_key > start_key) and (end_key is None or prefix_key <= end_key):
                    yield self._metadata(exact_file)
                return

        if not include_directories:
            for file in self._catalog.iter_files(prefix=prefix, start_after=start_key, end_at=end_key):
                yield self._metadata(file)
            return

        current_entry_key: Optional[str] = None
        current_entry_predecessor_key: Optional[str] = None
        previous_file_key: Optional[str] = None
        direct_file: Optional[ManifestFile] = None
        descendant_last_modified: Optional[datetime] = None

        def current_entry() -> Optional[ObjectMetadata]:
            if current_entry_key is None:
                return None
            if direct_file is not None:
                return self._metadata(direct_file)
            if descendant_last_modified is None:
                return None
            if self._directory_has_prior_shallow_entry(current_entry_key, current_entry_predecessor_key):
                return None
            return self._directory_metadata(current_entry_key, descendant_last_modified)

        for file in self._catalog.iter_files(prefix=prefix):
            remainder = file.key[len(prefix) :]
            segment, separator, _ = remainder.partition("/")
            entry_key = f"{prefix}{segment}"
            if current_entry_key is not None and entry_key != current_entry_key:
                entry = current_entry()
                if end_key is not None and current_entry_key > end_key:
                    return
                if entry is not None and (start_key is None or current_entry_key > start_key):
                    yield entry
                direct_file = None
                descendant_last_modified = None
                current_entry_key = None

            if current_entry_key is None:
                current_entry_key = entry_key
                current_entry_predecessor_key = previous_file_key
                for directory_key, directory_last_modified in self._preempted_directories(prefix, entry_key, file.key):
                    if start_key is not None and directory_key <= start_key:
                        continue
                    if end_key is not None and directory_key > end_key:
                        return
                    yield self._directory_metadata(directory_key, directory_last_modified)
            if not separator:
                direct_file = file
            elif descendant_last_modified is None or file.last_modified > descendant_last_modified:
                descendant_last_modified = file.last_modified
            previous_file_key = file.key

        entry = current_entry()
        if current_entry_key is not None and (end_key is None or current_entry_key <= end_key):
            if entry is not None and (start_key is None or current_entry_key > start_key):
                yield entry

    @staticmethod
    def _directory_has_prior_shallow_entry(directory_key: str, predecessor_key: Optional[str]) -> bool:
        """Return whether an exact file or a prior punctuation sibling takes this directory's shallow slot."""
        if predecessor_key is None or not predecessor_key.startswith(directory_key):
            return False
        suffix = predecessor_key[len(directory_key) :]
        return not suffix or suffix[0] < "/"

    def _preempted_directories(
        self,
        prefix: str,
        entry_key: str,
        source_key: str,
    ) -> Iterator[tuple[str, datetime]]:
        """Yield implicit directories whose punctuation siblings physically sort before their descendants."""
        segment = entry_key[len(prefix) :]
        for index in range(1, len(segment)):
            if segment[index] >= "/":
                continue
            directory_key = f"{prefix}{segment[:index]}"
            if not self._catalog.may_contain_descendants(directory_key):
                continue
            if self._first_preempting_sibling(directory_key) != source_key:
                continue
            directory_last_modified = self._catalog.directory_last_modified(directory_key)
            if directory_last_modified is not None:
                yield directory_key, directory_last_modified

    def _first_preempting_sibling(self, directory_key: str) -> Optional[str]:
        """Return the first physical file that would force one directory ahead of a shallow sibling."""
        files = self._catalog.iter_files(prefix=directory_key)
        try:
            try:
                first = next(files)
            except StopIteration:
                return None
            suffix = first.key[len(directory_key) :]
            if not suffix or suffix.startswith("/") or suffix[0] >= "/":
                return None
            return first.key
        finally:
            close = getattr(files, "close", None)
            if callable(close):
                close()

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
        return self._catalog.get_file(key)

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
            data = self._read_file(file, Range(offset=offset, size=size))
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
