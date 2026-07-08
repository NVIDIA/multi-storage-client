# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Internal lookup contract for virtual manifest catalogs."""

from __future__ import annotations

import logging
import sys
import threading
from bisect import bisect_left
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType, TracebackType
from typing import IO, Any, ContextManager, Generic, Optional, Protocol, TypeVar

from ..types import Range
from . import ManifestValidationError
from .bindings import ServiceBindings, SourceBindings
from .constants import DEFAULT_ROW_GROUP_CACHE_SIZE_BYTES, MAX_ROW_GROUP_CACHE_SIZE_BYTES
from .models import DownloadPlan, ManifestFile
from .parquet import (
    _BATCH_SIZE,
    decode_virtual_manifest_file,
    iter_virtual_manifest_files,
    validate_virtual_manifest_parquet_file,
)
from .planner import plan_download
from .schema import _require_pyarrow

_T = TypeVar("_T")
_LOG = logging.getLogger(__name__)


def _error(message: str) -> ManifestValidationError:
    return ManifestValidationError(f"Invalid virtual manifest v2: {message}")


class _CatalogClosedError(RuntimeError):
    """Terminal signal raised only when catalog closure rejects or interrupts work."""


@dataclass(frozen=True, slots=True)
class _RowGroupBounds:
    minimum: str
    maximum: str


@dataclass(slots=True)
class _LoadFlight:
    event: threading.Event
    error: Optional[BaseException] = None


@dataclass(eq=False, slots=True)
class _IteratorOwnedStream:
    """One unretained row-group stream that must be released by close or its iterator."""

    stream_context: ContextManager[IO[bytes]]
    _closed: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def close(
        self,
        exc_info: tuple[type[BaseException] | None, BaseException | None, TracebackType | None] = (None, None, None),
    ) -> None:
        """Exit the stream context once, tolerating concurrent catalog and iterator shutdown."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self.stream_context.__exit__(*exc_info)


class BinaryStreamFactory(Protocol):
    """Open independent catalog-owned binary streams for concurrent Parquet reads."""

    def __call__(self) -> ContextManager[IO[bytes]]:
        """Return a fresh readable, seekable stream positioned at byte zero.

        Every invocation is independent. The catalog enters and exits the
        returned context exactly once and may invoke the factory repeatedly.
        """
        ...


class RowGroupCache(Generic[_T]):
    """Thread-safe byte-bounded LRU of complete immutable row-group batches.

    ``get`` refreshes recency. ``put`` first canonicalizes its value to a tuple;
    the same tuple is weighed and stored. Replacing an entry makes it most
    recent. An oversized replacement leaves the prior entry unchanged. A zero
    budget rejects every admission, including zero-weight entries.
    """

    def __init__(
        self,
        max_size_bytes: int,
        weight: Callable[[tuple[_T, ...]], int],
    ) -> None:
        """Create a cache whose entries are weighed before atomic admission."""
        if (
            not isinstance(max_size_bytes, int)
            or isinstance(max_size_bytes, bool)
            or max_size_bytes < 0
            or max_size_bytes > MAX_ROW_GROUP_CACHE_SIZE_BYTES
        ):
            raise ValueError(f"max_size_bytes must be an integer from 0 through {MAX_ROW_GROUP_CACHE_SIZE_BYTES}")
        self._max_size_bytes = max_size_bytes
        self._weight = weight
        self._entries: OrderedDict[int, tuple[tuple[_T, ...], int]] = OrderedDict()
        self._size_bytes = 0
        self._lock = threading.RLock()

    @property
    def size_bytes(self) -> int:
        """Return the conservatively accounted retained bytes."""
        with self._lock:
            return self._size_bytes

    def get(self, row_group: int) -> Optional[tuple[_T, ...]]:
        """Return and refresh one complete cached row group."""
        with self._lock:
            entry = self._entries.get(row_group)
            if entry is None:
                return None
            self._entries.move_to_end(row_group)
            return entry[0]

    def put(self, row_group: int, batches: Sequence[_T]) -> bool:
        """Atomically admit a complete row group, or return ``False`` when oversized.

        Negative or non-integer weights raise ``ValueError``.
        """
        immutable_batches = tuple(batches)
        weight = self._weight(immutable_batches)
        if not isinstance(weight, int) or isinstance(weight, bool) or weight < 0:
            raise ValueError("row-group cache weight must be a non-negative integer")
        if self._max_size_bytes == 0 or weight > self._max_size_bytes:
            return False

        with self._lock:
            previous = self._entries.pop(row_group, None)
            if previous is not None:
                self._size_bytes -= previous[1]
            while self._entries and self._size_bytes + weight > self._max_size_bytes:
                _, (_, evicted_weight) = self._entries.popitem(last=False)
                self._size_bytes -= evicted_weight
            self._entries[row_group] = (immutable_batches, weight)
            self._size_bytes += weight
            return True

    def clear(self) -> None:
        """Release every retained row group."""
        with self._lock:
            self._entries.clear()
            self._size_bytes = 0


class ManifestCatalog(Protocol):
    """Thread-safe lookup over one immutable virtual-manifest snapshot.

    Implementations permit concurrent calls. ``close`` is idempotent, rejects
    new work with ``RuntimeError``, waits for the active loader, and makes an
    iterator fail before it performs more catalog I/O after closure.
    """

    def get_file(self, key: str) -> ManifestFile:
        """Return one fully validated logical file or raise ``FileNotFoundError``."""
        ...

    def plan(self, key: str, byte_range: Optional[Range] = None) -> DownloadPlan:
        """Return touched chunks or raise ``FileNotFoundError`` for a missing key."""
        ...

    def iter_files(
        self,
        prefix: str = "",
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
    ) -> Iterator[ManifestFile]:
        """Yield fully validated files in ascending physical key order.

        ``prefix`` is a literal logical-key prefix, ``start_after`` is
        exclusive, and ``end_at`` is inclusive.
        """
        ...

    def directory_last_modified(self, key: str) -> Optional[datetime]:
        """Return the maximum descendant timestamp, or ``None`` when absent."""
        ...

    def may_contain_descendants(self, key: str) -> bool:
        """Return whether footer metadata cannot rule out a descendant of ``key``."""
        ...

    def close(self) -> None:
        """Idempotently wait for the active loader and release retained resources."""
        ...


class PyArrowManifestCatalog:
    """Lazy bounded catalog over one single-file Parquet manifest."""

    def __init__(
        self,
        stream_factory: BinaryStreamFactory,
        source_bindings: SourceBindings,
        service_bindings: ServiceBindings,
        row_group_cache_size_bytes: int = DEFAULT_ROW_GROUP_CACHE_SIZE_BYTES,
    ) -> None:
        """Validate footer metadata and prepare lazy row-group access."""
        if (
            not isinstance(row_group_cache_size_bytes, int)
            or isinstance(row_group_cache_size_bytes, bool)
            or row_group_cache_size_bytes < 0
            or row_group_cache_size_bytes > MAX_ROW_GROUP_CACHE_SIZE_BYTES
        ):
            raise ValueError(
                f"row_group_cache_size_bytes must be an integer from 0 through {MAX_ROW_GROUP_CACHE_SIZE_BYTES}"
            )
        try:
            from .bindings import validate_manifest_bindings

            validate_manifest_bindings(source_bindings, service_bindings)
        except ValueError as exc:
            raise _error(str(exc)) from exc

        self._stream_factory = stream_factory
        self._source_bindings = MappingProxyType(dict(source_bindings))
        self._service_bindings = MappingProxyType(dict(service_bindings))
        self._cache_budget = row_group_cache_size_bytes
        self._cache = RowGroupCache[Any](
            row_group_cache_size_bytes,
            lambda batches: sum(batch.get_total_buffer_size() for batch in batches),
        )
        self._state_lock = threading.RLock()
        self._loader_lock = threading.Lock()
        self._flight_lock = threading.Lock()
        self._flights: dict[int, _LoadFlight] = {}
        self._iterator_streams: set[_IteratorOwnedStream] = set()
        self._closed = False

        pa = _require_pyarrow()
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError(
                "PyArrow is required for virtual manifest support. "
                "Install it with: pip install multi-storage-client[virtual-manifest]"
            ) from exc
        self._pq = pq

        try:
            with stream_factory() as stream:
                parquet_file = pq.ParquetFile(stream, pre_buffer=False)
                validate_virtual_manifest_parquet_file(parquet_file, pa)
                metadata = parquet_file.metadata
        except ManifestValidationError:
            raise
        except Exception as exc:
            raise _error("unable to read Parquet footer metadata") from exc

        self._metadata = metadata
        self._row_group_count = metadata.num_row_groups
        try:
            self._validate_empty_layout()
            bounds, complete_statistics, has_page_index = self._inspect_row_groups()
        except ManifestValidationError:
            raise
        except Exception as exc:
            raise _error("unable to validate Parquet row-group metadata") from exc
        self._bounds = bounds
        self._maximum_keys = tuple(bound.maximum for bound in bounds)
        self._uses_row_group_statistics = complete_statistics
        self._has_key_page_index = has_page_index

    @property
    def uses_row_group_statistics(self) -> bool:
        """Return whether every nonempty row group has authoritative key bounds."""
        return self._uses_row_group_statistics

    @property
    def has_key_page_index(self) -> bool:
        """Return whether every nonempty row group advertises both key page indexes."""
        return self._has_key_page_index

    @property
    def cached_bytes(self) -> int:
        """Return conservatively accounted retained Arrow buffer bytes."""
        return self._cache.size_bytes

    def get_file(self, key: str) -> ManifestFile:
        """Return one fully validated logical file or raise ``FileNotFoundError``."""
        try:
            self._ensure_open()
            rows = self._rows_for_key(key)
            if not rows:
                raise FileNotFoundError(key)
            return decode_virtual_manifest_file(rows, self._source_bindings, self._service_bindings)
        except (_CatalogClosedError, ManifestValidationError, FileNotFoundError):
            raise
        except RuntimeError as exc:
            raise _error("unable to decode virtual manifest file") from exc

    def plan(self, key: str, byte_range: Optional[Range] = None) -> DownloadPlan:
        """Return the touched chunks for one logical byte range."""
        return plan_download(self.get_file(key), byte_range)

    def iter_files(
        self,
        prefix: str = "",
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
    ) -> Iterator[ManifestFile]:
        """Yield fully validated files in ascending physical key order."""

        def files() -> Iterator[ManifestFile]:
            try:
                self._ensure_open()
                lower_bound = prefix
                if start_after is not None and start_after > lower_bound:
                    lower_bound = start_after
                first_row_group = self._first_row_group(lower_bound) if lower_bound else 0
                rows = self._iter_rows(first_row_group, lower_bound, prefix, end_at)
                for file in iter_virtual_manifest_files(rows, self._source_bindings, self._service_bindings):
                    self._ensure_open()
                    if start_after is not None and file.key <= start_after:
                        continue
                    if end_at is not None and file.key > end_at:
                        break
                    if prefix and not file.key.startswith(prefix):
                        if file.key > prefix:
                            break
                        continue
                    yield file
            except (_CatalogClosedError, ManifestValidationError):
                raise
            except RuntimeError as exc:
                raise _error("unable to decode virtual manifest file") from exc

        return files()

    def directory_last_modified(self, key: str) -> Optional[datetime]:
        """Return the maximum timestamp among segment-aware descendants.

        A nonempty ``key`` matches only logical keys beginning with ``key + '/'``
        and excludes an exact file named ``key``. The empty key matches every
        file in the manifest.
        """
        self._ensure_open()
        if self._uses_row_group_statistics and not self._bounds:
            return None
        prefix = f"{key}/" if key else ""
        if prefix and self._uses_row_group_statistics and not self._may_contain_prefix(prefix):
            return None
        maximum: Optional[datetime] = None
        for file in self.iter_files(prefix=prefix):
            if maximum is None or file.last_modified > maximum:
                maximum = file.last_modified
        return maximum

    def may_contain_descendants(self, key: str) -> bool:
        """Return ``False`` only when complete footer statistics rule out ``key`` descendants."""
        self._ensure_open()
        if self._uses_row_group_statistics and not self._bounds:
            return False
        if not key:
            return self._row_group_count > 0
        if not self._uses_row_group_statistics:
            return True
        return self._may_contain_prefix(f"{key}/")

    def _may_contain_prefix(self, prefix: str) -> bool:
        """Use complete row-group bounds to rule out a prefix without decoding batches."""
        if not self._bounds:
            return False
        first = self._first_row_group(prefix)
        if first >= len(self._bounds):
            return False
        return self._bounds[first].minimum < self._prefix_successor(prefix)

    @staticmethod
    def _prefix_successor(prefix: str) -> str:
        """Return the first lexical string strictly after every string beginning with ``prefix``."""
        for index in range(len(prefix) - 1, -1, -1):
            codepoint = ord(prefix[index])
            if codepoint < 0x10FFFF:
                return prefix[:index] + chr(codepoint + 1)
        return prefix + "\x00"

    def close(self) -> None:
        """Idempotently close the catalog after active loading finishes."""
        self._close_for_new_work()
        with self._loader_lock:
            self._close_iterator_streams()
        self._cache.clear()

    def _validate_empty_layout(self) -> None:
        if self._metadata.num_rows == 0:
            if self._row_group_count not in (0, 1):
                raise _error("an empty dataset must contain zero or one empty row group")
            if self._row_group_count == 1 and self._metadata.row_group(0).num_rows != 0:
                raise _error("empty manifest metadata conflicts with its row group")
            return
        for row_group in range(self._row_group_count):
            if self._metadata.row_group(row_group).num_rows == 0:
                raise _error("a nonempty dataset cannot contain an empty row group")

    def _inspect_row_groups(self) -> tuple[tuple[_RowGroupBounds, ...], bool, bool]:
        if self._metadata.num_rows == 0:
            return (), True, False

        bounds: list[_RowGroupBounds] = []
        statistics_complete = True
        page_indexes_complete = True
        previous: Optional[_RowGroupBounds] = None
        for row_group in range(self._row_group_count):
            column = self._metadata.row_group(row_group).column(0)
            page_indexes_complete = page_indexes_complete and column.has_column_index and column.has_offset_index
            statistics = column.statistics
            if statistics is None or not statistics.has_min_max:
                statistics_complete = False
                continue
            minimum = statistics.min
            maximum = statistics.max
            if not isinstance(minimum, str) or not isinstance(maximum, str):
                raise _error(f"row group {row_group} key statistics must be valid UTF-8 strings")
            if statistics.null_count not in (0, None):
                raise _error(f"row group {row_group} reports null keys")
            if minimum > maximum:
                raise _error(f"row group {row_group} key statistic bounds are inverted")
            current = _RowGroupBounds(minimum, maximum)
            if previous is not None and previous.maximum > current.minimum:
                raise _error("row-group key statistic intervals are not globally ordered")
            bounds.append(current)
            previous = current

        if not statistics_complete:
            _LOG.warning("Virtual manifest key statistics are incomplete; exact lookup will scan row groups linearly.")
            return (), False, page_indexes_complete
        if len(bounds) != self._row_group_count:
            raise _error("row-group key statistics are structurally inconsistent")
        return tuple(bounds), True, page_indexes_complete

    def _ensure_open(self) -> None:
        with self._state_lock:
            if self._closed:
                raise _CatalogClosedError("PyArrowManifestCatalog is closed.")

    def _first_row_group(self, key: str) -> int:
        if not self._uses_row_group_statistics:
            return 0
        return bisect_left(self._maximum_keys, key)

    def _candidate_row_groups(self, key: str) -> range:
        if not self._bounds:
            return range(0)
        first = self._first_row_group(key)
        last = first
        while last < self._row_group_count and self._bounds[last].minimum <= key:
            if key <= self._bounds[last].maximum:
                last += 1
                continue
            last += 1
        return range(first, last)

    @staticmethod
    def _position(row: Mapping[str, Any]) -> tuple[str, int]:
        key = row.get("key")
        chunk_index = row.get("chunk_index")
        if not isinstance(key, str):
            raise _error("key must be a non-null string")
        if not isinstance(chunk_index, int) or isinstance(chunk_index, bool):
            raise _error("chunk_index must be a non-null integer")
        return key, chunk_index

    @staticmethod
    def _rows_from_batch(batch: Any) -> list[Mapping[str, Any]]:
        try:
            rows = batch.to_pylist()
        except ManifestValidationError:
            raise
        except _CatalogClosedError:
            raise
        except RuntimeError as exc:
            raise _error("unable to convert Parquet row batch") from exc
        except Exception as exc:
            raise _error("unable to convert Parquet row batch") from exc
        if not isinstance(rows, list):
            raise _error("Parquet row batch conversion did not return a list")
        return rows

    @staticmethod
    def _close_batches(batches: Iterator[Any]) -> None:
        close = getattr(batches, "close", None)
        if callable(close):
            close()

    def _rows_for_key(self, key: str) -> list[Mapping[str, Any]]:
        if self._uses_row_group_statistics:
            row_groups: Iterable[int] = self._candidate_row_groups(key)
        else:
            row_groups = self._linear_candidate_row_groups(key)

        matches: list[Mapping[str, Any]] = []
        previous: Optional[tuple[str, int]] = None
        for row_group in row_groups:
            self._ensure_open()
            for batch in self._iter_row_group_batches(row_group):
                for row in self._rows_from_batch(batch):
                    position = self._position(row)
                    if previous is not None and position <= previous:
                        raise _error("rows must be strictly sorted by key and chunk_index")
                    previous = position
                    if position[0] == key:
                        matches.append(row)
        return matches

    def _linear_candidate_row_groups(self, key: str) -> tuple[int, ...]:
        candidates: list[int] = []
        previous: Optional[tuple[str, int]] = None
        passed_target = False
        for row_group in range(self._row_group_count):
            self._ensure_open()
            contains_target = False
            batches = self._iter_projected_row_group_batches(row_group, ("key", "chunk_index"))
            try:
                for batch in batches:
                    for row in self._rows_from_batch(batch):
                        position = self._position(row)
                        if previous is not None and position <= previous:
                            raise _error("rows must be strictly sorted by key and chunk_index")
                        previous = position
                        if position[0] == key:
                            contains_target = True
                        elif position[0] > key:
                            passed_target = True
                            break
                    if passed_target:
                        break
            finally:
                self._close_batches(batches)
            if contains_target:
                candidates.append(row_group)
            if passed_target:
                break
        return tuple(candidates)

    def _iter_rows(
        self,
        first_row_group: int,
        lower_bound: str,
        prefix: str,
        end_at: Optional[str],
    ) -> Iterator[Mapping[str, Any]]:
        for row_group in range(first_row_group, self._row_group_count):
            self._ensure_open()
            batches = self._iter_row_group_batches(row_group)
            try:
                for batch in batches:
                    for row in self._rows_from_batch(batch):
                        key, _ = self._position(row)
                        if lower_bound and key < lower_bound:
                            continue
                        if end_at is not None and key > end_at:
                            return
                        if prefix and not key.startswith(prefix) and key > prefix:
                            return
                        yield row
            finally:
                self._close_batches(batches)

    def _iter_row_group_batches(self, row_group: int) -> Iterator[Any]:
        cached = self._cache.get(row_group)
        if cached is not None:
            yield from cached
            return

        while True:
            with self._flight_lock:
                cached = self._cache.get(row_group)
                if cached is not None:
                    flight = None
                    leader = False
                else:
                    flight = self._flights.get(row_group)
                    leader = flight is None
                    if leader:
                        flight = _LoadFlight(threading.Event())
                        self._flights[row_group] = flight
            if cached is not None:
                yield from cached
                return
            assert flight is not None
            if leader:
                break
            self._wait_for_flight(flight)
            if flight.error is not None:
                raise flight.error
            self._ensure_open()

        flight_finished = False

        def finish_flight(error: Optional[BaseException] = None) -> None:
            nonlocal flight_finished
            if flight_finished:
                return
            self._finish_flight(row_group, flight, error)
            flight_finished = True

        try:
            cached_after_wait: Optional[tuple[Any, ...]] = None
            stream_context: Optional[ContextManager[IO[bytes]]] = None
            ownership: Optional[_IteratorOwnedStream] = None
            unretained_batches: Optional[Iterator[Any]] = None
            initial_batches: tuple[Any, ...] = ()
            with self._loader_lock:
                self._ensure_open()
                cached_after_wait = self._cache.get(row_group)
                if cached_after_wait is None:
                    stream_context = self._stream_factory()
                    try:
                        stream = stream_context.__enter__()
                        parquet_file = self._pq.ParquetFile(
                            stream,
                            metadata=self._metadata,
                            pre_buffer=False,
                        )
                        batches = iter(
                            parquet_file.iter_batches(
                                batch_size=_BATCH_SIZE,
                                row_groups=[row_group],
                            )
                        )
                        if self._cache_budget == 0:
                            unretained_batches = batches
                        else:
                            retained: list[Any] = []
                            retained_bytes = 0
                            for batch in batches:
                                batch_bytes = batch.get_total_buffer_size()
                                if retained_bytes + batch_bytes <= self._cache_budget:
                                    retained.append(batch)
                                    retained_bytes += batch_bytes
                                    continue
                                initial_batches = (*retained, batch)
                                retained.clear()
                                unretained_batches = batches
                                break
                            else:
                                cached_after_wait = tuple(retained)
                                self._cache.put(row_group, cached_after_wait)
                                assert stream_context is not None
                                context_to_close = stream_context
                                stream_context = None
                                context_to_close.__exit__(None, None, None)

                        if unretained_batches is not None:
                            assert stream_context is not None
                            ownership = _IteratorOwnedStream(stream_context)
                            stream_context = None
                            if not self._register_iterator_stream(ownership):
                                ownership.close()
                                ownership = None
                                raise _CatalogClosedError("PyArrowManifestCatalog is closed.")
                    except BaseException:
                        if stream_context is not None:
                            stream_context.__exit__(*sys.exc_info())
                        raise

            if cached_after_wait is not None:
                finish_flight()
                yield from cached_after_wait
                return

            assert ownership is not None
            assert unretained_batches is not None
            finish_flight()
            try:
                for batch in initial_batches:
                    self._ensure_open()
                    yield batch
                while True:
                    with self._loader_lock:
                        self._ensure_open()
                        try:
                            batch = next(unretained_batches)
                        except StopIteration:
                            break
                    yield batch
            finally:
                with self._loader_lock:
                    self._release_iterator_stream(ownership, sys.exc_info())
        except GeneratorExit:
            finish_flight()
            raise
        except (ManifestValidationError, _CatalogClosedError) as exc:
            finish_flight(exc)
            raise
        except Exception as exc:
            error = _error(f"unable to load Parquet row group {row_group}")
            finish_flight(error)
            raise error from exc
        except BaseException as exc:
            finish_flight(exc)
            raise

    def _iter_projected_row_group_batches(self, row_group: int, columns: Sequence[str]) -> Iterator[Any]:
        cached = self._cache.get(row_group)
        if cached is not None:
            for batch in cached:
                yield batch.select(columns)
            return

        try:
            stream_context: Optional[ContextManager[IO[bytes]]] = None
            ownership: Optional[_IteratorOwnedStream] = None
            batches: Optional[Iterator[Any]] = None
            with self._loader_lock:
                self._ensure_open()
                cached = self._cache.get(row_group)
                if cached is None:
                    stream_context = self._stream_factory()
                    try:
                        stream = stream_context.__enter__()
                        parquet_file = self._pq.ParquetFile(
                            stream,
                            metadata=self._metadata,
                            pre_buffer=False,
                        )
                        batches = iter(
                            parquet_file.iter_batches(
                                batch_size=_BATCH_SIZE,
                                row_groups=[row_group],
                                columns=list(columns),
                            )
                        )
                        assert stream_context is not None
                        ownership = _IteratorOwnedStream(stream_context)
                        stream_context = None
                        if not self._register_iterator_stream(ownership):
                            ownership.close()
                            ownership = None
                            raise _CatalogClosedError("PyArrowManifestCatalog is closed.")
                    except BaseException:
                        if stream_context is not None:
                            stream_context.__exit__(*sys.exc_info())
                        raise

            if cached is not None:
                for batch in cached:
                    yield batch.select(columns)
                return

            assert ownership is not None
            assert batches is not None
            try:
                while True:
                    with self._loader_lock:
                        self._ensure_open()
                        try:
                            batch = next(batches)
                        except StopIteration:
                            break
                    yield batch
            finally:
                with self._loader_lock:
                    self._release_iterator_stream(ownership, sys.exc_info())
        except (ManifestValidationError, _CatalogClosedError):
            raise
        except Exception as exc:
            raise _error(f"unable to scan Parquet row group {row_group} key columns") from exc

    def _finish_flight(
        self,
        row_group: int,
        flight: _LoadFlight,
        error: Optional[BaseException] = None,
    ) -> None:
        with self._flight_lock:
            flight.error = error
            if self._flights.get(row_group) is flight:
                del self._flights[row_group]
            flight.event.set()

    @staticmethod
    def _wait_for_flight(flight: _LoadFlight) -> None:
        flight.event.wait()

    def _register_iterator_stream(self, ownership: _IteratorOwnedStream) -> bool:
        """Register an unretained stream unless close has already rejected new work."""
        with self._state_lock:
            if self._closed:
                return False
            self._iterator_streams.add(ownership)
            return True

    def _release_iterator_stream(
        self,
        ownership: _IteratorOwnedStream,
        exc_info: tuple[type[BaseException] | None, BaseException | None, TracebackType | None],
    ) -> None:
        """Unregister and close one iterator stream exactly once."""
        with self._state_lock:
            self._iterator_streams.discard(ownership)
        ownership.close(exc_info)

    def _close_iterator_streams(self) -> None:
        """Release every iterator-held stream after close blocks further loading."""
        with self._state_lock:
            iterator_streams = tuple(self._iterator_streams)
            self._iterator_streams.clear()
        for ownership in iterator_streams:
            ownership.close()

    def _close_for_new_work(self) -> None:
        with self._state_lock:
            self._closed = True
        error = _CatalogClosedError("PyArrowManifestCatalog is closed.")
        with self._flight_lock:
            flights = tuple(self._flights.values())
            self._flights.clear()
            for flight in flights:
                flight.error = error
                flight.event.set()


__all__ = [
    "DEFAULT_ROW_GROUP_CACHE_SIZE_BYTES",
    "MAX_ROW_GROUP_CACHE_SIZE_BYTES",
    "BinaryStreamFactory",
    "ManifestCatalog",
    "PyArrowManifestCatalog",
    "RowGroupCache",
]
