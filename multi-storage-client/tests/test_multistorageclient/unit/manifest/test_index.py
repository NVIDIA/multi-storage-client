# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Lazy bounded lookup tests for virtual-manifest Parquet catalogs."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from io import BytesIO
from types import SimpleNamespace
from typing import Any, Iterator, Sequence

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import multistorageclient.manifest.index as index_module
from multistorageclient.manifest import ManifestValidationError
from multistorageclient.manifest.index import PyArrowManifestCatalog, RowGroupCache
from multistorageclient.manifest.models import ObjectChunk
from multistorageclient.manifest.schema import virtual_manifest_v2_schema

from .helpers import object_row, source_bindings, write_manifest


class _StreamFactory:
    def __init__(
        self,
        payload: bytes,
        *,
        block_open: int | None = None,
        fail_open: int | None = None,
    ) -> None:
        self._payload = payload
        self._lock = threading.Lock()
        self._block_open = block_open
        self._fail_open = fail_open
        self.opens = 0
        self.active_streams = 0
        self.closed_streams = 0
        self.load_started = threading.Event()
        self.release_load = threading.Event()

    @contextmanager
    def __call__(self) -> Iterator[BytesIO]:
        with self._lock:
            self.opens += 1
            open_number = self.opens
        if open_number == self._block_open:
            self.load_started.set()
            assert self.release_load.wait(timeout=5), "blocked manifest load was never released"
        if open_number == self._fail_open:
            raise OSError("synthetic manifest load failure")
        stream = BytesIO(self._payload)
        with self._lock:
            self.active_streams += 1
        try:
            yield stream
        finally:
            stream.close()
            with self._lock:
                self.active_streams -= 1
                self.closed_streams += 1


class _RuntimeErrorParquetFile:
    def iter_batches(self, **_kwargs: Any) -> Iterator[Any]:
        raise RuntimeError("synthetic Parquet loader failure")


def _catalog(
    rows: list[dict[str, object]],
    *,
    row_group_size: int = 1,
    write_statistics: bool | list[str] = True,
    write_page_index: bool = False,
    cache_size: int = 64 * 1024 * 1024,
    block_open: int | None = None,
    fail_open: int | None = None,
) -> tuple[PyArrowManifestCatalog, _StreamFactory]:
    payload = write_manifest(
        rows,
        row_group_size=row_group_size,
        write_statistics=write_statistics,
        write_page_index=write_page_index,
        max_rows_per_page=1 if write_page_index else None,
    ).getvalue()
    factory = _StreamFactory(payload, block_open=block_open, fail_open=fail_open)
    return PyArrowManifestCatalog(factory, source_bindings(), {}, cache_size), factory


def _close_iterator(iterator: Iterator[Any]) -> None:
    close = getattr(iterator, "close", None)
    if callable(close):
        close()


def test_row_group_cache_evicts_whole_entries_in_lru_order() -> None:
    cache = RowGroupCache[int](3, lambda values: sum(values))

    assert cache.put(0, [1])
    assert cache.put(1, [1])
    assert cache.get(0) == (1,)
    assert cache.put(2, [2])

    assert cache.get(0) == (1,)
    assert cache.get(1) is None
    assert cache.get(2) == (2,)
    assert cache.size_bytes == 3


def test_row_group_cache_zero_budget_and_oversized_entries_are_never_retained() -> None:
    disabled = RowGroupCache[int](0, lambda values: sum(values))
    bounded = RowGroupCache[int](2, lambda values: sum(values))

    assert not disabled.put(0, [])
    assert not disabled.put(1, [0])
    assert disabled.size_bytes == 0
    assert not bounded.put(0, [3])
    assert bounded.get(0) is None


def test_row_group_cache_oversized_replacement_preserves_the_previous_entry() -> None:
    cache = RowGroupCache[int](2, lambda values: sum(values))
    assert cache.put(0, [1])

    assert not cache.put(0, [3])

    assert cache.get(0) == (1,)
    assert cache.size_bytes == 1


def test_row_group_cache_exact_fit_replacement_tuple_canonicalization_and_clear() -> None:
    weighed: list[tuple[int, ...]] = []

    def weight(values: tuple[int, ...]) -> int:
        weighed.append(values)
        return sum(values)

    cache = RowGroupCache[int](3, weight)
    mutable = [1, 2]

    assert cache.put(0, mutable)
    mutable.append(100)
    assert cache.get(0) is weighed[0]
    assert weighed[0] == (1, 2)
    assert cache.put(0, [1])
    assert cache.size_bytes == 1

    cache.clear()

    assert cache.size_bytes == 0
    assert cache.get(0) is None


@pytest.mark.parametrize("invalid_budget", [-1, 1 << 63, True, 1.5])
def test_row_group_cache_rejects_invalid_budgets(invalid_budget: object) -> None:
    with pytest.raises(ValueError, match="max_size_bytes"):
        RowGroupCache(invalid_budget, lambda _values: 0)  # type: ignore[arg-type]


@pytest.mark.parametrize("invalid_weight", [-1, 1.5, True])
def test_row_group_cache_rejects_invalid_weights(invalid_weight: object) -> None:
    cache = RowGroupCache[object](10, lambda _values: invalid_weight)  # type: ignore[arg-type,return-value]

    with pytest.raises(ValueError, match="weight"):
        cache.put(0, [object()])


def test_catalog_detects_complete_row_group_statistics_and_page_indexes() -> None:
    catalog, factory = _catalog(
        [
            object_row(key="a", source_path="a.bin"),
            object_row(key="b", source_path="b.bin"),
        ],
        write_page_index=True,
    )

    assert catalog.uses_row_group_statistics
    assert catalog.has_key_page_index
    assert factory.opens == 1


def test_catalog_missing_key_statistics_selects_the_linear_fallback(caplog: pytest.LogCaptureFixture) -> None:
    catalog, _ = _catalog(
        [
            object_row(key="a", source_path="a.bin"),
            object_row(key="b", source_path="b.bin"),
        ],
        write_statistics=False,
    )

    assert not catalog.uses_row_group_statistics
    assert "scan row groups linearly" in caplog.text
    assert catalog.get_file("b").key == "b"


def test_complete_statistics_prune_exact_lookup_to_one_candidate_row_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _ = _catalog(
        [
            object_row(key="a", source_path="a.bin"),
            object_row(key="b", source_path="b.bin"),
            object_row(key="c", source_path="c.bin"),
        ]
    )
    loaded: list[int] = []
    original = catalog._iter_row_group_batches

    def track(row_group: int):
        loaded.append(row_group)
        yield from original(row_group)

    monkeypatch.setattr(catalog, "_iter_row_group_batches", track)

    assert catalog.get_file("b").key == "b"
    assert loaded == [1]


def test_missing_statistics_scan_until_the_first_key_greater_than_the_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _ = _catalog(
        [
            object_row(key="a", source_path="a.bin"),
            object_row(key="b", source_path="b.bin"),
            object_row(key="c", source_path="c.bin"),
            object_row(key="d", source_path="d.bin"),
        ],
        write_statistics=False,
    )
    projected: list[int] = []
    loaded: list[int] = []
    original_projected = catalog._iter_projected_row_group_batches
    original_loaded = catalog._iter_row_group_batches

    def track_projected(row_group: int, columns: Sequence[str]):
        projected.append(row_group)
        yield from original_projected(row_group, columns)

    def track_loaded(row_group: int):
        loaded.append(row_group)
        yield from original_loaded(row_group)

    monkeypatch.setattr(catalog, "_iter_projected_row_group_batches", track_projected)
    monkeypatch.setattr(catalog, "_iter_row_group_batches", track_loaded)

    assert catalog.get_file("b").key == "b"
    assert projected == [0, 1, 2]
    assert loaded == [1]


def test_missing_statistics_stops_and_closes_a_single_row_group_after_the_first_greater_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The linear fallback releases an uncached projected stream as soon as a key exceeds the target."""
    monkeypatch.setattr(index_module, "_BATCH_SIZE", 1)
    catalog, factory = _catalog(
        [
            object_row(key="a", source_path="a.bin"),
            object_row(key="b", source_path="b.bin"),
            object_row(key="c", source_path="c.bin"),
            object_row(key="d", source_path="d.bin"),
        ],
        row_group_size=4,
        write_statistics=False,
        cache_size=0,
    )
    consumed: list[str] = []
    closed = threading.Event()
    original = catalog._iter_projected_row_group_batches

    def track(row_group: int, columns: Sequence[str]) -> Iterator[Any]:
        batches = original(row_group, columns)
        try:
            for batch in batches:
                consumed.extend(row["key"] for row in batch.to_pylist())
                yield batch
        finally:
            _close_iterator(batches)
            closed.set()

    monkeypatch.setattr(catalog, "_iter_projected_row_group_batches", track)

    assert catalog.get_file("b").key == "b"
    assert consumed == ["a", "b", "c"]
    assert closed.is_set()
    assert factory.active_streams == 0


@pytest.mark.parametrize("write_statistics", [["size_bytes"], ["chunk_index"]])
def test_catalog_requires_statistics_for_the_key_column(write_statistics: list[str]) -> None:
    catalog, _ = _catalog(
        [object_row(key="a", source_path="a.bin")],
        write_statistics=write_statistics,
    )

    assert not catalog.uses_row_group_statistics


@pytest.mark.parametrize(
    ("write_statistics", "write_page_index", "expected"),
    [
        (True, False, False),
        (False, True, False),
        (True, True, True),
    ],
)
def test_catalog_requires_both_key_page_indexes(
    write_statistics: bool,
    write_page_index: bool,
    expected: bool,
) -> None:
    catalog, _ = _catalog(
        [object_row(key="a", source_path="a.bin")],
        write_statistics=write_statistics,
        write_page_index=write_page_index,
    )

    assert catalog.has_key_page_index is expected


def test_catalog_requires_statistics_and_page_indexes_on_every_nonempty_row_group() -> None:
    complete = SimpleNamespace(
        statistics=SimpleNamespace(has_min_max=True, min="a", max="a", null_count=0),
        has_column_index=True,
        has_offset_index=True,
    )
    missing_statistics = SimpleNamespace(
        statistics=None,
        has_column_index=True,
        has_offset_index=True,
    )
    missing_column_index = SimpleNamespace(
        statistics=SimpleNamespace(has_min_max=True, min="b", max="b", null_count=0),
        has_column_index=False,
        has_offset_index=True,
    )
    missing_offset_index = SimpleNamespace(
        statistics=SimpleNamespace(has_min_max=True, min="b", max="b", null_count=0),
        has_column_index=True,
        has_offset_index=False,
    )

    def metadata(columns: list[object]) -> SimpleNamespace:
        row_groups = [SimpleNamespace(column=lambda _index, column=column: column) for column in columns]
        return SimpleNamespace(
            num_rows=len(row_groups),
            row_group=lambda index: row_groups[index],
        )

    without_later_statistics = object.__new__(PyArrowManifestCatalog)
    without_later_statistics._metadata = metadata([complete, missing_statistics])
    without_later_statistics._row_group_count = 2
    bounds, statistics_complete, page_indexes_complete = without_later_statistics._inspect_row_groups()

    assert bounds == ()
    assert not statistics_complete
    assert page_indexes_complete

    without_later_page_index = object.__new__(PyArrowManifestCatalog)
    without_later_page_index._metadata = metadata([complete, missing_column_index])
    without_later_page_index._row_group_count = 2
    bounds, statistics_complete, page_indexes_complete = without_later_page_index._inspect_row_groups()

    assert len(bounds) == 2
    assert statistics_complete
    assert not page_indexes_complete

    without_later_offset_index = object.__new__(PyArrowManifestCatalog)
    without_later_offset_index._metadata = metadata([complete, missing_offset_index])
    without_later_offset_index._row_group_count = 2
    bounds, statistics_complete, page_indexes_complete = without_later_offset_index._inspect_row_groups()

    assert len(bounds) == 2
    assert statistics_complete
    assert not page_indexes_complete


def test_catalog_rejects_descending_row_group_statistic_intervals_at_construction() -> None:
    with pytest.raises(ManifestValidationError, match="statistics|bounds|order"):
        _catalog(
            [
                object_row(key="b", source_path="b.bin"),
                object_row(key="a", source_path="a.bin"),
            ]
        )


def test_catalog_defers_row_validation_and_never_reads_a_source_before_validation() -> None:
    catalog, _ = _catalog(
        [
            object_row(key="good", source_path="good.bin"),
            object_row(key="invalid", source_path="invalid.bin", source_offset=-1),
        ],
        row_group_size=2,
    )

    assert catalog.get_file("good").key == "good"
    with pytest.raises(ManifestValidationError, match="source offset"):
        catalog.get_file("invalid")


def test_catalog_assembles_one_file_spanning_equal_boundary_row_groups() -> None:
    catalog, _ = _catalog(
        [
            object_row(key="joined", size_bytes=6, chunk_index=0, source_path="head.bin"),
            object_row(key="joined", size_bytes=6, chunk_index=1, source_path="tail.bin"),
            object_row(key="later", source_path="later.bin"),
        ]
    )

    file = catalog.get_file("joined")

    assert file.cumulative_ends == (3, 6)
    assert file.chunks == (
        ObjectChunk(0, 3, "source", "head.bin", 7),
        ObjectChunk(1, 3, "source", "tail.bin", 7),
    )


def test_catalog_iteration_yields_valid_files_before_a_later_lazy_error() -> None:
    catalog, _ = _catalog(
        [
            object_row(key="a", source_path="a.bin"),
            object_row(key="b", source_path="b.bin", source_offset=-1),
        ]
    )
    files = catalog.iter_files()

    assert next(files).key == "a"
    with pytest.raises(ManifestValidationError, match="source offset"):
        next(files)


def test_catalog_linear_iteration_discovers_payload_ordering_corruption() -> None:
    catalog, _ = _catalog(
        [
            object_row(key="a", source_path="a.bin"),
            object_row(key="b", source_path="b.bin"),
            object_row(key="d", source_path="d.bin"),
            object_row(key="c", source_path="c.bin"),
        ],
        row_group_size=2,
    )

    assert catalog.get_file("a").key == "a"
    with pytest.raises(ManifestValidationError, match="sorted"):
        list(catalog.iter_files())


def test_catalog_page_index_diagnostics_do_not_change_lookup_results_or_stream_count() -> None:
    rows = [
        object_row(key="a", source_path="a.bin"),
        object_row(key="b", source_path="b.bin"),
    ]
    without_indexes, without_factory = _catalog(rows, write_page_index=False)
    with_indexes, with_factory = _catalog(rows, write_page_index=True)

    assert without_indexes.get_file("b") == with_indexes.get_file("b")
    assert without_factory.opens == with_factory.opens == 2


@pytest.mark.parametrize("cache_size", [0, 1])
def test_catalog_disabled_or_oversized_cache_bypasses_retention(cache_size: int) -> None:
    catalog, factory = _catalog([object_row(key="a", source_path="a.bin")], cache_size=cache_size)

    assert catalog.get_file("a").key == "a"
    assert catalog.get_file("a").key == "a"

    assert catalog.cached_bytes == 0
    assert factory.opens == 3


def test_catalog_reuses_a_retained_row_group_for_repeated_locality() -> None:
    catalog, factory = _catalog([object_row(key="a", source_path="a.bin")])

    assert catalog.get_file("a").key == "a"
    retained = catalog.cached_bytes
    assert catalog.get_file("a").key == "a"

    assert retained > 0
    assert catalog.cached_bytes == retained
    assert factory.opens == 2


def test_catalog_accounts_real_arrow_buffers_and_evicts_whole_lru_row_groups() -> None:
    rows = [
        object_row(key="a", source_path="a.bin"),
        object_row(key="b", source_path="b.bin"),
    ]
    payload = write_manifest(rows, row_group_size=1).getvalue()
    parquet_file = pq.ParquetFile(BytesIO(payload), pre_buffer=False)
    row_group_sizes = [
        sum(batch.get_total_buffer_size() for batch in parquet_file.iter_batches(row_groups=[row_group]))
        for row_group in range(2)
    ]
    budget = max(row_group_sizes)
    factory = _StreamFactory(payload)
    catalog = PyArrowManifestCatalog(factory, source_bindings(), {}, budget)

    assert catalog.get_file("a").key == "a"
    assert catalog.cached_bytes == row_group_sizes[0]
    assert catalog.get_file("b").key == "b"
    assert catalog.cached_bytes == row_group_sizes[1]
    assert catalog.cached_bytes <= budget
    opens_after_b = factory.opens

    assert catalog.get_file("a").key == "a"
    assert factory.opens == opens_after_b + 1
    assert catalog.cached_bytes == row_group_sizes[0]


def test_catalog_allows_an_exact_file_to_share_a_segment_aware_directory_prefix() -> None:
    exact_timestamp = datetime(2026, 1, 3, tzinfo=timezone.utc)
    child_timestamp = exact_timestamp - timedelta(days=1)
    catalog, _ = _catalog(
        [
            object_row(key="a", source_path="a.bin", last_modified=exact_timestamp),
            object_row(key="a-b", source_path="a-b.bin", last_modified=exact_timestamp),
            object_row(key="a/b", source_path="a-b-child.bin", last_modified=child_timestamp),
        ]
    )

    assert catalog.get_file("a").key == "a"
    assert [file.key for file in catalog.iter_files(prefix="a/")] == ["a/b"]
    assert catalog.directory_last_modified("a") == child_timestamp


def test_catalog_empty_single_row_group_directory_probes_report_missing() -> None:
    """A standard PyArrow empty table may still have one physical row group."""
    catalog, factory = _catalog([])

    assert catalog._row_group_count == 1
    assert catalog.uses_row_group_statistics
    assert catalog.directory_last_modified("missing") is None
    assert not catalog.may_contain_descendants("missing")
    assert not catalog.may_contain_descendants("")
    assert factory.opens == 1


def test_catalog_cache_is_shared_by_concurrent_reads_of_one_row_group(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog, factory = _catalog(
        [
            object_row(key="a", source_path="a.bin"),
            object_row(key="b", source_path="b.bin"),
        ],
        row_group_size=2,
        block_open=2,
    )
    follower_waiting = threading.Event()
    original_wait = catalog._wait_for_flight

    def wait_for_flight(flight) -> None:
        follower_waiting.set()
        original_wait(flight)

    monkeypatch.setattr(catalog, "_wait_for_flight", wait_for_flight)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(catalog.get_file, "a")
        assert factory.load_started.wait(timeout=5)
        second = executor.submit(catalog.get_file, "b")
        assert follower_waiting.wait(timeout=5)
        assert factory.opens == 2
        factory.release_load.set()
        futures = [first, second]
        assert [future.result(timeout=5).key for future in futures] == ["a", "b"]

    assert factory.opens == 2
    assert catalog.cached_bytes > 0


def test_catalog_failed_single_flight_wakes_waiters_and_allows_a_clean_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, factory = _catalog(
        [object_row(key="a", source_path="a.bin")],
        block_open=2,
        fail_open=2,
    )
    follower_waiting = threading.Event()
    original_wait = catalog._wait_for_flight

    def wait_for_flight(flight) -> None:
        follower_waiting.set()
        original_wait(flight)

    monkeypatch.setattr(catalog, "_wait_for_flight", wait_for_flight)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(catalog.get_file, "a")
        assert factory.load_started.wait(timeout=5)
        second = executor.submit(catalog.get_file, "a")
        assert follower_waiting.wait(timeout=5)
        factory.release_load.set()

        for future in (first, second):
            with pytest.raises(ManifestValidationError, match="decode|load|Parquet"):
                future.result(timeout=5)

    assert factory.opens == 2
    assert catalog.cached_bytes == 0
    assert catalog.get_file("a").key == "a"
    assert factory.opens == 3


def test_catalog_close_is_idempotent_and_rejects_new_and_resumed_work() -> None:
    catalog, factory = _catalog(
        [
            object_row(key="a", source_path="a.bin"),
            object_row(key="b", source_path="b.bin"),
        ]
    )
    files = catalog.iter_files()
    assert next(files).key == "a"

    catalog.close()
    catalog.close()
    opens_after_close = factory.opens

    assert catalog.cached_bytes == 0
    with pytest.raises(RuntimeError, match="closed"):
        catalog.get_file("a")
    with pytest.raises(RuntimeError, match="closed"):
        next(files)
    assert factory.opens == opens_after_close


def test_catalog_iterator_created_before_close_fails_without_opening_a_stream() -> None:
    catalog, factory = _catalog([object_row(key="a", source_path="a.bin")])
    files = catalog.iter_files()
    catalog.close()
    opens_after_close = factory.opens

    with pytest.raises(RuntimeError, match="closed"):
        next(files)

    assert factory.opens == opens_after_close


def test_catalog_directory_last_modified_rejects_closed_statistics_pruning() -> None:
    catalog, _ = _catalog([object_row(key="a", source_path="a.bin")])
    catalog.close()

    with pytest.raises(RuntimeError, match="closed"):
        catalog.directory_last_modified("missing")


def test_catalog_close_waits_for_an_active_load_to_finish(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog, factory = _catalog(
        [object_row(key="a", source_path="a.bin")],
        block_open=2,
    )
    close_started = threading.Event()
    close_finished = threading.Event()
    original_close_for_new_work = catalog._close_for_new_work

    def close_for_new_work() -> None:
        original_close_for_new_work()
        close_started.set()

    monkeypatch.setattr(catalog, "_close_for_new_work", close_for_new_work)

    with ThreadPoolExecutor(max_workers=2) as executor:
        read_future = executor.submit(catalog.get_file, "a")
        assert factory.load_started.wait(timeout=5)

        def close() -> None:
            catalog.close()
            close_finished.set()

        close_future = executor.submit(close)
        assert close_started.wait(timeout=5)
        assert not close_finished.wait(timeout=0.1)
        factory.release_load.set()

        assert read_future.result(timeout=5).key == "a"
        close_future.result(timeout=5)

    assert close_finished.is_set()
    assert catalog.cached_bytes == 0


def _catalog_with_out_of_range_timestamp() -> PyArrowManifestCatalog:
    schema = virtual_manifest_v2_schema()
    table = pa.Table.from_pylist([object_row(key="invalid-time", source_path="invalid-time.bin")], schema=schema)
    timestamp_index = table.schema.get_field_index("last_modified")
    timestamp_field = table.schema.field(timestamp_index)
    table = table.set_column(
        timestamp_index,
        timestamp_field,
        pa.array([253402300800000000], type=timestamp_field.type),
    )
    payload = BytesIO()
    pq.write_table(table, payload)
    return PyArrowManifestCatalog(_StreamFactory(payload.getvalue()), source_bindings(), {})


def test_catalog_exact_lookup_wraps_arrow_timestamp_conversion_errors() -> None:
    catalog = _catalog_with_out_of_range_timestamp()

    with pytest.raises(ManifestValidationError, match="convert Parquet row batch"):
        catalog.get_file("invalid-time")


def test_catalog_iteration_wraps_arrow_timestamp_conversion_errors() -> None:
    catalog = _catalog_with_out_of_range_timestamp()

    with pytest.raises(ManifestValidationError, match="convert Parquet row batch"):
        list(catalog.iter_files())


def test_catalog_wraps_full_row_group_loader_runtime_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog, _ = _catalog([object_row(key="a", source_path="a.bin")])
    monkeypatch.setattr(catalog._pq, "ParquetFile", lambda *_args, **_kwargs: _RuntimeErrorParquetFile())

    with pytest.raises(ManifestValidationError, match="unable to load Parquet row group 0"):
        catalog.get_file("a")


def test_catalog_wraps_projected_row_group_loader_runtime_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog, _ = _catalog([object_row(key="a", source_path="a.bin")], write_statistics=False)
    monkeypatch.setattr(catalog._pq, "ParquetFile", lambda *_args, **_kwargs: _RuntimeErrorParquetFile())

    with pytest.raises(ManifestValidationError, match="unable to scan Parquet row group 0 key columns"):
        catalog.get_file("a")


def test_catalog_wraps_full_row_group_decode_runtime_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog, _ = _catalog([object_row(key="a", source_path="a.bin")])

    def fail_decode(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("synthetic manifest decoder failure")

    monkeypatch.setattr(index_module, "decode_virtual_manifest_file", fail_decode)

    with pytest.raises(ManifestValidationError, match="unable to decode virtual manifest file"):
        catalog.get_file("a")


@pytest.mark.parametrize("cache_size", [0, 1], ids=["zero-budget", "oversized"])
def test_catalog_paused_uncached_iterator_releases_same_row_group_and_closes_its_stream(
    cache_size: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(index_module, "_BATCH_SIZE", 1)
    catalog, factory = _catalog(
        [
            object_row(key="a", source_path="a.bin"),
            object_row(key="b", source_path="b.bin"),
        ],
        row_group_size=2,
        cache_size=cache_size,
    )
    files = catalog.iter_files()

    assert next(files).key == "a"
    assert catalog.cached_bytes == 0
    assert factory.active_streams == 1

    with ThreadPoolExecutor(max_workers=1) as executor:
        lookup = executor.submit(catalog.get_file, "b")
        try:
            assert lookup.result(timeout=1).key == "b"
        except BaseException:
            _close_iterator(files)
            raise

    assert factory.opens == 3
    assert factory.active_streams == 1

    catalog.close()

    assert catalog.cached_bytes == 0
    assert factory.active_streams == 0
    with pytest.raises(RuntimeError, match="closed"):
        next(files)
    _close_iterator(files)
    assert factory.active_streams == 0


def test_catalog_end_bound_does_not_drain_remaining_uncached_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(index_module, "_BATCH_SIZE", 1)
    catalog, _ = _catalog(
        [
            object_row(key="a", source_path="a.bin"),
            object_row(key="b", source_path="b.bin"),
            object_row(key="c", source_path="c.bin"),
            object_row(key="d", source_path="d.bin"),
        ],
        row_group_size=4,
        cache_size=0,
    )
    loaded: list[Any] = []
    original = catalog._iter_row_group_batches

    def track(row_group: int) -> Iterator[Any]:
        batches = original(row_group)
        try:
            for batch in batches:
                loaded.append(batch)
                yield batch
        finally:
            _close_iterator(batches)

    monkeypatch.setattr(catalog, "_iter_row_group_batches", track)

    assert [file.key for file in catalog.iter_files(end_at="a")] == ["a"]
    assert len(loaded) == 2
