# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import threading
import time

from multistorageclient.sync.progress_bar import ProgressBar


def test_progress_bar_capped_percentage():
    progress = ProgressBar(desc="Syncing", show_progress=True)
    assert progress.pbar is not None
    progress.update_total(100_000)
    progress.update_progress(99_999)
    assert "99.9%" in str(progress.pbar)


def test_refresh_updates_display_without_changing_counters():
    progress = ProgressBar(desc="Syncing", show_progress=True, total_items=0)
    assert progress.pbar is not None

    refresh_count = {"count": 0}
    original_refresh = progress.pbar.refresh

    def counting_refresh(*args, **kwargs):
        refresh_count["count"] += 1
        return original_refresh(*args, **kwargs)

    progress.pbar.refresh = counting_refresh
    progress.refresh()

    assert progress.pbar.total == 0
    assert progress.pbar.n == 0
    assert refresh_count["count"] == 1

    progress.close()


def test_progress_update_interval():
    progress = ProgressBar(desc="Syncing", show_progress=True, total_items=2000)
    assert progress.pbar is not None

    # Track refresh calls by wrapping the original refresh method
    refresh_count = {"count": 0}
    original_refresh = progress.pbar.refresh

    def counting_refresh(*args, **kwargs):
        refresh_count["count"] += 1
        return original_refresh(*args, **kwargs)

    progress.pbar.refresh = counting_refresh

    # Simulate 500 updates per second for 2000 items over ~2.5 seconds
    # We'll do rapid updates without sleep to simulate high-frequency updates,
    # then add sleeps at key points to verify throttling
    start_time = time.time()
    total_items = 2000

    # Phase 1: Rapid updates without delay (should trigger only 1 refresh)
    for i in range(1, 501):
        progress.update_progress(1)

    refresh_after_phase1 = refresh_count["count"]

    # Phase 2: Wait 1.1 seconds and do more rapid updates (should trigger 1 more refresh)
    time.sleep(1.1)
    for i in range(501, 1001):
        progress.update_progress(1)

    refresh_after_phase2 = refresh_count["count"]

    # Phase 3: Wait another 1.1 seconds and complete remaining items
    time.sleep(1.1)
    for i in range(1001, total_items + 1):
        progress.update_progress(1)

    elapsed_time = time.time() - start_time

    # Verify the internal counter reached the total
    assert progress.pbar.n == total_items, f"Expected {total_items} items, got {progress.pbar.n}"

    # Verify throttling behavior:
    # - Phase 1: Multiple rapid updates should result in at most 1-2 refreshes
    assert refresh_after_phase1 <= 2, f"Too many refreshes in Phase 1: {refresh_after_phase1} for 500 rapid updates"

    # - Phase 2: After 1+ second wait, should have 1-2 more refreshes
    assert refresh_after_phase2 <= refresh_after_phase1 + 2, (
        f"Too many refreshes in Phase 2: {refresh_after_phase2} total"
    )

    # - Overall: Should have much fewer refreshes than total updates
    assert refresh_count["count"] < total_items / 100, (
        f"Refresh not throttled properly: {refresh_count['count']} refreshes for {total_items} updates"
    )

    # Verify elapsed time is reasonable (~2.2 seconds, allowing some overhead)
    assert 2.0 <= elapsed_time <= 3.0, f"Test timing incorrect: {elapsed_time:.2f}s elapsed"

    progress.close()


def test_progress_bar_thread_safety():
    progress = ProgressBar(desc="Syncing", show_progress=True, total_items=10000)
    assert progress.pbar is not None

    # Track refresh calls
    refresh_count = {"count": 0}
    original_refresh = progress.pbar.refresh

    def counting_refresh(*args, **kwargs):
        refresh_count["count"] += 1
        return original_refresh(*args, **kwargs)

    progress.pbar.refresh = counting_refresh

    errors = []

    def producer_thread():
        try:
            for i in range(100):
                progress.update_total(1000 + i)
                time.sleep(0.01)
        except Exception as e:
            errors.append(("producer", e))

    def consumer_thread():
        try:
            for i in range(500):
                progress.update_progress(1)
                time.sleep(0.001)
        except Exception as e:
            errors.append(("consumer", e))

    # Start multiple threads that concurrently update the progress bar
    threads = []
    threads.append(threading.Thread(target=producer_thread))
    threads.append(threading.Thread(target=consumer_thread))

    start_time = time.time()
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    elapsed_time = time.time() - start_time

    # Verify no exceptions occurred
    assert len(errors) == 0, f"Thread safety errors: {errors}"

    # Verify the progress bar was updated correctly (500 total updates from consumer)
    assert progress.pbar.n == 500, f"Expected 500 updates, got {progress.pbar.n}"

    # Verify refresh was throttled despite concurrent access
    # With 2 threads running concurrently for ~1 second, both update_total and update_progress
    # can each trigger refreshes. We expect roughly 1-2 refreshes per second per method,
    # so around 2-10 refreshes is reasonable (vs 600 unthrottled updates)
    assert refresh_count["count"] < 5, (
        f"Refresh not properly throttled with concurrent access: {refresh_count['count']} refreshes for {elapsed_time:.2f}s"
    )

    # Verify throttling is effective: should be significantly less than total updates
    assert refresh_count["count"] < 600 / 10, (
        f"Throttling not effective enough: {refresh_count['count']} refreshes for 600 total updates"
    )

    progress.close()
