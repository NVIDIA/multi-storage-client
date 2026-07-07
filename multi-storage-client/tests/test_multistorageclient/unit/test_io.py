# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, cast

import pytest

import multistorageclient.contrib.async_fs as async_fs_module
import multistorageclient.providers.manifest as manifest_module
from multistorageclient._io import write_all


class _FixedReturnWriter:
    def __init__(self, result: object) -> None:
        self._result = result
        self.calls: list[bytes] = []

    def write(self, data: memoryview) -> object:
        self.calls.append(bytes(data))
        return self._result


@pytest.mark.parametrize(
    ("result", "message"),
    [
        pytest.param(None, "no progress", id="none"),
        pytest.param(0, "no progress", id="zero"),
        pytest.param(-1, "no progress", id="negative"),
        pytest.param(True, "no progress", id="bool"),
        pytest.param("1", "no progress", id="non-integer"),
        pytest.param(4, "more bytes", id="overlong"),
    ],
)
def test_write_all_rejects_invalid_binary_writer_results(result: object, message: str) -> None:
    """Every non-progress or overlong write result is rejected before bytes are silently lost."""
    writer = _FixedReturnWriter(result)

    with pytest.raises(OSError, match=message):
        write_all(cast(Any, writer), b"abc")

    assert writer.calls == [b"abc"]


def test_write_all_retries_positive_short_writes_with_memoryview_slices() -> None:
    """Positive short writes advance a cursor without copying each remaining suffix."""

    class ShortWriter:
        def __init__(self) -> None:
            self.calls: list[bytes] = []

        def write(self, data: memoryview) -> int:
            assert isinstance(data, memoryview)
            count = min(2, len(data))
            self.calls.append(bytes(data[:count]))
            return count

    writer = ShortWriter()

    assert write_all(cast(Any, writer), b"abcdef") == 6
    assert writer.calls == [b"ab", b"cd", b"ef"]


def test_fsspec_and_manifest_downloads_share_the_internal_exact_writer() -> None:
    """Both download implementations use the one exact-write contract."""
    assert async_fs_module._write_all is write_all
    assert manifest_module._write_all is write_all
