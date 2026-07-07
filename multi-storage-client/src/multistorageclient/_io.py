# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Small internal binary I/O primitives shared by storage adapters."""

from typing import IO


def write_all(destination: IO[bytes], data: bytes) -> int:
    """Write ``data`` completely to a binary destination."""
    view = memoryview(data)
    offset = 0
    while offset < len(view):
        written = destination.write(view[offset:])
        if written is None or not isinstance(written, int) or isinstance(written, bool) or written <= 0:
            raise IOError("Binary destination made no progress while writing.")
        remaining = len(view) - offset
        if written > remaining:
            raise IOError("Binary destination reported writing more bytes than provided.")
        offset += written
    return offset
