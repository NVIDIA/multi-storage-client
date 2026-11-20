# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import functools
from collections.abc import Callable
from typing import Any, TypeVar

from multistorageclient_rust import RustClientError, RustRetryableError

from ..types import StreamingObjectWriter

_T = TypeVar("_T")


class RustStreamingWriter(StreamingObjectWriter):
    """
    :py:class:`StreamingObjectWriter` over a native ``multistorageclient_rust`` streaming writer.

    Rust owns the part buffers, uploads and backpressure. Providers supply their own error translation and metrics.
    """

    def __init__(
        self,
        native: Any,
        translate_errors: Callable[[Callable[[], Any]], Any],
        emit_write_metrics: Callable[[Callable[[], int]], int],
    ):
        """
        :param native: The native writer returned by ``RustClient.open_streaming_writer``.
        :param translate_errors: Runs a callable and maps its errors to the provider's exceptions.
        :param emit_write_metrics: Runs a callable inside the provider's write metrics.
        """
        self._native = native
        self._translate_errors = translate_errors
        self._emit_write_metrics = emit_write_metrics
        # Non-contiguous input is copied in pieces of half a part: ``tobytes()`` can briefly hold about two copies,
        # so conversion adds at most one part.
        self._conversion_bytes = max(int(native.stats()["part_size"]) // 2, 1)

    def _call(self, f: Callable[[], _T]) -> _T:
        try:
            return f()
        except (RustClientError, RustRetryableError) as error:

            def _reraise(caught: Exception = error) -> _T:
                raise caught

            try:
                return self._translate_errors(_reraise)
            except PermissionError:
                raise
            except Exception as translated:
                # A failed stream stays failed, so retrying this file cannot succeed; don't surface RetryableError.
                raise RuntimeError(
                    f"Streaming upload failed; this file cannot be resumed. {translated}"
                ) from translated

    def write(self, data: Any) -> int:
        """Copy a bytes-like object into the stream, blocking while the in-flight part budget is full."""
        view = data if isinstance(data, memoryview) else memoryview(data)
        if view.c_contiguous:
            if view.format != "B" or view.ndim != 1:
                view = view.cast("B")
            return self._call(lambda: self._native.write(view))
        return self._write_non_contiguous(view)

    def _write_non_contiguous(self, view: memoryview) -> int:
        # The native writer needs contiguous bytes. memoryview slices only along the first axis, so the smallest
        # piece is one element (1-D) or one row; reject a buffer whose pieces cannot fit before accepting any bytes.
        count = (view.shape or (0,))[0]
        if not count:
            return 0
        piece_bytes = view.nbytes // count
        if piece_bytes > self._conversion_bytes:
            raise ValueError(
                f"Streaming writes copy a non-contiguous buffer in pieces of at most {self._conversion_bytes} bytes "
                f"(half a part), but its {'elements' if view.ndim == 1 else 'rows'} are {piece_bytes} bytes; "
                "pass a contiguous buffer."
            )
        step = self._conversion_bytes // max(piece_bytes, 1)
        for start in range(0, count, step):
            chunk = memoryview(view[start : start + step].tobytes())
            self._call(functools.partial(self._native.write, chunk))
            # Free this piece before the next is allocated.
            del chunk
        return view.nbytes

    def tell(self) -> int:
        """Return the number of bytes accepted so far; raises after the upload failed."""
        return self._native.tell()

    def flush(self) -> None:
        """Raise errors from parts that already finished, without waiting for running parts."""
        self._call(self._native.flush)

    def sync(self) -> None:
        """Wait for every submitted part. Does not publish the object."""
        self._call(self._native.sync)

    def close(self) -> int:
        """Upload the remainder and publish the object; the provider's ``WRITE`` metric covers this call."""

        def _close() -> int:
            self._call(self._native.close)
            return self._native.tell()

        return self._emit_write_metrics(_close)

    def abort(self) -> None:
        """Abort the multipart upload without publishing; raises if an earlier abort did not complete."""
        self._call(self._native.abort)
