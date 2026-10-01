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

"""
S3-over-RDMA data plane for the S3 storage provider, backed by NVIDIA cuObject.

cuObject (``libcuobjclient``) registers a contiguous host (or, in a future
revision, device) buffer for RDMA and mints an RDMA descriptor (token). The
descriptor is carried to an RDMA-capable S3 endpoint as the signed
``x-amz-rdma-token`` header; the endpoint then transfers the object payload
directly into or out of the registered buffer over RDMA, leaving the HTTP body
empty. This offloads the bulk transfer from the CPU and the HTTP/TLS path.

This mirrors the PyTorch cuObject checkpoint backend (``torch.cuda.cuobj`` plus
``torch.distributed.checkpoint._cuobj_rdma_storage``): a thin set of token-API
primitives (:func:`is_available`, :func:`register_buffer`,
:func:`deregister_buffer`, :func:`get_rdma_token`, :func:`put_rdma_token`) and a
:class:`CuObjEngine` control plane that formats the descriptor and carries it on
the boto3 request -- the MSC equivalent of ``BotoCuObjClient``.

cuObject is a C++ library whose client object requires an I/O-ops callback table
at construction, so it cannot be driven directly from ctypes. The token API is
instead reached through the ``multistorageclient_rust`` extension: an
``extern "C"`` shim over ``cuObjClient`` (``rust/csrc/cuobj_shim.cc``) bound by a
PyO3 module (``rust/src/cuobj.rs``) and compiled into the wheel by Maturin. The
shim is behind the crate's optional ``rdma`` feature, so it is present only when
the wheel is built with cuObject support::

    maturin develop --features rdma          # into an existing venv, or
    maturin build   --features rdma           # a distributable wheel

built on a host with the cuObject runtime (``libcufile``/``libcuobjclient``).

This module is import-safe when the extension was built without the ``rdma``
feature (the ``cuobj_*`` symbols are absent): the five primitives raise
:class:`CuObjError` and :func:`is_available` returns ``False``, exactly like the
``_dummy_fn`` fallback in ``torch/cuda/cuobj.py``. The S3 provider only
instantiates :class:`CuObjEngine` when the ``rdma`` option is configured.
"""

import ctypes
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager

# The cuObject token API lives in the multistorageclient_rust extension behind
# the crate's `rdma` feature. Import the compiled module defensively: a default
# (non-rdma) wheel omits the cuobj_* functions entirely, and a source checkout
# may not have the extension built at all.
try:
    from multistorageclient_rust import multistorageclient_rust as _rust_ext
except Exception:  # pragma: no cover - extension unbuilt
    _rust_ext = None

_HAS_CUOBJ = _rust_ext is not None and hasattr(_rust_ext, "cuobj_available")

# The boto3 ``before-sign`` hook runs per request, possibly on transfer-manager
# worker threads, so the token in flight for the current request is kept in
# thread-local state rather than on the client (mirrors the thread-local token
# in BotoCuObjClient).
_thread_state = threading.local()

X_AMZ_RDMA_TOKEN = "x-amz-rdma-token"
X_AMZ_RDMA_REPLY = "x-amz-rdma-reply"
X_AMZ_RDMA_BYTES_TRANSFERRED = "x-amz-rdma-bytes-transferred"

RDMA_REPLY_SUCCESS = 200
RDMA_REPLY_NO_CONTENT = 204
RDMA_REPLY_PARTIAL_CONTENT = 206
RDMA_REPLY_NOT_IMPLEMENTED = 501

# The x-amz-rdma-token descriptor carries the window size in a 32-bit field, so
# a single registered buffer cannot describe more than this many bytes.
RDMA_MAX_MEMORY_REG_SIZE = (1 << 32) - 1

_DIGITS = re.compile(r"[0-9]+")


class CuObjError(RuntimeError):
    """Raised when a cuObject token-API call fails."""


def _require_cuobj() -> None:
    if _rust_ext is None or not _HAS_CUOBJ:
        raise CuObjError(
            "cuObject support is not available: the multistorageclient_rust extension was built "
            "without the 'rdma' feature (or is not built). Rebuild the wheel with cuObject support "
            "on a host with the cuObject runtime, e.g. `maturin develop --features rdma`."
        )


def is_available() -> bool:
    """Return whether NVIDIA cuObject (S3-over-RDMA) support is usable.

    ``True`` only when the extension was built with the ``rdma`` feature and a
    cuObject client connection can be established (RDMA-capable NIC and a
    reachable RDMA S3 endpoint).
    """
    if not _HAS_CUOBJ:
        return False
    assert _rust_ext is not None
    try:
        return bool(_rust_ext.cuobj_available())
    except Exception:
        return False


def register_buffer(addr: int, size: int) -> None:
    """Register a contiguous buffer with cuObject for RDMA transfers.

    Registration is required before requesting an RDMA token for the buffer.
    """
    _require_cuobj()
    assert _rust_ext is not None
    try:
        _rust_ext.cuobj_register_buffer(addr, size)
    except Exception as error:
        raise CuObjError(f"cuMemObjGetDescriptor failed for buffer at 0x{addr:x} ({size} bytes)") from error


def deregister_buffer(addr: int) -> None:
    """Deregister a buffer previously passed to :func:`register_buffer`."""
    _require_cuobj()
    assert _rust_ext is not None
    try:
        _rust_ext.cuobj_deregister_buffer(addr)
    except Exception as error:
        raise CuObjError(f"cuMemObjPutDescriptor failed for buffer at 0x{addr:x}") from error


def get_rdma_token(addr: int, size: int, offset: int = 0, is_put: bool = True) -> str:
    """Return an RDMA descriptor for a region of a registered buffer.

    ``is_put`` is ``True`` for a PUT (the server reads from the buffer), ``False``
    for a GET (the server writes into it). Release the descriptor with
    :func:`put_rdma_token` once the request finishes.
    """
    _require_cuobj()
    assert _rust_ext is not None
    try:
        return _rust_ext.cuobj_get_rdma_token(addr, size, offset, is_put)
    except Exception as error:
        raise CuObjError(f"cuMemObjGetRDMAToken failed for buffer at 0x{addr:x} ({size} bytes)") from error


def put_rdma_token(token: str) -> None:
    """Release an RDMA descriptor returned by :func:`get_rdma_token`."""
    _require_cuobj()
    assert _rust_ext is not None
    try:
        _rust_ext.cuobj_put_rdma_token(token)
    except Exception as error:
        raise CuObjError("cuMemObjPutRDMAToken failed") from error


def parse_rdma_reply(reply: str | None) -> int | None:
    """Parse an ``x-amz-rdma-reply`` header value.

    Returns ``RDMA_REPLY_NOT_IMPLEMENTED`` when the endpoint declined RDMA (an
    explicit ``501`` or an absent/empty header, which a non-RDMA endpoint never
    sets), the HTTP-style status code (100-599) otherwise, or ``None`` for a
    malformed value. The whole value must be ASCII digits, so ``"200xyz"``,
    ``" 200"`` or ``"+200"`` cannot masquerade as a success.
    """
    if not reply or reply == str(RDMA_REPLY_NOT_IMPLEMENTED):
        return RDMA_REPLY_NOT_IMPLEMENTED
    if not _DIGITS.fullmatch(reply):
        return None
    code = int(reply)
    return code if 100 <= code <= 599 else None


def parse_rdma_bytes_transferred(response, size: int) -> int:
    """Return the byte count an accepted RDMA GET delivered into a ``size``-byte buffer.

    ``x-amz-rdma-bytes-transferred`` is authoritative and may be below ``size``
    for a ranged read. The endpoint sets it only when bytes moved, so its
    absence means zero bytes.
    """
    value = response["ResponseMetadata"]["HTTPHeaders"].get(X_AMZ_RDMA_BYTES_TRANSFERRED)
    if not value:
        return 0
    if not _DIGITS.fullmatch(value) or int(value) > size:
        raise CuObjError(f"invalid {X_AMZ_RDMA_BYTES_TRANSFERRED}={value!r} for a {size}-byte buffer")
    return int(value)


def _buffer_address(buffer: bytearray | memoryview, nbytes: int) -> int:
    """Return the address of a writable, contiguous buffer for RDMA registration.

    A writable buffer is required: GET delivers payload into it over RDMA, and a
    PUT source is copied into one by the caller so cuObject can pin a stable,
    non-immutable region. ``nbytes`` is the byte length (``memoryview.nbytes``),
    which differs from ``len()`` for multi-byte item formats.
    """
    array = (ctypes.c_char * nbytes).from_buffer(buffer)
    return ctypes.addressof(array)


class CuObjEngine:
    """cuObject RDMA control plane for a single S3 provider instance.

    The MSC analog of ``BotoCuObjClient``: it owns the per-request token
    lifecycle and the boto3 hooks that carry the descriptor on the wire. The S3
    provider supplies the buffer and issues the (body-less) ``PutObject`` /
    ``GetObject`` inside :meth:`transfer`.
    """

    def __init__(self) -> None:
        if not is_available():
            raise CuObjError(
                "cuObject client is not connected to an RDMA fabric. Check the RDMA NIC, the "
                "cuFile/cuObject JSON config (CUFILE_ENV_PATH_JSON), and version-matched "
                "libcufile/libcuobjclient libraries."
            )

    @staticmethod
    def client_config_overrides() -> dict:
        """botocore ``Config`` keys the S3 client must use for the RDMA wire contract.

        The payload travels over RDMA, so the HTTP body is empty and must not be
        signed or checksummed; otherwise SigV4 / content checksums computed over
        the empty body are rejected by the endpoint.
        """
        return {
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
            "s3": {"payload_signing_enabled": False},
        }

    def install_hooks(self, s3_client) -> None:
        """Register token-injection hooks for S3 RDMA transfer operations."""
        events = s3_client.meta.events
        events.register("before-sign.s3.PutObject", self._inject_token)
        events.register("before-sign.s3.GetObject", self._inject_token)
        events.register("before-sign.s3.UploadPart", self._inject_token)

    @staticmethod
    def _inject_token(request, **kwargs) -> None:
        """Mint a fresh RDMA token for every signing attempt of the in-flight request.

        botocore re-signs each retry, so this releases the previous attempt's
        token and mints a new one rather than resending a stale descriptor.
        """
        region = getattr(_thread_state, "rdma_region", None)
        if region is None:
            return
        _release_thread_token()
        addr, nbytes, is_put = region
        token = get_rdma_token(addr, nbytes, 0, is_put)
        _thread_state.rdma_token = token
        # SigV4 signs every x-amz-* header, so the token must be present
        # before signing (before-sign), not after.
        request.headers[X_AMZ_RDMA_TOKEN] = token

    @staticmethod
    def check_reply(response, is_put: bool) -> None:
        """Validate the endpoint's ``x-amz-rdma-reply`` for an RDMA request.

        There is no silent TCP fallback: a declined, malformed, or
        HTTP-mismatched reply raises :class:`CuObjError`. A PUT or UploadPart
        succeeds on HTTP 200 with an ETag; the reply is optional there, but when
        present it must be ``200`` or ``204`` (the CRC64NVME checksum rejects an
        endpoint that ignored the token). A GET must pair HTTP 200 with reply
        ``200`` or HTTP 206 with ``206``; an absent reply means it was declined.
        """
        metadata = response["ResponseMetadata"]
        status = metadata.get("HTTPStatusCode")
        reply = metadata["HTTPHeaders"].get(X_AMZ_RDMA_REPLY)
        code = parse_rdma_reply(reply)
        if is_put:
            accepted = (
                status == 200
                and bool(response.get("ETag"))
                and (not reply or code in (RDMA_REPLY_SUCCESS, RDMA_REPLY_NO_CONTENT))
            )
        else:
            accepted = (status, code) in (
                (200, RDMA_REPLY_SUCCESS),
                (206, RDMA_REPLY_PARTIAL_CONTENT),
            )
        if accepted:
            return
        if code == RDMA_REPLY_NOT_IMPLEMENTED and (reply or not is_put):
            raise _declined(status, reply)
        raise CuObjError(f"RDMA {'PUT' if is_put else 'GET'} failed (http={status}, {X_AMZ_RDMA_REPLY}={reply!r})")

    @contextmanager
    def transfer(self, buffer: bytearray | memoryview, is_put: bool) -> Iterator[None]:
        """Register ``buffer`` for the wrapped request, then release its token and deregister it.

        Tokens are minted per signing attempt by the ``before-sign`` hook. The
        cuObject descriptor is sent verbatim as ``x-amz-rdma-token``: it already
        carries the buffer address and transfer size in its own fields.
        """
        nbytes = memoryview(buffer).nbytes
        if nbytes > RDMA_MAX_MEMORY_REG_SIZE:
            raise CuObjError(
                f"RDMA buffer of {nbytes} bytes exceeds the {RDMA_MAX_MEMORY_REG_SIZE}-byte "
                f"{X_AMZ_RDMA_TOKEN} window; split the transfer into parts or ranges."
            )
        addr = _buffer_address(buffer, nbytes)
        register_buffer(addr, nbytes)
        _thread_state.rdma_region = (addr, nbytes, is_put)
        try:
            yield
        finally:
            _thread_state.rdma_region = None
            # Deregister the buffer even if releasing the token raises, so a
            # failed release never leaks the pinned region.
            try:
                _release_thread_token()
            finally:
                deregister_buffer(addr)


def _release_thread_token() -> None:
    token = getattr(_thread_state, "rdma_token", None)
    _thread_state.rdma_token = None
    if token is not None:
        put_rdma_token(token)


def _declined(status: int | None, reply: str | None) -> CuObjError:
    return CuObjError(
        f"S3 endpoint declined RDMA (http={status}, {X_AMZ_RDMA_REPLY}={reply!r}). Use the 's3' provider "
        "if the endpoint is not RDMA-capable."
    )


def raise_if_declined(error_response: dict) -> None:
    """Raise :class:`CuObjError` when a failed request's response carries an RDMA decline.

    An endpoint declines with an HTTP error status plus ``x-amz-rdma-reply: 501``;
    botocore surfaces that as a ``ClientError`` before :meth:`CuObjEngine.check_reply`
    can see it.
    """
    metadata = error_response.get("ResponseMetadata", {})
    reply = metadata.get("HTTPHeaders", {}).get(X_AMZ_RDMA_REPLY)
    if reply == str(RDMA_REPLY_NOT_IMPLEMENTED):
        raise _declined(metadata.get("HTTPStatusCode"), reply)
