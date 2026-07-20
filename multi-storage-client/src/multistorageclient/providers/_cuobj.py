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
import threading
from contextlib import contextmanager
from typing import Iterator, Optional, Union

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


class CuObjError(RuntimeError):
    """Raised when a cuObject token-API call fails."""


def _require_cuobj() -> None:
    if not _HAS_CUOBJ:
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
    try:
        return bool(_rust_ext.cuobj_available())
    except Exception:
        return False


def register_buffer(addr: int, size: int) -> None:
    """Register a contiguous buffer with cuObject for RDMA transfers.

    Registration is required before requesting an RDMA token for the buffer.
    """
    _require_cuobj()
    try:
        _rust_ext.cuobj_register_buffer(addr, size)
    except Exception as error:
        raise CuObjError(f"cuMemObjGetDescriptor failed for buffer at 0x{addr:x} ({size} bytes)") from error


def deregister_buffer(addr: int) -> None:
    """Deregister a buffer previously passed to :func:`register_buffer`."""
    _require_cuobj()
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
    try:
        return _rust_ext.cuobj_get_rdma_token(addr, size, offset, is_put)
    except Exception as error:
        raise CuObjError(f"cuMemObjGetRDMAToken failed for buffer at 0x{addr:x} ({size} bytes)") from error


def put_rdma_token(token: str) -> None:
    """Release an RDMA descriptor returned by :func:`get_rdma_token`."""
    _require_cuobj()
    try:
        _rust_ext.cuobj_put_rdma_token(token)
    except Exception as error:
        raise CuObjError("cuMemObjPutRDMAToken failed") from error


def _buffer_address(buffer: Union[bytearray, memoryview], nbytes: int) -> int:
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
        events = s3_client.meta.events
        events.register("before-sign.s3.PutObject", self._inject_token)
        events.register("before-sign.s3.GetObject", self._inject_token)
        events.register("before-sign.s3.UploadPart", self._inject_token)

    @staticmethod
    def _inject_token(request, **kwargs) -> None:
        token = getattr(_thread_state, "rdma_token", None)
        if token is not None:
            # SigV4 signs every x-amz-* header, so the token must be present
            # before signing (before-sign), not after.
            request.headers["x-amz-rdma-token"] = token

    @staticmethod
    def check_reply(response) -> None:
        """Fail loudly when an endpoint did not honor the RDMA request.

        With ``rdma`` explicitly enabled there is no silent TCP fallback: a
        missing or ``501`` reply means the payload did not move over RDMA.
        """
        headers = response["ResponseMetadata"]["HTTPHeaders"]
        reply = headers.get("x-amz-rdma-reply")
        if not reply or reply == "501":
            raise CuObjError(
                f"S3 endpoint declined RDMA (x-amz-rdma-reply={reply!r}); the endpoint is not "
                "RDMA-capable. Disable the 'rdma' option to use the standard TCP data plane."
            )

    @contextmanager
    def transfer(self, buffer: Union[bytearray, memoryview], is_put: bool) -> Iterator[None]:
        """Register ``buffer``, publish its RDMA token for the wrapped request, then clean up.

        The descriptor is formatted ``<cuobject-descriptor>:<hex addr>:<hex size>``
        so the endpoint can locate the exact registered region.
        """
        nbytes = memoryview(buffer).nbytes
        addr = _buffer_address(buffer, nbytes)
        register_buffer(addr, nbytes)
        token: Optional[str] = None
        try:
            token = get_rdma_token(addr, nbytes, 0, is_put)
            _thread_state.rdma_token = f"{token}:{addr:016x}:{nbytes:016x}"
            try:
                yield
            finally:
                _thread_state.rdma_token = None
        finally:
            # Deregister the buffer even if releasing the token raises, so a
            # failed release never leaks the pinned region.
            try:
                if token is not None:
                    put_rdma_token(token)
            finally:
                deregister_buffer(addr)
