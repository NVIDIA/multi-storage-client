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
at construction, so it cannot be driven directly from ctypes. PyTorch compiles
the equivalent calls into ``torch._C`` via ``CuObjClient.cpp``; MSC has no such
extension build, so a thin ``extern "C"`` shim (``cuobj_shim.cpp`` in this
directory) wraps a process-wide ``cuObjClient`` and exposes the five calls this
module needs. Build it into ``libmsc_cuobj.so`` on a host with the cuObject SDK
(see ``cuobj_shim.cpp`` for the command) and point :env:`MSC_CUOBJ_SHIM` at it.

This module is import-safe without the shim or the native library present; the
five primitives raise and :func:`is_available` returns ``False``, exactly like
the ``_dummy_fn`` fallback in ``torch/cuda/cuobj.py``. The S3 provider only
instantiates :class:`CuObjEngine` when the ``rdma`` option is configured.
"""

import ctypes
import os
import threading
from contextlib import contextmanager
from typing import Iterator, Optional, Union

# Default shim filename searched on the loader path when MSC_CUOBJ_SHIM is unset.
_DEFAULT_SHIM = "libmsc_cuobj.so"

# The boto3 ``before-sign`` hook runs per request, possibly on transfer-manager
# worker threads, so the token in flight for the current request is kept in
# thread-local state rather than on the client (mirrors the thread-local token
# in BotoCuObjClient).
_thread_state = threading.local()


class CuObjError(RuntimeError):
    """Raised when a cuObject token-API call fails."""


class _Shim:
    """Lazily-loaded, process-wide ctypes binding to the cuObject ``extern "C"`` shim."""

    _instance: Optional["_Shim"] = None
    _load_error: Optional[Exception] = None
    _lock = threading.Lock()

    def __init__(self, path: str) -> None:
        lib = ctypes.CDLL(path, mode=ctypes.RTLD_GLOBAL)

        lib.cuobj_available.restype = ctypes.c_int
        lib.cuobj_available.argtypes = []

        lib.cuobj_register_buffer.restype = ctypes.c_int
        lib.cuobj_register_buffer.argtypes = [ctypes.c_void_p, ctypes.c_size_t]

        lib.cuobj_deregister_buffer.restype = ctypes.c_int
        lib.cuobj_deregister_buffer.argtypes = [ctypes.c_void_p]

        # The shim owns the descriptor string in an internal registry and frees
        # it by value in cuobj_put_rdma_token (as CuObjClient.cpp does), so the
        # returned char* is only ever read here -- c_char_p is safe.
        lib.cuobj_get_rdma_token.restype = ctypes.c_char_p
        lib.cuobj_get_rdma_token.argtypes = [
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_size_t,
            ctypes.c_int,
        ]

        lib.cuobj_put_rdma_token.restype = ctypes.c_int
        lib.cuobj_put_rdma_token.argtypes = [ctypes.c_char_p]

        self._lib = lib
        self.path = path

    @classmethod
    def load(cls) -> Optional["_Shim"]:
        if cls._instance is not None or cls._load_error is not None:
            return cls._instance
        with cls._lock:
            if cls._instance is None and cls._load_error is None:
                path = os.environ.get("MSC_CUOBJ_SHIM", _DEFAULT_SHIM)
                try:
                    cls._instance = cls(path)
                except OSError as error:
                    cls._load_error = error
        return cls._instance


def _require_shim() -> _Shim:
    shim = _Shim.load()
    if shim is None:
        path = os.environ.get("MSC_CUOBJ_SHIM", _DEFAULT_SHIM)
        raise CuObjError(
            f"cuObject support is not available: failed to load the shim ({path!r}). Build "
            f"cuobj_shim.cpp into {_DEFAULT_SHIM} on a host with the cuObject SDK and set "
            f"MSC_CUOBJ_SHIM, or ensure it is on the loader path. Original error: {_Shim._load_error}"
        )
    return shim


def is_available() -> bool:
    """Return whether NVIDIA cuObject (S3-over-RDMA) support is usable.

    ``True`` only when the shim loads and a cuObject client connection can be
    established (RDMA-capable NIC and a reachable RDMA S3 endpoint).
    """
    shim = _Shim.load()
    if shim is None:
        return False
    try:
        return bool(shim._lib.cuobj_available())
    except OSError:
        return False


def register_buffer(addr: int, size: int) -> None:
    """Register a contiguous buffer with cuObject for RDMA transfers.

    Registration is required before requesting an RDMA token for the buffer.
    """
    if _require_shim()._lib.cuobj_register_buffer(ctypes.c_void_p(addr), ctypes.c_size_t(size)) != 0:
        raise CuObjError(f"cuMemObjGetDescriptor failed for buffer at 0x{addr:x} ({size} bytes)")


def deregister_buffer(addr: int) -> None:
    """Deregister a buffer previously passed to :func:`register_buffer`."""
    if _require_shim()._lib.cuobj_deregister_buffer(ctypes.c_void_p(addr)) != 0:
        raise CuObjError(f"cuMemObjPutDescriptor failed for buffer at 0x{addr:x}")


def get_rdma_token(addr: int, size: int, offset: int = 0, is_put: bool = True) -> str:
    """Return an RDMA descriptor for a region of a registered buffer.

    ``is_put`` is ``True`` for a PUT (the server reads from the buffer), ``False``
    for a GET (the server writes into it). Release the descriptor with
    :func:`put_rdma_token` once the request finishes.
    """
    descriptor = _require_shim()._lib.cuobj_get_rdma_token(
        ctypes.c_void_p(addr), ctypes.c_size_t(size), ctypes.c_size_t(offset), ctypes.c_int(1 if is_put else 0)
    )
    if not descriptor:
        raise CuObjError(f"cuMemObjGetRDMAToken failed for buffer at 0x{addr:x} ({size} bytes)")
    return descriptor.decode("ascii")


def put_rdma_token(token: str) -> None:
    """Release an RDMA descriptor returned by :func:`get_rdma_token`."""
    if _require_shim()._lib.cuobj_put_rdma_token(token.encode("ascii")) != 0:
        raise CuObjError("cuMemObjPutRDMAToken failed")


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
