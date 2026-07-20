// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! PyO3 bindings for the NVIDIA cuObject (S3-over-RDMA) token API.
//!
//! Compiled only under the `rdma` cargo feature. The C++ `cuObjClient` class is
//! reached through the `miniors_cuobj_*` C ABI shim (`csrc/cuobj_shim.cc`),
//! which this module declares by hand -- no bindgen. The safe wrappers mirror
//! the minio-rs S3 RDMA client; the token registry (descriptor string -> owning
//! pointer) lets Python mint a token in one call and release it in another,
//! matching the manual-token pattern the Python `_cuobj.py` control plane drives.

#![allow(non_camel_case_types)]

use std::collections::HashMap;
use std::ffi::{c_void, CStr};
use std::ptr::NonNull;
use std::sync::{LazyLock, Mutex, OnceLock};

use libc::{c_char, c_int, size_t};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;

#[repr(C)]
struct miniors_cuobj_client {
    _private: [u8; 0],
}

const MINIORS_CUOBJ_SUCCESS: c_int = 0;
const MINIORS_CUOBJ_OP_GET: c_int = 0;
const MINIORS_CUOBJ_OP_PUT: c_int = 1;

extern "C" {
    fn miniors_cuobj_client_new() -> *mut miniors_cuobj_client;
    fn miniors_cuobj_client_free(client: *mut miniors_cuobj_client);
    fn miniors_cuobj_is_connected(client: *mut miniors_cuobj_client) -> c_int;
    fn miniors_cuobj_get_descriptor(
        client: *mut miniors_cuobj_client,
        ptr: *mut c_void,
        size: size_t,
    ) -> c_int;
    fn miniors_cuobj_put_descriptor(client: *mut miniors_cuobj_client, ptr: *mut c_void) -> c_int;
    fn miniors_cuobj_get_rdma_token(
        client: *mut miniors_cuobj_client,
        ptr: *mut c_void,
        size: size_t,
        offset: size_t,
        op: c_int,
        token_out: *mut *mut c_char,
    ) -> c_int;
    fn miniors_cuobj_put_rdma_token(client: *mut miniors_cuobj_client, token: *mut c_char)
        -> c_int;
}

/// Safe handle to the NVIDIA `cuObjClient` C++ instance owned by the shim.
struct CuObjClient {
    raw: NonNull<miniors_cuobj_client>,
}

// The underlying cuObjClient is driven through a process-wide singleton whose
// individual calls are serialized by cuObject's own internal locking.
unsafe impl Send for CuObjClient {}
unsafe impl Sync for CuObjClient {}

impl CuObjClient {
    fn new() -> Option<Self> {
        let raw = unsafe { miniors_cuobj_client_new() };
        NonNull::new(raw).map(|raw| Self { raw })
    }

    fn is_connected(&self) -> bool {
        unsafe { miniors_cuobj_is_connected(self.raw.as_ptr()) != 0 }
    }

    unsafe fn get_descriptor(&self, ptr: *mut c_void, size: usize) -> bool {
        miniors_cuobj_get_descriptor(self.raw.as_ptr(), ptr, size) == MINIORS_CUOBJ_SUCCESS
    }

    unsafe fn put_descriptor(&self, ptr: *mut c_void) -> bool {
        miniors_cuobj_put_descriptor(self.raw.as_ptr(), ptr) == MINIORS_CUOBJ_SUCCESS
    }

    unsafe fn get_rdma_token(
        &self,
        ptr: *mut c_void,
        size: usize,
        offset: usize,
        op: c_int,
    ) -> Option<*mut c_char> {
        let mut token: *mut c_char = std::ptr::null_mut();
        let rc = miniors_cuobj_get_rdma_token(self.raw.as_ptr(), ptr, size, offset, op, &mut token);
        if rc != MINIORS_CUOBJ_SUCCESS || token.is_null() {
            None
        } else {
            Some(token)
        }
    }

    unsafe fn put_rdma_token(&self, token: *mut c_char) -> bool {
        miniors_cuobj_put_rdma_token(self.raw.as_ptr(), token) == MINIORS_CUOBJ_SUCCESS
    }
}

impl Drop for CuObjClient {
    fn drop(&mut self) {
        unsafe { miniors_cuobj_client_free(self.raw.as_ptr()) };
    }
}

/// Process-wide shared `cuObjClient`. Constructing per-call is racy and corrupts
/// malloc state under concurrent workers, so one instance is shared per process.
fn shared() -> Option<&'static CuObjClient> {
    static INSTANCE: OnceLock<Option<CuObjClient>> = OnceLock::new();
    INSTANCE.get_or_init(CuObjClient::new).as_ref()
}

fn client() -> PyResult<&'static CuObjClient> {
    shared().ok_or_else(|| {
        PyRuntimeError::new_err(
            "cuObject client unavailable: cuObjClient construction failed (missing RDMA NIC, \
             cuFile/cuObject config, or version-matched libcuobjclient/libcufile)",
        )
    })
}

/// Descriptors minted by `cuMemObjGetRDMAToken` are owned by cuObject and must be
/// released by the original pointer. Keyed by descriptor string so Python can
/// mint a token in one call and release it (by value) in a later one; the value
/// is the owning `*mut c_char` stored as `usize` to keep the map `Send`/`Sync`.
static TOKEN_REGISTRY: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Return whether cuObject is usable: the client constructed and connected to an
/// RDMA fabric.
#[pyfunction]
fn cuobj_available() -> bool {
    shared().map(CuObjClient::is_connected).unwrap_or(false)
}

/// Register a contiguous buffer with cuObject for RDMA. Raises on failure.
#[pyfunction]
fn cuobj_register_buffer(addr: usize, size: usize) -> PyResult<()> {
    let client = client()?;
    if unsafe { client.get_descriptor(addr as *mut c_void, size) } {
        Ok(())
    } else {
        Err(PyRuntimeError::new_err(format!(
            "cuMemObjGetDescriptor failed for buffer at 0x{addr:x} ({size} bytes)"
        )))
    }
}

/// Deregister a buffer previously passed to `cuobj_register_buffer`. Raises on
/// failure.
#[pyfunction]
fn cuobj_deregister_buffer(addr: usize) -> PyResult<()> {
    let client = client()?;
    if unsafe { client.put_descriptor(addr as *mut c_void) } {
        Ok(())
    } else {
        Err(PyRuntimeError::new_err(format!(
            "cuMemObjPutDescriptor failed for buffer at 0x{addr:x}"
        )))
    }
}

/// Mint an RDMA descriptor for a region of a registered buffer. `is_put` selects
/// PUT (server reads) vs GET (server writes). Release it with
/// `cuobj_put_rdma_token`. Raises on failure.
#[pyfunction]
fn cuobj_get_rdma_token(addr: usize, size: usize, offset: usize, is_put: bool) -> PyResult<String> {
    let client = client()?;
    let op = if is_put {
        MINIORS_CUOBJ_OP_PUT
    } else {
        MINIORS_CUOBJ_OP_GET
    };
    let ptr = unsafe { client.get_rdma_token(addr as *mut c_void, size, offset, op) }.ok_or_else(
        || {
            PyRuntimeError::new_err(format!(
                "cuMemObjGetRDMAToken failed for buffer at 0x{addr:x} ({size} bytes)"
            ))
        },
    )?;
    match unsafe { CStr::from_ptr(ptr) }.to_str() {
        Ok(s) => {
            let desc = s.to_owned();
            TOKEN_REGISTRY
                .lock()
                .unwrap()
                .insert(desc.clone(), ptr as usize);
            Ok(desc)
        }
        Err(e) => {
            // Release the descriptor we cannot represent, so a non-ASCII token
            // never leaks the pinned registration.
            unsafe { client.put_rdma_token(ptr) };
            Err(PyRuntimeError::new_err(format!(
                "cuObject returned a non-UTF-8 RDMA descriptor: {e}"
            )))
        }
    }
}

/// Release an RDMA descriptor returned by `cuobj_get_rdma_token`. Unknown or
/// already-released tokens are a no-op. Raises when the release call fails,
/// keeping the registry entry so the caller can retry.
#[pyfunction]
fn cuobj_put_rdma_token(token: String) -> PyResult<()> {
    let client = client()?;
    let ptr = match TOKEN_REGISTRY.lock().unwrap().get(&token) {
        Some(&ptr) => ptr,
        None => return Ok(()),
    };
    if !unsafe { client.put_rdma_token(ptr as *mut c_char) } {
        return Err(PyRuntimeError::new_err("cuMemObjPutRDMAToken failed"));
    }
    TOKEN_REGISTRY.lock().unwrap().remove(&token);
    Ok(())
}

/// Register the cuObject token-API functions on the extension module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cuobj_available, m)?)?;
    m.add_function(wrap_pyfunction!(cuobj_register_buffer, m)?)?;
    m.add_function(wrap_pyfunction!(cuobj_deregister_buffer, m)?)?;
    m.add_function(wrap_pyfunction!(cuobj_get_rdma_token, m)?)?;
    m.add_function(wrap_pyfunction!(cuobj_put_rdma_token, m)?)?;
    Ok(())
}
