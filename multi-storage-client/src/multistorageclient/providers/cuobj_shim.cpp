// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// extern "C" shim over NVIDIA cuObject (libcuobjclient) implementing the
// "manual RDMA token" pattern (cuObjClient API spec section 1.12.4), so the
// pure-Python multi-storage-client can drive the cuObject token API over
// ctypes. This is the MSC analog of PyTorch's torch/csrc/cuda/CuObjClient.cpp;
// it owns only the RDMA data plane (buffer registration and RDMA descriptor
// lifetime). The S3 control plane (issuing the GET/PUT that carries the
// x-amz-rdma-token header) lives in Python (providers/_cuobj.py).
//
// Build on a host with the cuObject SDK (CUDA Toolkit >= 13.1.1):
//
//   g++ -O2 -fPIC -shared cuobj_shim.cpp -o libmsc_cuobj.so \
//       -I"$CUOBJ_INC" -L"$CUOBJ_LIB" -lcuobjclient -lcufile
//
// where CUOBJ_INC/CUOBJ_LIB point at the SDK's include/lib (the toolkit also
// ships cuobjclient.h + libcuobjclient.so under /usr/local/cuda). Run with a
// version-matched libcufile/libcufile_rdma and a cuFile/cuObject JSON config
// (CUFILE_ENV_PATH_JSON) describing the RDMA NIC. Point MSC_CUOBJ_SHIM at the
// resulting libmsc_cuobj.so.

#include <cuobjclient.h>

#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>

namespace {

// cuObject get/put callbacks are unused in the manual-token pattern (Python
// drives the S3 request out of band), but the cuObjClient constructor requires
// an ops table. Provide stubs.
ssize_t cuobj_stub_get(
    const void* /*handle*/,
    char* /*ptr*/,
    size_t /*size*/,
    loff_t /*offset*/,
    const cufileRDMAInfo_t* /*rdma_info*/) {
  return -EOPNOTSUPP;
}

ssize_t cuobj_stub_put(
    const void* /*handle*/,
    const char* /*ptr*/,
    size_t /*size*/,
    loff_t /*offset*/,
    const cufileRDMAInfo_t* /*rdma_info*/) {
  return -EOPNOTSUPP;
}

// Process-wide cuObjClient. Lazily constructed on first use.
cuObjClient* getClient() {
  static CUObjOps_t ops = {cuobj_stub_get, cuobj_stub_put};
  static cuObjClient client(ops);
  return &client;
}

// Descriptors returned by cuMemObjGetRDMAToken are owned by cuObject and must
// be released via cuMemObjPutRDMAToken once the S3 request completes. Keep the
// original pointer keyed by descriptor value so Python can free it by string,
// the way CuObjClient.cpp does -- this lets the ctypes binding read the
// descriptor as a plain c_char_p without owning the lifetime.
std::mutex& tokenMutex() {
  static std::mutex m;
  return m;
}

std::unordered_map<std::string, char*>& tokenRegistry() {
  static std::unordered_map<std::string, char*> r;
  return r;
}

} // namespace

extern "C" {

int cuobj_available() {
  return getClient()->isConnected() ? 1 : 0;
}

int cuobj_register_buffer(void* ptr, size_t size) {
  return getClient()->cuMemObjGetDescriptor(ptr, size) == CU_OBJ_SUCCESS ? 0 : -1;
}

int cuobj_deregister_buffer(void* ptr) {
  return getClient()->cuMemObjPutDescriptor(ptr) == CU_OBJ_SUCCESS ? 0 : -1;
}

// Returns the descriptor string for [offset, offset+size) of the registered
// buffer, or nullptr on failure. is_put selects PUT (1) vs GET (0). The caller
// must release it via cuobj_put_rdma_token after the S3 request completes.
const char* cuobj_get_rdma_token(void* ptr, size_t size, size_t offset, int is_put) {
  char* desc = nullptr;
  cuObjOpType_t op = is_put ? CUOBJ_PUT : CUOBJ_GET;
  cuObjErr_t status = getClient()->cuMemObjGetRDMAToken(ptr, size, offset, op, &desc);
  if (status != CU_OBJ_SUCCESS || desc == nullptr) {
    return nullptr;
  }
  std::lock_guard<std::mutex> lock(tokenMutex());
  tokenRegistry()[std::string(desc)] = desc;
  return desc;
}

int cuobj_put_rdma_token(const char* token) {
  if (token == nullptr) {
    return -1;
  }
  char* desc = nullptr;
  {
    std::lock_guard<std::mutex> lock(tokenMutex());
    auto it = tokenRegistry().find(std::string(token));
    if (it == tokenRegistry().end()) {
      return 0;
    }
    desc = it->second;
    tokenRegistry().erase(it);
  }
  return getClient()->cuMemObjPutRDMAToken(desc) == CU_OBJ_SUCCESS ? 0 : -1;
}

} // extern "C"
