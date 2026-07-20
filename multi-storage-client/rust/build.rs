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

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=csrc/cuobj_shim.cc");
    println!("cargo:rerun-if-changed=csrc/cuobj_shim.h");
    println!("cargo:rerun-if-env-changed=MSC_CUOBJ_HOME");
    println!("cargo:rerun-if-env-changed=CUDA_HOME");
    println!("cargo:rerun-if-env-changed=CUDA_PATH");

    // The cuObject (RDMA) data plane is opt-in. Without the `rdma` feature the
    // crate builds as a pure-Rust cdylib with no C++ compiler or cuObject SDK,
    // so build.rs is a no-op.
    if env::var("CARGO_FEATURE_RDMA").is_err() {
        return;
    }

    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
        panic!("`rdma` feature is only supported on Linux");
    }

    let arch = match env::var("CARGO_CFG_TARGET_ARCH").as_deref() {
        Ok("x86_64") => "x86_64",
        Ok("aarch64") => "aarch64",
        Ok(other) => panic!("`rdma` feature: unsupported target arch `{other}`"),
        Err(_) => panic!("CARGO_CFG_TARGET_ARCH not set"),
    };

    // Resolve the cuObject SDK (headers + libs) at build time. Nothing is
    // vendored into the repository: the RDMA feature expects the NVIDIA
    // cuObject SDK (CUDA Toolkit >= 13.1) to be installed. Resolution order:
    //   1. MSC_CUOBJ_HOME    -> {include, lib}
    //   2. CUDA_HOME/CUDA_PATH -> targets/<triple>/{include, lib}
    //   3. ./vendor/cuobj    -> {include, lib/<arch>}  (local dev only)
    let (include_dir, lib_dir) = resolve_cuobj_dirs(arch)
        .expect(
            "`rdma` feature: could not locate the cuObject SDK. Set MSC_CUOBJ_HOME (a directory \
             with include/ and lib/), or CUDA_HOME/CUDA_PATH to a CUDA Toolkit (>= 13.1) that \
             ships cuobjclient.h + libcuobjclient.",
        );

    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file("csrc/cuobj_shim.cc")
        .include(&include_dir)
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-fvisibility=hidden")
        .compile("miniors_cuobj_shim");

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());

    for lib in &[
        "cuobjclient",
        "cufile",
        "ibverbs",
        "rdmacm",
        "numa",
        "pthread",
        "dl",
        "rt",
    ] {
        println!("cargo:rustc-link-lib=dylib={lib}");
    }
    println!("cargo:rustc-link-lib=dylib=stdc++");
}

fn resolve_cuobj_dirs(arch: &str) -> Option<(PathBuf, PathBuf)> {
    let has_header = |inc: &PathBuf| inc.join("cuobjclient.h").exists();

    if let Ok(home) = env::var("MSC_CUOBJ_HOME") {
        let base = PathBuf::from(home);
        let inc = base.join("include");
        let lib = base.join("lib");
        if has_header(&inc) {
            return Some((inc, lib));
        }
    }

    let triple = match arch {
        "x86_64" => "x86_64-linux",
        "aarch64" => "sbsa-linux",
        _ => "x86_64-linux",
    };
    for var in ["CUDA_HOME", "CUDA_PATH", "CUDA_ROOT"] {
        if let Ok(cuda) = env::var(var) {
            let base = PathBuf::from(cuda).join("targets").join(triple);
            let inc = base.join("include");
            let lib = base.join("lib");
            if has_header(&inc) {
                return Some((inc, lib));
            }
        }
    }

    // Local dev fallback: a vendored SDK copy (git-ignored, never committed).
    let vendor = PathBuf::from(env::var("CARGO_MANIFEST_DIR").ok()?)
        .join("vendor")
        .join("cuobj");
    let inc = vendor.join("include");
    let lib = vendor.join("lib").join(arch);
    if has_header(&inc) {
        return Some((inc, lib));
    }

    None
}
