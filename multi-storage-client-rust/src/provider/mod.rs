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

//! Object-safe storage boundary and process-local provider factories.
//!
//! Most applications use [`StorageClient`](crate::StorageClient). Implement
//! [`StorageProvider`] to add a backend, or construct a built-in
//! `posix::PosixProvider` directly for a local root on Unix. S3 types are available
//! under `s3` when the default `s3` Cargo feature is enabled.
mod ranges;
mod registry;
mod traits;

pub mod posix;
#[cfg(feature = "s3")]
pub mod s3;

pub use registry::{ProviderFactory, ProviderRegistry};
pub use traits::StorageProvider;

pub(crate) use ranges::suffix_fallback;
#[cfg(feature = "s3")]
pub(crate) use ranges::{range_bytes, requested_bounds};
