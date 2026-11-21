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
//! [`StorageBackend`] and wrap it in [`BaseStorageProvider`] to add a backend.
//! Built-in adapters are available under [`posix`] on Unix and `s3` when the
//! default `s3` Cargo feature is enabled. Builders wrap adapters automatically.
mod backend;
mod base;
mod metadata;
mod ranges;
mod registry;
mod traits;

pub mod manifest;
pub mod posix;
#[cfg(feature = "s3")]
pub mod s3;

pub use backend::StorageBackend;
pub use base::BaseStorageProvider;
pub(crate) use base::DEFAULT_MEMORY_LIMIT;
#[cfg(feature = "s3")]
pub(crate) use base::current_provider_retry_policy;
pub use registry::{ProviderFactory, ProviderRegistry};
pub use traits::StorageProvider;

pub(crate) use metadata::resolve_metadata;
pub use metadata::{MetadataProvider, ResolvedPath, ResolvedPathState};
pub(crate) use ranges::suffix_fallback;
#[cfg(feature = "s3")]
pub(crate) use ranges::{range_bytes, requested_bounds};
