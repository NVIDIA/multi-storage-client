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

//! Application-facing client and construction over a shared storage provider.
mod api;
mod builder;

use crate::{ProviderRegistry, RetryPolicy, StorageProvider};
use std::sync::Arc;

/// A cheap-to-clone client for a physical storage namespace.
/// All operations take keys relative to the selected profile root.
#[derive(Clone)]
pub struct StorageClient {
    provider: Arc<dyn StorageProvider>,
}

/// Select a profile, backend registry, retry policy and bounded read settings.
pub struct StorageClientBuilder {
    source: builder::ClientSource,
    registry: ProviderRegistry,
    retry: Option<RetryPolicy>,
    max_in_memory_bytes: u64,
}
