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
mod metadata;

use crate::{
    BaseStorageProvider, MetadataProvider, MetadataProviderRegistry, ProviderRegistry, RetryPolicy,
    StorageBackend,
};
use std::sync::Arc;
use tokio::sync::Mutex;

const METADATA_LOCK_COUNT: usize = 64;

/// A cheap-to-clone client for a storage namespace with optional logical metadata.
#[derive(Clone)]
pub struct StorageClient {
    pub(crate) provider: BaseStorageProvider<dyn StorageBackend>,
    metadata_provider: Option<Arc<dyn MetadataProvider>>,
    metadata_locks: Option<Arc<[Mutex<()>; METADATA_LOCK_COUNT]>>,
    profile: Option<String>,
}

/// Select a profile, backend registry, retry policy and bounded read settings.
pub struct StorageClientBuilder {
    source: builder::ClientSource,
    registry: ProviderRegistry,
    metadata_provider_registry: MetadataProviderRegistry,
    metadata_provider: Option<Arc<dyn MetadataProvider>>,
    retry: Option<RetryPolicy>,
    max_in_memory_bytes: u64,
}
