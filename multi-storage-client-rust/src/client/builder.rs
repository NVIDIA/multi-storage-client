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

use super::{ClientState, DEFAULT_MEMORY_LIMIT, StorageClient, StorageClientBuilder};
use crate::{ProviderRegistry, Result, RetryPolicy, StorageProvider, config::ResolvedConfig};
use std::sync::Arc;

pub(super) enum ClientSource {
    Profile {
        config: ResolvedConfig,
        profile: String,
    },
    Provider(Arc<dyn StorageProvider>),
}

impl StorageClientBuilder {
    pub(super) fn from_profile(config: ResolvedConfig, profile: String) -> Self {
        Self::new(ClientSource::Profile { config, profile })
    }

    /// Select an application-supplied provider with default client settings.
    /// Construction is completed by [`Self::build`]; builder settings apply normally.
    #[must_use]
    pub fn from_provider(provider: Arc<dyn StorageProvider>) -> Self {
        Self::new(ClientSource::Provider(provider))
    }

    fn new(source: ClientSource) -> Self {
        Self {
            source,
            registry: ProviderRegistry::default(),
            retry: None,
            max_in_memory_bytes: DEFAULT_MEMORY_LIMIT,
        }
    }

    /// Supply custom storage factories for profile-based construction; register matching
    /// tags with the loader as well so configuration validation recognizes them.
    #[must_use]
    pub fn with_provider_registry(mut self, registry: ProviderRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Override the profile or default operation retry policy.
    ///
    /// # Errors
    /// Returns invalid retry settings before constructing the provider.
    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Result<Self> {
        retry.validate()?;
        self.retry = Some(retry);
        Ok(self)
    }

    /// Explicitly raise the bounded `read` limit, or use `u64::MAX` to opt out.
    #[must_use]
    pub fn with_max_in_memory_bytes(mut self, maximum: u64) -> Self {
        self.max_in_memory_bytes = maximum;
        self
    }

    /// Construct the selected physical provider and shared policy.
    ///
    /// # Errors
    /// Returns configuration, credential or provider construction errors.
    pub async fn build(self) -> Result<StorageClient> {
        let (provider, default_retry) = match self.source {
            ClientSource::Profile { config, profile } => {
                let profile = config.profile(&profile)?;
                (self.registry.build(profile).await?, profile.retry.clone())
            }
            ClientSource::Provider(provider) => (provider, RetryPolicy::default()),
        };
        Ok(StorageClient {
            state: Arc::new(ClientState {
                provider,
                retry: self.retry.unwrap_or(default_retry),
                max_in_memory_bytes: self.max_in_memory_bytes,
            }),
        })
    }
}
