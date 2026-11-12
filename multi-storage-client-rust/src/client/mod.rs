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

//! Shared client policy over asynchronous physical providers.
//!
//! Clients are cheap to clone and share providers and cleanup owners.
//! Applications own the Tokio runtime and await [`StorageClient::shutdown`].
mod builder;
mod delete;
mod download;
mod listing;
mod read;
mod reader;
mod write;
mod writer;

pub use reader::ObjectReader;
pub use writer::ObjectWriter;

use crate::{
    BatchFailure, BatchResult, ErrorKind, GetConditions, MscError, ObjectKind, ObjectMetadata,
    ObjectPath, PresignMethod, ProviderCapabilities, ProviderRegistry, Result, RetryPolicy,
    StorageProvider, config::ResolvedConfig,
};
use std::{future::Future, sync::Arc, time::Duration};

const DEFAULT_MEMORY_LIMIT: u64 = 64 * 1024 * 1024;
const DEFAULT_READ_BUFFER: usize = 1024 * 1024;

tokio::task_local! {
    static PROVIDER_RETRY_POLICY: RetryPolicy;
}
#[cfg(feature = "s3")]
pub(crate) fn current_provider_retry_policy() -> Option<RetryPolicy> {
    PROVIDER_RETRY_POLICY.try_with(Clone::clone).ok()
}
#[derive(Clone)]
struct ClientState {
    provider: Arc<dyn StorageProvider>,
    retry: RetryPolicy,
    max_in_memory_bytes: u64,
    read_buffer_size: usize,
}
/// A cheap-to-clone client for a physical storage namespace.
/// All operations take profile-relative keys. Providers apply `base_path` once.
#[derive(Clone)]
pub struct StorageClient {
    state: Arc<ClientState>,
}
/// Select a profile, provider registry, retry policy and bounded read settings.
pub struct StorageClientBuilder {
    config: ResolvedConfig,
    profile: String,
    registry: ProviderRegistry,
    max_in_memory_bytes: u64,
    read_buffer_size: usize,
}
impl StorageClientBuilder {
    /// Supply custom storage factories; register matching tags with the loader
    /// as well so configuration validation recognizes them.
    #[must_use]
    pub fn with_provider_registry(mut self, registry: ProviderRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Explicitly raise the bounded `read` limit, or use `u64::MAX` to opt out.
    #[must_use]
    pub fn with_max_in_memory_bytes(mut self, maximum: u64) -> Self {
        self.max_in_memory_bytes = maximum;
        self
    }

    /// Set the seekable reader's range-buffer size in bytes; defaults to 1 MiB.
    /// An absent [`crate::ReadOptions::buffer_size`] inherits this setting; supplied
    /// positive per-call sizes override it.
    /// Zero is rejected when [`Self::build`] is called.
    #[must_use]
    pub fn with_read_buffer_size(mut self, bytes: usize) -> Self {
        self.read_buffer_size = bytes;
        self
    }
}

impl StorageClient {
    /// Select a profile. Construction and credentials are deferred until build.
    #[must_use]
    pub fn builder(config: ResolvedConfig, profile: impl Into<String>) -> StorageClientBuilder {
        StorageClientBuilder {
            config,
            profile: profile.into(),
            registry: ProviderRegistry::default(),
            max_in_memory_bytes: DEFAULT_MEMORY_LIMIT,
            read_buffer_size: DEFAULT_READ_BUFFER,
        }
    }
    /// Wrap an application-supplied provider with the default client retry policy.
    #[must_use]
    pub fn from_provider(provider: Arc<dyn StorageProvider>) -> Self {
        Self {
            state: Arc::new(ClientState {
                provider,
                retry: RetryPolicy::default(),
                max_in_memory_bytes: DEFAULT_MEMORY_LIMIT,
                read_buffer_size: DEFAULT_READ_BUFFER,
            }),
        }
    }
    /// Set the operation retry budget on this clone.
    ///
    /// # Errors
    /// Returns invalid retry settings.
    pub fn with_retry_policy(self, retry: RetryPolicy) -> Result<Self> {
        retry.validate()?;
        let mut state = (*self.state).clone();
        state.retry = retry;
        Ok(Self {
            state: Arc::new(state),
        })
    }
    /// Describe optional provider behavior.
    #[must_use]
    pub fn capabilities(&self) -> ProviderCapabilities {
        self.state.provider.capabilities()
    }
    /// Return a non-secret backend identity.
    #[must_use]
    pub fn backend_fingerprint(&self) -> String {
        self.state.provider.backend_fingerprint()
    }
    /// Return the physical prefix for diagnostics. Operations remain profile-relative.
    #[must_use]
    pub fn physical_root(&self) -> ObjectPath {
        self.state.provider.physical_root()
    }
    /// Whether this client uses POSIX storage.
    #[must_use]
    pub fn is_posix(&self) -> bool {
        self.state.provider.is_posix()
    }
    /// Whether this provider advertises no streaming writes.
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        !self.capabilities().streaming_writes
    }
    async fn retry<T, F, Fut>(&self, safe: bool, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.retry_operation(safe, false, operation).await
    }

    async fn retry_mutation<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.retry_operation(true, true, operation).await
    }

    async fn retry_operation<T, F, Fut>(
        &self,
        safe: bool,
        mutation: bool,
        mut operation: F,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match PROVIDER_RETRY_POLICY
                .scope(self.state.retry.clone(), operation())
                .await
            {
                Err(error) if self.can_retry(&error, safe, attempts) => {
                    self.retry_delay(&error, attempts).await;
                }
                Err(error)
                    if mutation && attempts > 1 && error.kind == ErrorKind::PreconditionFailed =>
                {
                    return Err(error.ambiguous_outcome());
                }
                result => return result,
            }
        }
    }

    fn can_retry(&self, error: &MscError, safe: bool, attempts: u32) -> bool {
        safe && error.retryable
            && error.kind != ErrorKind::Cancelled
            && attempts < self.state.retry.max_attempts
    }

    async fn retry_delay(&self, error: &MscError, attempt: u32) {
        tokio::time::sleep(
            error
                .retry_after
                .unwrap_or_else(|| self.state.retry.delay(attempt)),
        )
        .await;
    }

    /// Generate a URL when the requested GET/PUT presigning capability exists.
    ///
    /// # Errors
    /// Returns path, unsupported-operation, expiration, or presigning errors.
    pub async fn presigned_url(
        &self,
        path: impl AsRef<str>,
        method: PresignMethod,
        expires: Duration,
    ) -> Result<String> {
        let mut path = nonroot_path(path)?;
        let supported = match method {
            PresignMethod::Get => self.capabilities().presigned_get,
            PresignMethod::Put => self.capabilities().presigned_put,
        };
        if !supported {
            return Err(MscError::unsupported("presigned URLs"));
        }
        if method == PresignMethod::Get && self.capabilities().symlinks {
            match self.info(&path, crate::InfoOptions::default()).await {
                Ok(metadata) => {
                    require_file(&metadata)?;
                    path = metadata.key;
                }
                Err(error) if error.kind == ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        self.retry(true, || {
            self.state.provider.presigned_url(&path, method, expires)
        })
        .await
    }

    /// Drain provider-owned cleanup, including cancelled multipart uploads.
    /// Call this on success and failure while the Tokio runtime is alive.
    ///
    /// # Errors
    /// Returns cleanup or task-join failures. Calling it again can retry cleanup.
    pub async fn shutdown(&self) -> Result<()> {
        self.state.provider.shutdown().await
    }
}
fn nonroot_path(path: impl AsRef<str>) -> Result<ObjectPath> {
    let path = ObjectPath::new(path)?;
    if path.is_root() {
        return Err(MscError::new(
            ErrorKind::InvalidPath,
            "operation requires an object key",
        ));
    }
    Ok(path)
}

fn require_file(metadata: &ObjectMetadata) -> Result<()> {
    if metadata.kind != ObjectKind::File {
        return Err(MscError::new(
            ErrorKind::InvalidPath,
            "operation requires a regular object",
        ));
    }
    Ok(())
}

fn stable_conditions(metadata: &ObjectMetadata, requested: &GetConditions) -> GetConditions {
    let mut conditions = requested.clone();
    if conditions
        .if_match
        .as_deref()
        .is_none_or(|etag| etag == "*")
    {
        conditions.if_match.clone_from(&metadata.etag);
    }
    if conditions.version.is_none() {
        conditions.version.clone_from(&metadata.source_version);
    }
    conditions
}

fn record_transfer(
    batch: &mut BatchResult,
    index: usize,
    source: ObjectPath,
    destination: Option<ObjectPath>,
    outcome: Result<ObjectMetadata>,
) {
    match outcome {
        Ok(_) => batch.succeeded.push(index),
        Err(error) => batch.failed.push(BatchFailure {
            index,
            source,
            destination,
            error,
        }),
    }
}
