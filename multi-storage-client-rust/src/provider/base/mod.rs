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

//! Shared policies and transfers for any public backend implementation.
mod api;
mod delete;
mod download;
mod listing;
mod read;
mod write;

use crate::{
    ErrorKind, GetConditions, MscError, ObjectKind, ObjectMetadata, ObjectPath, Result,
    RetryPolicy, StorageBackend,
};
use std::{future::Future, sync::Arc};

pub(crate) const DEFAULT_MEMORY_LIMIT: u64 = 64 * 1024 * 1024;

tokio::task_local! {
    static PROVIDER_RETRY_POLICY: RetryPolicy;
}

#[cfg(feature = "s3")]
pub(crate) fn current_provider_retry_policy() -> Option<RetryPolicy> {
    PROVIDER_RETRY_POLICY.try_with(Clone::clone).ok()
}

struct ProviderState<B: StorageBackend + ?Sized> {
    provider: Arc<B>,
    retry: RetryPolicy,
    max_in_memory_bytes: u64,
}

/// Shared provider behavior over a backend's SDK I/O hooks.
/// Clones share the backend, immutable policy, connection pools, and cleanup.
pub struct BaseStorageProvider<B: StorageBackend + ?Sized> {
    state: Arc<ProviderState<B>>,
}

impl<B: StorageBackend + ?Sized> Clone for BaseStorageProvider<B> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<B: StorageBackend + ?Sized + 'static> BaseStorageProvider<B> {
    /// Wrap an SDK backend with the default retry policy and bounded read limit.
    #[must_use]
    pub fn new(backend: Arc<B>) -> Self {
        Self::configured(backend, RetryPolicy::default(), DEFAULT_MEMORY_LIMIT)
    }

    /// Set the operation retry budget before sharing this provider.
    ///
    /// # Errors
    /// Returns invalid retry settings without issuing backend requests.
    pub fn with_retry_policy(self, retry: RetryPolicy) -> Result<Self> {
        retry.validate()?;
        Ok(Self::configured(
            self.state.provider.clone(),
            retry,
            self.state.max_in_memory_bytes,
        ))
    }

    /// Set the default bound for byte reads; file downloads remain streaming.
    #[must_use]
    pub fn with_max_in_memory_bytes(self, maximum: u64) -> Self {
        Self::configured(
            self.state.provider.clone(),
            self.state.retry.clone(),
            maximum,
        )
    }

    pub(crate) fn configured(
        backend: Arc<B>,
        retry: RetryPolicy,
        max_in_memory_bytes: u64,
    ) -> Self {
        Self {
            state: Arc::new(ProviderState {
                provider: backend,
                retry,
                max_in_memory_bytes,
            }),
        }
    }

    /// Whether this provider uses POSIX storage.
    pub(crate) fn is_posix_file_storage_provider(&self) -> bool {
        self.state.provider.is_posix()
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
