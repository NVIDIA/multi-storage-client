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
use crate::{
    config::{ProfileConfig, ProviderConfig},
    error::{ErrorKind, MscError, Result},
    path::ObjectPath,
    types::{
        BatchFailure, BatchResult, ByteRange, ByteStream, CopyOptions, DeleteOptions,
        GetConditions, ListOptions, MetadataStream, ObjectMetadata, PresignMethod,
        ProviderCapabilities, PutResult, WriteOptions,
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, future::BoxFuture, stream};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub mod posix;
#[cfg(feature = "s3")]
pub mod s3;

/// Asynchronous primitive I/O for validated, profile-relative keys.
///
/// Providers never receive URLs or profile names and do not own a runtime.
/// Disable SDK request retries: [`StorageClient`](crate::StorageClient) owns the
/// operation budget. Safe multipart parts use that profile's budget per part;
/// whole-object publication is not retried around those part attempts.
/// Advertise optional features accurately, reject unsupported
/// conditions before mutation, and return typed, secret-safe errors. A stream
/// may fail after opening, so its errors are part of the operation contract.
#[async_trait]
pub trait StorageProvider: Send + Sync {
    /// Non-secret backend tag used in errors and provider metrics.
    fn name(&self) -> &str;
    /// Non-secret identity used to detect backend aliases.
    fn backend_fingerprint(&self) -> String {
        format!("{}:{:p}", self.name(), self)
    }
    /// Configured physical prefix for overlap checks. The default is the root.
    fn physical_root(&self) -> ObjectPath {
        ObjectPath::default()
    }
    /// Whether this adapter accesses a POSIX filesystem.
    fn is_posix(&self) -> bool {
        false
    }
    /// Preserve configured local timestamps and permissions after a transfer.
    /// `expected` protects the destination generation. Return updated metadata
    /// on success, or `None` when preservation is unavailable or disabled.
    async fn preserve_metadata(
        &self,
        _key: &ObjectPath,
        _source: &ObjectMetadata,
        _expected: Option<&crate::SourceVersion>,
    ) -> Result<Option<ObjectMetadata>> {
        Ok(None)
    }
    /// Immutable optional operation and consistency capabilities.
    fn capabilities(&self) -> ProviderCapabilities;
    /// Return metadata for a key without consuming file content.
    async fn head(&self, key: &ObjectPath) -> Result<ObjectMetadata>;
    /// Return metadata for the requested generation, or fail if it is unavailable.
    async fn head_version(
        &self,
        key: &ObjectPath,
        version: &crate::SourceVersion,
    ) -> Result<ObjectMetadata> {
        let metadata = self.head(key).await?;
        if metadata.source_version.as_ref() != Some(version) {
            return Err(MscError::new(
                ErrorKind::PreconditionFailed,
                "requested object generation is unavailable",
            ));
        }
        Ok(metadata)
    }
    /// Open a conditional byte stream; later transfer failures are stream items.
    async fn get(&self, key: &ObjectPath, conditions: &GetConditions) -> Result<ByteStream>;
    /// Read a half-open range, clipping it at EOF and enforcing conditions.
    async fn get_range(
        &self,
        key: &ObjectPath,
        range: ByteRange,
        conditions: &GetConditions,
    ) -> Result<Bytes>;
    /// Read the last bytes of an object. Native cloud adapters use one ranged GET.
    /// The default resolves metadata and falls back to an offset range.
    async fn get_suffix(
        &self,
        key: &ObjectPath,
        range: crate::SuffixRange,
        conditions: &GetConditions,
    ) -> Result<Bytes> {
        suffix_fallback(self, key, range, conditions).await
    }
    /// Open a native read-only POSIX handle using anchored path resolution.
    /// Remote providers return `Unsupported`.
    #[cfg(unix)]
    async fn open_posix_file(
        &self,
        _key: &ObjectPath,
        _conditions: &GetConditions,
    ) -> Result<tokio::fs::File> {
        Err(MscError::unsupported("native POSIX file handles"))
    }
    /// Consume a byte stream and atomically publish the object when complete.
    /// `size`, when supplied, must match the actual bytes. Cancellation must not
    /// publish a partial object; multipart cleanup remains owned until shutdown.
    async fn put(
        &self,
        key: &ObjectPath,
        stream: ByteStream,
        size: Option<u64>,
        options: &WriteOptions,
    ) -> Result<PutResult>;
    /// Delete a key with backend-enforced conditions. Client deletion treats
    /// an absent object as success; a primitive may return `NotFound`.
    async fn delete(&self, key: &ObjectPath, conditions: &DeleteOptions) -> Result<()>;
    /// Bounded fallback retaining the original index of every item.
    async fn delete_many(&self, keys: &[ObjectPath], options: &DeleteOptions) -> BatchResult {
        let mut outcomes =
            stream::iter(keys.iter().cloned().enumerate())
                .map(|(index, key)| async move {
                    (index, key.clone(), self.delete(&key, options).await)
                })
                .buffer_unordered(16);
        let mut result = BatchResult::default();
        while let Some((index, source, outcome)) = outcomes.next().await {
            match outcome {
                Ok(()) => result.succeeded.push(index),
                Err(error) if error.kind == ErrorKind::NotFound => result.succeeded.push(index),
                Err(error) => result.failed.push(BatchFailure {
                    index,
                    source,
                    destination: None,
                    error,
                }),
            }
        }
        result.succeeded.sort_unstable();
        result.failed.sort_by_key(|failure| failure.index);
        result
    }
    /// Perform native copy within the advertised scope, or return `Unsupported`
    /// before mutation so a client can choose a streaming fallback.
    async fn copy(
        &self,
        _source: &ObjectPath,
        _destination: &ObjectPath,
        _options: &CopyOptions,
    ) -> Result<PutResult> {
        Err(MscError::unsupported("copy"))
    }
    /// Stream metadata according to listing options. Advertise
    /// `native_lexical_listing` when file-only entries are globally key-ordered.
    async fn list(&self, options: &ListOptions) -> Result<MetadataStream>;
    /// Create a relative symbolic link if this representation is supported.
    async fn make_symlink(&self, _key: &ObjectPath, _target: &str) -> Result<()> {
        Err(MscError::unsupported("make_symlink"))
    }
    /// Sign a GET or PUT URL for the requested lifetime if supported.
    async fn presigned_url(
        &self,
        _key: &ObjectPath,
        _method: PresignMethod,
        _expires: Duration,
    ) -> Result<String> {
        Err(MscError::unsupported("presigned_url"))
    }
    /// Await provider-owned cleanup tasks before the application exits.
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

pub(crate) async fn suffix_fallback<P: StorageProvider + ?Sized>(
    provider: &P,
    key: &ObjectPath,
    range: crate::SuffixRange,
    conditions: &GetConditions,
) -> Result<Bytes> {
    let mut key = key.clone();
    let mut visited = std::collections::BTreeSet::new();
    for _ in 0..=8 {
        if !visited.insert(key.clone()) {
            break;
        }
        let metadata = if let Some(version) = &conditions.version
            && !provider.is_posix()
        {
            provider.head_version(&key, version).await?
        } else {
            provider.head(&key).await?
        };
        if let Some(target) = metadata.symlink_target {
            key = key.resolve_link(&target)?;
            continue;
        }
        if metadata.kind != crate::ObjectKind::File {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "suffix read requires a file",
            ));
        }
        if conditions
            .version
            .as_ref()
            .is_some_and(|version| metadata.source_version.as_ref() != Some(version))
            || conditions
                .if_match
                .as_ref()
                .is_some_and(|etag| etag != "*" && metadata.etag.as_ref() != Some(etag))
        {
            return Err(MscError::new(
                ErrorKind::PreconditionFailed,
                "suffix read condition failed",
            ));
        }
        if conditions
            .if_none_match
            .as_ref()
            .is_some_and(|etag| etag == "*" || metadata.etag.as_ref() == Some(etag))
        {
            return Err(MscError::new(
                ErrorKind::NotModified,
                "suffix read condition found an unchanged object",
            ));
        }
        let length = range.length().min(metadata.content_length);
        if length == 0 {
            return Ok(Bytes::new());
        }
        let mut conditions = conditions.clone();
        if conditions
            .if_match
            .as_deref()
            .is_none_or(|etag| etag == "*")
        {
            conditions.if_match = metadata.etag;
        }
        if conditions.version.is_none() {
            conditions.version = metadata.source_version;
        }
        return provider
            .get_range(
                &key,
                ByteRange::new(metadata.content_length - length, length)?,
                &conditions,
            )
            .await;
    }
    Err(MscError::new(
        ErrorKind::Integrity,
        "suffix symlink cycle or hop limit",
    ))
}

#[cfg(feature = "s3")]
pub(crate) fn requested_bounds(header: &str, total: u64) -> Option<(u64, u64)> {
    let (start, end) = header.strip_prefix("bytes=")?.split_once('-')?;
    if start.is_empty() {
        let length = end.parse::<u64>().ok()?;
        if length == 0 {
            return None;
        }
        Some((total.saturating_sub(length), total.checked_sub(1)?))
    } else {
        let start = start.parse::<u64>().ok()?;
        let end = end.parse::<u64>().ok()?.min(total.checked_sub(1)?);
        (start <= end).then_some((start, end))
    }
}

#[cfg(feature = "s3")]
pub(crate) async fn range_bytes(mut body: crate::ByteStream, expected: u64) -> Result<Bytes> {
    let mut output = bytes::BytesMut::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        if chunk.len() as u64 > expected.saturating_sub(output.len() as u64) {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "range exceeds requested bytes",
            ));
        }
        output.extend_from_slice(&chunk);
    }
    if output.len() as u64 != expected {
        return Err(MscError::new(
            ErrorKind::Integrity,
            "range response is truncated",
        ));
    }
    Ok(output.freeze())
}

/// Factory for a Rust-registered backend; no dynamic libraries are loaded.
pub type ProviderFactory = Arc<
    dyn Fn(ProfileConfig) -> BoxFuture<'static, Result<Arc<dyn StorageProvider>>> + Send + Sync,
>;
/// Built-in storage tags plus application-owned factories. `s8k` uses the S3
/// adapter and the same `s3` Cargo feature. Registration is process-local;
/// no Python imports or dynamic libraries are loaded.
#[derive(Clone, Default)]
pub struct ProviderRegistry {
    factories: BTreeMap<String, ProviderFactory>,
}
impl std::fmt::Debug for ProviderRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderRegistry")
            .field("tags", &self.factories.keys().collect::<Vec<_>>())
            .finish()
    }
}
impl ProviderRegistry {
    /// Start with built-in tags and no custom factories.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Register a custom tag, rejecting accidental replacement.
    ///
    /// # Errors
    /// Returns `InvalidConfig` when the tag is already a built-in or registered.
    pub fn register(&mut self, tag: impl Into<String>, factory: ProviderFactory) -> Result<()> {
        let tag = tag.into();
        if self.contains(&tag) {
            return Err(MscError::new(
                ErrorKind::InvalidConfig,
                format!("provider tag {tag} is already registered"),
            ));
        }
        self.factories.insert(tag, factory);
        Ok(())
    }
    /// Whether a tag is known. Built-in tags remain reserved even when a feature
    /// is disabled or deferred; this is not a guarantee that construction succeeds.
    #[must_use]
    pub fn contains(&self, tag: &str) -> bool {
        matches!(tag, "s3" | "s8k" | "file" | "gcs" | "azure") || self.factories.contains_key(tag)
    }
    /// Validate and build the backend selected by a single profile.
    ///
    /// # Errors
    /// Returns configuration, credential, or unsupported-provider errors before I/O.
    pub async fn build(&self, profile: &ProfileConfig) -> Result<Arc<dyn StorageProvider>> {
        profile.validate_runtime_support()?;
        profile.retry.validate()?;
        let config = profile.storage_provider.as_ref().ok_or_else(|| {
            MscError::unsupported("composite/provider-bundle profiles in the S3 iteration")
        })?;
        if let Some(factory) = self.factories.get(&config.provider_type) {
            return factory(profile.clone()).await;
        }
        match config.provider_type.as_str() {
            "s3" | "s8k" => build_s3_provider(profile, config).await,
            "file" => build_posix_provider(config),
            "gcs" | "azure" => Err(MscError::unsupported(
                "GCS and Azure are deferred in the Rust foundation",
            )),
            _ => Err(MscError::new(
                ErrorKind::Unsupported,
                "register the configured provider through ProviderRegistry",
            )),
        }
    }
}

fn build_posix_provider(config: &ProviderConfig) -> Result<Arc<dyn StorageProvider>> {
    #[cfg(unix)]
    {
        let options = serde_json::from_value(config.options_value()).map_err(|_| {
            MscError::new(
                ErrorKind::InvalidConfig,
                "invalid POSIX options; check names and types",
            )
        })?;
        Ok(Arc::new(posix::PosixProvider::new(options)?))
    }
    #[cfg(not(unix))]
    {
        let _ = config;
        Err(MscError::unsupported(
            "POSIX storage requires a Unix platform",
        ))
    }
}

#[cfg(feature = "s3")]
async fn build_s3_provider(
    profile: &ProfileConfig,
    config: &ProviderConfig,
) -> Result<Arc<dyn StorageProvider>> {
    let mut options = s3::S3Options::from_config(config)?;
    if config.provider_type == "s8k" && !config.options.contains_key("infer_content_type") {
        options.infer_content_type = true;
    }
    let credentials = crate::credentials::credentials_from_profile(profile).await?;
    let provider = if let Some(credentials) = credentials {
        s3::S3Provider::new_with_credentials_provider(options, credentials)?
    } else {
        s3::S3Provider::new_with_default_credentials(options).await?
    };
    let provider = provider.with_retry_policy(profile.retry.clone()).with_name(
        if config.provider_type == "s8k" {
            "s8k"
        } else {
            "s3"
        },
    );
    Ok(Arc::new(provider))
}

#[cfg(not(feature = "s3"))]
#[allow(clippy::unused_async)] // Keep one async call site for both feature configurations.
async fn build_s3_provider(
    _profile: &ProfileConfig,
    _config: &ProviderConfig,
) -> Result<Arc<dyn StorageProvider>> {
    Err(MscError::new(
        ErrorKind::Unsupported,
        "S3 requires the Cargo feature `s3`",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn unknown_s3_options_name_the_field_without_echoing_its_value() {
        let mut profile = ProfileConfig::implicit("s3", "bucket");
        profile
            .storage_provider
            .as_mut()
            .unwrap()
            .options
            .insert("misspelled_timeout".into(), "sensitive-option-value".into());
        let error = ProviderRegistry::new().build(&profile).await.err().unwrap();
        assert_eq!(error.kind, ErrorKind::InvalidConfig);
        assert!(error.message.contains("misspelled_timeout"));
        assert!(!format!("{error:?}").contains("sensitive-option-value"));
    }

    #[test]
    fn s8k_is_reserved_as_a_builtin_s3_alias() {
        let mut registry = ProviderRegistry::new();
        assert!(registry.contains("s8k"));
        let factory: ProviderFactory = Arc::new(|_| {
            Box::pin(async { Err(MscError::unsupported("custom factory must not run")) })
        });
        assert_eq!(
            registry.register("s8k", factory).unwrap_err().kind,
            ErrorKind::InvalidConfig
        );
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn s8k_builds_the_same_backend_with_python_credentials() {
        let mut profile = ProfileConfig::implicit("s8k", "bucket/project");
        let options = &mut profile.storage_provider.as_mut().unwrap().options;
        options.insert("region_name".into(), "us-west-2".into());
        options.insert("endpoint_url".into(), "http://127.0.0.1:1".into());
        profile.credentials_provider = Some(ProviderConfig {
            provider_type: "S3Credentials".into(),
            options: BTreeMap::from([
                ("access_key".into(), "test-access".into()),
                ("secret_key".into(), "test-secret".into()),
            ]),
        });
        let registry = ProviderRegistry::new();
        let s8k = registry.build(&profile).await.unwrap();
        profile.storage_provider.as_mut().unwrap().provider_type = "s3".into();
        let s3 = registry.build(&profile).await.unwrap();
        assert_eq!(s8k.backend_fingerprint(), s3.backend_fingerprint());
        assert_eq!(s8k.physical_root(), ObjectPath::new("project").unwrap());
        assert!(s8k.capabilities().multipart_upload);
        assert!(s8k.capabilities().presigned_get);
        s8k.shutdown().await.unwrap();
        s3.shutdown().await.unwrap();
    }

    #[cfg(not(feature = "s3"))]
    #[tokio::test]
    async fn s8k_requires_the_existing_s3_feature() {
        let profile = ProfileConfig::implicit("s8k", "bucket");
        let error = ProviderRegistry::new().build(&profile).await.err().unwrap();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(error.message, "S3 requires the Cargo feature `s3`");
    }

    #[tokio::test]
    async fn direct_factory_build_cannot_bypass_profile_validation() {
        let mut profile = ProfileConfig::implicit("s3", "bucket");
        profile.retry.max_attempts = 0;
        let result = ProviderRegistry::new().build(&profile).await;
        assert!(matches!(result, Err(error) if error.kind == ErrorKind::InvalidConfig));
    }

    #[tokio::test]
    async fn unsupported_provider_error_does_not_echo_configuration_values() {
        let profile = ProfileConfig::implicit("private-provider-selector", "bucket");
        let error = ProviderRegistry::new().build(&profile).await.err().unwrap();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert!(!format!("{error:?} {error}").contains("private-provider-selector"));

        #[cfg(feature = "s3")]
        {
            let mut profile = ProfileConfig::implicit("s3", "bucket");
            profile.credentials_provider = Some(ProviderConfig {
                provider_type: "private-credentials-selector".into(),
                options: BTreeMap::new(),
            });
            let error = ProviderRegistry::new().build(&profile).await.err().unwrap();
            assert_eq!(error.kind, ErrorKind::Unsupported);
            assert!(!format!("{error:?} {error}").contains("private-credentials-selector"));
        }
    }
}
