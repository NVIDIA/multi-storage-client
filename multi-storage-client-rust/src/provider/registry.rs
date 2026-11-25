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

//! Built-in provider construction and application-owned factories.
use super::{MetadataProvider, StorageBackend};
use crate::{
    ErrorKind, MscError, Result,
    config::{ProfileConfig, ProviderConfig},
};
use futures::future::BoxFuture;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

/// Factory for a Rust-registered SDK backend. The client builder adds shared
/// provider behavior; factories must not wrap or retry complete MSC operations.
pub type ProviderFactory =
    Arc<dyn Fn(ProfileConfig) -> BoxFuture<'static, Result<Arc<dyn StorageBackend>>> + Send + Sync>;

/// An async factory that receives the profile's `metadata_provider.options` mapping.
/// Error messages supplied by the factory must omit secrets and raw option values.
pub type MetadataProviderFactory = Arc<
    dyn Fn(BTreeMap<String, Value>) -> BoxFuture<'static, Result<Arc<dyn MetadataProvider>>>
        + Send
        + Sync,
>;

/// Application-owned factories for logical metadata providers.
#[derive(Clone, Default)]
pub struct MetadataProviderRegistry {
    factories: BTreeMap<String, MetadataProviderFactory>,
}

impl MetadataProviderRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a factory for a `metadata_provider.type` tag.
    ///
    /// # Errors
    /// Returns `InvalidConfig` if the tag is already registered.
    pub fn register(
        &mut self,
        tag: impl Into<String>,
        factory: MetadataProviderFactory,
    ) -> Result<()> {
        let tag = tag.into();
        if self.contains(&tag) {
            return Err(MscError::new(
                ErrorKind::InvalidConfig,
                format!("metadata provider tag {tag:?} is already registered"),
            ));
        }
        self.factories.insert(tag, factory);
        Ok(())
    }

    /// Check whether the application has registered a tag.
    #[must_use]
    pub fn contains(&self, tag: &str) -> bool {
        self.factories.contains_key(tag)
    }

    pub(crate) async fn build(
        &self,
        profile: &str,
        config: &ProviderConfig,
    ) -> Result<Arc<dyn MetadataProvider>> {
        let factory = self.factories.get(&config.provider_type).ok_or_else(|| {
            MscError::new(
                ErrorKind::InvalidConfig,
                format!("unknown metadata_provider.type in profile {profile:?}. Register the tag through MetadataProviderRegistry"),
            )
        })?;
        factory(config.options.clone()).await.map_err(|mut error| {
            error.message = format!(
                "metadata provider factory {:?} failed for profile {profile:?}: {}",
                config.provider_type, error.message
            );
            error
        })
    }
}

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

    /// Validate and build the raw backend selected by a single profile.
    /// Wrap it in [`super::BaseStorageProvider`] for public provider operations.
    ///
    /// # Errors
    /// Returns configuration, credential, or unsupported-provider errors before I/O.
    pub async fn build(&self, profile: &ProfileConfig) -> Result<Arc<dyn StorageBackend>> {
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

fn build_posix_provider(config: &ProviderConfig) -> Result<Arc<dyn StorageBackend>> {
    #[cfg(unix)]
    {
        let options = serde_json::from_value(config.options_value()).map_err(|_| {
            MscError::new(
                ErrorKind::InvalidConfig,
                "invalid POSIX options; check names and types",
            )
        })?;
        Ok(Arc::new(super::posix::PosixProvider::new(options)?))
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
) -> Result<Arc<dyn StorageBackend>> {
    let mut options = super::s3::S3Options::from_config(config)?;
    if config.provider_type == "s8k" && !config.options.contains_key("infer_content_type") {
        options.infer_content_type = true;
    }
    let credentials = crate::credentials::credentials_from_profile(profile).await?;
    let provider = if let Some(credentials) = credentials {
        super::s3::S3Provider::new_with_credentials_provider(options, credentials)?
    } else {
        super::s3::S3Provider::new_with_default_credentials(options).await?
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
) -> Result<Arc<dyn StorageBackend>> {
    Err(MscError::new(
        ErrorKind::Unsupported,
        "S3 requires the Cargo feature `s3`",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "s3")]
    use crate::ObjectPath;

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
