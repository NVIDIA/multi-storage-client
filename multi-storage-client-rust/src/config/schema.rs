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

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use super::ConfigError;
use super::redaction::redact;

/// Provider-specific options remain open; MSC-owned fields are strict.
///
/// Secret-bearing provider configurations intentionally do not implement
/// `Serialize`. Print [`super::ResolvedConfig::redacted_document`] instead.
#[derive(Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    /// Built-in or registered tag, serialized as the YAML/JSON field `type`.
    #[serde(rename = "type")]
    pub provider_type: String,
    /// Provider-owned options. Built-in adapters validate their own option schema.
    #[serde(default)]
    pub options: BTreeMap<String, Value>,
}

impl std::fmt::Debug for ProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderConfig")
            // Cloned profiles do not retain environment-expansion provenance.
            .field("provider_type", &"[REDACTED]")
            .field("options", &redact(&self.options_value(), true))
            .finish()
    }
}

impl ProviderConfig {
    pub(crate) fn options_value(&self) -> Value {
        Value::Object(self.options.clone().into_iter().collect())
    }

    fn document(&self) -> Value {
        serde_json::json!({"type": self.provider_type, "options": self.options})
    }
}

/// A single physical backend and its client retry policy.
#[derive(Clone, PartialEq, Deserialize)]
pub struct ProfileConfig {
    /// Backend configuration. Required for configured profiles.
    pub storage_provider: Option<ProviderConfig>,
    /// Explicit credentials; omission enables backend discovery.
    #[serde(default)]
    pub credentials_provider: Option<ProviderConfig>,
    /// Shared operation retry budget.
    #[serde(default)]
    pub retry: crate::RetryPolicy,
    /// Human-readable annotation.
    #[serde(default)]
    pub comment: Option<String>,
    /// Recognized settings for features outside the physical-client foundation.
    #[serde(flatten)]
    pub deferred: BTreeMap<String, Value>,
}
impl std::fmt::Debug for ProfileConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfileConfig")
            .field("storage_provider", &self.storage_provider)
            .field("credentials_provider", &self.credentials_provider)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}
impl ProfileConfig {
    pub(super) fn document(&self) -> Value {
        let mut document = serde_json::json!({
            "storage_provider": self.storage_provider.as_ref().map(ProviderConfig::document),
            "credentials_provider": self.credentials_provider.as_ref().map(ProviderConfig::document),
            "retry": self.retry,
            "comment": self.comment,
        });
        document
            .as_object_mut()
            .expect("profile document")
            .extend(self.deferred.clone());
        document
    }
    pub(crate) fn filesystem() -> Self {
        Self::implicit("file", "/")
    }
    pub(crate) fn implicit(provider_type: &str, base_path: &str) -> Self {
        Self {
            storage_provider: Some(ProviderConfig {
                provider_type: provider_type.into(),
                options: BTreeMap::from([("base_path".into(), Value::String(base_path.into()))]),
            }),
            credentials_provider: None,
            retry: crate::RetryPolicy::default(),
            comment: None,
            deferred: BTreeMap::new(),
        }
    }
    /// Validate that the profile selects a supported physical backend.
    ///
    /// # Errors
    /// Returns an actionable diagnostic for a deferred or missing backend.
    pub fn validate_runtime_support(&self) -> Result<(), ConfigError> {
        for (field, value) in &self.deferred {
            if !value.is_null() && value != &Value::Bool(false) {
                return Err(ConfigError::new(
                    format!("/{field}"),
                    "this profile behavior is deferred in the Rust foundation",
                ));
            }
        }
        let provider = self.storage_provider.as_ref().ok_or_else(|| {
            ConfigError::new(
                "/storage_provider",
                "a physical storage provider is required",
            )
        })?;
        if matches!(provider.provider_type.as_str(), "gcs" | "azure" | "dss") {
            return Err(ConfigError::new(
                "/storage_provider/type",
                "this backend is deferred; use file, s3, or s8k",
            ));
        }
        Ok(())
    }
}
