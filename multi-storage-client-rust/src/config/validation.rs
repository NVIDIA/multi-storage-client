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

//! Strict MSC field shapes and profile reference validation.
use std::collections::BTreeMap;

use serde_json::Value;

use super::ConfigError;
use super::schema::ProfileConfig;

pub(super) fn validate_document(document: &Value) -> Result<(), ConfigError> {
    validate_profile_fields(document)?;
    Ok(())
}

fn validate_profile_fields(document: &Value) -> Result<(), ConfigError> {
    if let Some(profiles) = document.get("profiles") {
        let profiles = profiles
            .as_object()
            .ok_or_else(|| ConfigError::new("/profiles", "profiles must be a mapping"))?;
        for (name, profile) in profiles {
            let path = format!("/profiles/{}", pointer(name));
            reject_unknown(
                profile,
                &[
                    "storage_provider",
                    "credentials_provider",
                    "metadata_provider",
                    "provider_bundle",
                    "storage_provider_profiles",
                    "replicas",
                    "retry",
                    "autocommit",
                    "comment",
                    "caching_enabled",
                ],
                &path,
            )?;
            validate_provider_fields(profile, &path)?;
            if let Some(retry) = profile.get("retry") {
                reject_unknown(
                    retry,
                    &[
                        "max_attempts",
                        "initial_delay_ms",
                        "multiplier",
                        "max_delay_ms",
                        "jitter",
                        "attempts",
                        "delay",
                        "backoff_multiplier",
                    ],
                    &format!("{path}/retry"),
                )?;
            }
        }
    }
    Ok(())
}

fn validate_provider_fields(profile: &Value, path: &str) -> Result<(), ConfigError> {
    for kind in ["storage_provider", "credentials_provider"] {
        if let Some(provider) = profile.get(kind).filter(|provider| !provider.is_null()) {
            let path = format!("{path}/{kind}");
            reject_unknown(provider, &["type", "options"], &path)?;
            if provider
                .get("options")
                .is_some_and(|options| !options.is_object())
            {
                return Err(ConfigError::new(
                    format!("{path}/options"),
                    "provider options must be a mapping",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn reject_unknown(
    value: &Value,
    allowed: &[&str],
    path: &str,
) -> Result<(), ConfigError> {
    let fields = value
        .as_object()
        .ok_or_else(|| ConfigError::new(path, "expected a mapping"))?;
    for field in fields.keys() {
        if !allowed.contains(&field.as_str()) {
            return Err(ConfigError::new(
                format!("{path}/{}", pointer(field)),
                "unknown MSC configuration field",
            ));
        }
    }
    Ok(())
}

pub(super) fn pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

pub(super) fn validate_profiles(
    profiles: &BTreeMap<String, ProfileConfig>,
    registry: &crate::ProviderRegistry,
) -> Result<(), ConfigError> {
    for (name, profile) in profiles {
        let path = format!("/profiles/{}", pointer(name));
        if name.is_empty() || (name.starts_with('_') && name != "__filesystem__") {
            return Err(ConfigError::new(
                &path,
                "profile names cannot be empty or begin with an underscore",
            ));
        }
        profile.retry.validate().map_err(|_| {
            ConfigError::new(
                format!("{path}/retry"),
                "retry attempts, delays, and multiplier are invalid",
            )
        })?;
        if let Some(provider) = &profile.storage_provider {
            let provider_path = format!("{path}/storage_provider");
            if registry.contains(&provider.provider_type)
                && !provider
                    .options
                    .get("base_path")
                    .is_some_and(Value::is_string)
            {
                return Err(ConfigError::new(
                    format!("{provider_path}/options/base_path"),
                    "storage providers require a string base_path, which may be empty",
                ));
            }
        }
    }
    Ok(())
}
