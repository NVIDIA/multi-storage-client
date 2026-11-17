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

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use super::ConfigError;
use super::document::{absolute, locate, merge_document, read_document, record_origins};
use super::redaction::redact;
use super::schema::ProfileConfig;
use super::validation::{pointer, validate_document, validate_profiles};

/// Loads the first existing path, then its ordered one-level includes.
#[derive(Clone, Default)]
pub struct ConfigLoader {
    explicit_paths: Option<Vec<PathBuf>>,
    registry: crate::ProviderRegistry,
    environment: Option<BTreeMap<String, String>>,
}

impl ConfigLoader {
    /// Use normal configuration discovery and process environment expansion.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace discovery with ordered candidates; load the first existing file.
    /// Explicit candidates suppress environment and default-path discovery.
    #[must_use]
    pub fn with_explicit_paths<I, P>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        self.explicit_paths = Some(paths.into_iter().map(Into::into).collect());
        self
    }

    /// Recognize custom storage tags while validating profile references.
    /// Supply the same registry to the client builder to construct those providers.
    #[must_use]
    pub fn with_registry(mut self, registry: crate::ProviderRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Supplies a deterministic environment instead of reading process variables.
    #[must_use]
    pub fn with_environment(mut self, environment: BTreeMap<String, String>) -> Self {
        self.environment = Some(environment);
        self
    }

    /// Loads and validates configuration without contacting providers or exporters.
    ///
    /// # Errors
    /// Returns a source-aware diagnostic for unreadable files, invalid schemas,
    /// missing environment variables, include conflicts, or invalid routing.
    #[allow(clippy::too_many_lines)] // Keep the ordered resolution and provenance flow visible.
    pub fn load(&self) -> Result<ResolvedConfig, ConfigError> {
        let environment = self.environment.clone().unwrap_or_else(|| {
            std::env::vars_os()
                .filter_map(|(key, value)| {
                    Some((key.into_string().ok()?, value.into_string().ok()?))
                })
                .collect()
        });
        let paths = self
            .explicit_paths
            .clone()
            .unwrap_or_else(|| discovery_paths(&environment));
        let mut primary = None;
        for path in paths {
            let path = absolute(&path)?;
            match std::fs::metadata(&path) {
                Ok(_) => {
                    primary = Some(path);
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    return Err(
                        ConfigError::new("/", "cannot inspect configuration path").at_source(path)
                    );
                }
            }
        }
        let mut origins = BTreeMap::new();
        let mut sources = Vec::new();
        let mut secret_paths = BTreeSet::new();
        let mut document = Value::Object(Map::new());
        if let Some(primary) = primary {
            document = read_document(&primary, &environment, &mut secret_paths)?;
            let include_paths = document
                .as_object_mut()
                .ok_or_else(|| ConfigError::new("/", "expected a mapping").at_source(&primary))?
                .remove("include")
                .unwrap_or_else(|| Value::Array(Vec::new()));
            let include_paths = include_paths.as_array().ok_or_else(|| {
                ConfigError::new("/include", "include must be an ordered list").at_source(&primary)
            })?;
            record_origins(&document, "", &primary, &mut origins);
            sources.push(primary.clone());
            for (index, include) in include_paths.iter().enumerate() {
                let include = include.as_str().ok_or_else(|| {
                    ConfigError::new(format!("/include/{index}"), "include path must be a string")
                        .at_source(&primary)
                })?;
                let source = absolute(&primary.parent().unwrap_or(Path::new("/")).join(include))?;
                let mut included_secrets = BTreeSet::new();
                let included = read_document(&source, &environment, &mut included_secrets)?;
                if included.get("include").is_some() {
                    return Err(
                        ConfigError::new("/include", "nested includes are not supported")
                            .at_source(&source),
                    );
                }
                secret_paths.extend(included_secrets);
                merge_document(&mut document, &included, &source, &mut origins)?;
                sources.push(source);
            }
        }
        validate_document(&document).map_err(|error| locate(error, &origins))?;
        let mut profiles: BTreeMap<String, ProfileConfig> =
            deserialize(document.get("profiles"), "/profiles")
                .map_err(|error| locate(error, &origins))?
                .unwrap_or_default();
        let filesystem = ProfileConfig::filesystem();
        if profiles
            .get("__filesystem__")
            .is_some_and(|profile| profile != &filesystem)
        {
            return Err(locate(
                ConfigError::new(
                    "/profiles/__filesystem__",
                    "the reserved filesystem profile must match the implicit root configuration",
                ),
                &origins,
            ));
        }
        profiles
            .entry("__filesystem__".into())
            .or_insert(filesystem);
        validate_profiles(&profiles, &self.registry).map_err(|error| locate(error, &origins))?;
        let path_mapping: BTreeMap<String, String> =
            deserialize(document.get("path_mapping"), "/path_mapping")
                .map_err(|error| locate(error, &origins))?
                .unwrap_or_default();
        super::urls::validate_mappings(&path_mapping, &profiles)
            .map_err(|error| locate(error, &origins))?;
        Ok(ResolvedConfig {
            profiles,
            path_mapping,
            sources,
            origins,
            document,
            secret_paths,
        })
    }
}

/// Effective configuration. Debug and Display always redact secret values.
#[derive(Clone)]
pub struct ResolvedConfig {
    /// Effective profiles, including the implicit `__filesystem__` profile.
    /// Raw provider options may contain secrets; use [`Self::redacted_document`]
    /// for displays.
    pub profiles: BTreeMap<String, ProfileConfig>,
    /// Path-prefix substitutions; the longest matching prefix wins.
    pub path_mapping: BTreeMap<String, String>,
    /// Primary and included files that contributed to this configuration.
    pub sources: Vec<PathBuf>,
    origins: BTreeMap<String, PathBuf>,
    document: Value,
    secret_paths: BTreeSet<String>,
}

impl ResolvedConfig {
    /// Looks up a configured profile by its exact name.
    ///
    /// # Errors
    /// Returns a diagnostic when the profile does not exist.
    pub fn profile(&self, name: &str) -> Result<&ProfileConfig, ConfigError> {
        self.profiles.get(name).ok_or_else(|| {
            ConfigError::new(
                format!("/profiles/{}", pointer(name)),
                "profile does not exist",
            )
        })
    }

    /// Returns a printable document, including the implicit filesystem profile.
    #[must_use]
    pub fn redacted_document(&self) -> Value {
        let mut document = self.document.clone();
        if let Some(root) = document.as_object_mut() {
            root.insert(
                "profiles".into(),
                Value::Object(
                    self.profiles
                        .iter()
                        .map(|(name, profile)| (name.clone(), profile.document()))
                        .collect(),
                ),
            );
        }
        for path in &self.secret_paths {
            if let Some(secret) = self.document.pointer(path) {
                super::redaction::redact_matching(&mut document, secret);
            }
        }
        redact(&document, false)
    }

    /// Look up the contributing file for a JSON-pointer-style field path.
    #[must_use]
    pub fn source_for(&self, field_path: &str) -> Option<&Path> {
        self.origins.get(field_path).map(PathBuf::as_path)
    }

    /// Checks global features before client construction.
    ///
    /// # Errors
    /// Returns an actionable error for deferred runtime functionality.
    pub fn validate_runtime_support(&self) -> Result<(), ConfigError> {
        for (name, profile) in &self.profiles {
            profile.validate_runtime_support().map_err(|mut error| {
                error.field_path = format!("/profiles/{}{}", pointer(name), error.field_path);
                locate(error, &self.origins)
            })?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for ResolvedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedConfig")
            .field("document", &self.redacted_document())
            .field("sources", &self.sources)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ResolvedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let document =
            serde_json::to_string_pretty(&self.redacted_document()).map_err(|_| std::fmt::Error)?;
        f.write_str(&document)
    }
}

fn deserialize<T: serde::de::DeserializeOwned>(
    value: Option<&Value>,
    path: &str,
) -> Result<Option<T>, ConfigError> {
    value
        .map(|value| {
            serde_json::from_value(value.clone()).map_err(|_| {
                ConfigError::new(
                    path,
                    concat!(
                        "value does not match the typed MSC schema; ",
                        "substituted strings are not coerced into numbers or booleans"
                    ),
                )
            })
        })
        .transpose()
}

pub(super) fn discovery_paths(environment: &BTreeMap<String, String>) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(path) = environment
        .get("MSC_CONFIG")
        .filter(|path| !path.is_empty())
    {
        paths.push(PathBuf::from(path));
    }
    let mut config_pair = |base: PathBuf, stem: &str| {
        paths.push(base.join(format!("{stem}.yaml")));
        paths.push(base.join(format!("{stem}.json")));
    };
    if let Some(base) = environment
        .get("XDG_CONFIG_HOME")
        .filter(|base| !base.is_empty())
    {
        config_pair(Path::new(base).join("msc"), "config");
    }
    if let Some(home) = environment.get("HOME").filter(|home| !home.is_empty()) {
        config_pair(PathBuf::from(home), ".msc_config");
        config_pair(Path::new(home).join(".config/msc"), "config");
    }
    for base in environment
        .get("XDG_CONFIG_DIRS")
        .map_or("/etc/xdg", String::as_str)
        .split(':')
        .filter(|base| !base.is_empty())
    {
        config_pair(Path::new(base).join("msc"), "config");
    }
    config_pair(PathBuf::from("/etc"), "msc_config");
    paths
}
