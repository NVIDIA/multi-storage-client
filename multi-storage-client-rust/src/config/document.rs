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

//! Parsed documents, environment expansion, ordered includes, and field provenance.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use serde_json::Value;

use super::ConfigError;
use super::validation::{pointer, reject_unknown};

pub(super) fn absolute(path: &Path) -> Result<PathBuf, ConfigError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| ConfigError::new("/", "cannot determine the current directory"))?
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    Ok(normalized)
}

pub(super) fn read_document(
    source: &Path,
    environment: &BTreeMap<String, String>,
    secrets: &mut BTreeSet<String>,
) -> Result<Value, ConfigError> {
    let extension = source
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if !matches!(extension.as_str(), "json" | "yaml" | "yml") {
        return Err(ConfigError::new(
            "/",
            "unsupported configuration extension; use .json, .yaml, or .yml",
        )
        .at_source(source));
    }
    let content = std::fs::read_to_string(source).map_err(|_| {
        ConfigError::new("/", "cannot read UTF-8 configuration file").at_source(source)
    })?;
    let mut document: Value = if extension == "json" {
        serde_json::from_str(&content).map_err(|error| {
            ConfigError::new(
                "/",
                format!(
                    "invalid JSON at line {} column {}",
                    error.line(),
                    error.column()
                ),
            )
            .at_source(source)
        })?
    } else {
        serde_yaml::from_str(&content).map_err(|error| {
            let location = error.location().map_or(String::new(), |location| {
                format!(" at line {} column {}", location.line(), location.column())
            });
            ConfigError::new("/", format!("invalid YAML{location}")).at_source(source)
        })?
    };
    reject_unknown(
        &document,
        &[
            "profiles",
            "cache",
            "path_mapping",
            "experimental_features",
            "opentelemetry",
            "posix",
            "include",
        ],
        "",
    )
    .map_err(|error| error.at_source(source))?;
    expand(&mut document, "", environment, secrets).map_err(|error| error.at_source(source))?;
    Ok(document)
}

fn expand(
    value: &mut Value,
    path: &str,
    environment: &BTreeMap<String, String>,
    secrets: &mut BTreeSet<String>,
) -> Result<(), ConfigError> {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                expand(
                    value,
                    &format!("{path}/{}", pointer(key)),
                    environment,
                    secrets,
                )?;
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter_mut().enumerate() {
                expand(value, &format!("{path}/{index}"), environment, secrets)?;
            }
        }
        Value::String(string) => {
            let mut expanded = String::new();
            let mut chars = string.chars().peekable();
            while let Some(character) = chars.next() {
                if character != '$' {
                    expanded.push(character);
                    continue;
                }
                let mut name = String::new();
                if chars.peek() == Some(&'{') {
                    chars.next();
                    loop {
                        match chars.next() {
                            Some('}') => break,
                            Some(character) => name.push(character),
                            None => {
                                return Err(ConfigError::new(
                                    path,
                                    "unterminated environment substitution",
                                ));
                            }
                        }
                    }
                } else {
                    while chars.peek().is_some_and(|character| {
                        character.is_ascii_alphanumeric() || *character == '_'
                    }) {
                        name.push(chars.next().expect("peeked character"));
                    }
                    if name.is_empty() {
                        expanded.push('$');
                        continue;
                    }
                }
                if name.is_empty()
                    || !name
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '_')
                {
                    return Err(ConfigError::new(
                        path,
                        "invalid environment variable syntax",
                    ));
                }
                let replacement = environment.get(&name).ok_or_else(|| {
                    ConfigError::new(path, format!("environment variable {name} is not set"))
                })?;
                expanded.push_str(replacement);
                // Expanded values are treated as secret even for custom option names.
                secrets.insert(path.to_owned());
            }
            *string = expanded;
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn record_origins(
    value: &Value,
    path: &str,
    source: &Path,
    origins: &mut BTreeMap<String, PathBuf>,
) {
    origins
        .entry(path.to_owned())
        .or_insert_with(|| source.to_path_buf());
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                record_origins(value, &format!("{path}/{}", pointer(key)), source, origins);
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                record_origins(value, &format!("{path}/{index}"), source, origins);
            }
        }
        _ => {}
    }
}

pub(super) fn locate(mut error: ConfigError, origins: &BTreeMap<String, PathBuf>) -> ConfigError {
    let mut path = error.field_path.as_str();
    loop {
        if let Some(source) = origins.get(path) {
            error.source = Some(source.clone());
            return error;
        }
        let Some((parent, _)) = path.rsplit_once('/') else {
            return error;
        };
        path = parent;
    }
}

pub(super) fn merge_document(
    base: &mut Value,
    included: &Value,
    source: &Path,
    origins: &mut BTreeMap<String, PathBuf>,
) -> Result<(), ConfigError> {
    let base = base.as_object_mut().expect("validated root");
    for (section, value) in included.as_object().expect("validated root") {
        let path = format!("/{section}");
        match base.get_mut(section) {
            None => {
                base.insert(section.clone(), value.clone());
                record_origins(value, &path, source, origins);
            }
            Some(existing) if matches!(section.as_str(), "profiles" | "path_mapping") => {
                let existing = existing.as_object_mut().ok_or_else(|| {
                    locate(ConfigError::new(&path, "expected a mapping"), origins)
                })?;
                let entries = value.as_object().ok_or_else(|| {
                    ConfigError::new(&path, "expected a mapping").at_source(source)
                })?;
                for (key, value) in entries {
                    let entry_path = format!("{path}/{}", pointer(key));
                    match existing.get(key) {
                        Some(prior) if prior != value => {
                            return Err(conflict(&entry_path, source, origins));
                        }
                        Some(_) => {}
                        None => {
                            existing.insert(key.clone(), value.clone());
                            record_origins(value, &entry_path, source, origins);
                        }
                    }
                }
            }
            Some(existing) if existing == value => {}
            Some(_) => return Err(conflict(&path, source, origins)),
        }
    }
    Ok(())
}

fn conflict(path: &str, source: &Path, origins: &BTreeMap<String, PathBuf>) -> ConfigError {
    let prior = locate(ConfigError::new(path, ""), origins).source;
    let message = prior.map_or_else(
        || "conflicting configuration value".into(),
        |prior| format!("conflicts with definition in {}", prior.display()),
    );
    ConfigError::new(path, message).at_source(source)
}
