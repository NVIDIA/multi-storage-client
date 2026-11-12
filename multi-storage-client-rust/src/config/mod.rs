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

//! Strict, source-aware MSC configuration and provider-relative URL resolution.

mod document;
mod loader;
mod redaction;
mod schema;
mod urls;
mod validation;

pub use loader::{ConfigLoader, ResolvedConfig};
pub use schema::{ProfileConfig, ProviderConfig};
pub use urls::ResolvedUrl;

use std::path::PathBuf;

/// A configuration diagnostic. Messages deliberately omit configuration values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError {
    /// Contributing configuration file, when known.
    pub source: Option<PathBuf>,
    /// JSON-pointer-style location of the invalid field.
    pub field_path: String,
    /// Explanation that omits raw configuration values and secrets.
    pub message: String,
}

impl ConfigError {
    pub(crate) fn new(field_path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            source: None,
            field_path: field_path.into(),
            message: message.into(),
        }
    }

    pub(crate) fn at_source(mut self, source: impl Into<PathBuf>) -> Self {
        self.source = Some(source.into());
        self
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(source) = &self.source {
            write!(f, "{}:", source.display())?;
        }
        write!(f, "{}: {}", self.field_path, self.message)
    }
}

impl std::error::Error for ConfigError {}

impl From<ConfigError> for crate::MscError {
    fn from(error: ConfigError) -> Self {
        Self::new(crate::ErrorKind::InvalidConfig, error.to_string())
    }
}

#[cfg(test)]
mod tests;
