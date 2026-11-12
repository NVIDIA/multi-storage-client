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

//! Validated provider-relative object paths.
use crate::error::{ErrorKind, MscError, Result};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

/// A normalized UTF-8 key relative to a profile's configured base path.
/// The empty key denotes the profile root; leading slashes and dot segments
/// are rejected. URLs and absolute local paths belong to
/// [`ResolvedConfig::resolve`](crate::ResolvedConfig::resolve), not this type.
///
/// ```
/// use multi_storage_client::ObjectPath;
/// let root = ObjectPath::default();
/// let key = ObjectPath::new("datasets//train/")?;
/// assert_eq!(key.as_str(), "datasets/train");
/// assert_eq!(root.join(&key)?, key);
/// assert!(ObjectPath::new("../outside").is_err());
/// # Ok::<(), multi_storage_client::MscError>(())
/// ```
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ObjectPath(String);

impl<'de> Deserialize<'de> for ObjectPath {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let key = String::deserialize(deserializer)?;
        Self::new(key).map_err(serde::de::Error::custom)
    }
}
impl ObjectPath {
    /// Normalize duplicate separators and reject absolute keys or traversal.
    ///
    /// # Errors
    /// Returns `InvalidPath` for absolute paths, dot segments, backslashes, or NULs.
    pub fn new(key: impl AsRef<str>) -> Result<Self> {
        let key = key.as_ref();
        if key.starts_with('/') || key.contains('\\') || key.contains('\0') {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "keys must be relative and use forward slashes",
            ));
        }
        let segments: Vec<_> = key.split('/').filter(|part| !part.is_empty()).collect();
        if segments.iter().any(|part| matches!(*part, "." | "..")) {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "dot and parent segments are forbidden",
            ));
        }
        Ok(Self(segments.join("/")))
    }
    /// Borrow the normalized relative key without a leading or trailing slash.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Whether this key denotes the configured profile root.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }
    /// Append a normalized key beneath this prefix.
    ///
    /// # Errors
    /// Returns `InvalidPath` if the combined path violates key validation.
    pub fn join(&self, suffix: &Self) -> Result<Self> {
        Self::new(if self.is_root() {
            suffix.0.clone()
        } else if suffix.is_root() {
            self.0.clone()
        } else {
            format!("{}/{}", self.0, suffix.0)
        })
    }
    /// Resolve a relative symlink target without permitting escape from root.
    ///
    /// # Errors
    /// Returns `InvalidPath` for absolute targets or traversal beyond the root.
    pub fn resolve_link(&self, target: &str) -> Result<Self> {
        if target.starts_with('/') || target.contains('\\') || target.contains('\0') {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "symlink targets must be relative",
            ));
        }
        let mut parts: Vec<&str> = self.0.split('/').collect();
        parts.pop();
        for part in target.split('/') {
            match part {
                "" | "." => (),
                ".." => {
                    if parts.pop().is_none() {
                        return Err(MscError::new(
                            ErrorKind::InvalidPath,
                            "symlink target escapes the profile root",
                        ));
                    }
                }
                _ => parts.push(part),
            }
        }
        Self::new(parts.join("/"))
    }
}
impl fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl AsRef<str> for ObjectPath {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl TryFrom<&str> for ObjectPath {
    type Error = MscError;
    fn try_from(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paths_reject_escape_and_normalize() {
        assert_eq!(ObjectPath::new("α//a/").unwrap().as_str(), "α/a");
        for key in ["/a", "a/../b", "a/./b", "a\\b", "a\0b"] {
            assert!(ObjectPath::new(key).is_err());
        }
        let key = ObjectPath::new("a/link").unwrap();
        assert_eq!(key.resolve_link("../target").unwrap().as_str(), "target");
        assert!(key.resolve_link("../../target").is_err());
    }
}
