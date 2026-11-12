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

//! Descriptor-relative POSIX storage with atomic publication and bounded I/O.
#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::PosixProvider;

use serde::Deserialize;
use std::path::PathBuf;

/// Local filesystem settings. The root must already exist and be absolute.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PosixOptions {
    /// Existing absolute directory anchoring all profile-relative operations.
    pub base_path: PathBuf,
    /// Maximum concurrent local operations; defaults to 32 and must be positive.
    #[serde(default = "default_concurrency")]
    pub max_concurrency: usize,
    /// Preserve source permission bits during transfers; disabled by default.
    #[serde(default)]
    pub preserve_permissions: bool,
    /// Preserve source modification time during transfers; disabled by default.
    #[serde(default)]
    pub preserve_mtime: bool,
}

fn default_concurrency() -> usize {
    32
}

impl PosixOptions {
    /// Select an existing root with 32-operation concurrency and preservation disabled.
    #[must_use]
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
            max_concurrency: default_concurrency(),
            preserve_permissions: false,
            preserve_mtime: false,
        }
    }
}
