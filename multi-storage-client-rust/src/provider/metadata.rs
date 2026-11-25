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

//! Logical metadata providers and shared physical symlink resolution.
use super::StorageProvider;
use crate::{
    ErrorKind, InfoOptions, ListOptions, MetadataStream, MscError, ObjectKind, ObjectMetadata,
    ObjectPath, Result,
};
use async_trait::async_trait;

/// The state of a logical object in committed metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedPathState {
    /// An object currently exists.
    Exists,
    /// A previously tracked object was soft-deleted.
    Deleted,
    /// The path has never been tracked or has been hard-deleted.
    Untracked,
}

/// A logical path's resolution within a storage profile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPath {
    /// Key relative to the selected physical storage profile.
    pub physical_path: ObjectPath,
    /// State in the committed metadata view.
    pub state: ResolvedPathState,
    /// Optional name of the physical storage profile. `None` uses the current profile.
    pub profile: Option<String>,
}

/// Logical metadata and path mapping, with the same operations as Python MSC.
/// Implementations may perform network I/O and coordinate their own allocations,
/// staging, and commits across concurrent callers.
#[async_trait]
pub trait MetadataProvider: Send + Sync {
    /// List committed logical objects under a directory, with lexical bounds and filtering.
    async fn list_objects(&self, options: ListOptions) -> Result<MetadataStream>;

    /// Get file or directory metadata, optionally including staged changes.
    async fn get_object_metadata(
        &self,
        path: &str,
        include_pending: bool,
    ) -> Result<ObjectMetadata>;

    /// Match committed logical keys by pattern and an optional attribute expression.
    async fn glob(
        &self,
        pattern: &str,
        attribute_filter_expression: Option<&str>,
    ) -> Result<Vec<ObjectPath>>;

    /// Resolve a logical key in committed metadata, following logical symlinks.
    async fn realpath(&self, logical_path: &str) -> Result<ResolvedPath>;

    /// Resolve copy bytes and inherited attributes from one committed catalog view.
    ///
    /// Return metadata for the resolved file with the logical source entry's
    /// attributes and the physical source version if the key can be overwritten.
    /// Without a version the key must identify immutable bytes for the copy.
    async fn resolve_copy_source(&self, _path: &str) -> Result<(ResolvedPath, ObjectMetadata)> {
        Err(MscError::unsupported("atomic logical copy source lookup"))
    }

    /// Plan deletion from one committed catalog view. Return `None` to retain
    /// physical bytes and remove only the logical entry.
    ///
    /// Return a physical target only for an exclusively owned regular file.
    /// The provider must ensure the target cannot be reassigned, overwritten,
    /// or shared while the delete and subsequent metadata staging are in flight.
    async fn resolve_delete_target(&self, path: &str) -> Result<Option<ResolvedPath>> {
        let entry = self.get_object_metadata(path, false).await?;
        if entry.kind == ObjectKind::Directory {
            return Err(MscError::unsupported("logical directory deletion"));
        }
        if self.should_use_soft_delete() || entry.kind == ObjectKind::Symlink {
            return Ok(None);
        }
        Err(MscError::unsupported(
            "hard delete without exclusive catalog ownership",
        ))
    }

    /// Allocate a physical key for a new object or overwrite.
    /// Independently built clients sharing a catalog must coordinate writes
    /// through the provider, including allocation and staging.
    async fn generate_physical_path(
        &self,
        _logical_path: &str,
        _for_overwrite: bool,
    ) -> Result<ResolvedPath> {
        Err(MscError::unsupported("metadata write path allocation"))
    }

    /// Stage an idempotent addition or update using metadata for the physical object.
    async fn add_file(&self, _path: &str, _metadata: ObjectMetadata) -> Result<()> {
        Err(MscError::unsupported("metadata addition"))
    }

    /// Stage an idempotent removal by logical key.
    async fn remove_file(&self, _path: &str) -> Result<()> {
        Err(MscError::unsupported("metadata removal"))
    }

    /// Persist staged updates and make them visible to committed readers.
    async fn commit_updates(&self) -> Result<()> {
        Err(MscError::unsupported("metadata commit"))
    }

    /// Whether the provider supports mutations.
    fn is_writable(&self) -> bool {
        false
    }

    /// Whether the provider permits overwriting a tracked path.
    fn allow_overwrites(&self) -> bool {
        false
    }

    /// Whether removals retain physical data and record a deleted logical state.
    fn should_use_soft_delete(&self) -> bool {
        false
    }
}

pub(super) const MAX_SYMLINK_HOPS: usize = 8;

pub(crate) async fn resolve_metadata<P: StorageProvider + ?Sized>(
    provider: &P,
    path: &str,
    options: InfoOptions,
) -> Result<ObjectMetadata> {
    let mut key = ObjectPath::new(path)?;
    let mut visited = std::collections::BTreeSet::new();
    let mut path = path.to_owned();
    for hop in 0..=MAX_SYMLINK_HOPS {
        if !visited.insert(key.clone()) {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "symlink cycle detected",
            ));
        }
        let metadata = provider.get_object_metadata(&path, options).await?;
        if metadata.kind != ObjectKind::Symlink {
            return Ok(metadata);
        }
        if hop == MAX_SYMLINK_HOPS {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "symlink hop limit exceeded",
            ));
        }
        let target = metadata
            .symlink_target
            .ok_or_else(|| MscError::new(ErrorKind::Integrity, "symlink has no target"))?;
        key = key.resolve_link(&target)?;
        path = key.to_string();
    }
    unreachable!("bounded symlink resolution")
}
