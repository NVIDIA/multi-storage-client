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

use super::StorageClient;
use crate::{
    BatchFailure, BatchResult, CopyOptions, DeleteOptions, ErrorKind, MetadataProvider, MscError,
    ObjectKind, ObjectMetadata, ObjectPath, ResolvedPath, ResolvedPathState, Result,
    StorageProvider, WriteOptions,
};
use futures::{StreamExt, stream};
use serde_json::Value;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    hash::{BuildHasher, BuildHasherDefault, DefaultHasher},
};
use tokio::sync::MutexGuard;

const METADATA_DELETE_CONCURRENCY: usize = 32;

impl StorageClient {
    pub(super) async fn read_path<'a>(&self, path: &'a str) -> Result<Cow<'a, str>> {
        let Some(metadata) = self.metadata_provider.as_deref() else {
            return Ok(Cow::Borrowed(path));
        };
        Ok(Cow::Owned(
            self.existing_physical_path(metadata.realpath(path).await?)?
                .into_string(),
        ))
    }

    fn existing_physical_path(&self, resolved: ResolvedPath) -> Result<ObjectPath> {
        if resolved.state != ResolvedPathState::Exists {
            return Err(MscError::new(
                ErrorKind::NotFound,
                "logical object does not exist",
            ));
        }
        self.physical_path(resolved)
    }

    pub(super) async fn write_path(
        &self,
        metadata: &dyn MetadataProvider,
        path: &str,
        options: &WriteOptions,
    ) -> Result<ObjectPath> {
        if path.is_empty() {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "logical destination must be a nonempty key",
            ));
        }
        if options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.version.is_some()
        {
            return Err(MscError::unsupported("conditional logical destination"));
        }
        let overwrite = self.logical_entry_exists(metadata, path).await?;
        if overwrite && !metadata.allow_overwrites() {
            return Err(MscError::new(
                ErrorKind::AlreadyExists,
                "logical object already exists",
            ));
        }
        let resolved = metadata.generate_physical_path(path, overwrite).await?;
        self.physical_path(resolved)
    }

    pub(super) async fn logical_entry_exists(
        &self,
        metadata: &dyn MetadataProvider,
        path: &str,
    ) -> Result<bool> {
        for include_pending in [true, false] {
            match metadata.get_object_metadata(path, include_pending).await {
                Ok(_) => return Ok(true),
                Err(error) if error.kind == ErrorKind::NotFound => (),
                Err(error) => return Err(error),
            }
        }
        Ok(metadata.realpath(path).await?.state == ResolvedPathState::Deleted)
    }

    pub(super) async fn lock_metadata(&self, path: &ObjectPath) -> Option<MutexGuard<'_, ()>> {
        let locks = self.metadata_locks.as_deref()?;
        let hash = BuildHasherDefault::<DefaultHasher>::default().hash_one(path);
        let index = usize::try_from(hash % locks.len() as u64).expect("lock index fits in usize");
        Some(locks[index].lock().await)
    }

    pub(super) async fn lock_all_metadata(&self) -> Vec<MutexGuard<'_, ()>> {
        let mut guards = Vec::new();
        if let Some(locks) = &self.metadata_locks {
            for lock in locks.iter() {
                guards.push(lock.lock().await);
            }
        }
        guards
    }

    fn physical_path(&self, resolved: ResolvedPath) -> Result<ObjectPath> {
        if resolved.profile.is_some() && resolved.profile != self.profile {
            return Err(MscError::unsupported("routing to another storage profile"));
        }
        Ok(resolved.physical_path)
    }

    pub(super) fn writable_metadata(&self) -> Result<Option<&dyn MetadataProvider>> {
        let metadata = self.metadata_provider.as_deref();
        if metadata.is_some_and(|provider| !provider.is_writable()) {
            return Err(MscError::unsupported(
                "mutation of a read-only metadata namespace",
            ));
        }
        Ok(metadata)
    }

    pub(super) async fn register_write(
        metadata: &dyn MetadataProvider,
        logical: &str,
        entry: ObjectMetadata,
    ) -> Result<()> {
        let physical = entry.key.clone();
        metadata.add_file(logical, entry).await.map_err(|_| {
            MscError::new(
                ErrorKind::AmbiguousOutcome,
                "physical object was written before logical metadata staging failed",
            )
            .context("register_write", "metadata", physical.as_str())
        })
    }

    pub(super) async fn mapped_copy_source(
        &self,
        metadata: &dyn MetadataProvider,
        path: &str,
        options: &mut CopyOptions,
        attributes: &mut BTreeMap<String, Value>,
    ) -> Result<ObjectPath> {
        if !attributes.is_empty() {
            return self.existing_physical_path(metadata.realpath(path).await?);
        }
        let (resolved, entry) = metadata.resolve_copy_source(path).await?;
        let physical = self.existing_physical_path(resolved)?;
        if entry.kind != ObjectKind::File {
            return Err(MscError::unsupported(
                "logical copy source must be a regular file",
            ));
        }
        if let Some(version) = entry.source_version {
            if options
                .source_conditions
                .version
                .as_ref()
                .is_some_and(|value| *value != version)
            {
                return Err(MscError::new(
                    ErrorKind::PreconditionFailed,
                    "copy source version differs from the logical snapshot",
                ));
            }
            options.source_conditions.version = Some(version);
        }
        *attributes = entry.attributes;
        Ok(physical)
    }

    pub(super) async fn delete_mapped(
        &self,
        metadata: &dyn MetadataProvider,
        path: &str,
        options: DeleteOptions,
    ) -> Result<()> {
        if let Some(physical) = self.mapped_delete_path(metadata, path, &options).await? {
            let missing_ok = options.is_unconditional();
            match self
                .provider
                .delete_object(physical.as_str(), options)
                .await
            {
                Ok(()) => (),
                Err(error) if missing_ok && error.kind == ErrorKind::NotFound => (),
                Err(error) => return Err(error),
            }
        }
        metadata.remove_file(path).await
    }

    async fn mapped_delete_path(
        &self,
        metadata: &dyn MetadataProvider,
        path: &str,
        options: &DeleteOptions,
    ) -> Result<Option<ObjectPath>> {
        let target = metadata.resolve_delete_target(path).await?;
        if target.is_none() && !options.is_unconditional() {
            return Err(MscError::unsupported("conditional metadata-only deletion"));
        }
        target
            .map(|target| self.existing_physical_path(target))
            .transpose()
    }

    pub(super) async fn delete_many_mapped(
        &self,
        metadata: &dyn MetadataProvider,
        paths: &[ObjectPath],
        options: DeleteOptions,
    ) -> Result<BatchResult> {
        let mut result = BatchResult::default();
        let mut physical = BTreeMap::<ObjectPath, Vec<usize>>::new();
        let mut stage = Vec::new();
        {
            let mut plans = stream::iter(paths.iter().enumerate())
                .map(|(index, path)| {
                    let options = &options;
                    async move {
                        (
                            index,
                            self.mapped_delete_path(metadata, path.as_str(), options)
                                .await,
                        )
                    }
                })
                .buffer_unordered(METADATA_DELETE_CONCURRENCY);
            while let Some((index, plan)) = plans.next().await {
                match plan {
                    Ok(Some(key)) => physical.entry(key).or_default().push(index),
                    Ok(None) => stage.push(index),
                    Err(error)
                        if options.is_unconditional() && error.kind == ErrorKind::NotFound =>
                    {
                        result.succeeded.push(index);
                    }
                    Err(error) => result
                        .failed
                        .push(mapped_delete_failure(paths, index, error)),
                }
            }
        }

        if !physical.is_empty() {
            let (keys, logical_indices): (Vec<_>, Vec<_>) = physical.into_iter().unzip();
            let deleted = self.provider.delete_objects(&keys, options).await?;
            for index in deleted.succeeded {
                stage.extend_from_slice(&logical_indices[index]);
            }
            for failure in deleted.failed {
                for &index in &logical_indices[failure.index] {
                    result
                        .failed
                        .push(mapped_delete_failure(paths, index, failure.error.clone()));
                }
            }
        }

        let mut staging = stream::iter(stage)
            .map(|index| async move { (index, metadata.remove_file(paths[index].as_str()).await) })
            .buffer_unordered(METADATA_DELETE_CONCURRENCY);
        while let Some((index, outcome)) = staging.next().await {
            match outcome {
                Ok(()) => result.succeeded.push(index),
                Err(error) => result
                    .failed
                    .push(mapped_delete_failure(paths, index, error)),
            }
        }
        result.succeeded.sort_unstable();
        result.failed.sort_by_key(|failure| failure.index);
        Ok(result)
    }
}

fn mapped_delete_failure(paths: &[ObjectPath], index: usize, error: MscError) -> BatchFailure {
    BatchFailure {
        index,
        source: paths[index].clone(),
        destination: None,
        error,
    }
}
