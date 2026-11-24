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

//! Public client operations over private policy and transfer implementations.
use super::{StorageClient, StorageClientBuilder};
use crate::{
    BatchResult, CopyOptions, DeleteOptions, ErrorKind, InfoOptions, ListOptions, MetadataStream,
    MscError, ObjectKind, ObjectMetadata, ObjectPath, ReadOptions, Result, StorageProvider,
    WriteOptions, config::ResolvedConfig,
};
use bytes::Bytes;
use futures::StreamExt;
use std::path::Path;

impl StorageClient {
    /// Select a profile. Construction and credentials are deferred until build.
    #[must_use]
    pub fn builder(config: ResolvedConfig, profile: impl Into<String>) -> StorageClientBuilder {
        StorageClientBuilder::from_profile(config, profile.into())
    }

    /// Query metadata with strict virtual-directory detection by default.
    /// With logical metadata, return the logical entry. Otherwise, follow physical symlinks.
    ///
    /// # Errors
    /// Returns invalid paths, missing objects, symlink cycles, or provider errors.
    pub async fn info(
        &self,
        path: impl AsRef<str>,
        options: InfoOptions,
    ) -> Result<ObjectMetadata> {
        if let Some(metadata) = &self.metadata_provider {
            if matches!(path.as_ref(), "" | "/") {
                return Ok(ObjectMetadata::directory(ObjectPath::default()));
            }
            return metadata
                .get_object_metadata(path.as_ref(), !options.strict)
                .await;
        }
        crate::provider::resolve_metadata(&self.provider, path.as_ref(), options).await
    }

    /// Test whether a path resolves to a regular object. Missing objects return false.
    ///
    /// # Errors
    /// Returns path, symlink-resolution, or provider errors other than missing objects.
    pub async fn is_file(&self, path: impl AsRef<str>) -> Result<bool> {
        if self.metadata_provider.is_some() {
            return match self.info(path.as_ref(), InfoOptions::default()).await {
                Ok(entry) if entry.kind == ObjectKind::Symlink => {
                    match self.read_path(entry.key.as_str()).await {
                        Ok(physical) => self.provider.is_file(&physical).await,
                        Err(error) if error.kind == ErrorKind::NotFound => Ok(false),
                        Err(error) => Err(error),
                    }
                }
                Ok(entry) => Ok(entry.kind == ObjectKind::File),
                Err(error) if error.kind == ErrorKind::NotFound => Ok(false),
                Err(error) => Err(error),
            };
        }
        self.provider.is_file(path.as_ref()).await
    }

    /// Test whether the backend or logical namespace lists any keys for the given prefix.
    /// With a metadata provider, the prefix selects an exact logical key or a
    /// directory and its descendants.
    ///
    /// # Errors
    /// Returns invalid paths or listing errors.
    pub async fn is_empty(&self, prefix: impl AsRef<str>) -> Result<bool> {
        let mut listing = self
            .list(ListOptions {
                prefix: ObjectPath::new(prefix)?,
                ..ListOptions::default()
            })
            .await?;
        Ok(listing.next().await.transpose()?.is_none())
    }

    /// Read bounded bytes, optionally selecting a half-open range or suffix.
    /// Default options inherit the client's configured in-memory size limit.
    ///
    /// # Errors
    /// Returns path, provider, size-limit, version-condition, or integrity errors.
    pub async fn read(&self, path: impl AsRef<str>, options: ReadOptions) -> Result<Bytes> {
        let path = self.read_path(path.as_ref()).await?;
        self.provider.get_object(&path, options).await
    }

    /// Download `remote` to `local`, creating missing parent directories and
    /// atomically publishing a temporary sibling after length/version validation.
    /// Large generation-pinned objects use the provider's multipart transfer settings.
    ///
    /// # Errors
    /// Returns path, provider, failed-condition, integrity, or local-file errors.
    pub async fn download_file(
        &self,
        remote: impl AsRef<str>,
        local: impl AsRef<Path>,
        options: ReadOptions,
    ) -> Result<()> {
        let remote = self.read_path(remote.as_ref()).await?;
        self.provider
            .download_file(&remote, local.as_ref(), options)
            .await
    }

    /// Publish bytes with the profile retry budget and validate publication.
    /// With a metadata provider, user attributes are staged in logical metadata.
    ///
    /// # Errors
    /// Returns invalid paths, failed conditions, upload, or post-upload metadata errors.
    pub async fn write(
        &self,
        path: impl AsRef<str>,
        bytes: Bytes,
        mut options: WriteOptions,
    ) -> Result<()> {
        let Some(metadata) = self.writable_metadata()? else {
            return self
                .provider
                .put_object(path.as_ref(), bytes, options)
                .await;
        };
        let logical = ObjectPath::new(path)?;
        let _guard = self.lock_metadata(&logical).await;
        let physical = self
            .write_path(metadata, logical.as_str(), &options)
            .await?;
        let attributes = std::mem::take(&mut options.attributes);
        let mut entry = self
            .provider
            .write_impl(&physical, bytes, options, true)
            .await?;
        entry.attributes.extend(attributes);
        Self::register_write(metadata, logical.as_str(), entry).await
    }

    /// Upload a regular local file to `remote` from its anchored, replayable file handle.
    ///
    /// Memory stays bounded; no whole-file temporary copy is created.
    /// The final source component is opened without following symlinks.
    /// With a metadata provider, user attributes are staged in logical metadata.
    ///
    /// # Errors
    /// Returns path, local-file, unsupported-symlink, upload, or metadata errors.
    pub async fn upload_file(
        &self,
        remote: impl AsRef<str>,
        local: impl AsRef<Path>,
        mut options: WriteOptions,
    ) -> Result<()> {
        let Some(metadata) = self.writable_metadata()? else {
            return self
                .provider
                .upload_file(remote.as_ref(), local.as_ref(), options)
                .await;
        };
        let logical = ObjectPath::new(remote)?;
        let _guard = self.lock_metadata(&logical).await;
        let physical = self
            .write_path(metadata, logical.as_str(), &options)
            .await?;
        let attributes = std::mem::take(&mut options.attributes);
        let mut entry = self
            .provider
            .upload_file_impl(&physical, local.as_ref(), options, true)
            .await?;
        entry.attributes.extend(attributes);
        Self::register_write(metadata, logical.as_str(), entry).await
    }

    /// Copy an object using the provider or a bounded streaming fallback.
    /// Empty destination attributes inherit the source attributes; nonempty
    /// attributes replace them. An omitted content type inherits the source type.
    /// With a metadata provider, attribute inheritance and replacement apply to
    /// the logical entries. The physical copy retains the source storage attributes.
    ///
    /// # Errors
    /// Returns path, unsupported-operation, condition, copy, or metadata errors.
    pub async fn copy(
        &self,
        source: impl AsRef<str>,
        destination: impl AsRef<str>,
        mut options: CopyOptions,
    ) -> Result<()> {
        let Some(metadata) = self.writable_metadata()? else {
            return self
                .provider
                .copy_object(source.as_ref(), destination.as_ref(), options)
                .await;
        };
        let logical = ObjectPath::new(destination)?;
        let _guard = self.lock_metadata(&logical).await;
        let mut attributes = std::mem::take(&mut options.destination.attributes);
        let source = self
            .mapped_copy_source(metadata, source.as_ref(), &mut options, &mut attributes)
            .await?;
        let physical = self
            .write_path(metadata, logical.as_str(), &options.destination)
            .await?;
        let mut entry = self
            .provider
            .copy_impl(&source, &physical, options, true)
            .await?;
        entry.attributes = attributes;
        Self::register_write(metadata, logical.as_str(), entry).await
    }

    /// Create a link to a target key relative to the profile root.
    ///
    /// # Errors
    /// Returns invalid/escaping targets, unsupported symlinks, or provider errors.
    pub async fn make_symlink(&self, path: impl AsRef<str>, target: &str) -> Result<()> {
        let path = path.as_ref();
        if let Some(metadata) = self.writable_metadata()? {
            let key = ObjectPath::new(path)?;
            let target = ObjectPath::new(target)?;
            if key.is_root() || target.is_root() {
                return Err(MscError::new(
                    ErrorKind::InvalidPath,
                    "symlink requires nonempty keys",
                ));
            }
            let _guard = self.lock_metadata(&key).await;
            if !metadata.allow_overwrites()
                && self.logical_entry_exists(metadata, key.as_str()).await?
            {
                return Err(MscError::new(
                    ErrorKind::AlreadyExists,
                    "logical object already exists",
                ));
            }
            let mut entry = ObjectMetadata::file(key.clone(), 0);
            entry.kind = ObjectKind::Symlink;
            entry.symlink_target = Some(crate::path::relative_link_target(&key, &target));
            entry.last_modified = Some(chrono::Utc::now());
            return metadata.add_file(key.as_str(), entry).await;
        }
        self.provider.make_symlink(path, target).await
    }

    /// Delete one exact key, failing if the requested object does not exist.
    /// Recursive deletion is outside the foundation subset.
    ///
    /// # Errors
    /// Returns invalid keys, missing objects, unsupported recursion/conditions, or provider errors.
    pub async fn delete(
        &self,
        path: impl AsRef<str>,
        recursive: bool,
        options: DeleteOptions,
    ) -> Result<()> {
        if recursive {
            return Err(crate::MscError::unsupported(
                "recursive deletion; list and delete exact keys instead",
            ));
        }
        if let Some(metadata) = self.writable_metadata()? {
            let key = ObjectPath::new(path)?;
            let _guard = self.lock_metadata(&key).await;
            return self.delete_mapped(metadata, key.as_str(), options).await;
        }
        self.provider.delete_object(path.as_ref(), options).await
    }

    /// Delete in bounded batches while preserving original input indices.
    ///
    /// # Errors
    /// Item errors and invalid provider outcomes appear in the batch result.
    pub async fn delete_many(
        &self,
        paths: &[ObjectPath],
        options: DeleteOptions,
    ) -> Result<BatchResult> {
        if let Some(metadata) = self.writable_metadata()? {
            let _guards = self.lock_all_metadata().await;
            return self.delete_many_mapped(metadata, paths, options).await;
        }
        self.provider.delete_objects(paths, options).await
    }

    /// Return provider-ordered metadata. File-only streams resume after their last key.
    ///
    /// # Errors
    /// Returns invalid listing bounds; provider/ordering errors appear in the stream.
    pub async fn list(&self, options: ListOptions) -> Result<MetadataStream> {
        if let Some(metadata) = &self.metadata_provider {
            return metadata.list_objects(options).await;
        }
        self.provider.list_objects(options).await
    }

    /// List all descendant keys while preserving ordering and bounds.
    ///
    /// # Errors
    /// Returns invalid listing bounds; provider/ordering errors appear in the stream.
    pub async fn list_recursive(&self, mut options: ListOptions) -> Result<MetadataStream> {
        options.recursive = true;
        self.list(options).await
    }

    /// Persist staged logical metadata changes.
    ///
    /// # Errors
    /// Returns unsupported for read-only metadata and commit errors from writable providers.
    pub async fn commit_updates(&self) -> Result<()> {
        if let Some(metadata) = self.writable_metadata()? {
            let _guards = self.lock_all_metadata().await;
            metadata.commit_updates().await?;
        }
        Ok(())
    }

    /// Drain provider-owned cleanup, including cancelled multipart uploads.
    /// Call this on success and failure while the Tokio runtime is alive.
    ///
    /// # Errors
    /// Returns cleanup or task-join failures. Calling it again can retry cleanup.
    pub async fn shutdown(&self) -> Result<()> {
        self.provider.shutdown().await
    }
}
