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
    BatchResult, CopyOptions, DeleteOptions, InfoOptions, ListOptions, MetadataStream,
    ObjectMetadata, ObjectPath, ReadOptions, Result, WriteOptions, config::ResolvedConfig,
};
use bytes::Bytes;
use std::path::Path;

impl StorageClient {
    /// Select a profile. Construction and credentials are deferred until build.
    #[must_use]
    pub fn builder(config: ResolvedConfig, profile: impl Into<String>) -> StorageClientBuilder {
        StorageClientBuilder::from_profile(config, profile.into())
    }

    /// Query metadata with strict virtual-directory detection by default.
    /// Relative symlinks resolve internally with an eight-hop bound.
    ///
    /// # Errors
    /// Returns invalid paths, missing objects, symlink cycles, or provider errors.
    pub async fn info(
        &self,
        path: impl AsRef<str>,
        options: InfoOptions,
    ) -> Result<ObjectMetadata> {
        self.info_impl(path, options).await
    }

    /// Test whether a path resolves to a regular object. Missing objects return false.
    ///
    /// # Errors
    /// Returns path, symlink-resolution, or provider errors other than missing objects.
    pub async fn is_file(&self, path: impl AsRef<str>) -> Result<bool> {
        self.is_file_impl(path).await
    }

    /// Test whether any keys start with the normalized lexical prefix.
    ///
    /// # Errors
    /// Returns invalid paths or listing errors.
    pub async fn is_empty(&self, prefix: impl AsRef<str>) -> Result<bool> {
        self.is_empty_impl(prefix).await
    }

    /// Read bounded bytes, optionally selecting a half-open range or suffix.
    /// Default options inherit the client's configured in-memory size limit.
    ///
    /// # Errors
    /// Returns path, provider, size-limit, version-condition, or integrity errors.
    pub async fn read(&self, path: impl AsRef<str>, mut options: ReadOptions) -> Result<Bytes> {
        if let Some(range) = options.range.take() {
            return self.read_range_impl(path, range, options).await;
        }
        self.read_with_options_impl(path, options).await
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
        self.download_atomic(remote, local.as_ref(), options)
            .await
            .map(drop)
    }

    /// Publish bytes with the profile retry budget and validate publication.
    ///
    /// # Errors
    /// Returns invalid paths, failed conditions, upload, or post-upload metadata errors.
    pub async fn write(
        &self,
        path: impl AsRef<str>,
        bytes: Bytes,
        options: WriteOptions,
    ) -> Result<()> {
        self.write_impl(path, bytes, options).await.map(drop)
    }

    /// Upload a regular local file to `remote` from its anchored, replayable file handle.
    ///
    /// Memory stays bounded; no whole-file temporary copy is created.
    /// The final source component is opened without following symlinks.
    ///
    /// # Errors
    /// Returns path, local-file, unsupported-symlink, upload, or metadata errors.
    pub async fn upload_file(
        &self,
        remote: impl AsRef<str>,
        local: impl AsRef<Path>,
        options: WriteOptions,
    ) -> Result<()> {
        self.upload_file_impl(remote, local, options)
            .await
            .map(drop)
    }

    /// Copy an object using the provider or a bounded streaming fallback.
    /// Empty destination attributes inherit the source attributes; nonempty
    /// attributes replace them. An omitted content type inherits the source type.
    ///
    /// # Errors
    /// Returns path, unsupported-operation, condition, copy, or metadata errors.
    pub async fn copy(
        &self,
        source: impl AsRef<str>,
        destination: impl AsRef<str>,
        options: CopyOptions,
    ) -> Result<()> {
        self.copy_impl(source, destination, options).await.map(drop)
    }

    /// Create a normalized link when the provider supports symlinks.
    ///
    /// # Errors
    /// Returns invalid/escaping targets, unsupported symlinks, or provider errors.
    pub async fn make_symlink(&self, path: impl AsRef<str>, target: &str) -> Result<()> {
        self.make_symlink_impl(path, target).await.map(drop)
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
        self.delete_impl(path, recursive, options).await
    }

    /// Delete in bounded batches while preserving original input indices.
    ///
    /// # Errors
    /// Returns invalid provider batch responses. Item errors appear in the batch result.
    pub async fn delete_many(
        &self,
        paths: &[ObjectPath],
        options: DeleteOptions,
    ) -> Result<BatchResult> {
        self.delete_many_impl(paths, options).await
    }

    /// Return provider-ordered metadata. File-only streams resume after their last key.
    ///
    /// # Errors
    /// Returns invalid listing bounds; provider/ordering errors appear in the stream.
    pub async fn list(&self, options: ListOptions) -> Result<MetadataStream> {
        self.list_impl(options).await
    }

    /// List all descendant keys while preserving ordering and bounds.
    ///
    /// # Errors
    /// Returns invalid listing bounds; provider/ordering errors appear in the stream.
    pub async fn list_recursive(&self, mut options: ListOptions) -> Result<MetadataStream> {
        options.recursive = true;
        self.list(options).await
    }

    /// Drain provider-owned cleanup, including cancelled multipart uploads.
    /// Call this on success and failure while the Tokio runtime is alive.
    ///
    /// # Errors
    /// Returns cleanup or task-join failures. Calling it again can retry cleanup.
    pub async fn shutdown(&self) -> Result<()> {
        self.state.provider.shutdown().await
    }
}
