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

//! Public provider operations, following Python `StorageProvider` terminology.
use crate::{
    BatchResult, CopyOptions, DeleteOptions, ErrorKind, InfoOptions, ListOptions, MetadataStream,
    ObjectKind, ObjectMetadata, ObjectPath, ReadOptions, Result, WriteOptions,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;

/// Provider-neutral operations on keys relative to a profile root.
///
/// Implement [`StorageBackend`](super::StorageBackend) and wrap it in
/// [`BaseStorageProvider`](super::BaseStorageProvider) to reuse MSC policy and
/// transfers. SDK clients and backend-specific hooks stay behind that boundary.
/// Applications own the Tokio runtime. Listing errors may arrive as stream items.
#[async_trait]
pub trait StorageProvider: Send + Sync {
    /// Inspect an entry without following its final symlink, matching Python.
    /// Strict options enable virtual-directory detection.
    async fn get_object_metadata(&self, path: &str, options: InfoOptions)
    -> Result<ObjectMetadata>;

    /// Test for a regular object. Missing objects return false.
    async fn is_file(&self, path: &str) -> Result<bool> {
        match super::resolve_metadata(self, path, InfoOptions { strict: false }).await {
            Ok(metadata) => Ok(metadata.kind == ObjectKind::File),
            Err(error) if error.kind == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Read bounded bytes, optionally selecting a half-open range or suffix.
    async fn get_object(&self, path: &str, options: ReadOptions) -> Result<Bytes>;

    /// Publish bytes and validate the completed write.
    async fn put_object(&self, path: &str, body: Bytes, options: WriteOptions) -> Result<()>;

    /// Upload a local file. The remote key precedes the local path.
    async fn upload_file(&self, remote: &str, local: &Path, options: WriteOptions) -> Result<()>;

    /// Download through staging, validating and synchronizing before publication.
    async fn download_file(&self, remote: &str, local: &Path, options: ReadOptions) -> Result<()>;

    /// Copy using native requests or a bounded streaming fallback.
    async fn copy_object(
        &self,
        source: &str,
        destination: &str,
        options: CopyOptions,
    ) -> Result<()>;

    /// Create a link whose path and target are both relative to the profile root.
    async fn make_symlink(&self, path: &str, target: &str) -> Result<()>;

    /// Delete an exact key, returning `NotFound` when it does not exist.
    async fn delete_object(&self, path: &str, options: DeleteOptions) -> Result<()>;

    /// Delete keys in batches, retaining original indices and individual failures.
    async fn delete_objects(
        &self,
        paths: &[ObjectPath],
        options: DeleteOptions,
    ) -> Result<BatchResult>;

    /// Stream metadata with provider ordering, bounds, and attribute filtering.
    async fn list_objects(&self, options: ListOptions) -> Result<MetadataStream>;

    /// List descendants with the same bounds and filtering contract.
    async fn list_objects_recursive(&self, mut options: ListOptions) -> Result<MetadataStream> {
        options.recursive = true;
        self.list_objects(options).await
    }

    /// Await provider-owned cleanup while the application's runtime is alive.
    async fn shutdown(&self) -> Result<()>;
}
