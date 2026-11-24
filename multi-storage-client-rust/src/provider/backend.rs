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

//! Backend I/O hooks for built-in and application-owned adapters.
use super::suffix_fallback;
use crate::{
    BatchFailure, BatchResult, ByteRange, ByteStream, CopyOptions, DeleteOptions, ErrorKind,
    GetConditions, ListOptions, MetadataStream, MscError, ObjectMetadata, ObjectPath,
    ProviderCapabilities, PutResult, Result, WriteOptions,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};

/// Asynchronous primitive I/O for validated, profile-relative keys.
///
/// Providers never receive URLs or profile names and do not own a runtime.
/// Disable SDK request retries: [`BaseStorageProvider`](super::BaseStorageProvider) owns the
/// operation budget. Safe multipart parts use that profile's budget per part;
/// whole-object publication is not retried around those part attempts.
/// Advertise optional features accurately, reject unsupported
/// conditions before mutation, and return typed, secret-safe errors. A stream
/// may fail after opening, so its errors are part of the operation contract.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Non-secret backend tag used in errors and provider metrics.
    fn name(&self) -> &str;

    /// Non-secret identity used to detect backend aliases.
    fn backend_fingerprint(&self) -> String {
        format!("{}:{:p}", self.name(), self)
    }

    /// Configured physical prefix for overlap checks. The default is the root.
    fn physical_root(&self) -> ObjectPath {
        ObjectPath::default()
    }

    /// Whether this adapter accesses a POSIX filesystem.
    fn is_posix(&self) -> bool {
        false
    }

    /// Preserve configured local timestamps and permissions after a transfer.
    /// `expected` protects the destination generation. Return updated metadata
    /// on success, or `None` when preservation is unavailable or disabled.
    async fn preserve_metadata(
        &self,
        _key: &ObjectPath,
        _source: &ObjectMetadata,
        _expected: Option<&crate::SourceVersion>,
    ) -> Result<Option<ObjectMetadata>> {
        Ok(None)
    }

    /// Immutable optional operation and consistency capabilities.
    fn capabilities(&self) -> ProviderCapabilities;

    /// Return metadata for a key without consuming file content.
    async fn get_object_metadata(&self, key: &ObjectPath) -> Result<ObjectMetadata>;

    /// Test a delimiter-qualified virtual directory prefix without probing child objects.
    /// Providers with only explicit filesystem entries have no virtual directories.
    async fn virtual_directory_exists(&self, _key: &ObjectPath) -> Result<bool> {
        Ok(false)
    }

    /// Return metadata for the requested generation, or fail if it is unavailable.
    async fn get_object_metadata_version(
        &self,
        key: &ObjectPath,
        version: &crate::SourceVersion,
    ) -> Result<ObjectMetadata> {
        let metadata = self.get_object_metadata(key).await?;
        if metadata.source_version.as_ref() != Some(version) {
            return Err(MscError::new(
                ErrorKind::PreconditionFailed,
                "requested object generation is unavailable",
            ));
        }
        Ok(metadata)
    }

    /// Open a conditional byte stream; later transfer failures are stream items.
    async fn get_object(&self, key: &ObjectPath, conditions: &GetConditions) -> Result<ByteStream>;

    /// Read a half-open range, clipping it at EOF and enforcing conditions.
    async fn get_object_range(
        &self,
        key: &ObjectPath,
        range: ByteRange,
        conditions: &GetConditions,
    ) -> Result<Bytes>;

    /// Return `(part_size, concurrency)` when a file should use multipart downloads.
    /// Providers opt in only when they support generation-pinned range streams.
    fn download_part_config(&self, _size: u64) -> Option<(u64, usize)> {
        None
    }

    /// Open one file-download range attempt, without nested retries or resumes.
    /// The caller supplies the observed total size and validates the streamed length.
    async fn get_download_part(
        &self,
        _key: &ObjectPath,
        _range: ByteRange,
        _conditions: &GetConditions,
        _total_size: u64,
    ) -> Result<ByteStream> {
        Err(MscError::unsupported("multipart file downloads"))
    }

    /// Read the last bytes of an object. Native cloud adapters use one ranged GET.
    /// The default resolves metadata and falls back to an offset range.
    async fn get_object_suffix(
        &self,
        key: &ObjectPath,
        range: crate::SuffixRange,
        conditions: &GetConditions,
    ) -> Result<Bytes> {
        suffix_fallback(self, key, range, conditions).await
    }

    /// Consume a byte stream and atomically publish the object when complete.
    /// `size`, when supplied, must match the actual bytes. Cancellation must not
    /// publish a partial object; multipart cleanup remains owned until shutdown.
    async fn put_object(
        &self,
        key: &ObjectPath,
        stream: ByteStream,
        size: Option<u64>,
        options: &WriteOptions,
    ) -> Result<PutResult>;

    /// Publish an already buffered object. Backends may avoid multipart setup
    /// when a complete, replayable payload fits their single-request limit.
    async fn put_object_bytes(
        &self,
        key: &ObjectPath,
        body: Bytes,
        options: &WriteOptions,
    ) -> Result<PutResult> {
        let size = body.len() as u64;
        self.put_object(
            key,
            Box::pin(stream::once(async move { Ok(body) })),
            Some(size),
            options,
        )
        .await
    }

    /// Delete a key with backend-enforced conditions. An unconditional delete
    /// of an absent key may return success or `NotFound`.
    async fn delete_object(&self, key: &ObjectPath, conditions: &DeleteOptions) -> Result<()>;

    /// Bounded fallback retaining the original index of every item.
    async fn delete_objects(&self, keys: &[ObjectPath], options: &DeleteOptions) -> BatchResult {
        let mut outcomes = stream::iter(keys.iter().cloned().enumerate())
            .map(|(index, key)| async move {
                (index, key.clone(), self.delete_object(&key, options).await)
            })
            .buffer_unordered(16);
        let mut result = BatchResult::default();
        while let Some((index, source, outcome)) = outcomes.next().await {
            match outcome {
                Ok(()) => result.succeeded.push(index),
                Err(error) if options.is_unconditional() && error.kind == ErrorKind::NotFound => {
                    result.succeeded.push(index);
                }
                Err(error) => result.failed.push(BatchFailure {
                    index,
                    source,
                    destination: None,
                    error,
                }),
            }
        }
        result.succeeded.sort_unstable();
        result.failed.sort_by_key(|failure| failure.index);
        result
    }

    /// Perform native copy within the advertised scope, or return `Unsupported`
    /// before mutation so the shared provider can choose a streaming fallback.
    async fn copy_object(
        &self,
        _source: &ObjectPath,
        _destination: &ObjectPath,
        _options: &CopyOptions,
    ) -> Result<PutResult> {
        Err(MscError::unsupported("copy"))
    }

    /// Stream metadata according to listing options. Advertise
    /// `native_lexical_listing` when file-only entries are globally key-ordered.
    async fn list_objects(&self, options: &ListOptions) -> Result<MetadataStream>;

    /// Create a link between profile-relative keys, storing a parent-relative target.
    async fn make_symlink(&self, _key: &ObjectPath, _target: &str) -> Result<()> {
        Err(MscError::unsupported("make_symlink"))
    }

    /// Await provider-owned cleanup tasks before the application exits.
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}
