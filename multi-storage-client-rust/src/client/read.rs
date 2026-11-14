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

//! Metadata, bounded reads, seekable handles, and atomic downloads.
use super::{ObjectReader, StorageClient, record_transfer, require_file, stable_conditions};
use crate::{
    BatchResult, ByteRange, ErrorKind, GetConditions, InfoOptions, ListOptions, MscError,
    ObjectKind, ObjectMetadata, ObjectPath, ReadOptions, Result,
};
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, stream};
use std::path::{Path, PathBuf};

pub(super) const DOWNLOAD_CONCURRENCY: usize = 32;

impl StorageClient {
    pub(super) async fn read_metadata(
        &self,
        path: &ObjectPath,
        conditions: &GetConditions,
    ) -> Result<ObjectMetadata> {
        if let Some(version) = &conditions.version
            && !self.is_posix()
        {
            return self
                .retry(true, || self.state.provider.head_version(path, version))
                .await;
        }
        let metadata = self.info(path, InfoOptions::default()).await?;
        if conditions
            .version
            .as_ref()
            .is_some_and(|version| metadata.source_version.as_ref() != Some(version))
        {
            return Err(MscError::new(
                ErrorKind::PreconditionFailed,
                "requested object generation is unavailable",
            ));
        }
        Ok(metadata)
    }
    /// Open an anchored native read-only POSIX file.
    /// The returned handle supports asynchronous reading and seeking. Closing it
    /// releases its descriptor; remote storage returns `Unsupported`.
    ///
    /// # Errors
    /// Returns missing files, invalid paths, unsupported providers, or changed generations.
    #[cfg(unix)]
    pub async fn open_posix_file(&self, path: impl AsRef<str>) -> Result<tokio::fs::File> {
        if !self.is_posix() {
            return Err(MscError::unsupported("native POSIX file handles"));
        }
        let metadata = self.info(path, InfoOptions::default()).await?;
        require_file(&metadata)?;
        self.state
            .provider
            .open_posix_file(
                &metadata.key,
                &stable_conditions(&metadata, &GetConditions::default()),
            )
            .await
    }
    /// Resolve metadata, optionally following at most eight symlink hops.
    ///
    /// # Errors
    /// Returns invalid paths, missing objects, symlink cycles, or provider errors.
    pub async fn info(
        &self,
        path: impl AsRef<str>,
        options: InfoOptions,
    ) -> Result<ObjectMetadata> {
        let mut path = ObjectPath::new(path)?;
        if path.is_root() {
            return Ok(ObjectMetadata::directory(path));
        }
        let mut visited = std::collections::BTreeSet::new();
        for hop in 0..=8 {
            if !visited.insert(path.clone()) {
                return Err(MscError::new(
                    ErrorKind::Integrity,
                    "symlink cycle detected",
                ));
            }
            let metadata = self.retry(true, || self.state.provider.head(&path)).await?;
            if metadata.kind != ObjectKind::Symlink || !options.follow_symlinks {
                return Ok(metadata);
            }
            if hop == 8 {
                return Err(MscError::new(
                    ErrorKind::Integrity,
                    "symlink hop limit exceeded",
                ));
            }
            let target = metadata
                .symlink_target
                .ok_or_else(|| MscError::new(ErrorKind::Integrity, "symlink has no target"))?;
            path = path.resolve_link(&target)?;
        }
        unreachable!("bounded symlink resolution")
    }

    /// Test whether a path resolves to a regular object. Missing objects return false.
    ///
    /// # Errors
    /// Returns path, symlink-resolution, or provider errors other than missing objects.
    pub async fn is_file(&self, path: impl AsRef<str>) -> Result<bool> {
        match self.info(path, InfoOptions::default()).await {
            Ok(metadata) => Ok(metadata.kind == ObjectKind::File),
            Err(error) if error.kind == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Test whether any keys start with the normalized lexical prefix.
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

    /// Read a small object. Large transfers should use a reader or download.
    ///
    /// # Errors
    /// Returns path, provider, size-limit, version-condition, or integrity errors.
    pub async fn read(&self, path: impl AsRef<str>) -> Result<Bytes> {
        self.read_with_options(
            path,
            ReadOptions {
                max_in_memory_bytes: self.state.max_in_memory_bytes,
                buffer_size: Some(self.state.read_buffer_size),
                ..ReadOptions::default()
            },
        )
        .await
    }

    /// Read with explicit conditions and an in-memory size limit.
    ///
    /// # Errors
    /// Returns path, provider, size-limit, version-condition, or integrity errors.
    pub async fn read_with_options(
        &self,
        path: impl AsRef<str>,
        options: ReadOptions,
    ) -> Result<Bytes> {
        let path = ObjectPath::new(path)?;
        let resolve_first = self.is_posix() || options.conditions != GetConditions::default();
        let (key, conditions, expected) = if resolve_first {
            let metadata = self.read_metadata(&path, &options.conditions).await?;
            require_file(&metadata)?;
            (
                metadata.key.clone(),
                stable_conditions(&metadata, &options.conditions),
                Some(metadata.content_length),
            )
        } else {
            (path.clone(), options.conditions.clone(), None)
        };
        let bytes = self
            .read_bytes(&key, &conditions, options.max_in_memory_bytes, expected)
            .await?;
        if bytes.is_empty() && !resolve_first && self.capabilities().symlinks {
            let metadata = self.info(&path, InfoOptions::default()).await?;
            require_file(&metadata)?;
            if metadata.key != path {
                return self
                    .read_bytes(
                        &metadata.key,
                        &stable_conditions(&metadata, &options.conditions),
                        options.max_in_memory_bytes,
                        Some(metadata.content_length),
                    )
                    .await;
            }
        }
        Ok(bytes)
    }

    async fn read_bytes(
        &self,
        path: &ObjectPath,
        conditions: &GetConditions,
        limit: u64,
        expected: Option<u64>,
    ) -> Result<Bytes> {
        if expected.is_some_and(|length| length > limit) {
            return Err(MscError::unsupported(
                "object exceeds the in-memory read limit",
            ));
        }
        self.retry(true, || async {
            let mut body = self.state.provider.get(path, conditions).await?;
            let mut output = BytesMut::new();
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                if (output.len() as u64).saturating_add(chunk.len() as u64) > limit {
                    return Err(MscError::unsupported(
                        "object exceeds the in-memory read limit",
                    ));
                }
                output.extend_from_slice(&chunk);
            }
            if expected.is_some_and(|length| length != output.len() as u64) {
                return Err(MscError::new(
                    ErrorKind::Integrity,
                    "downloaded object length differs from metadata",
                ));
            }
            Ok(output.freeze())
        })
        .await
    }

    /// Read a half-open range, clipped by the provider at EOF.
    ///
    /// # Errors
    /// Returns invalid paths/ranges, size-limit, unsupported-range, or provider errors.
    pub async fn read_range(
        &self,
        path: impl AsRef<str>,
        range: impl Into<crate::ReadRange> + Send,
        options: ReadOptions,
    ) -> Result<Bytes> {
        let range = match range.into() {
            crate::ReadRange::Bounded(range) => range,
            crate::ReadRange::Suffix(range) => return self.read_suffix(path, range, options).await,
        };
        range.end()?;
        let path = ObjectPath::new(path)?;
        if range.length == 0 {
            return Ok(Bytes::new());
        }
        if range.length > options.max_in_memory_bytes {
            return Err(MscError::new(
                ErrorKind::Unsupported,
                "range exceeds the in-memory read limit",
            ));
        }
        if self.is_posix() || options.conditions != GetConditions::default() {
            let metadata = self.read_metadata(&path, &options.conditions).await?;
            require_file(&metadata)?;
            self.range_with_conditions(&metadata.key, range, &options.conditions)
                .await
        } else {
            let bytes = self
                .range_with_conditions(&path, range, &options.conditions)
                .await?;
            if bytes.is_empty() && self.capabilities().symlinks {
                let metadata = self.info(&path, InfoOptions::default()).await?;
                if metadata.key != path {
                    return self
                        .range_with_conditions(&metadata.key, range, &options.conditions)
                        .await;
                }
            }
            Ok(bytes)
        }
    }

    /// Read the last bytes, using a single request for ordinary cloud objects.
    ///
    /// # Errors
    /// Returns path, memory-limit, condition, or provider errors.
    pub async fn read_suffix(
        &self,
        path: impl AsRef<str>,
        range: crate::SuffixRange,
        options: ReadOptions,
    ) -> Result<Bytes> {
        if range.length() > options.max_in_memory_bytes {
            return Err(MscError::unsupported(
                "suffix exceeds the in-memory read limit",
            ));
        }
        let path = super::nonroot_path(path)?;
        if !self.capabilities().ranged_reads {
            return Err(MscError::unsupported("suffix reads"));
        }
        self.retry(true, || {
            self.state
                .provider
                .get_suffix(&path, range, &options.conditions)
        })
        .await
    }

    pub(super) async fn range_with_conditions(
        &self,
        path: &ObjectPath,
        range: ByteRange,
        conditions: &GetConditions,
    ) -> Result<Bytes> {
        if !self.capabilities().ranged_reads {
            return Err(MscError::unsupported("ranged reads"));
        }
        self.retry(true, || {
            self.state.provider.get_range(path, range, conditions)
        })
        .await
    }

    /// Create a seekable handle pinned to the observed object generation.
    ///
    /// # Errors
    /// Returns invalid buffer sizes/paths, unsupported ranges, or metadata errors.
    pub async fn reader(
        &self,
        path: impl AsRef<str>,
        options: ReadOptions,
    ) -> Result<ObjectReader> {
        let path = ObjectPath::new(path)?;
        let buffer_size = options.buffer_size.unwrap_or(self.state.read_buffer_size);
        if !self.capabilities().ranged_reads {
            return Err(MscError::unsupported("seekable readers"));
        }
        if buffer_size == 0 {
            return Err(MscError::new(
                ErrorKind::InvalidConfig,
                "reader buffer size must be positive",
            ));
        }
        let metadata = self.read_metadata(&path, &options.conditions).await?;
        require_file(&metadata)?;
        let conditions = stable_conditions(&metadata, &options.conditions);
        Ok(ObjectReader::new(
            self.clone(),
            metadata.key,
            conditions,
            metadata.content_length,
            buffer_size as u64,
        ))
    }

    /// Create missing parent directories, stream to a temporary sibling, validate
    /// length/version, and atomically rename.
    ///
    /// # Errors
    /// Returns path, provider, failed-condition, integrity, or local-file errors.
    pub async fn download_to(
        &self,
        remote: impl AsRef<str>,
        local: impl AsRef<Path>,
        options: ReadOptions,
    ) -> Result<ObjectMetadata> {
        self.download_atomic(remote, local.as_ref(), options).await
    }

    /// Open a bounded provider stream, resolving symlinks when needed.
    ///
    /// # Errors
    /// Returns logical symlink-resolution or provider-open errors.
    pub async fn read_stream(
        &self,
        path: impl AsRef<str>,
        options: ReadOptions,
    ) -> Result<crate::ByteStream> {
        let path = ObjectPath::new(path)?;
        if self.is_posix() || options.conditions != GetConditions::default() {
            let metadata = self.read_metadata(&path, &options.conditions).await?;
            require_file(&metadata)?;
            let conditions = stable_conditions(&metadata, &options.conditions);
            return self
                .retry(true, || self.state.provider.get(&metadata.key, &conditions))
                .await;
        }
        let mut body = self
            .retry(true, || self.state.provider.get(&path, &options.conditions))
            .await?;
        while let Some(chunk) = body.next().await {
            if chunk.as_ref().is_ok_and(bytes::Bytes::is_empty) {
                continue;
            }
            return Ok(Box::pin(stream::once(async move { chunk }).chain(body)));
        }
        if self.capabilities().symlinks {
            let metadata = self.info(&path, InfoOptions::default()).await?;
            require_file(&metadata)?;
            if metadata.key != path {
                let conditions = stable_conditions(&metadata, &options.conditions);
                return self
                    .retry(true, || self.state.provider.get(&metadata.key, &conditions))
                    .await;
            }
        }
        Ok(Box::pin(stream::empty()))
    }

    /// Download files with at most 32 concurrent transfers per backend.
    /// Failure keys refer to remote objects; indices refer to the original input order.
    ///
    /// # Errors
    /// Item errors are returned in `BatchResult::failed`, preserving input indices.
    pub async fn download_many(
        &self,
        files: &[(ObjectPath, PathBuf)],
        options: ReadOptions,
    ) -> Result<BatchResult> {
        let mut transfers =
            stream::iter(files.iter().enumerate().map(|(index, (remote, local))| {
                let options = options.clone();
                async move {
                    (
                        index,
                        remote.clone(),
                        self.download_to(&remote, &local, options).await,
                    )
                }
            }))
            .buffer_unordered(DOWNLOAD_CONCURRENCY);
        let mut result = BatchResult::default();
        while let Some((index, source, outcome)) = transfers.next().await {
            record_transfer(&mut result, index, source, None, outcome);
        }
        result.succeeded.sort_unstable();
        result.failed.sort_by_key(|failure| failure.index);
        Ok(result)
    }
}
