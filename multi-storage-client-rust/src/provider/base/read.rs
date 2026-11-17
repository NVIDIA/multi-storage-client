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

//! Metadata, byte reads, and conditional ranges.
use super::{BaseStorageProvider, require_file, stable_conditions};
use crate::{
    ByteRange, ErrorKind, GetConditions, InfoOptions, MscError, ObjectMetadata, ObjectPath,
    ReadOptions, Result,
};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;

impl<B: crate::StorageBackend + ?Sized + 'static> BaseStorageProvider<B> {
    pub(super) async fn read_metadata(
        &self,
        path: &ObjectPath,
        conditions: &GetConditions,
    ) -> Result<ObjectMetadata> {
        if let Some(version) = &conditions.version
            && !self.is_posix_file_storage_provider()
        {
            return self
                .retry(true, || {
                    self.state
                        .provider
                        .get_object_metadata_version(path, version)
                })
                .await;
        }
        let metadata = self.info_impl(path, InfoOptions { strict: false }).await?;
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

    pub(super) async fn info_impl(
        &self,
        path: impl AsRef<str>,
        options: InfoOptions,
    ) -> Result<ObjectMetadata> {
        super::super::resolve_metadata(self, path.as_ref(), options).await
    }

    pub(super) async fn metadata_impl(
        &self,
        path: &str,
        options: InfoOptions,
    ) -> Result<ObjectMetadata> {
        let directory_hint = path.ends_with('/') && !self.is_posix_file_storage_provider();
        let path = ObjectPath::new(path)?;
        if path.is_root() {
            return Ok(ObjectMetadata::directory(path));
        }
        self.lookup_metadata(&path, options.strict, directory_hint)
            .await
    }

    async fn lookup_metadata(
        &self,
        path: &ObjectPath,
        strict: bool,
        directory_hint: bool,
    ) -> Result<ObjectMetadata> {
        self.retry(true, || async {
            let missing = if directory_hint {
                MscError::new(ErrorKind::NotFound, "directory does not exist").context(
                    "info",
                    self.state.provider.name(),
                    path.as_str(),
                )
            } else {
                match self.state.provider.get_object_metadata(path).await {
                    Ok(metadata) => return Ok(metadata),
                    Err(error) if error.kind == ErrorKind::NotFound && strict => error,
                    Err(error) => return Err(error),
                }
            };
            if self.state.provider.virtual_directory_exists(path).await? {
                Ok(ObjectMetadata::directory(path.clone()))
            } else {
                Err(missing)
            }
        })
        .await
    }

    /// Read with explicit conditions and an in-memory size limit.
    ///
    /// # Errors
    /// Returns path, provider, size-limit, version-condition, or integrity errors.
    pub(super) async fn read_with_options_impl(
        &self,
        path: impl AsRef<str>,
        options: ReadOptions,
    ) -> Result<Bytes> {
        let path = ObjectPath::new(path)?;
        let resolve_first =
            self.is_posix_file_storage_provider() || options.conditions != GetConditions::default();
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
            .read_bytes(
                &key,
                &conditions,
                options
                    .max_in_memory_bytes
                    .unwrap_or(self.state.max_in_memory_bytes),
                expected,
            )
            .await?;
        if bytes.is_empty() && !resolve_first && self.state.provider.capabilities().symlinks {
            let metadata = self.info_impl(&path, InfoOptions { strict: false }).await?;
            require_file(&metadata)?;
            if metadata.key != path {
                return self
                    .read_bytes(
                        &metadata.key,
                        &stable_conditions(&metadata, &options.conditions),
                        options
                            .max_in_memory_bytes
                            .unwrap_or(self.state.max_in_memory_bytes),
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
            let mut body = self.state.provider.get_object(path, conditions).await?;
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
    pub(super) async fn read_range_impl(
        &self,
        path: impl AsRef<str>,
        range: impl Into<crate::ReadRange> + Send,
        options: ReadOptions,
    ) -> Result<Bytes> {
        let range = match range.into() {
            crate::ReadRange::Bounded(range) => range,
            crate::ReadRange::Suffix(range) => {
                return self.read_suffix_impl(path, range, options).await;
            }
        };
        range.end()?;
        let path = ObjectPath::new(path)?;
        if range.length == 0 {
            return Ok(Bytes::new());
        }
        if range.length
            > options
                .max_in_memory_bytes
                .unwrap_or(self.state.max_in_memory_bytes)
        {
            return Err(MscError::new(
                ErrorKind::Unsupported,
                "range exceeds the in-memory read limit",
            ));
        }
        if self.is_posix_file_storage_provider() || options.conditions != GetConditions::default() {
            let metadata = self.read_metadata(&path, &options.conditions).await?;
            require_file(&metadata)?;
            self.range_with_conditions(&metadata.key, range, &options.conditions)
                .await
        } else {
            let bytes = self
                .range_with_conditions(&path, range, &options.conditions)
                .await?;
            if bytes.is_empty() && self.state.provider.capabilities().symlinks {
                let metadata = self.info_impl(&path, InfoOptions { strict: false }).await?;
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
    pub(super) async fn read_suffix_impl(
        &self,
        path: impl AsRef<str>,
        range: crate::SuffixRange,
        options: ReadOptions,
    ) -> Result<Bytes> {
        if range.length()
            > options
                .max_in_memory_bytes
                .unwrap_or(self.state.max_in_memory_bytes)
        {
            return Err(MscError::unsupported(
                "suffix exceeds the in-memory read limit",
            ));
        }
        let path = super::nonroot_path(path)?;
        if !self.state.provider.capabilities().ranged_reads {
            return Err(MscError::unsupported("suffix reads"));
        }
        self.retry(true, || {
            self.state
                .provider
                .get_object_suffix(&path, range, &options.conditions)
        })
        .await
    }

    async fn range_with_conditions(
        &self,
        path: &ObjectPath,
        range: ByteRange,
        conditions: &GetConditions,
    ) -> Result<Bytes> {
        if !self.state.provider.capabilities().ranged_reads {
            return Err(MscError::unsupported("ranged reads"));
        }
        self.retry(true, || {
            self.state
                .provider
                .get_object_range(path, range, conditions)
        })
        .await
    }
}
