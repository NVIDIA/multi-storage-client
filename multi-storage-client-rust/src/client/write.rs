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

//! Replayable uploads, sequential writers, and object mutations.
use super::{
    ObjectWriter, StorageClient, nonroot_path, record_transfer, require_file, stable_conditions,
};
use crate::{
    BatchResult, CopyOptions, ErrorKind, InfoOptions, MscError, ObjectMetadata, ObjectPath,
    PutResult, Result, WriteOptions,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

impl StorageClient {
    /// Publish bytes with the profile retry budget and return publication metadata.
    ///
    /// # Errors
    /// Returns invalid paths, failed conditions, upload, or post-upload metadata errors.
    pub async fn write(
        &self,
        path: impl AsRef<str>,
        bytes: Bytes,
        options: WriteOptions,
    ) -> Result<ObjectMetadata> {
        if self.is_read_only() {
            return Err(MscError::unsupported("read-only client write"));
        }
        let path = nonroot_path(path)?;
        let result = self
            .retry_mutation(|| {
                let bytes = bytes.clone();
                let size = bytes.len() as u64;
                self.state.provider.put(
                    &path,
                    Box::pin(stream::once(async move { Ok(bytes) })),
                    Some(size),
                    &options,
                )
            })
            .await?;
        self.metadata_after_put(&path, result).await
    }

    /// Create an unpublished sequential upload. `size_hint` is advisory.
    ///
    /// # Errors
    /// Returns invalid paths, unsupported writes, or temporary-file errors.
    pub async fn writer(
        &self,
        path: impl AsRef<str>,
        _size_hint: Option<u64>,
        options: WriteOptions,
    ) -> Result<ObjectWriter> {
        if !self.capabilities().streaming_writes {
            return Err(MscError::unsupported("streaming writers"));
        }
        ObjectWriter::new(self.clone(), nonroot_path(path)?, options).await
    }

    /// Upload a regular local file from its anchored, replayable file handle.
    ///
    /// Memory stays bounded; no whole-file temporary copy is created.
    /// The final source component is opened without following symlinks.
    ///
    /// # Errors
    /// Returns path, local-file, unsupported-symlink, upload, or metadata errors.
    pub async fn upload_from(
        &self,
        remote: impl AsRef<str>,
        local: impl AsRef<Path>,
        options: WriteOptions,
    ) -> Result<ObjectMetadata> {
        self.upload_from_with_callback(remote, local, options, None)
            .await
    }

    /// Upload a regular file with cumulative progress for each upload attempt.
    ///
    /// # Errors
    /// Returns source-file, upload, condition or metadata errors.
    pub async fn upload_from_with_callback(
        &self,
        remote: impl AsRef<str>,
        local: impl AsRef<Path>,
        options: WriteOptions,
        callback: Option<crate::TransferCallback>,
    ) -> Result<ObjectMetadata> {
        if self.is_read_only() {
            return Err(MscError::unsupported("read-only client upload"));
        }
        let path = nonroot_path(remote)?;
        let local = local.as_ref().to_path_buf();
        let source = tokio::task::spawn_blocking(move || open_upload_source(&local))
            .await
            .map_err(|_| {
                MscError::new(ErrorKind::Cancelled, "source-file opening task failed")
            })??;
        let source = tokio::fs::File::from_std(source);
        let metadata = source.metadata().await?;
        if metadata.file_type().is_symlink() {
            return Err(MscError::new(
                ErrorKind::Unsupported,
                "local symlink upload requires an explicit containment policy",
            ));
        }
        if !metadata.is_file() {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "upload source must be a regular file",
            ));
        }
        self.upload_file_with_callback(&path, &source, metadata.len(), &options, callback)
            .await
    }

    pub(super) async fn upload_file(
        &self,
        remote: &ObjectPath,
        local: &tokio::fs::File,
        length: u64,
        options: &WriteOptions,
    ) -> Result<ObjectMetadata> {
        self.upload_file_with_callback(remote, local, length, options, None)
            .await
    }

    async fn upload_file_with_callback(
        &self,
        remote: &ObjectPath,
        local: &tokio::fs::File,
        length: u64,
        options: &WriteOptions,
        callback: Option<crate::TransferCallback>,
    ) -> Result<ObjectMetadata> {
        let file = std::sync::Arc::new(tokio::sync::Mutex::new(local.try_clone().await?));
        let expected = std::sync::Arc::new(file.lock().await.metadata().await?);
        let buffer_size = self.state.read_buffer_size;
        let result = self
            .retry_mutation(|| async {
                let file = file.clone();
                let callback = callback.clone();
                let expected = expected.clone();
                crate::transfer::progress(callback.as_ref(), 0, length);
                let body = stream::try_unfold((file, 0_u64), move |(file, offset)| {
                    let callback = callback.clone();
                    let expected = expected.clone();
                    async move {
                        let mut handle = file.lock().await;
                        if offset == 0 && !same_source(&handle.metadata().await?, &expected) {
                            return Err(changed_source());
                        }
                        if offset == 0 {
                            handle.seek(std::io::SeekFrom::Start(0)).await?;
                        }
                        let mut bytes = vec![0; buffer_size];
                        let read = handle.read(&mut bytes).await?;
                        if read == 0 {
                            if offset != length
                                || !same_source(&handle.metadata().await?, &expected)
                            {
                                return Err(changed_source());
                            }
                            return Ok::<_, MscError>(None);
                        }
                        if offset.saturating_add(read as u64) > length {
                            return Err(changed_source());
                        }
                        drop(handle);
                        bytes.truncate(read);
                        let offset = offset + read as u64;
                        crate::transfer::progress(callback.as_ref(), offset, length);
                        Ok(Some((Bytes::from(bytes), (file, offset))))
                    }
                });
                self.state
                    .provider
                    .put(remote, Box::pin(body), Some(length), options)
                    .await
            })
            .await?;
        self.metadata_after_put(remote, result).await
    }

    /// Upload files with at most 32 concurrent transfers. Failure keys refer
    /// to the remote object; indices refer to the original input order.
    ///
    /// # Errors
    /// Item errors are returned in `BatchResult::failed`, preserving input indices.
    pub async fn upload_many(
        &self,
        files: &[(ObjectPath, PathBuf)],
        options: WriteOptions,
    ) -> Result<BatchResult> {
        let mut transfers =
            stream::iter(files.iter().enumerate().map(|(index, (remote, local))| {
                let options = options.clone();
                async move {
                    (
                        index,
                        remote.clone(),
                        self.upload_from(remote, local, options).await,
                    )
                }
            }))
            .buffer_unordered(32);
        let mut result = BatchResult::default();
        while let Some((index, source, outcome)) = transfers.next().await {
            let destination = Some(source.clone());
            record_transfer(&mut result, index, source, destination, outcome);
        }
        result.succeeded.sort_unstable();
        result.failed.sort_by_key(|failure| failure.index);
        Ok(result)
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
    ) -> Result<ObjectMetadata> {
        if self.is_read_only() {
            return Err(MscError::unsupported("read-only client copy"));
        }
        let mut source = nonroot_path(source)?;
        if self.is_posix() {
            let metadata = self.info(source, InfoOptions::default()).await?;
            require_file(&metadata)?;
            source = metadata.key;
        }
        let destination = nonroot_path(destination)?;
        let result = if self.capabilities().server_side_copy == crate::CopyScope::None {
            self.streaming_copy(&source, &destination, &options).await?
        } else {
            match self
                .retry_mutation(|| self.state.provider.copy(&source, &destination, &options))
                .await
            {
                Err(error) if error.kind == ErrorKind::Unsupported => {
                    self.streaming_copy(&source, &destination, &options).await?
                }
                result => result?,
            }
        };
        self.metadata_after_put(&destination, result).await
    }

    async fn streaming_copy(
        &self,
        source: &ObjectPath,
        destination: &ObjectPath,
        options: &CopyOptions,
    ) -> Result<PutResult> {
        if !self.capabilities().streaming_reads || !self.capabilities().streaming_writes {
            return Err(MscError::unsupported("copy"));
        }
        let metadata = self.info(source, InfoOptions::default()).await?;
        require_file(&metadata)?;
        let conditions = stable_conditions(&metadata, &options.source_conditions);
        let mut destination_options = options.destination.clone();
        if destination_options.attributes.is_empty() {
            destination_options
                .attributes
                .clone_from(&metadata.attributes);
        }
        if destination_options.content_type.is_none() {
            destination_options.content_type =
                metadata.provider_fields.get("content_type").cloned();
        }
        self.retry_mutation(|| async {
            let body = self.state.provider.get(&metadata.key, &conditions).await?;
            self.state
                .provider
                .put(
                    destination,
                    body,
                    Some(metadata.content_length),
                    &destination_options,
                )
                .await
        })
        .await
    }

    pub(super) async fn metadata_after_put(
        &self,
        path: &ObjectPath,
        result: PutResult,
    ) -> Result<ObjectMetadata> {
        if let Some(metadata) = result.metadata {
            return Ok(metadata);
        }
        let metadata = self
            .info(
                path,
                InfoOptions {
                    follow_symlinks: false,
                },
            )
            .await
            .map_err(|error| {
                error.context(
                    "info_after_write",
                    self.state.provider.name(),
                    path.as_str(),
                )
            })?;
        if result
            .etag
            .as_ref()
            .is_some_and(|written| metadata.etag.as_ref() != Some(written))
            || result
                .source_version
                .as_ref()
                .is_some_and(|written| metadata.source_version.as_ref() != Some(written))
        {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "object changed after upload and before metadata lookup",
            )
            .context(
                "info_after_write",
                self.state.provider.name(),
                path.as_str(),
            ));
        }
        Ok(metadata)
    }

    /// Create a normalized link when the provider supports symlinks.
    ///
    /// # Errors
    /// Returns invalid/escaping targets, unsupported symlinks, or provider errors.
    pub async fn make_symlink(
        &self,
        path: impl AsRef<str>,
        target: &str,
    ) -> Result<ObjectMetadata> {
        let path = nonroot_path(path)?;
        path.resolve_link(target)?;
        if !self.capabilities().symlinks {
            return Err(MscError::unsupported("symlinks"));
        }
        self.state.provider.make_symlink(&path, target).await?;
        self.info(
            &path,
            InfoOptions {
                follow_symlinks: false,
            },
        )
        .await
    }
}

fn changed_source() -> MscError {
    MscError::new(
        ErrorKind::Integrity,
        "upload source changed during transfer",
    )
}
fn same_source(actual: &std::fs::Metadata, expected: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if (actual.dev(), actual.ino()) != (expected.dev(), expected.ino()) {
            return false;
        }
    }
    actual.len() == expected.len() && actual.modified().ok() == expected.modified().ok()
}

pub(super) async fn create_spool_file() -> Result<tokio::fs::File> {
    let file = tokio::task::spawn_blocking(tempfile::tempfile)
        .await
        .map_err(|_| {
            MscError::new(ErrorKind::Cancelled, "temporary-file creation task failed")
        })??;
    Ok(tokio::fs::File::from_std(file))
}

#[cfg(unix)]
fn open_upload_source(local: &Path) -> Result<std::fs::File> {
    use rustix::fs::{Mode, OFlags, open};
    open(
        local,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(std::fs::File::from)
    .map_err(|error| {
        if error == rustix::io::Errno::LOOP {
            MscError::new(
                ErrorKind::Unsupported,
                "local symlink upload requires an explicit containment policy",
            )
        } else {
            MscError::from(std::io::Error::from(error))
        }
    })
}

#[cfg(windows)]
fn open_upload_source(local: &Path) -> Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    // FILE_FLAG_OPEN_REPARSE_POINT opens the link itself for metadata validation.
    const OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(OPEN_REPARSE_POINT)
        .open(local)
        .map_err(MscError::from)
}

#[cfg(not(any(unix, windows)))]
fn open_upload_source(_local: &Path) -> Result<std::fs::File> {
    Err(MscError::unsupported(
        "local uploads with no-follow source opening",
    ))
}
