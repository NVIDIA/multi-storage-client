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

//! Atomic streamed and parallel ranged file downloads.
//!
//! Range planning and source pinning adapt the accelerator's download module.
//! Small files use ephemeral staging; multipart checkpoints survive interrupted calls.
mod multipart;
mod state;
use super::{BaseStorageProvider, require_file, stable_conditions};
use crate::{ErrorKind, GetConditions, MscError, ObjectMetadata, ReadOptions, Result};
use bytes::Bytes;
use futures::StreamExt;
use std::{path::Path, sync::Arc};

struct Staging {
    file: tempfile::NamedTempFile,
    directory: tempfile::TempDir,
    #[cfg(not(unix))]
    cursor: std::sync::Mutex<()>,
}

impl Staging {
    fn create(parent: &Path) -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix(".msc-tmp-download-")
            .tempdir_in(parent)?;
        let file = tempfile::NamedTempFile::new_in(directory.path())?;
        Ok(Self {
            file,
            directory,
            #[cfg(not(unix))]
            cursor: std::sync::Mutex::new(()),
        })
    }

    fn write_at(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.as_file().write_all_at(bytes, offset)?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, Write};
            let _guard = self.cursor.lock().map_err(|_| {
                MscError::new(ErrorKind::Cancelled, "download file cursor poisoned")
            })?;
            let mut file = self.file.as_file();
            file.seek(std::io::SeekFrom::Start(offset))?;
            file.write_all(bytes)?;
        }
        Ok(())
    }

    fn synchronize(&self, length: u64) -> Result<()> {
        if self.file.as_file().metadata()?.len() != length {
            return Err(invalid_length());
        }
        self.file.as_file().sync_all()?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let original = self.file.as_file().metadata()?;
            let current = std::fs::symlink_metadata(self.file.path())?;
            if current.dev() != original.dev() || current.ino() != original.ino() {
                return Err(MscError::new(
                    ErrorKind::Integrity,
                    "download staging entry was replaced",
                ));
            }
        }
        Ok(())
    }
}

impl<B: crate::StorageBackend + ?Sized + 'static> BaseStorageProvider<B> {
    pub(super) async fn download_atomic(
        &self,
        remote: impl AsRef<str>,
        local: &Path,
        options: ReadOptions,
    ) -> Result<ObjectMetadata> {
        let remote = super::nonroot_path(remote)?;
        let metadata = self.read_metadata(&remote, &options.conditions).await?;
        require_file(&metadata)?;
        let parent = local
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent).await?;
        check_conditions(&metadata, &options.conditions)?;
        let conditions = stable_conditions(&metadata, &options.conditions);
        let parts = self
            .state
            .provider
            .download_part_config(metadata.content_length)
            .filter(|_| {
                self.state.provider.capabilities().ranged_reads && generation_pinned(&conditions)
            });
        if let Some((part_size, concurrency)) = parts {
            if part_size == 0 || !(1..=64).contains(&concurrency) {
                return Err(MscError::new(
                    ErrorKind::InvalidConfig,
                    "download parts require a positive size and concurrency between 1 and 64",
                ));
            }
            self.download_resumable(&metadata, &conditions, local, part_size, concurrency)
                .await?;
            return Ok(metadata);
        }
        let parent = parent.to_path_buf();
        let staging = blocking(move || Staging::create(&parent).map(Arc::new)).await?;
        self.download_stream(&metadata, &conditions, &staging)
            .await?;
        let length = metadata.content_length;
        let synchronized = staging.clone();
        blocking(move || synchronized.synchronize(length)).await?;
        let staging = Arc::try_unwrap(staging).map_err(|_| {
            MscError::new(
                ErrorKind::Integrity,
                "download filesystem work is still outstanding",
            )
        })?;
        staging
            .file
            .persist(local)
            .map_err(|error| MscError::from(error.error))?;
        drop(staging.directory);
        Ok(metadata)
    }

    async fn download_stream(
        &self,
        metadata: &ObjectMetadata,
        conditions: &GetConditions,
        staging: &Arc<Staging>,
    ) -> Result<()> {
        self.retry(true, || async {
            let reset = staging.clone();
            blocking(move || reset.file.as_file().set_len(0).map_err(MscError::from)).await?;
            let mut body = self
                .state
                .provider
                .get_object(&metadata.key, conditions)
                .await?;
            let mut length = 0_u64;
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                if chunk.len() as u64 > metadata.content_length.saturating_sub(length) {
                    return Err(invalid_length());
                }
                let count = chunk.len() as u64;
                write_part(staging.clone(), length, chunk).await?;
                length += count;
            }
            if length != metadata.content_length {
                return Err(invalid_length());
            }
            Ok(())
        })
        .await
    }
}

fn check_conditions(metadata: &ObjectMetadata, conditions: &GetConditions) -> Result<()> {
    if conditions
        .if_match
        .as_ref()
        .is_some_and(|etag| etag != "*" && metadata.etag.as_ref() != Some(etag))
    {
        return Err(MscError::new(
            ErrorKind::PreconditionFailed,
            "download source does not match the requested ETag",
        ));
    }
    if conditions
        .if_none_match
        .as_ref()
        .is_some_and(|etag| etag == "*" || metadata.etag.as_ref() == Some(etag))
    {
        return Err(MscError::new(
            ErrorKind::NotModified,
            "download source matches the excluded ETag",
        ));
    }
    Ok(())
}

fn generation_pinned(conditions: &GetConditions) -> bool {
    conditions.version.is_some()
        || conditions.if_match.as_deref().is_some_and(|etag| {
            let etag = etag.trim();
            !etag.is_empty() && etag != "*" && !etag.starts_with("W/") && !etag.contains(',')
        })
}

async fn write_part(staging: Arc<Staging>, offset: u64, bytes: Bytes) -> Result<()> {
    blocking(move || staging.write_at(offset, &bytes)).await
}

fn invalid_length() -> MscError {
    MscError::new(
        ErrorKind::Integrity,
        "downloaded object length differs from metadata",
    )
}

async fn blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| MscError::new(ErrorKind::Cancelled, "download filesystem task failed"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_positioned_write_retains_staging_until_completion() {
        use std::{
            future::Future,
            task::{Context, Waker},
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let parent = tempfile::tempdir().unwrap();
            let staging = Arc::new(Staging::create(parent.path()).unwrap());
            let path = staging.directory.path().to_path_buf();
            let (release, receiver) = std::sync::mpsc::channel();
            let (ready, started) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                ready.send(()).unwrap();
                receiver.recv().unwrap();
            });
            started.await.unwrap();
            let mut write = Box::pin(write_part(staging.clone(), 0, Bytes::from_static(b"part")));
            assert!(
                write
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(write);
            drop(staging);
            assert!(path.exists());
            release.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while path.exists() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
        });
    }
}
