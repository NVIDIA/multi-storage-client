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

//! Bounded streaming downloads with atomic destination publication.
use super::{StorageClient, require_file, stable_conditions};
use crate::{ErrorKind, MscError, ObjectMetadata, ReadOptions, Result};
use futures::StreamExt;
use std::path::Path;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

impl StorageClient {
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
        self.download_stream(&metadata, local, &options).await?;
        Ok(metadata)
    }
    async fn download_stream(
        &self,
        metadata: &ObjectMetadata,
        local: &Path,
        options: &crate::ReadOptions,
    ) -> Result<()> {
        let parent = local
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let (directory, temporary) = blocking(move || {
            let directory = tempfile::Builder::new()
                .prefix(".msc-tmp-download-")
                .tempdir_in(parent)?;
            let temporary = tempfile::NamedTempFile::new_in(directory.path())?;
            Ok((directory, temporary))
        })
        .await?;
        let file =
            tokio::sync::Mutex::new(tokio::fs::File::from_std(temporary.as_file().try_clone()?));
        let conditions = stable_conditions(metadata, &options.conditions);
        self.retry(true, || async {
            let mut file = file.lock().await;
            file.flush().await?;
            file.set_len(0).await?;
            file.seek(std::io::SeekFrom::Start(0)).await?;
            let mut body = self.state.provider.get(&metadata.key, &conditions).await?;
            let mut length = 0_u64;
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                length = length
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(invalid_length)?;
                if length > metadata.content_length {
                    return Err(invalid_length());
                }
                file.write_all(&chunk).await?;
            }
            if length != metadata.content_length {
                return Err(invalid_length());
            }
            file.flush().await?;
            file.sync_all().await?;
            Ok(())
        })
        .await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let original = temporary.as_file().metadata()?;
            let current = std::fs::symlink_metadata(temporary.path())?;
            if current.dev() != original.dev() || current.ino() != original.ino() {
                return Err(MscError::new(
                    ErrorKind::Integrity,
                    "download staging entry was replaced",
                ));
            }
        }
        temporary
            .persist(local)
            .map_err(|error| MscError::from(error.error))?;
        drop(directory);
        Ok(())
    }
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
