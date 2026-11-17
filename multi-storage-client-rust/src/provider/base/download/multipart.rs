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

//! Bounded missing-part downloads and atomic checkpoint publication.
use super::super::BaseStorageProvider;
use super::{
    blocking, invalid_length,
    state::{Chunk, Directory, DownloadState},
};
use crate::{ByteRange, ErrorKind, GetConditions, MscError, ObjectMetadata, Result};
use futures::StreamExt;
use std::{path::Path, sync::Arc};
use tokio::task::JoinSet;

impl<B: crate::StorageBackend + ?Sized + 'static> BaseStorageProvider<B> {
    pub(super) async fn download_resumable(
        &self,
        metadata: &ObjectMetadata,
        conditions: &GetConditions,
        local: &Path,
        part_size: u64,
        concurrency: usize,
    ) -> Result<()> {
        let version = metadata.source_version.as_ref().and_then(|v| {
            if v.value.starts_with("etag:") {
                None
            } else {
                Some(
                    v.value
                        .strip_prefix("version:")
                        .unwrap_or(&v.value)
                        .to_owned(),
                )
            }
        });
        let etag = metadata
            .etag
            .clone()
            .filter(|v| !v.is_empty() && !v.starts_with("W/") && v != "*" && !v.contains(','));
        let state = DownloadState::new(
            self.state
                .provider
                .physical_root()
                .join(&metadata.key)?
                .to_string(),
            metadata.content_length,
            etag,
            version,
            part_size,
            self.state.provider.backend_fingerprint(),
        );
        let local_owned = local.to_owned();
        let directory = blocking(move || Directory::open(&local_owned, state)).await?;
        let mut tasks = JoinSet::new();
        let result: Result<()> = async {
            for index in 0..directory.state.count() {
                let inspect = directory.clone();
                if blocking(move || inspect.has_chunk(index)).await? {
                    continue;
                }
                if tasks.len() >= concurrency {
                    finish_part(&mut tasks).await?;
                }
                let client = self.clone();
                let metadata = metadata.clone();
                let conditions = conditions.clone();
                let directory = directory.clone();
                tasks.spawn(async move {
                    client
                        .download_chunk(&metadata, &conditions, directory, index)
                        .await
                });
            }
            while !tasks.is_empty() {
                finish_part(&mut tasks).await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            tasks.shutdown().await;
            return Err(error);
        }
        let merge = directory.clone();
        let merged = blocking(move || merge.merge()).await?;
        let directory = merged.publish(local)?;
        blocking(move || {
            if let Err(error) = directory.cleanup() {
                log::warn!("download published but checkpoint cleanup failed: {error}");
            }
            Ok(())
        })
        .await
    }

    async fn download_chunk(
        &self,
        metadata: &ObjectMetadata,
        conditions: &GetConditions,
        directory: Arc<Directory>,
        index: u64,
    ) -> Result<()> {
        let length = directory.state.length(index);
        let range = ByteRange {
            offset: index * directory.state.chunk_size,
            length,
        };
        self.retry(true, || {
            let directory = directory.clone();
            async move {
                let chunk = blocking(move || Chunk::create(directory, index)).await?;
                let mut body = self
                    .state
                    .provider
                    .get_download_part(&metadata.key, range, conditions, metadata.content_length)
                    .await?;
                let mut written = 0_u64;
                while let Some(bytes) = body.next().await {
                    let bytes = bytes?;
                    if bytes.len() as u64 > length - written {
                        return Err(invalid_length());
                    }
                    written += bytes.len() as u64;
                    let output = chunk.clone();
                    blocking(move || output.write(&bytes)).await?;
                }
                if written != length {
                    return Err(invalid_length());
                }
                blocking(move || chunk.publish()).await
            }
        })
        .await
    }
}

async fn finish_part(tasks: &mut JoinSet<Result<()>>) -> Result<()> {
    match tasks.join_next().await {
        Some(Ok(result)) => result,
        Some(Err(_)) => Err(MscError::new(
            ErrorKind::Cancelled,
            "download part task failed",
        )),
        None => Ok(()),
    }
}
