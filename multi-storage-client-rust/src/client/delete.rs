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

//! Exact-key, idempotent deletion and bounded partial-result batches.
use super::{StorageClient, nonroot_path};
use crate::{BatchFailure, BatchResult, DeleteOptions, ErrorKind, MscError, ObjectPath, Result};
use futures::{StreamExt, stream};

const DELETE_BATCH_SIZE: usize = 1000;

impl StorageClient {
    /// Delete one exact key, treating missing objects as success.
    /// Recursive deletion is outside the foundation subset.
    ///
    /// # Errors
    /// Returns invalid keys, unsupported recursion/conditions, or provider errors.
    pub async fn delete(
        &self,
        path: impl AsRef<str>,
        recursive: bool,
        options: DeleteOptions,
    ) -> Result<()> {
        if recursive {
            return Err(MscError::unsupported(
                "recursive deletion; list and delete exact keys instead",
            ));
        }
        let path = nonroot_path(path)?;
        self.retry_mutation(|| async {
            match self.state.provider.delete(&path, &options).await {
                Err(error) if error.kind == ErrorKind::NotFound => Ok(()),
                result => result,
            }
        })
        .await
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
        let mut output = BatchResult::default();
        for (chunk_index, chunk) in paths.chunks(DELETE_BATCH_SIZE).enumerate() {
            let offset = chunk_index * DELETE_BATCH_SIZE;
            if !self.capabilities().batch_delete {
                let options = &options;
                let mut deletes = stream::iter(chunk.iter().cloned().enumerate().map(
                    |(index, path)| async move {
                        (
                            offset + index,
                            path.clone(),
                            self.delete(&path, false, options.clone()).await,
                        )
                    },
                ))
                .buffer_unordered(32);
                while let Some((index, path, result)) = deletes.next().await {
                    match result {
                        Ok(()) => output.succeeded.push(index),
                        Err(error) => output.failed.push(BatchFailure {
                            index,
                            source: path,
                            destination: None,
                            error,
                        }),
                    }
                }
                continue;
            }
            let mut pending: Vec<_> = chunk
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, path)| (offset + index, path))
                .collect();
            let mut attempts = 0;
            while !pending.is_empty() {
                attempts += 1;
                let keys: Vec<_> = pending.iter().map(|(_, path)| path.clone()).collect();
                let batch = self.state.provider.delete_many(&keys, &options).await;
                validate_batch(&batch, pending.len())?;
                for index in batch.succeeded {
                    output.succeeded.push(pending[index].0);
                }
                let mut retry = Vec::new();
                let mut retry_error = None;
                for failure in batch.failed {
                    let (index, source) = pending[failure.index].clone();
                    if failure.error.kind == ErrorKind::NotFound {
                        output.succeeded.push(index);
                    } else if self.can_retry(&failure.error, true, attempts) {
                        retry_error = Some(failure.error);
                        retry.push((index, source));
                    } else {
                        let error = if attempts > 1
                            && failure.error.kind == ErrorKind::PreconditionFailed
                        {
                            failure.error.ambiguous_outcome()
                        } else {
                            failure.error
                        };
                        output.failed.push(BatchFailure {
                            index,
                            source,
                            destination: failure.destination,
                            error,
                        });
                    }
                }
                if let Some(error) = retry_error {
                    self.retry_delay(&error, attempts).await;
                }
                pending = retry;
            }
        }
        output.succeeded.sort_unstable();
        output.failed.sort_by_key(|failure| failure.index);
        Ok(output)
    }
}

fn validate_batch(batch: &BatchResult, expected: usize) -> Result<()> {
    let mut indices = std::collections::BTreeSet::new();
    for index in batch
        .succeeded
        .iter()
        .copied()
        .chain(batch.failed.iter().map(|failure| failure.index))
    {
        if index >= expected || !indices.insert(index) {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "provider returned invalid or duplicate batch indices",
            ));
        }
    }
    if indices.len() != expected {
        return Err(MscError::new(
            ErrorKind::Integrity,
            "provider omitted batch results",
        ));
    }
    Ok(())
}
