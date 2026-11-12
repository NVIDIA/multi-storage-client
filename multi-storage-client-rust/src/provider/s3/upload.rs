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

//! Bounded part buffering and ownership of cancelled multipart uploads.
use super::{S3Provider, options::config_error, transport::translate};
use crate::{
    error::{ErrorKind, MscError, Result},
    types::{ByteStream, PutResult},
};
use bytes::{Bytes, BytesMut};
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, FuturesUnordered},
};
use object_store::{
    Attributes, MultipartUpload, ObjectStore, PutMultipartOptions,
    client::HttpErrorKind,
    path::Path,
    retry::{RetryContext, RetryFailure},
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(super) const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
pub(super) const MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;
pub(super) const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024;
pub(super) const MAX_PARTS: usize = 10_000;

/// Only a multipart part can safely be replayed after an uncertain response.
/// The SDK retains its upload ID, part number, and bytes, and signs each attempt.
#[derive(Debug)]
struct PartRetryPolicy {
    policy: crate::RetryPolicy,
    timeout: Duration,
}
#[async_trait::async_trait]
impl object_store::retry::RetryPolicy for PartRetryPolicy {
    async fn retry(&self, context: RetryContext) -> bool {
        let transient = match context.failure {
            RetryFailure::Status(status) => matches!(status.as_u16(), 408 | 429 | 500..=599),
            RetryFailure::Transport(kind) => matches!(
                kind,
                HttpErrorKind::Timeout
                    | HttpErrorKind::Connect
                    | HttpErrorKind::Interrupted
                    | HttpErrorKind::Request
            ),
            _ => false,
        };
        if !transient {
            return false;
        }
        let Ok(attempt) = u32::try_from(context.attempt) else {
            return false;
        };
        if attempt >= self.policy.max_attempts || context.elapsed >= self.timeout {
            return false;
        }
        let delay = self.policy.delay(attempt);
        if context
            .elapsed
            .checked_add(delay)
            .is_none_or(|elapsed| elapsed >= self.timeout)
        {
            return false;
        }
        let started = Instant::now();
        tokio::time::sleep(delay).await;
        context
            .elapsed
            .checked_add(started.elapsed())
            .is_some_and(|elapsed| elapsed < self.timeout)
    }
}

impl S3Provider {
    pub(super) async fn multipart(
        &self,
        path: &Path,
        input: ByteStream,
        size: Option<u64>,
        attributes: Attributes,
    ) -> Result<PutResult> {
        let required = multipart_part_size(self.options.part_size, size);
        let part_size = usize::try_from(required)
            .map_err(|_| config_error("multipart part size exceeds address space"))?;
        let mut guard = self.initiate(path, attributes).await?;
        let outcome = async {
            let mut reader = PartReader {
                input,
                pending: Bytes::new(),
                total: 0,
                finished: false,
            };
            let mut requests = FuturesUnordered::new();
            let mut count = 0;
            while let Some(part) = reader.next(part_size).await? {
                if size.is_some_and(|size| reader.total > size) {
                    return Err(MscError::new(
                        ErrorKind::Integrity,
                        "upload stream exceeds its declared size",
                    ));
                }
                if count == MAX_PARTS {
                    return Err(MscError::new(
                        ErrorKind::Integrity,
                        "S3 upload exceeds 10,000 parts; supply its size to allow part resizing",
                    ));
                }
                while requests.len() >= self.options.multipart_concurrency {
                    requests
                        .try_next()
                        .await
                        .map_err(|error| translate(error, "put", path.as_ref()))?;
                }
                requests.push(
                    guard
                        .upload
                        .as_mut()
                        .expect("active upload")
                        .put_part(part.into()),
                );
                count += 1;
            }
            check_size(size, reader.total)?;
            while requests
                .try_next()
                .await
                .map_err(|error| translate(error, "put", path.as_ref()))?
                .is_some()
            {}
            let result = guard
                .upload
                .as_mut()
                .expect("active upload")
                .complete()
                .await
                .map_err(|error| translate(error, "put", path.as_ref()))?;
            let mut result = self.put_result(result);
            result.content_length = Some(reader.total);
            Ok(result)
        }
        .await;
        if outcome.is_ok() {
            guard.upload.take();
        } else if let Some(upload) = guard.upload.as_mut()
            && abort_upload(upload.as_mut()).await.is_ok()
        {
            guard.upload.take();
        }
        outcome
    }
    async fn initiate(&self, path: &Path, attributes: Attributes) -> Result<UploadGuard> {
        let _ = self.cleanup.drain().await;
        let acquire = Arc::clone(&self.cleanup.permits).acquire_owned();
        tokio::pin!(acquire);
        let permit = loop {
            tokio::select! {
                permit = &mut acquire => break permit.map_err(|_| MscError::new(ErrorKind::Cancelled, "multipart uploads are shutting down"))?,
                () = self.cleanup.ready.notified() => { let _ = self.cleanup.drain().await; },
            }
        };
        // Let initiation finish if its caller is cancelled, so a returned upload
        // ID always acquires a cleanup guard before the task result is dropped.
        let store = Arc::clone(&self.store);
        let upload_path = path.clone();
        let cleanup = Arc::clone(&self.cleanup);
        let retry_policy = Arc::new(PartRetryPolicy {
            // Capture before spawning initiation: task-local scopes belong to
            // the caller and are not implicitly inherited by new Tokio tasks.
            policy: crate::client::current_provider_retry_policy()
                .unwrap_or_else(|| self.retry_policy.clone()),
            timeout: Duration::from_millis(self.options.read_timeout_ms),
        });
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let result = store
                .put_multipart_opts(
                    &upload_path,
                    PutMultipartOptions {
                        attributes,
                        ..Default::default()
                    }
                    .with_retry_policy(retry_policy),
                )
                .await
                .map(|upload| UploadGuard {
                    upload: Some(upload),
                    cleanup,
                    permit: Some(permit),
                });
            // A cancelled receiver drops its result and queues the abort.
            let _ = sender.send(result);
        });
        {
            let mut tasks = self.cleanup.initiations.lock().expect("cleanup task lock");
            tasks.retain(|task| !task.is_finished());
            tasks.push(task);
        }
        let guard = receiver
            .await
            .map_err(|_| {
                MscError::new(
                    ErrorKind::Cancelled,
                    "S3 multipart initiation task was cancelled",
                )
            })?
            .map_err(|error| translate(error, "put", path.as_ref()))?;
        Ok(guard)
    }
}

async fn abort_upload(upload: &mut dyn MultipartUpload) -> Result<()> {
    match upload
        .abort()
        .await
        .map_err(|error| translate(error, "abort", ""))
    {
        Err(error) if error.kind == ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

type PendingUpload = (
    Box<dyn MultipartUpload>,
    Option<tokio::sync::OwnedSemaphorePermit>,
);

/// Drop only records cleanup; explicit shutdown owns and awaits all network I/O.
pub(super) struct Cleanup {
    uploads: Mutex<VecDeque<PendingUpload>>,
    initiations: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    permits: Arc<tokio::sync::Semaphore>,
    ready: tokio::sync::Notify,
}
impl Cleanup {
    pub(super) fn new(maximum: usize) -> Self {
        Self {
            uploads: Mutex::new(VecDeque::new()),
            initiations: Mutex::new(Vec::new()),
            permits: Arc::new(tokio::sync::Semaphore::new(maximum)),
            ready: tokio::sync::Notify::new(),
        }
    }
    async fn drain(self: &Arc<Self>) -> Result<()> {
        let pending = self.uploads.lock().expect("cleanup upload lock").len();
        let mut first_error = None;
        for _ in 0..pending {
            let next = self
                .uploads
                .lock()
                .expect("cleanup upload lock")
                .pop_front();
            let Some((upload, permit)) = next else {
                break;
            };
            let mut guard = UploadGuard {
                upload: Some(upload),
                cleanup: Arc::clone(self),
                permit,
            };
            match abort_upload(guard.upload.as_mut().expect("active upload").as_mut()).await {
                Ok(()) => {
                    guard.upload.take();
                }
                Err(error) => {
                    guard.permit.take();
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

struct UploadGuard {
    upload: Option<Box<dyn MultipartUpload>>,
    cleanup: Arc<Cleanup>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
struct InitiationGuard {
    task: Option<tokio::task::JoinHandle<()>>,
    cleanup: Arc<Cleanup>,
}
impl Drop for InitiationGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.cleanup
                .initiations
                .lock()
                .expect("cleanup task lock")
                .push(task);
        }
    }
}
impl Drop for UploadGuard {
    fn drop(&mut self) {
        if let Some(upload) = self.upload.take() {
            let wake = self.permit.is_some();
            self.cleanup
                .uploads
                .lock()
                .expect("cleanup upload lock")
                .push_back((upload, self.permit.take()));
            if wake {
                self.cleanup.ready.notify_one();
            }
        }
    }
}
pub(super) struct PartReader {
    pub(super) input: ByteStream,
    pub(super) pending: Bytes,
    pub(super) total: u64,
    pub(super) finished: bool,
}
impl PartReader {
    pub(super) async fn next(&mut self, capacity: usize) -> Result<Option<Bytes>> {
        let mut buffer = BytesMut::new();
        while buffer.len() < capacity {
            if self.pending.is_empty() {
                if self.finished {
                    break;
                }
                if let Some(value) = self.input.next().await {
                    self.pending = value?;
                    if self.pending.is_empty() {
                        continue;
                    }
                } else {
                    self.finished = true;
                    break;
                }
            }
            let length = (capacity - buffer.len()).min(self.pending.len());
            buffer.extend_from_slice(&self.pending.split_to(length));
            self.total = self
                .total
                .checked_add(length as u64)
                .ok_or_else(|| MscError::new(ErrorKind::Integrity, "upload byte count overflow"))?;
            if self.total > MAX_OBJECT_SIZE {
                return Err(MscError::new(
                    ErrorKind::Integrity,
                    "S3 object exceeds 5 TiB",
                ));
            }
        }
        Ok((!buffer.is_empty()).then(|| buffer.freeze()))
    }
    pub(super) fn remainder(self) -> ByteStream {
        if self.finished {
            return Box::pin(stream::once(async { Ok(self.pending) }));
        }
        Box::pin(stream::once(async { Ok(self.pending) }).chain(self.input))
    }
}
pub(super) fn check_size(expected: Option<u64>, actual: u64) -> Result<()> {
    if expected.is_some_and(|size| size != actual) {
        return Err(MscError::new(
            ErrorKind::Integrity,
            "upload stream length differs from its declared size",
        ));
    }
    Ok(())
}

impl Cleanup {
    pub(super) async fn shutdown(self: &Arc<Self>) -> Result<()> {
        loop {
            let next = self.initiations.lock().expect("cleanup task lock").pop();
            let Some(task) = next else {
                break;
            };
            let mut guard = InitiationGuard {
                task: Some(task),
                cleanup: Arc::clone(self),
            };
            let result = guard.task.as_mut().expect("initiation task").await;
            guard.task.take();
            result.map_err(|_| {
                MscError::new(
                    ErrorKind::Cancelled,
                    "multipart initiation task did not finish during shutdown",
                )
            })?;
        }
        self.drain().await
    }
}
fn multipart_part_size(configured: u64, size: Option<u64>) -> u64 {
    size.map_or(configured, |size| {
        size.div_ceil(MAX_PARTS as u64).max(configured)
    })
}

#[cfg(test)]
mod tests {
    use super::super::S3Options;
    use super::*;
    use crate::{
        credentials::{AwsCredentials, SecretString},
        provider::StorageProvider,
    };
    use std::time::Duration;
    #[test]
    fn multipart_resizes_at_the_part_limit() {
        assert_eq!(multipart_part_size(MIN_PART_SIZE, None), MIN_PART_SIZE);
        assert_eq!(
            multipart_part_size(MIN_PART_SIZE, Some(MIN_PART_SIZE * MAX_PARTS as u64)),
            MIN_PART_SIZE
        );
        let resized =
            multipart_part_size(MIN_PART_SIZE, Some(MIN_PART_SIZE * MAX_PARTS as u64 + 1));
        assert!(resized > MIN_PART_SIZE);
        assert!((MIN_PART_SIZE * MAX_PARTS as u64 + 1).div_ceil(resized) <= MAX_PARTS as u64);
        assert!(multipart_part_size(MIN_PART_SIZE, Some(MAX_OBJECT_SIZE)) <= MAX_PART_SIZE);
    }

    #[derive(Debug)]
    struct AbortFailure(bool);
    #[async_trait::async_trait]
    impl MultipartUpload for AbortFailure {
        fn put_part(&mut self, _data: object_store::PutPayload) -> object_store::UploadPart {
            unreachable!("cleanup only")
        }
        async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
            unreachable!("cleanup only")
        }
        async fn abort(&mut self) -> object_store::Result<()> {
            if self.0 {
                Err(object_store::Error::NotFound {
                    path: "expired".into(),
                    source: std::io::Error::other("gone").into(),
                })
            } else {
                Err(object_store::Error::Generic {
                    store: "test",
                    source: std::io::Error::other("abort unavailable").into(),
                })
            }
        }
    }
    #[tokio::test]
    async fn failed_aborts_retain_cleanup_without_holding_upload_admission() {
        let cleanup = Arc::new(Cleanup::new(1));
        let permit = cleanup.permits.clone().acquire_owned().await.unwrap();
        drop(UploadGuard {
            upload: Some(Box::new(AbortFailure(false))),
            cleanup: cleanup.clone(),
            permit: Some(permit),
        });
        cleanup.ready.notified().await;
        assert!(cleanup.drain().await.is_err());
        assert!(futures::FutureExt::now_or_never(cleanup.ready.notified()).is_none());
        let next = cleanup
            .permits
            .clone()
            .try_acquire_owned()
            .expect("failed cleanup must allow another upload");
        assert_eq!(cleanup.uploads.lock().unwrap().len(), 1);
        drop(next);
        assert!(cleanup.shutdown().await.is_err());
        assert_eq!(cleanup.uploads.lock().unwrap().len(), 1);
        cleanup.uploads.lock().unwrap().front_mut().unwrap().0 = Box::new(AbortFailure(true));
        cleanup.shutdown().await.unwrap();
        assert!(cleanup.uploads.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn part_reader_splits_chunks_and_preserves_source_failures() {
        let input = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"abcdef")),
            Ok(Bytes::new()),
            Ok(Bytes::from_static(b"g")),
        ]));
        let mut reader = PartReader {
            input,
            pending: Bytes::new(),
            total: 0,
            finished: false,
        };
        assert_eq!(
            reader.next(3).await.unwrap().unwrap(),
            Bytes::from_static(b"abc")
        );
        assert_eq!(
            reader.next(3).await.unwrap().unwrap(),
            Bytes::from_static(b"def")
        );
        assert_eq!(
            reader.next(3).await.unwrap().unwrap(),
            Bytes::from_static(b"g")
        );
        assert!(reader.next(3).await.unwrap().is_none());
        assert_eq!(reader.total, 7);
        assert_eq!(
            check_size(Some(8), reader.total).unwrap_err().kind,
            ErrorKind::Integrity
        );
        let mut reader = PartReader {
            input: Box::pin(stream::once(async {
                Err(MscError::new(ErrorKind::Io, "source failed"))
            })),
            pending: Bytes::new(),
            total: 0,
            finished: false,
        };
        assert_eq!(reader.next(3).await.unwrap_err().kind, ErrorKind::Io);
    }
    #[tokio::test]
    async fn part_reader_remembers_eof_of_a_non_fused_body_after_a_short_part() {
        let input = stream::unfold(Some(Bytes::from_static(b"abcdefg")), |state| async move {
            state.map(|bytes| (Ok(bytes), None))
        });
        let mut reader = PartReader {
            input: Box::pin(input),
            pending: Bytes::new(),
            total: 0,
            finished: false,
        };
        assert_eq!(
            reader.next(5).await.unwrap().unwrap(),
            Bytes::from_static(b"abcde")
        );
        assert_eq!(
            reader.next(5).await.unwrap().unwrap(),
            Bytes::from_static(b"fg")
        );
        assert!(reader.next(5).await.unwrap().is_none());
        assert!(reader.next(5).await.unwrap().is_none());
        assert_eq!(reader.total, 7);
    }
    #[tokio::test]
    async fn cancelled_shutdown_keeps_initiation_tasks_owned() {
        let provider = S3Provider::new(
            S3Options {
                base_path: "bucket".into(),
                ..Default::default()
            },
            Some(AwsCredentials {
                access_key_id: SecretString::new("test-access"),
                secret_access_key: SecretString::new("test-secret"),
                session_token: None,
                expires_at: None,
            }),
        )
        .unwrap();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            receiver.await.unwrap();
        });
        provider.cleanup.initiations.lock().unwrap().push(task);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), provider.shutdown())
                .await
                .is_err()
        );
        assert_eq!(provider.cleanup.initiations.lock().unwrap().len(), 1);
        sender.send(()).unwrap();
        provider.shutdown().await.unwrap();
        assert!(provider.cleanup.initiations.lock().unwrap().is_empty());
    }
}
