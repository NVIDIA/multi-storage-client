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

//! Bounded, forward-only streaming upload of a single object.
//!
//! Bytes are copied into one partial part. Each full part is handed to a multipart
//! upload once fewer than `max_inflight_parts` parts are in flight, so retained
//! memory stays below `(max_inflight_parts + 1) * part_size`. The multipart upload
//! is created lazily: objects smaller than one part are written with a single PUT.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::BytesMut;
use object_store::path::Path;
use object_store::{
    Attributes, MultipartUpload, ObjectStore, PutPayload, PutResult, UploadPart, WriteMultipart,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum StreamError {
    #[error("I/O operation on a closed or aborted stream")]
    Closed,
    #[error("Streaming upload failed earlier and cannot be resumed: {0}")]
    Failed(String),
    #[error("{0}")]
    Limit(String),
    #[error(transparent)]
    Store(#[from] object_store::Error),
    #[error("Multipart completion failed; the object may or may not have been published: {0}")]
    AmbiguousCompletion(String),
}

/// Destination of one streamed object.
#[async_trait]
pub(crate) trait StreamTarget: Send + Sync + fmt::Debug {
    async fn put(&self, payload: PutPayload) -> object_store::Result<PutResult>;
    async fn start_multipart(&self) -> object_store::Result<Box<dyn MultipartUpload>>;
}

#[derive(Debug)]
pub(crate) struct StoreTarget {
    pub store: Arc<dyn ObjectStore>,
    pub path: Path,
    pub attributes: Attributes,
}

#[async_trait]
impl StreamTarget for StoreTarget {
    async fn put(&self, payload: PutPayload) -> object_store::Result<PutResult> {
        self.store
            .put_opts(&self.path, payload, self.attributes.clone().into())
            .await
    }

    async fn start_multipart(&self) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store
            .put_multipart_opts(&self.path, self.attributes.clone().into())
            .await
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamConfig {
    pub part_size: usize,
    pub max_inflight_parts: usize,
    pub max_parts: u64,
    /// How long abort waits for in-flight parts before cancelling them.
    pub abort_drain_timeout: Duration,
}

/// Waits for every in-flight part, returning the first part error.
async fn drain(writer: &mut WriteMultipart) -> object_store::Result<()> {
    let mut first_err = None;
    while let Err(err) = writer.wait_for_capacity(0).await {
        first_err.get_or_insert(err);
    }
    first_err.map_or(Ok(()), Err)
}

/// Aborts the multipart upload after in-flight parts settle.
///
/// A part request the service already received can still succeed after an abort, recreating the upload, so parts
/// are drained first and only cancelled once `timeout` expires.
async fn drain_and_abort(
    mut writer: WriteMultipart,
    timeout: Duration,
) -> object_store::Result<()> {
    let _ = tokio::time::timeout(timeout, drain(&mut writer)).await;
    writer.abort().await
}

#[derive(Debug)]
struct StreamCounters {
    origin: Instant,
    submitted_bytes: AtomicU64,
    completed_bytes: AtomicU64,
    released_bytes: AtomicU64,
    parts_submitted: AtomicU64,
    parts_completed: AtomicU64,
    inflight_parts: AtomicU64,
    peak_inflight_parts: AtomicU64,
    first_part_submit_ns: AtomicU64,
    first_part_complete_ns: AtomicU64,
    completion_attempted: AtomicBool,
    abort_attempted: AtomicBool,
    abort_failed: AtomicBool,
}

impl StreamCounters {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
            submitted_bytes: AtomicU64::new(0),
            completed_bytes: AtomicU64::new(0),
            released_bytes: AtomicU64::new(0),
            parts_submitted: AtomicU64::new(0),
            parts_completed: AtomicU64::new(0),
            inflight_parts: AtomicU64::new(0),
            peak_inflight_parts: AtomicU64::new(0),
            first_part_submit_ns: AtomicU64::new(0),
            first_part_complete_ns: AtomicU64::new(0),
            completion_attempted: AtomicBool::new(false),
            abort_attempted: AtomicBool::new(false),
            abort_failed: AtomicBool::new(false),
        }
    }

    fn elapsed_ns(&self) -> u64 {
        // Zero means "not observed", so the earliest observable time is 1 ns.
        (self.origin.elapsed().as_nanos() as u64).max(1)
    }
}

/// Releases a part's accounting when its upload future finishes or is dropped.
struct PartGuard {
    counters: Arc<StreamCounters>,
    bytes: u64,
}

impl Drop for PartGuard {
    fn drop(&mut self) {
        self.counters
            .released_bytes
            .fetch_add(self.bytes, Ordering::Relaxed);
        self.counters.inflight_parts.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct CountingUpload {
    inner: Box<dyn MultipartUpload>,
    counters: Arc<StreamCounters>,
}

#[async_trait]
impl MultipartUpload for CountingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = data.content_length() as u64;
        let counters = Arc::clone(&self.counters);
        counters.submitted_bytes.fetch_add(bytes, Ordering::Relaxed);
        counters.parts_submitted.fetch_add(1, Ordering::Relaxed);
        let inflight = counters.inflight_parts.fetch_add(1, Ordering::Relaxed) + 1;
        counters
            .peak_inflight_parts
            .fetch_max(inflight, Ordering::Relaxed);
        let _ = counters.first_part_submit_ns.compare_exchange(
            0,
            counters.elapsed_ns(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        let part = self.inner.put_part(data);
        let guard = PartGuard { counters, bytes };
        Box::pin(async move {
            part.await?;
            let counters = &guard.counters;
            counters.completed_bytes.fetch_add(bytes, Ordering::Relaxed);
            counters.parts_completed.fetch_add(1, Ordering::Relaxed);
            let _ = counters.first_part_complete_ns.compare_exchange(
                0,
                counters.elapsed_ns(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.counters
            .completion_attempted
            .store(true, Ordering::Relaxed);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.counters.abort_attempted.store(true, Ordering::Relaxed);
        let result = self.inner.abort().await;
        if result.is_err() {
            self.counters.abort_failed.store(true, Ordering::Relaxed);
        }
        result
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    Open,
    Closed,
    Aborted,
    Failed(String),
}

/// Point-in-time stream counters. Times are nanoseconds since the stream opened; zero means not observed.
#[derive(Debug, Clone, Default)]
pub(crate) struct StreamStats {
    pub accepted_bytes: u64,
    pub submitted_bytes: u64,
    pub completed_bytes: u64,
    pub retained_bytes: u64,
    pub peak_retained_bytes: u64,
    pub parts_submitted: u64,
    pub parts_completed: u64,
    pub inflight_parts: u64,
    pub peak_inflight_parts: u64,
    pub capacity_wait_ns: u64,
    pub first_part_submit_ns: u64,
    pub first_part_complete_ns: u64,
    pub close_complete_ns: u64,
    pub part_size: u64,
    pub max_inflight_parts: u64,
    pub multipart: bool,
    pub completion_attempted: bool,
    pub abort_attempted: bool,
    pub abort_failed: bool,
    pub state: &'static str,
}

pub(crate) struct StreamCore {
    target: Arc<dyn StreamTarget>,
    config: StreamConfig,
    partial: BytesMut,
    writer: Option<WriteMultipart>,
    started_multipart: bool,
    state: State,
    accepted: u64,
    peak_retained: u64,
    capacity_wait_ns: u64,
    close_complete_ns: u64,
    counters: Arc<StreamCounters>,
}

impl fmt::Debug for StreamCore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamCore")
            .field("target", &self.target)
            .field("config", &self.config)
            .field("state", &self.state)
            .field("accepted", &self.accepted)
            .finish()
    }
}

impl StreamCore {
    pub fn new(target: Arc<dyn StreamTarget>, config: StreamConfig) -> Self {
        assert!(config.part_size > 0 && config.max_inflight_parts > 0 && config.max_parts > 0);
        Self {
            target,
            config,
            partial: BytesMut::new(),
            writer: None,
            started_multipart: false,
            state: State::Open,
            accepted: 0,
            peak_retained: 0,
            capacity_wait_ns: 0,
            close_complete_ns: 0,
            counters: Arc::new(StreamCounters::new()),
        }
    }

    pub fn is_open(&self) -> bool {
        self.state == State::Open
    }

    pub fn accepted(&self) -> u64 {
        self.accepted
    }

    fn ensure_open(&self) -> Result<(), StreamError> {
        match &self.state {
            State::Open => Ok(()),
            State::Failed(msg) => Err(StreamError::Failed(msg.clone())),
            State::Closed | State::Aborted => Err(StreamError::Closed),
        }
    }

    /// Copies as much of `data` as fits in the current part. Never blocks.
    ///
    /// A [`StreamError::Limit`] leaves the stream open; pass it to [`StreamCore::fail`] to abort.
    pub fn accept(&mut self, data: &[u8]) -> Result<usize, StreamError> {
        self.ensure_open()?;
        if data.is_empty() {
            return Ok(0);
        }
        let room = self.config.part_size - self.partial.len();
        let n = room.min(data.len());
        let limit = self.config.part_size as u64 * self.config.max_parts;
        if self.accepted + n as u64 > limit {
            return Err(StreamError::Limit(format!(
                "Streaming upload exceeds {} bytes ({} parts of {} bytes); configure a larger part size",
                limit, self.config.max_parts, self.config.part_size
            )));
        }
        let needed = self.partial.len() + n;
        if needed > self.partial.capacity() {
            let target = needed
                .max(self.partial.capacity() * 2)
                .min(self.config.part_size);
            self.partial.reserve(target - self.partial.len());
        }
        self.partial.extend_from_slice(&data[..n]);
        self.accepted += n as u64;
        let released = self.counters.released_bytes.load(Ordering::Relaxed);
        self.peak_retained = self.peak_retained.max(self.accepted - released);
        Ok(n)
    }

    pub fn part_ready(&self) -> bool {
        self.partial.len() == self.config.part_size
    }

    /// Hands the full partial part to the multipart upload, waiting for in-flight capacity first.
    pub async fn submit_part(&mut self) -> Result<(), StreamError> {
        self.ensure_open()?;
        if !self.part_ready() {
            return Ok(());
        }
        match self.submit_part_inner().await {
            Ok(()) => Ok(()),
            Err(err) => Err(self.fail(err.into()).await),
        }
    }

    async fn submit_part_inner(&mut self) -> object_store::Result<()> {
        if self.writer.is_none() {
            let upload = self.target.start_multipart().await?;
            self.started_multipart = true;
            let counting = CountingUpload {
                inner: upload,
                counters: Arc::clone(&self.counters),
            };
            self.writer = Some(WriteMultipart::new_with_chunk_size(
                Box::new(counting),
                self.config.part_size,
            ));
        }
        let writer = self.writer.as_mut().expect("writer exists");
        let start = Instant::now();
        let waited = writer
            .wait_for_capacity(self.config.max_inflight_parts)
            .await;
        self.capacity_wait_ns += start.elapsed().as_nanos() as u64;
        waited?;
        writer.put(self.partial.split().freeze());
        Ok(())
    }

    /// Copies all of `data`, submitting parts as they fill. Used by tests and non-Python callers.
    #[cfg(test)]
    pub async fn write_all(&mut self, data: &[u8]) -> Result<(), StreamError> {
        let mut offset = 0;
        while offset < data.len() {
            offset += match self.accept(&data[offset..]) {
                Ok(n) => n,
                Err(err @ StreamError::Limit(_)) => return Err(self.fail(err).await),
                Err(err) => return Err(err),
            };
            self.submit_part().await?;
        }
        Ok(())
    }

    /// Collects parts that already finished and raises their errors, without waiting for running parts.
    ///
    /// Serializers such as `torch.save` flush after every record, so waiting here would serialize the upload.
    pub async fn flush(&mut self) -> Result<(), StreamError> {
        self.ensure_open()?;
        let finished = match self.writer.as_mut() {
            Some(writer) => writer.poll_for_capacity(&mut Context::from_waker(Waker::noop()), 0),
            None => Poll::Ready(Ok(())),
        };
        if let Poll::Ready(Err(err)) = finished {
            return Err(self.fail(err.into()).await);
        }
        Ok(())
    }

    /// Waits for every submitted part. A partial part stays buffered; nothing is published.
    pub async fn sync(&mut self) -> Result<(), StreamError> {
        self.ensure_open()?;
        if let Some(writer) = self.writer.as_mut() {
            let start = Instant::now();
            let drained = writer.wait_for_capacity(0).await;
            self.capacity_wait_ns += start.elapsed().as_nanos() as u64;
            if let Err(err) = drained {
                return Err(self.fail(err.into()).await);
            }
        }
        Ok(())
    }

    /// Uploads the remainder and publishes the object. Idempotent after success.
    pub async fn close(&mut self) -> Result<(), StreamError> {
        match &self.state {
            State::Open => {}
            State::Closed => return Ok(()),
            State::Aborted => return Err(StreamError::Closed),
            State::Failed(msg) => return Err(StreamError::Failed(msg.clone())),
        }
        let rest = self.partial.split().freeze();
        let result = match self.writer.take() {
            None => self
                .target
                .put(PutPayload::from_bytes(rest))
                .await
                .map(|_| ())
                .map_err(StreamError::from),
            Some(mut writer) => {
                if !rest.is_empty() {
                    let start = Instant::now();
                    let waited = writer
                        .wait_for_capacity(self.config.max_inflight_parts)
                        .await;
                    self.capacity_wait_ns += start.elapsed().as_nanos() as u64;
                    if let Err(err) = waited {
                        self.writer = Some(writer);
                        return Err(self.fail(err.into()).await);
                    }
                    writer.put(rest);
                }
                if let Err(err) = drain(&mut writer).await {
                    self.writer = Some(writer);
                    return Err(self.fail(err.into()).await);
                }
                // With no parts in flight, `finish` only completes, aborting the upload if that fails.
                writer.finish().await.map(|_| ()).map_err(|err| {
                    if self.counters.completion_attempted.load(Ordering::Relaxed) {
                        StreamError::AmbiguousCompletion(err.to_string())
                    } else {
                        StreamError::from(err)
                    }
                })
            }
        };
        match result {
            Ok(()) => {
                self.state = State::Closed;
                self.close_complete_ns = self.counters.elapsed_ns();
                Ok(())
            }
            Err(err) => {
                self.state = State::Failed(err.to_string());
                Err(err)
            }
        }
    }

    /// Drains or cancels in-flight parts and aborts the multipart upload. Never deletes an existing object.
    pub async fn abort(&mut self) -> Result<(), StreamError> {
        match &self.state {
            State::Open => {}
            State::Aborted | State::Failed(_) => return Ok(()),
            State::Closed => return Err(StreamError::Closed),
        }
        self.state = State::Aborted;
        self.partial = BytesMut::new();
        match self.writer.take() {
            Some(writer) => drain_and_abort(writer, self.config.abort_drain_timeout)
                .await
                .map_err(StreamError::from),
            None => Ok(()),
        }
    }

    /// Records a sticky failure, aborts the multipart upload, and returns `err`.
    pub async fn fail(&mut self, err: StreamError) -> StreamError {
        let mut msg = err.to_string();
        self.partial = BytesMut::new();
        if let Some(writer) = self.writer.take() {
            if let Err(abort_err) = drain_and_abort(writer, self.config.abort_drain_timeout).await {
                msg = format!("{msg}; aborting the multipart upload also failed: {abort_err}");
            }
        }
        self.state = State::Failed(msg);
        err
    }

    pub fn stats(&self) -> StreamStats {
        let c = &self.counters;
        let released = c.released_bytes.load(Ordering::Relaxed);
        StreamStats {
            accepted_bytes: self.accepted,
            submitted_bytes: c.submitted_bytes.load(Ordering::Relaxed),
            completed_bytes: c.completed_bytes.load(Ordering::Relaxed),
            retained_bytes: if self.state == State::Open {
                self.accepted.saturating_sub(released)
            } else {
                0
            },
            peak_retained_bytes: self.peak_retained,
            parts_submitted: c.parts_submitted.load(Ordering::Relaxed),
            parts_completed: c.parts_completed.load(Ordering::Relaxed),
            inflight_parts: c.inflight_parts.load(Ordering::Relaxed),
            peak_inflight_parts: c.peak_inflight_parts.load(Ordering::Relaxed),
            capacity_wait_ns: self.capacity_wait_ns,
            first_part_submit_ns: c.first_part_submit_ns.load(Ordering::Relaxed),
            first_part_complete_ns: c.first_part_complete_ns.load(Ordering::Relaxed),
            close_complete_ns: self.close_complete_ns,
            part_size: self.config.part_size as u64,
            max_inflight_parts: self.config.max_inflight_parts as u64,
            multipart: self.started_multipart,
            completion_attempted: c.completion_attempted.load(Ordering::Relaxed),
            abort_attempted: c.abort_attempted.load(Ordering::Relaxed),
            abort_failed: c.abort_failed.load(Ordering::Relaxed),
            state: match self.state {
                State::Open => "open",
                State::Closed => "closed",
                State::Aborted => "aborted",
                State::Failed(_) => "failed",
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use tokio::sync::Semaphore;

    #[derive(Debug, Default)]
    struct Faults {
        fail_part: Option<usize>,
        fail_complete: bool,
        commit_then_fail_complete: bool,
        fail_put: bool,
        fail_abort: bool,
    }

    #[derive(Debug)]
    struct TestTarget {
        store: Arc<InMemory>,
        path: Path,
        gate: Option<Arc<Semaphore>>,
        faults: Faults,
        puts: AtomicUsize,
        multipart_starts: AtomicUsize,
        aborts: Arc<AtomicUsize>,
        parts_done_at_abort: Arc<AtomicUsize>,
    }

    impl TestTarget {
        fn new(faults: Faults, gate: Option<Arc<Semaphore>>) -> Arc<Self> {
            Arc::new(Self {
                store: Arc::new(InMemory::new()),
                path: Path::from("bucket/key.bin"),
                gate,
                faults,
                puts: AtomicUsize::new(0),
                multipart_starts: AtomicUsize::new(0),
                aborts: Arc::new(AtomicUsize::new(0)),
                parts_done_at_abort: Arc::new(AtomicUsize::new(0)),
            })
        }

        async fn read(&self) -> Option<Vec<u8>> {
            match self.store.get(&self.path).await {
                Ok(r) => Some(r.bytes().await.unwrap().to_vec()),
                Err(object_store::Error::NotFound { .. }) => None,
                Err(e) => panic!("{e}"),
            }
        }
    }

    fn injected() -> object_store::Error {
        object_store::Error::Generic {
            store: "test",
            source: "injected 503 Service Unavailable".into(),
        }
    }

    #[derive(Debug)]
    struct TestUpload {
        inner: Box<dyn MultipartUpload>,
        gate: Option<Arc<Semaphore>>,
        next_part: usize,
        fail_part: Option<usize>,
        fail_complete: bool,
        commit_then_fail_complete: bool,
        fail_abort: bool,
        aborts: Arc<AtomicUsize>,
        parts_done: Arc<AtomicUsize>,
        parts_done_at_abort: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl MultipartUpload for TestUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let idx = self.next_part;
            self.next_part += 1;
            let fail = self.fail_part == Some(idx);
            let gate = self.gate.clone();
            let parts_done = Arc::clone(&self.parts_done);
            let part = self.inner.put_part(data);
            Box::pin(async move {
                if let Some(gate) = gate {
                    gate.acquire().await.unwrap().forget();
                }
                if fail {
                    return Err(injected());
                }
                part.await?;
                parts_done.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            if self.fail_complete {
                return Err(injected());
            }
            let result = self.inner.complete().await?;
            if self.commit_then_fail_complete {
                return Err(injected());
            }
            Ok(result)
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            self.parts_done_at_abort
                .store(self.parts_done.load(Ordering::SeqCst), Ordering::SeqCst);
            if self.fail_abort {
                return Err(injected());
            }
            self.inner.abort().await
        }
    }

    #[async_trait]
    impl StreamTarget for TestTarget {
        async fn put(&self, payload: PutPayload) -> object_store::Result<PutResult> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            if self.faults.fail_put {
                return Err(injected());
            }
            self.store.put(&self.path, payload).await
        }

        async fn start_multipart(&self) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.multipart_starts.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(TestUpload {
                inner: self.store.put_multipart(&self.path).await?,
                gate: self.gate.clone(),
                next_part: 0,
                fail_part: self.faults.fail_part,
                fail_complete: self.faults.fail_complete,
                commit_then_fail_complete: self.faults.commit_then_fail_complete,
                fail_abort: self.faults.fail_abort,
                aborts: Arc::clone(&self.aborts),
                parts_done: Arc::new(AtomicUsize::new(0)),
                parts_done_at_abort: Arc::clone(&self.parts_done_at_abort),
            }))
        }
    }

    fn config(part_size: usize, max_inflight_parts: usize) -> StreamConfig {
        StreamConfig {
            part_size,
            max_inflight_parts,
            max_parts: crate::S3_MAX_MULTIPART_PARTS,
            abort_drain_timeout: Duration::from_millis(200),
        }
    }

    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 % 251) as u8).collect()
    }

    async fn roundtrip(len: usize, part_size: usize, write_size: usize) -> Arc<TestTarget> {
        let target = TestTarget::new(Faults::default(), None);
        let mut core = StreamCore::new(target.clone(), config(part_size, 2));
        let data = payload(len);
        for chunk in data.chunks(write_size.max(1)) {
            core.write_all(chunk).await.unwrap();
        }
        core.close().await.unwrap();
        assert_eq!(
            target.read().await.unwrap(),
            data,
            "len={len} part={part_size} write={write_size}"
        );
        let stats = core.stats();
        assert_eq!(stats.accepted_bytes, len as u64);
        assert_eq!(stats.state, "closed");
        assert_eq!(stats.retained_bytes, 0);
        target
    }

    #[tokio::test]
    async fn test_byte_exact_sizes_and_write_patterns() {
        for (len, write) in [
            (0, 1),
            (1, 1),
            (7, 3),
            (8, 8),
            (9, 9),
            (24, 8),
            (25, 4),
            (100, 1),
            (100, 100),
            (1000, 333),
        ] {
            roundtrip(len, 8, write).await;
        }
    }

    #[tokio::test]
    async fn test_small_objects_use_single_put() {
        for len in [0, 1, 7] {
            let target = roundtrip(len, 8, 64).await;
            assert_eq!(target.puts.load(Ordering::SeqCst), 1);
            assert_eq!(target.multipart_starts.load(Ordering::SeqCst), 0);
        }
        let target = roundtrip(8, 8, 64).await;
        assert_eq!(target.puts.load(Ordering::SeqCst), 0);
        assert_eq!(target.multipart_starts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_huge_single_write_is_incremental() {
        let target = TestTarget::new(Faults::default(), None);
        let mut core = StreamCore::new(target.clone(), config(8, 2));
        let data = payload(8 * 50 + 3);
        core.write_all(&data).await.unwrap();
        core.close().await.unwrap();
        let stats = core.stats();
        assert_eq!(stats.parts_submitted, 51);
        assert!(stats.peak_retained_bytes <= 8 * 3, "{stats:?}");
        assert!(stats.peak_inflight_parts <= 2, "{stats:?}");
        assert_eq!(target.read().await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_sync_drains_without_publishing() {
        let target = TestTarget::new(Faults::default(), None);
        let mut core = StreamCore::new(target.clone(), config(8, 4));
        let data = payload(20);
        core.write_all(&data).await.unwrap();
        core.sync().await.unwrap();
        let stats = core.stats();
        assert_eq!(stats.inflight_parts, 0);
        assert_eq!(stats.completed_bytes, 16);
        assert_eq!(stats.retained_bytes, 4);
        assert!(target.read().await.is_none());
        core.flush().await.unwrap();
        core.close().await.unwrap();
        core.close().await.unwrap();
        assert_eq!(target.read().await.unwrap(), data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_backpressure_bounds_retained_bytes() {
        let gate = Arc::new(Semaphore::new(0));
        let target = TestTarget::new(Faults::default(), Some(gate.clone()));
        let mut core = StreamCore::new(target.clone(), config(8, 2));
        let counters = Arc::clone(&core.counters);
        let data = payload(8 * 20);
        let expected = data.clone();
        let producer = tokio::spawn(async move {
            core.write_all(&data).await.unwrap();
            core.close().await.unwrap();
            core
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!producer.is_finished());
        assert_eq!(counters.parts_submitted.load(Ordering::Relaxed), 2);
        assert_eq!(counters.inflight_parts.load(Ordering::Relaxed), 2);
        assert_eq!(counters.completed_bytes.load(Ordering::Relaxed), 0);

        gate.add_permits(1);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(counters.parts_submitted.load(Ordering::Relaxed), 3);
        assert_eq!(counters.completed_bytes.load(Ordering::Relaxed), 8);

        gate.add_permits(1_000);
        let core = producer.await.unwrap();
        let stats = core.stats();
        assert!(stats.peak_inflight_parts <= 2, "{stats:?}");
        assert!(stats.peak_retained_bytes <= 8 * 3, "{stats:?}");
        assert!(stats.capacity_wait_ns > 0);
        assert!(
            stats.first_part_submit_ns > 0
                && stats.first_part_complete_ns >= stats.first_part_submit_ns
        );
        assert_eq!(target.read().await.unwrap(), expected);
    }

    async fn seeded_target(faults: Faults) -> Arc<TestTarget> {
        let target = TestTarget::new(faults, None);
        target
            .store
            .put(&target.path, PutPayload::from_static(b"old"))
            .await
            .unwrap();
        target
    }

    #[tokio::test]
    async fn test_part_failure_is_sticky_and_preserves_old_object() {
        let target = seeded_target(Faults {
            fail_part: Some(1),
            ..Default::default()
        })
        .await;
        let mut core = StreamCore::new(target.clone(), config(8, 1));
        let err = core.write_all(&payload(64)).await.unwrap_err();
        assert!(matches!(err, StreamError::Store(_)), "{err}");
        assert!(matches!(
            core.write_all(b"x").await,
            Err(StreamError::Failed(_))
        ));
        assert!(matches!(core.flush().await, Err(StreamError::Failed(_))));
        assert!(matches!(core.close().await, Err(StreamError::Failed(_))));
        assert!(core.abort().await.is_ok());
        assert_eq!(target.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(core.stats().retained_bytes, 0);
        assert_eq!(target.read().await.unwrap(), b"old");
    }

    #[tokio::test]
    async fn test_failure_surfaced_by_close() {
        let target = seeded_target(Faults {
            fail_part: Some(0),
            ..Default::default()
        })
        .await;
        let mut core = StreamCore::new(target.clone(), config(8, 4));
        core.write_all(&payload(12)).await.unwrap();
        let err = core.close().await.unwrap_err();
        assert!(matches!(err, StreamError::Store(_)), "{err}");
        assert!(matches!(core.close().await, Err(StreamError::Failed(_))));
        assert_eq!(target.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(target.read().await.unwrap(), b"old");
    }

    #[tokio::test]
    async fn test_completion_failures_are_ambiguous() {
        for faults in [
            Faults {
                fail_complete: true,
                ..Default::default()
            },
            Faults {
                commit_then_fail_complete: true,
                ..Default::default()
            },
        ] {
            let commits = faults.commit_then_fail_complete;
            let target = seeded_target(faults).await;
            let mut core = StreamCore::new(target.clone(), config(8, 2));
            core.write_all(&payload(20)).await.unwrap();
            let err = core.close().await.unwrap_err();
            assert!(matches!(err, StreamError::AmbiguousCompletion(_)), "{err}");
            assert!(matches!(core.close().await, Err(StreamError::Failed(_))));
            let expected: Vec<u8> = if commits {
                payload(20)
            } else {
                b"old".to_vec()
            };
            assert_eq!(target.read().await.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn test_single_put_failure_is_sticky() {
        let target = seeded_target(Faults {
            fail_put: true,
            ..Default::default()
        })
        .await;
        let mut core = StreamCore::new(target.clone(), config(8, 2));
        core.write_all(b"tiny").await.unwrap();
        assert!(matches!(core.close().await, Err(StreamError::Store(_))));
        assert!(matches!(core.close().await, Err(StreamError::Failed(_))));
        assert_eq!(target.read().await.unwrap(), b"old");
    }

    #[tokio::test]
    async fn test_abort_cancels_without_deleting() {
        let gate = Arc::new(Semaphore::new(0));
        let target = TestTarget::new(Faults::default(), Some(gate));
        target
            .store
            .put(&target.path, PutPayload::from_static(b"old"))
            .await
            .unwrap();
        let mut core = StreamCore::new(target.clone(), config(8, 2));
        core.accept(&payload(8)).unwrap();
        core.submit_part().await.unwrap();
        core.accept(&payload(3)).unwrap();
        core.abort().await.unwrap();
        core.abort().await.unwrap();
        assert_eq!(target.aborts.load(Ordering::SeqCst), 1);
        assert!(matches!(
            core.write_all(b"x").await,
            Err(StreamError::Closed)
        ));
        assert!(matches!(core.close().await, Err(StreamError::Closed)));
        let stats = core.stats();
        assert_eq!(stats.state, "aborted");
        assert_eq!(stats.inflight_parts, 0);
        assert_eq!(target.read().await.unwrap(), b"old");
    }

    #[tokio::test]
    async fn test_abort_failure_is_reported() {
        let target = TestTarget::new(
            Faults {
                fail_part: Some(0),
                fail_abort: true,
                ..Default::default()
            },
            None,
        );
        let mut core = StreamCore::new(target.clone(), config(8, 1));
        assert!(core.write_all(&payload(32)).await.is_err());
        match core.close().await {
            Err(StreamError::Failed(msg)) => assert!(
                msg.contains("aborting the multipart upload also failed"),
                "{msg}"
            ),
            other => panic!("{other:?}"),
        }
        assert!(core.stats().abort_failed);
    }

    #[tokio::test]
    async fn test_abort_after_close_is_rejected() {
        let target = TestTarget::new(Faults::default(), None);
        let mut core = StreamCore::new(target.clone(), config(8, 2));
        core.write_all(&payload(10)).await.unwrap();
        core.close().await.unwrap();
        assert!(matches!(core.abort().await, Err(StreamError::Closed)));
        assert_eq!(target.read().await.unwrap(), payload(10));
    }

    #[tokio::test]
    async fn test_part_limit_fails_before_accepting() {
        let target = TestTarget::new(Faults::default(), None);
        let mut core = StreamCore::new(
            target.clone(),
            StreamConfig {
                part_size: 8,
                max_inflight_parts: 2,
                max_parts: 2,
                abort_drain_timeout: Duration::from_millis(200),
            },
        );
        core.write_all(&payload(16)).await.unwrap();
        assert!(matches!(
            core.write_all(b"x").await,
            Err(StreamError::Limit(_))
        ));
        assert_eq!(core.accepted(), 16);
        assert!(matches!(core.close().await, Err(StreamError::Failed(_))));
        assert_eq!(target.aborts.load(Ordering::SeqCst), 1);
        assert!(target.read().await.is_none());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_abort_waits_for_inflight_parts_before_aborting() {
        let gate = Arc::new(Semaphore::new(0));
        let target = TestTarget::new(Faults::default(), Some(gate.clone()));
        let mut core = StreamCore::new(target.clone(), config(8, 4));
        core.write_all(&payload(24)).await.unwrap();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            gate.add_permits(3);
        });
        core.abort().await.unwrap();
        release.await.unwrap();
        assert_eq!(target.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(target.parts_done_at_abort.load(Ordering::SeqCst), 3);
        assert_eq!(core.stats().inflight_parts, 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_does_not_wait_but_surfaces_finished_failures() {
        let gate = Arc::new(Semaphore::new(0));
        let target = TestTarget::new(Faults::default(), Some(gate.clone()));
        let mut core = StreamCore::new(target.clone(), config(8, 4));
        core.write_all(&payload(24)).await.unwrap();
        tokio::time::timeout(Duration::from_millis(100), core.flush())
            .await
            .expect("flush must not wait for running parts")
            .unwrap();
        assert_eq!(core.stats().inflight_parts, 3);
        gate.add_permits(3);
        core.sync().await.unwrap();
        core.close().await.unwrap();
        assert_eq!(target.read().await.unwrap(), payload(24));

        let target = TestTarget::new(
            Faults {
                fail_part: Some(0),
                ..Default::default()
            },
            None,
        );
        let mut core = StreamCore::new(target.clone(), config(8, 4));
        core.write_all(&payload(8)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(matches!(core.flush().await, Err(StreamError::Store(_))));
        assert!(matches!(core.flush().await, Err(StreamError::Failed(_))));
    }
}
