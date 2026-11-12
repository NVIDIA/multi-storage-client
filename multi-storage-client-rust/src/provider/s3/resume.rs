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

//! GET bodies that continue on another connection when theirs is throttled.
use super::{concrete_etag, transport::LowSpeed};
use bytes::Bytes;
use futures::{StreamExt as _, stream::BoxStream};
use object_store::{GetOptions, GetResult, ObjectStore, aws::AmazonS3, path::Path};
use std::{ops::Range, sync::Arc};
use tokio::time::Instant;

/// Upper bound, in windows, on the wait between resumes that keep landing on
/// slow connections. Waits double from one window, so a uniformly slow link
/// reconnects rarely, while a transfer stranded on a throttled connection
/// still leaves it soon after other connections recover.
const MAX_RESUME_BACKOFF: u32 = 4;

/// Return the response body, resuming its remaining range on another connection
/// whenever a window delivers less than `low_speed` allows. Resumed requests
/// are pinned to the first response's version or `ETag`; without either, the
/// body is returned unchanged.
pub(super) fn body(
    store: &Arc<AmazonS3>,
    path: &Path,
    mut options: GetOptions,
    result: GetResult,
    low_speed: Option<LowSpeed>,
) -> BoxStream<'static, object_store::Result<Bytes>> {
    let Some(low_speed) = low_speed else {
        return result.into_stream();
    };
    if options.version.is_none() {
        match result
            .meta
            .e_tag
            .as_deref()
            .filter(|etag| concrete_etag(etag))
        {
            Some(etag) => options.if_match = Some(etag.to_owned()),
            None => return result.into_stream(),
        }
    }
    let remaining = result.range.clone();
    let state = Resume {
        store: Arc::clone(store),
        path: path.clone(),
        options,
        low_speed,
        remaining,
        body: result.into_stream(),
        window_start: Instant::now(),
        window_bytes: 0,
        resumes: 0,
        retry_after: Instant::now(),
    };
    futures::stream::unfold(state, |mut state| async move {
        let item = state.next().await?;
        Some((item, state))
    })
    .boxed()
}

struct Resume {
    store: Arc<AmazonS3>,
    path: Path,
    options: GetOptions,
    low_speed: LowSpeed,
    /// Absolute byte range not yet yielded.
    remaining: Range<u64>,
    body: BoxStream<'static, object_store::Result<Bytes>>,
    window_start: Instant,
    window_bytes: u64,
    /// Consecutive resumes without an intervening window at full speed.
    resumes: u32,
    retry_after: Instant,
}

impl Resume {
    async fn next(&mut self) -> Option<object_store::Result<Bytes>> {
        loop {
            let deadline = self.window_start + self.low_speed.window();
            // Stream polling is cancel-safe: an unfinished frame stays buffered.
            let Ok(item) = tokio::time::timeout_at(deadline, self.body.next()).await else {
                self.end_window().await;
                continue;
            };
            if let Some(Ok(chunk)) = &item {
                let length = chunk.len() as u64;
                self.remaining.start = self.remaining.start.saturating_add(length);
                self.window_bytes = self.window_bytes.saturating_add(length);
                if Instant::now() >= deadline {
                    self.end_window().await;
                }
            }
            return item;
        }
    }

    async fn end_window(&mut self) {
        let now = Instant::now();
        let slow = self
            .low_speed
            .slow(self.window_bytes, now - self.window_start);
        if !slow {
            self.resumes = 0;
            self.retry_after = now;
        } else if now >= self.retry_after && !self.remaining.is_empty() {
            let backoff = 1 << self.resumes.min(MAX_RESUME_BACKOFF.ilog2());
            self.resumes = self.resumes.saturating_add(1);
            self.retry_after = now + self.low_speed.window() * backoff;
            let mut options = self.options.clone();
            options.range = Some(self.remaining.clone().into());
            // Keep the current body unless the replacement starts exactly where
            // it stopped; a failed resume does not interrupt a working transfer.
            if let Ok(result) = self.store.get_opts(&self.path, options).await
                && result.range == self.remaining
            {
                self.body = result.into_stream();
            }
        }
        self.window_start = Instant::now();
        self.window_bytes = 0;
    }
}
