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

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

use crate::{ByteRange, GetConditions, ObjectPath, Result};

use super::StorageClient;

type PendingRead = Pin<Box<dyn Future<Output = (Result<Bytes>, Option<io::ErrorKind>)> + Send>>;

/// A [`tokio::io::AsyncRead`] + [`tokio::io::AsyncSeek`] handle backed by bounded,
/// version-conditional range reads. Construct it with [`StorageClient::reader`].
///
/// Seeking updates the logical cursor without downloading intervening bytes.
/// Provider failures are wrapped in `std::io::Error`; the underlying cause can
/// be inspected as an [`MscError`](crate::MscError). When the provider supplies
/// stable generation tokens, subsequent requests require the observed generation.
pub struct ObjectReader {
    client: StorageClient,
    path: ObjectPath,
    conditions: GetConditions,
    size: u64,
    position: u64,
    buffer: Bytes,
    buffer_start: u64,
    pending: Option<PendingRead>,
    chunk_size: u64,
}

impl ObjectReader {
    pub(super) fn new(
        client: StorageClient,
        path: ObjectPath,
        conditions: GetConditions,
        size: u64,
        chunk_size: u64,
    ) -> Self {
        Self {
            client,
            path,
            conditions,
            size,
            position: 0,
            buffer: Bytes::new(),
            buffer_start: 0,
            pending: None,
            chunk_size,
        }
    }
    /// The object's size as observed when the handle was constructed.
    pub fn len(&self) -> u64 {
        self.size
    }

    /// Whether the size observed when this handle opened was zero.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Current zero-based byte cursor, which may be beyond the observed end after seeking.
    pub fn position(&self) -> u64 {
        self.position
    }
}

impl AsyncRead for ObjectReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 || this.position >= this.size {
            return Poll::Ready(Ok(()));
        }
        loop {
            let available = this
                .position
                .checked_sub(this.buffer_start)
                .filter(|offset| *offset < this.buffer.len() as u64);
            if let Some(offset) = available {
                let offset = usize::try_from(offset).expect("bounded range buffer");
                let count = output.remaining().min(this.buffer.len() - offset);
                output.put_slice(&this.buffer[offset..offset + count]);
                this.position += count as u64;
                return Poll::Ready(Ok(()));
            }
            if this.pending.is_none() {
                let range = match ByteRange::new(
                    this.position,
                    this.chunk_size.min(this.size - this.position),
                ) {
                    Ok(range) => range,
                    Err(error) => return Poll::Ready(Err(io::Error::other(error))),
                };
                let client = this.client.clone();
                let path = this.path.clone();
                let conditions = this.conditions.clone();
                this.pending = Some(Box::pin(async move {
                    let mut validation_kind = None;
                    let result = (async {
                        let bytes = client
                            .range_with_conditions(&path, range, &conditions)
                            .await?;
                        if bytes.is_empty() {
                            validation_kind = Some(io::ErrorKind::UnexpectedEof);
                            return Err(crate::MscError::new(
                                crate::ErrorKind::Integrity,
                                "object ended before its observed size",
                            ));
                        }
                        if bytes.len() as u64 > range.length {
                            validation_kind = Some(io::ErrorKind::InvalidData);
                            return Err(crate::MscError::new(
                                crate::ErrorKind::Integrity,
                                "provider returned more bytes than the requested range",
                            ));
                        }
                        Ok(bytes)
                    })
                    .await;
                    (result, validation_kind)
                }));
            }
            let (result, validation_kind) = match this
                .pending
                .as_mut()
                .expect("pending range")
                .as_mut()
                .poll(cx)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result,
            };
            this.pending = None;
            match result {
                Ok(bytes) => {
                    this.buffer_start = this.position;
                    this.buffer = bytes;
                }
                Err(error) => {
                    return Poll::Ready(Err(io::Error::new(
                        validation_kind.unwrap_or(io::ErrorKind::Other),
                        error,
                    )));
                }
            }
        }
    }
}

impl AsyncSeek for ObjectReader {
    fn start_seek(self: Pin<&mut Self>, seek: io::SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        let position = match seek {
            io::SeekFrom::Start(position) => i128::from(position),
            io::SeekFrom::Current(offset) => i128::from(this.position) + i128::from(offset),
            io::SeekFrom::End(offset) => i128::from(this.size) + i128::from(offset),
        };
        let position = u64::try_from(position).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek is outside the supported range",
            )
        })?;
        this.position = position;
        this.pending = None;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}
