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
    io,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{ObjectMetadata, ObjectPath, Result, WriteOptions};

use super::StorageClient;

/// A sequential upload handle. Only [`Self::finish`] publishes the object.
///
/// Input is spooled to a temporary file so memory use stays bounded and a
/// replayable upload can use the profile retry budget.
/// [`tokio::io::AsyncWriteExt::flush`], asynchronous writer shutdown, and dropping
/// this handle do not publish. Await [`Self::finish`] to upload, or [`Self::abort`]
/// to discard. Construct a handle with [`StorageClient::writer`].
pub struct ObjectWriter {
    client: StorageClient,
    path: ObjectPath,
    options: WriteOptions,
    file: tokio::fs::File,
    written: u64,
    input_closed: bool,
}

impl ObjectWriter {
    pub(super) async fn new(
        client: StorageClient,
        path: ObjectPath,
        options: WriteOptions,
    ) -> Result<Self> {
        let file = super::write::create_spool_file().await?;
        Ok(Self {
            client,
            path,
            options,
            file,
            written: 0,
            input_closed: false,
        })
    }

    /// Number of bytes accepted into the private spool, before publication.
    pub fn bytes_written(&self) -> u64 {
        self.written
    }

    /// Complete the upload and return authoritative object metadata.
    ///
    /// # Errors
    /// Returns spool-file, upload, failed-condition, or post-upload metadata errors.
    pub async fn finish(mut self) -> Result<ObjectMetadata> {
        self.file.flush().await?;
        self.client
            .upload_file(&self.path, &self.file, self.written, &self.options)
            .await
    }

    /// Discard all input and remove temporary state without publishing.
    ///
    /// # Errors
    /// Returns local errors while draining or removing the temporary spool file.
    pub async fn abort(self) -> Result<()> {
        // Dropping a Tokio file can leave a blocking write in flight. Closing
        // input first waits for those writes before deleting the spool file.
        let Self { mut file, .. } = self;
        file.flush().await?;
        drop(file);
        Ok(())
    }
}

impl AsyncWrite for ObjectWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.input_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "writer input is closed",
            )));
        }
        match Pin::new(&mut this.file).poll_write(cx, bytes) {
            Poll::Ready(Ok(written)) => {
                this.written = match this.written.checked_add(written as u64) {
                    Some(total) => total,
                    None => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "object size exceeds the supported range",
                        )));
                    }
                };
                Poll::Ready(Ok(written))
            }
            result => result,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.file).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                this.input_closed = true;
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}
