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

//! File-transfer progress callbacks.
use std::sync::Arc;

/// Cumulative bytes delivered during one file-transfer attempt.
#[derive(Clone, Copy, Debug)]
pub struct TransferProgress {
    /// Bytes read or accepted by the destination stream. Retries can reset this count.
    pub bytes_transferred: u64,
    /// Expected total file size.
    pub total_bytes: u64,
}
/// Synchronous, thread-safe progress observer. Callbacks may run concurrently.
pub type TransferCallback = Arc<dyn Fn(TransferProgress) + Send + Sync>;

pub(crate) fn progress(callback: Option<&TransferCallback>, bytes: u64, total: u64) {
    if let Some(callback) = callback {
        callback(TransferProgress {
            bytes_transferred: bytes,
            total_bytes: total,
        });
    }
}
