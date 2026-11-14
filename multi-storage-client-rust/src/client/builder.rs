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

use super::{ClientState, StorageClient, StorageClientBuilder};
use crate::{ErrorKind, MscError, Result};
use std::sync::Arc;

impl StorageClientBuilder {
    /// Construct the selected physical provider and shared policy.
    ///
    /// # Errors
    /// Returns configuration, credential or provider construction errors.
    pub async fn build(self) -> Result<StorageClient> {
        if self.read_buffer_size == 0 {
            return Err(MscError::new(
                ErrorKind::InvalidConfig,
                "reader buffer size must be positive",
            ));
        }
        let profile = self.config.profile(&self.profile)?;
        let provider = self.registry.build(profile).await?;
        Ok(StorageClient {
            state: Arc::new(ClientState {
                provider,
                retry: profile.retry.clone(),
                max_in_memory_bytes: self.max_in_memory_bytes,
                read_buffer_size: self.read_buffer_size,
            }),
        })
    }
}
