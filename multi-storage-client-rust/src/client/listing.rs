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

//! Ordered listings with retry cursors and attribute filtering.
use super::StorageClient;
use crate::filter::AttributeFilter;
use crate::{ErrorKind, ListOptions, MetadataStream, MscError, ObjectPath, Result};
use futures::{StreamExt, stream};

impl StorageClient {
    #[allow(clippy::unused_async)] // Opening and retries happen when the stream is polled.
    pub(super) async fn list_impl(&self, mut options: ListOptions) -> Result<MetadataStream> {
        options.validate()?;
        let filter = AttributeFilter::compile(options.attribute_filter_expression.as_deref())?;
        let include_attributes = options.include_attributes;
        if filter.is_some() {
            options.include_attributes = true;
        }
        options.attribute_filter_expression = None;
        let state = ListingState {
            client: self.clone(),
            options,
            current: None,
            last: None,
            attempts: 0,
            done: false,
            filter,
            include_attributes,
        };
        Ok(Box::pin(stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            loop {
                let next = if let Some(current) = state.current.as_mut() {
                    current.next().await
                } else {
                    state.attempts += 1;
                    match state.client.state.provider.list(&state.options).await {
                        Ok(current) => {
                            state.current = Some(current);
                            continue;
                        }
                        Err(error) => Some(Err(error)),
                    }
                };
                match next {
                    None => {
                        state.done = true;
                        return None;
                    }
                    Some(Ok(mut metadata)) => {
                        if !state.options.include_directories
                            && state
                                .last
                                .as_ref()
                                .is_some_and(|last| last >= &metadata.key)
                        {
                            state.done = true;
                            return Some((
                                Err(MscError::new(
                                    ErrorKind::Integrity,
                                    "provider listing is unsorted or contains duplicates",
                                )),
                                state,
                            ));
                        }
                        state.last = Some(metadata.key.clone());
                        if state
                            .filter
                            .as_ref()
                            .is_some_and(|filter| !filter.matches(&metadata.attributes))
                        {
                            continue;
                        }
                        if !state.include_attributes {
                            metadata.attributes.clear();
                        }
                        return Some((Ok(metadata), state));
                    }
                    Some(Err(error))
                        if (!state.options.include_directories || state.last.is_none())
                            && state.client.can_retry(&error, true, state.attempts) =>
                    {
                        state.client.retry_delay(&error, state.attempts).await;
                        if let Some(last) = &state.last {
                            state.options.start_after = Some(last.clone());
                        }
                        state.current = None;
                    }
                    Some(Err(error)) => {
                        state.done = true;
                        return Some((Err(error), state));
                    }
                }
            }
        })))
    }
}

struct ListingState {
    client: StorageClient,
    options: ListOptions,
    current: Option<MetadataStream>,
    last: Option<ObjectPath>,
    attempts: u32,
    done: bool,
    filter: Option<AttributeFilter>,
    include_attributes: bool,
}
