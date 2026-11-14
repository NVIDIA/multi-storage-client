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

//! Directory-local traversal using Python's file and directory sort keys.
use super::{PosixProvider, blocking, errno};
use crate::{
    ErrorKind, ListOptions, MetadataStream, MscError, ObjectKind, ObjectMetadata, ObjectPath,
    Result,
};
use futures::stream;
use rustix::fs;
use std::collections::VecDeque;

pub(super) fn open(provider: PosixProvider, options: ListOptions) -> Result<MetadataStream> {
    options.validate()?;
    let state = Listing {
        provider,
        options,
        initialized: false,
        pending: VecDeque::new(),
    };
    Ok(Box::pin(stream::try_unfold(
        state,
        |mut state| async move {
            Ok::<_, MscError>(state.next().await?.map(|metadata| (metadata, state)))
        },
    )))
}
enum Pending {
    Object(ObjectMetadata),
    Expand(ObjectPath),
}
struct Listing {
    provider: PosixProvider,
    options: ListOptions,
    initialized: bool,
    pending: VecDeque<Pending>,
}
impl Listing {
    async fn next(&mut self) -> Result<Option<ObjectMetadata>> {
        if !self.initialized {
            self.initialized = true;
            let provider = self.provider.clone();
            let key = self.options.prefix.clone();
            match blocking(move || provider.metadata_with_attributes(&key, false)).await {
                Ok(metadata) => self
                    .pending
                    .push_back(if metadata.kind == ObjectKind::Directory {
                        Pending::Expand(metadata.key)
                    } else {
                        Pending::Object(metadata)
                    }),
                Err(error)
                    if matches!(error.kind, ErrorKind::NotFound | ErrorKind::Unsupported) => {}
                Err(error) => return Err(error),
            }
        }
        while let Some(item) = self.pending.pop_front() {
            match item {
                Pending::Expand(key) => {
                    if self.options.end_at.as_ref().is_some_and(|end| {
                        !key.is_root() && format!("{key}/").as_str() > end.as_str()
                    }) {
                        continue;
                    }
                    let children = read_directory(&self.provider, key).await?;
                    for child in children.into_iter().rev() {
                        self.pending.push_front(Pending::Object(child));
                    }
                }
                Pending::Object(metadata) => {
                    if metadata.kind == ObjectKind::Directory {
                        if self.options.recursive {
                            self.pending
                                .push_front(Pending::Expand(metadata.key.clone()));
                        }
                        if !self.options.include_directories {
                            continue;
                        }
                    }
                    if self.options.contains(&metadata.key) {
                        if self.options.include_attributes && metadata.kind == ObjectKind::File {
                            let provider = self.provider.clone();
                            return blocking(move || provider.metadata(&metadata.key))
                                .await
                                .map(Some);
                        }
                        return Ok(Some(metadata));
                    }
                }
            }
        }
        Ok(None)
    }
}

async fn read_directory(provider: &PosixProvider, key: ObjectPath) -> Result<Vec<ObjectMetadata>> {
    let permit = provider
        .semaphore
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| MscError::new(ErrorKind::Cancelled, "POSIX provider closed"))?;
    let provider = provider.clone();
    blocking(move || {
        let _permit = permit;
        let descriptor = provider.directory(&key, false)?;
        let mut children = Vec::new();
        for entry in fs::Dir::read_from(&descriptor).map_err(errno)? {
            let entry = entry.map_err(errno)?;
            let name = entry.file_name().to_str().map_err(|_| {
                MscError::new(ErrorKind::InvalidPath, "POSIX listing name is not UTF-8")
            })?;
            if matches!(name, "." | "..") || name.starts_with(".msc-tmp-") {
                continue;
            }
            let child = key.join(&ObjectPath::new(name)?)?;
            match provider.metadata_with_attributes(&child, false) {
                Ok(metadata) => children.push(metadata),
                Err(error)
                    if matches!(error.kind, ErrorKind::NotFound | ErrorKind::Unsupported) => {}
                Err(error) => return Err(error),
            }
        }
        children.sort_by_cached_key(|metadata| {
            if metadata.kind == ObjectKind::Directory {
                format!("{}/", metadata.key)
            } else {
                metadata.key.to_string()
            }
        });
        Ok(children)
    })
    .await
}
