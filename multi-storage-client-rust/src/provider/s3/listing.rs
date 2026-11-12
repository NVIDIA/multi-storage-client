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

//! Native S3 pagination, preserving Python's prefix and object ordering.
use super::S3Provider;
use crate::{
    ErrorKind, ListOptions, MetadataStream, MscError, ObjectKind, ObjectMetadata, ObjectPath,
    Result, StorageProvider,
};
use futures::{StreamExt, stream};
use serde::Deserialize;
use std::collections::VecDeque;

const MAX_LISTING_BYTES: usize = 16 * 1024 * 1024;

pub(super) fn list(provider: S3Provider, options: &ListOptions) -> Result<MetadataStream> {
    options.validate()?;
    let state = Listing {
        provider,
        options: options.clone(),
        page: VecDeque::new(),
        token: None,
        done: false,
        previous: None,
    };
    Ok(Box::pin(stream::try_unfold(
        state,
        |mut state| async move {
            Ok::<_, MscError>(state.next().await?.map(|metadata| (metadata, state)))
        },
    )))
}
struct Listing {
    provider: S3Provider,
    options: ListOptions,
    page: VecDeque<ObjectMetadata>,
    token: Option<String>,
    done: bool,
    previous: Option<String>,
}
impl Listing {
    fn key(&self, raw: &str) -> Result<Option<ObjectPath>> {
        let relative = if raw.len() > 1024 {
            None
        } else if self.provider.base.is_root() {
            Some(raw)
        } else {
            raw.strip_prefix(&format!("{}/", self.provider.base))
        };
        let key = relative.and_then(|raw| {
            ObjectPath::new(raw).ok().filter(|key| {
                !key.is_root() && key.as_str() == raw && !raw.chars().any(char::is_control)
            })
        });
        if key.is_none() && !self.options.skip_unsupported_keys {
            return Err(listing_failure(ErrorKind::InvalidPath));
        }
        Ok(key)
    }
    async fn fetch(&mut self) -> Result<()> {
        let path = self.provider.path(&self.options.prefix)?;
        let prefix = if self.options.recursive && !self.options.prefix.is_root() {
            path.to_string()
        } else if path.as_ref().is_empty() {
            String::new()
        } else {
            format!("{path}/")
        };
        let mut url = self.provider.bucket_url()?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("list-type", "2")
                .append_pair("prefix", &prefix)
                .append_pair(
                    "max-keys",
                    &self.provider.options.list_page_size.to_string(),
                );
            if !self.options.recursive {
                query.append_pair("delimiter", "/");
            }
            if let Some(token) = &self.token {
                query.append_pair("continuation-token", token);
            }
            if let Some(key) = &self.options.start_after {
                query.append_pair("start-after", self.provider.path(key)?.as_ref());
            }
        }
        let request = http::Request::builder()
            .method("GET")
            .uri(url.as_str())
            .body(bytes::Bytes::new().into())
            .map_err(|_| listing_failure(ErrorKind::InvalidConfig))?;
        let response = self
            .provider
            .send_native(request, "list", &self.options.prefix)
            .await?;
        let mut body = response.into_body().bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|error| {
                super::translate(
                    object_store::Error::Generic {
                        store: "S3",
                        source: Box::new(error),
                    },
                    "list",
                    self.options.prefix.as_str(),
                )
            })?;
            if bytes.len().saturating_add(chunk.len()) > MAX_LISTING_BYTES {
                return Err(listing_failure(ErrorKind::Integrity));
            }
            bytes.extend_from_slice(&chunk);
        }
        validate_listing_envelope(&bytes)?;
        let page: Page = quick_xml::de::from_reader(bytes.as_slice())
            .map_err(|_| listing_failure(ErrorKind::Integrity))?;
        self.accept_page(page)
    }
    fn accept_page(&mut self, page: Page) -> Result<()> {
        if page
            .contents
            .len()
            .saturating_add(page.common_prefixes.len())
            > 1000
            || page
                .next_continuation_token
                .as_ref()
                .is_some_and(|token| token.is_empty() || token.len() > 16 * 1024)
            || (self.token.is_some() && self.token == page.next_continuation_token)
            || (page.is_truncated && page.next_continuation_token.is_none())
        {
            return Err(listing_failure(ErrorKind::Integrity));
        }
        let mut entries = VecDeque::new();
        let mut past_end = false;
        for prefix in page.common_prefixes {
            let raw = prefix
                .prefix
                .strip_suffix('/')
                .ok_or_else(|| listing_failure(ErrorKind::InvalidPath))?;
            if let Some(key) = self.key(raw)? {
                if self.options.end_at.as_ref().is_some_and(|end| key > *end) {
                    past_end = true;
                }
                if self.options.include_directories {
                    entries.push_back(ObjectMetadata::directory(key));
                }
            }
        }
        for object in page.contents {
            let directory = object.key.ends_with('/');
            let raw = if directory {
                object.key.strip_suffix('/').expect("directory delimiter")
            } else {
                &object.key
            };
            if directory && raw == self.provider.base.as_str() {
                continue;
            }
            let Some(key) = self.key(raw)? else {
                continue;
            };
            if directory && !self.options.include_directories {
                continue;
            }
            let mut metadata = if directory {
                ObjectMetadata::directory(key)
            } else {
                ObjectMetadata::file(key, object.size)
            };
            metadata.last_modified = Some(object.last_modified);
            metadata.source_version = self.provider.version(None, object.etag.as_deref());
            metadata.etag = object.etag;
            entries.push_back(metadata);
        }
        self.token = page.next_continuation_token;
        self.done = self.token.is_none() || past_end;
        self.page = entries;
        Ok(())
    }
    async fn next(&mut self) -> Result<Option<ObjectMetadata>> {
        loop {
            if let Some(mut metadata) = self.page.pop_front() {
                if self.options.recursive && self.options.include_directories {
                    let mut parents = Vec::new();
                    for (index, character) in metadata.key.as_str().char_indices() {
                        if character != '/' {
                            continue;
                        }
                        let raw = &metadata.key.as_str()[..index];
                        if raw.starts_with(self.options.prefix.as_str())
                            && raw != self.options.prefix.as_str()
                            && self
                                .previous
                                .as_ref()
                                .is_none_or(|previous| !previous.starts_with(&format!("{raw}/")))
                        {
                            parents.push(ObjectMetadata::directory(ObjectPath::new(raw)?));
                        }
                    }
                    self.previous = Some(metadata.key.to_string());
                    if !parents.is_empty() {
                        self.page.push_front(metadata);
                        for parent in parents.into_iter().rev() {
                            self.page.push_front(parent);
                        }
                        continue;
                    }
                }
                if metadata.kind == ObjectKind::Directory && self.options.recursive {
                    self.previous = Some(format!("{}/", metadata.key));
                }
                if !self.options.contains(&metadata.key) {
                    if metadata.kind == ObjectKind::File
                        && self.options.end_at.as_ref().is_some_and(|end| {
                            metadata.key > *end
                                && (!self.options.recursive
                                    || !self.options.include_directories
                                    || metadata.key.as_str() > format!("{end}/").as_str())
                        })
                    {
                        return Ok(None);
                    }
                    continue;
                }
                if metadata.kind == ObjectKind::File
                    && (self.options.include_attributes || metadata.content_length == 0)
                {
                    metadata = match self.provider.head(&metadata.key).await {
                        Ok(metadata) => metadata,
                        Err(error) if error.kind == ErrorKind::NotFound => continue,
                        Err(error)
                            if self.options.include_attributes
                                || error.kind == ErrorKind::Integrity =>
                        {
                            return Err(error);
                        }
                        Err(_) => metadata,
                    };
                }
                if !self.options.include_attributes {
                    metadata.attributes.clear();
                }
                return Ok(Some(metadata));
            }
            if self.done {
                return Ok(None);
            }
            self.fetch().await?;
        }
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Page {
    #[serde(default)]
    is_truncated: bool,
    #[serde(default)]
    contents: Vec<ListedObject>,
    #[serde(default)]
    common_prefixes: Vec<Prefix>,
    next_continuation_token: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListedObject {
    key: String,
    size: u64,
    last_modified: chrono::DateTime<chrono::Utc>,
    #[serde(rename = "ETag")]
    etag: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Prefix {
    prefix: String,
}
fn listing_failure(kind: ErrorKind) -> MscError {
    let mut error = MscError::new(
        kind,
        "S3 listing returned an unsupported key or invalid page",
    );
    error.http_status = Some(200);
    error
}
fn validate_listing_envelope(bytes: &[u8]) -> Result<()> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(bytes);
    let mut depth = 0usize;
    let mut seen = false;
    loop {
        match reader
            .read_event()
            .map_err(|_| listing_failure(ErrorKind::Integrity))?
        {
            Event::Start(element) => {
                if depth == 0 {
                    if seen || element.local_name().as_ref() != b"ListBucketResult" {
                        return Err(listing_failure(ErrorKind::Integrity));
                    }
                    seen = true;
                }
                depth += 1;
                if depth > 64 {
                    return Err(listing_failure(ErrorKind::Integrity));
                }
            }
            Event::Empty(element) if depth == 0 => {
                if seen || element.local_name().as_ref() != b"ListBucketResult" {
                    return Err(listing_failure(ErrorKind::Integrity));
                }
                seen = true;
            }
            Event::End(_) => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| listing_failure(ErrorKind::Integrity))?;
            }
            Event::Eof if seen && depth == 0 => return Ok(()),
            Event::Eof | Event::DocType(_) => return Err(listing_failure(ErrorKind::Integrity)),
            Event::Decl(_) if seen => return Err(listing_failure(ErrorKind::Integrity)),
            Event::Text(text) if depth == 0 && !text.iter().all(u8::is_ascii_whitespace) => {
                return Err(listing_failure(ErrorKind::Integrity));
            }
            Event::CData(_) | Event::GeneralRef(_) if depth == 0 => {
                return Err(listing_failure(ErrorKind::Integrity));
            }
            _ => {}
        }
    }
}
