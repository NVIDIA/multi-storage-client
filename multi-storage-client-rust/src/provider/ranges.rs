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

//! Validated range collection and source-pinned suffix fallbacks.
use super::StorageBackend;
use crate::{ByteRange, ErrorKind, GetConditions, MscError, ObjectPath, Result};
use bytes::Bytes;

#[cfg(feature = "s3")]
use futures::StreamExt;

pub(crate) async fn suffix_fallback<P: StorageBackend + ?Sized>(
    provider: &P,
    key: &ObjectPath,
    range: crate::SuffixRange,
    conditions: &GetConditions,
) -> Result<Bytes> {
    let mut key = key.clone();
    let mut visited = std::collections::BTreeSet::new();
    for _ in 0..=8 {
        if !visited.insert(key.clone()) {
            break;
        }
        let metadata = if let Some(version) = &conditions.version
            && !provider.is_posix()
        {
            provider.get_object_metadata_version(&key, version).await?
        } else {
            provider.get_object_metadata(&key).await?
        };
        if let Some(target) = metadata.symlink_target {
            key = key.resolve_link(&target)?;
            continue;
        }
        if metadata.kind != crate::ObjectKind::File {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "suffix read requires a file",
            ));
        }
        if conditions
            .version
            .as_ref()
            .is_some_and(|version| metadata.source_version.as_ref() != Some(version))
            || conditions
                .if_match
                .as_ref()
                .is_some_and(|etag| etag != "*" && metadata.etag.as_ref() != Some(etag))
        {
            return Err(MscError::new(
                ErrorKind::PreconditionFailed,
                "suffix read condition failed",
            ));
        }
        if conditions
            .if_none_match
            .as_ref()
            .is_some_and(|etag| etag == "*" || metadata.etag.as_ref() == Some(etag))
        {
            return Err(MscError::new(
                ErrorKind::NotModified,
                "suffix read condition found an unchanged object",
            ));
        }
        let length = range.length().min(metadata.content_length);
        if length == 0 {
            return Ok(Bytes::new());
        }
        let mut conditions = conditions.clone();
        if conditions
            .if_match
            .as_deref()
            .is_none_or(|etag| etag == "*")
        {
            conditions.if_match = metadata.etag;
        }
        if conditions.version.is_none() {
            conditions.version = metadata.source_version;
        }
        return provider
            .get_object_range(
                &key,
                ByteRange::new(metadata.content_length - length, length)?,
                &conditions,
            )
            .await;
    }
    Err(MscError::new(
        ErrorKind::Integrity,
        "suffix symlink cycle or hop limit",
    ))
}

#[cfg(feature = "s3")]
pub(crate) fn requested_bounds(header: &str, total: u64) -> Option<(u64, u64)> {
    let (start, end) = header.strip_prefix("bytes=")?.split_once('-')?;
    if start.is_empty() {
        let length = end.parse::<u64>().ok()?;
        if length == 0 {
            return None;
        }
        Some((total.saturating_sub(length), total.checked_sub(1)?))
    } else {
        let start = start.parse::<u64>().ok()?;
        let end = end.parse::<u64>().ok()?.min(total.checked_sub(1)?);
        (start <= end).then_some((start, end))
    }
}

#[cfg(feature = "s3")]
pub(crate) async fn range_bytes(mut body: crate::ByteStream, expected: u64) -> Result<Bytes> {
    let mut output = bytes::BytesMut::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        if chunk.len() as u64 > expected.saturating_sub(output.len() as u64) {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "range exceeds requested bytes",
            ));
        }
        output.extend_from_slice(&chunk);
    }
    if output.len() as u64 != expected {
        return Err(MscError::new(
            ErrorKind::Integrity,
            "range response is truncated",
        ));
    }
    Ok(output.freeze())
}
