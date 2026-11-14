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

//! Types shared by providers and the public client.
use crate::{
    error::{ErrorKind, MscError, Result},
    path::ObjectPath,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, pin::Pin, time::Duration};

/// Owned, asynchronous byte chunks. Opening succeeds before all bytes arrive;
/// callers must propagate errors yielded later by the stream.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;
/// Asynchronous metadata entries and per-item errors. File-only listings are
/// globally lexical; directory entries follow provider order.
pub type MetadataStream = Pin<Box<dyn Stream<Item = Result<ObjectMetadata>> + Send>>;

/// Half-open byte range; construction rejects overflow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteRange {
    /// Zero-based starting byte offset.
    pub offset: u64,
    /// Requested byte count; reads are clipped at the object's end.
    pub length: u64,
}

/// A positive byte count measured backwards from the end of an object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SuffixRange(u64);
impl SuffixRange {
    /// Construct a tail range, returning `InvalidPath` for zero bytes.
    ///
    /// # Errors
    /// Returns an error when `length` is zero.
    pub fn new(length: u64) -> Result<Self> {
        if length == 0 {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "suffix length must be positive",
            ));
        }
        Ok(Self(length))
    }
    /// Maximum number of bytes to return, clipped to the object size.
    #[must_use]
    pub fn length(self) -> u64 {
        self.0
    }
}

/// A bounded offset range or the last bytes of an object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadRange {
    /// An offset and byte count.
    Bounded(ByteRange),
    /// A byte count measured from EOF.
    Suffix(SuffixRange),
}
impl From<ByteRange> for ReadRange {
    fn from(range: ByteRange) -> Self {
        Self::Bounded(range)
    }
}
impl From<SuffixRange> for ReadRange {
    fn from(range: SuffixRange) -> Self {
        Self::Suffix(range)
    }
}
impl ByteRange {
    /// Construct a checked half-open range.
    ///
    /// # Errors
    /// Returns `InvalidPath` if the range end overflows `u64`.
    pub fn new(offset: u64, length: u64) -> Result<Self> {
        offset
            .checked_add(length)
            .ok_or_else(|| MscError::new(ErrorKind::InvalidPath, "byte range overflows u64"))?;
        Ok(Self { offset, length })
    }
    /// Obtain the checked exclusive end, also validating directly constructed ranges.
    ///
    /// # Errors
    /// Returns `InvalidPath` if the range end overflows `u64`.
    pub fn end(self) -> Result<u64> {
        self.offset
            .checked_add(self.length)
            .ok_or_else(|| MscError::new(ErrorKind::InvalidPath, "byte range overflows u64"))
    }
}
/// File, directory, or logical/physical symbolic link classification.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    /// An object with readable bytes; the default kind.
    #[default]
    File,
    /// A physical or synthesized directory.
    Directory,
    /// A symbolic link whose target is recorded in metadata.
    Symlink,
}
/// Opaque generation scoped to the backend which issued it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceVersion {
    /// Non-secret backend scope. Tokens from different scopes are incomparable.
    pub scope: String,
    /// Opaque generation token interpreted only by its issuing provider.
    pub value: String,
}
/// Provider-neutral object information with profile-relative keys.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectMetadata {
    /// Normalized key relative to the selected client profile.
    pub key: ObjectPath,
    /// Whether this entry is a file, directory, or symbolic link.
    pub kind: ObjectKind,
    /// Size of file content in bytes; synthesized directories have size zero.
    pub content_length: u64,
    /// Modification time in UTC, when the backend supplies one.
    pub last_modified: Option<DateTime<Utc>>,
    /// Backend entity tag; it is not necessarily a content hash.
    pub etag: Option<String>,
    /// Backend-scoped generation for conditional operations.
    pub source_version: Option<SourceVersion>,
    /// Relative link target when [`Self::kind`] is [`ObjectKind::Symlink`].
    pub symlink_target: Option<String>,
    /// User-defined JSON attributes. Cloud-native metadata requires string values;
    /// POSIX storage preserves all JSON values without conversion.
    pub attributes: BTreeMap<String, serde_json::Value>,
    /// Provider-specific fields, such as local permissions or modification times.
    pub provider_fields: BTreeMap<String, String>,
}
impl ObjectMetadata {
    /// Construct file metadata without timestamps, generations, or attributes.
    #[must_use]
    pub fn file(key: ObjectPath, content_length: u64) -> Self {
        Self {
            key,
            kind: ObjectKind::File,
            content_length,
            last_modified: None,
            etag: None,
            source_version: None,
            symlink_target: None,
            attributes: BTreeMap::new(),
            provider_fields: BTreeMap::new(),
        }
    }
    /// Construct a zero-sized directory entry without a backend generation.
    #[must_use]
    pub fn directory(key: ObjectPath) -> Self {
        Self {
            kind: ObjectKind::Directory,
            ..Self::file(key, 0)
        }
    }
}
/// Ordered listing selection. Defaults to recursive files beneath the root,
/// without custom attributes or directory entries.
#[derive(Clone, Debug)]
#[allow(clippy::struct_excessive_bools)] // Independent listing selections.
pub struct ListOptions {
    /// Normalized key prefix; an empty key selects the profile root.
    pub prefix: ObjectPath,
    /// Visit all descendants when true; otherwise use a delimited listing.
    pub recursive: bool,
    /// Exclusive lexical lower key boundary.
    pub start_after: Option<ObjectPath>,
    /// Inclusive lexical upper key boundary.
    pub end_at: Option<ObjectPath>,
    /// Include physical and synthesized directory entries.
    pub include_directories: bool,
    /// Request custom attributes, potentially requiring additional backend I/O.
    pub include_attributes: bool,
    /// Skip S3 keys outside this foundation's canonical path subset.
    pub skip_unsupported_keys: bool,
    /// Python-compatible comparisons of custom attributes. Equality compares
    /// string forms; ordering uses numbers when both sides convert. `AND` and
    /// `OR` group to the right with equal precedence; use parentheses explicitly.
    /// Quoted literals retain backslash escapes. Empty strings disable filtering.
    /// Expressions are limited to 64 KiB and 256 nested/grouped clauses.
    pub attribute_filter_expression: Option<String>,
}
impl Default for ListOptions {
    fn default() -> Self {
        Self {
            prefix: ObjectPath::default(),
            recursive: true,
            start_after: None,
            end_at: None,
            include_directories: false,
            include_attributes: false,
            skip_unsupported_keys: false,
            attribute_filter_expression: None,
        }
    }
}
impl ListOptions {
    /// Validate the exclusive lower and inclusive upper boundaries.
    ///
    /// # Errors
    /// Returns `InvalidPath` if the lower boundary is not less than the upper.
    pub fn validate(&self) -> Result<()> {
        if self
            .start_after
            .as_ref()
            .zip(self.end_at.as_ref())
            .is_some_and(|(start, end)| start >= end)
        {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "start_after must be less than end_at",
            ));
        }
        Ok(())
    }
    /// Test lexical bounds only; this does not check the prefix or object kind.
    #[must_use]
    pub fn contains(&self, key: &ObjectPath) -> bool {
        self.start_after.as_ref().is_none_or(|start| key > start)
            && self.end_at.as_ref().is_none_or(|end| key <= end)
    }
}
/// Optional read preconditions. Unsupported conditions fail explicitly rather
/// than silently allowing a different object generation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GetConditions {
    /// Require the matching entity tag, or `"*"` for an existing object.
    pub if_match: Option<String>,
    /// Reject a matching entity tag, or `"*"` for any existing object.
    pub if_none_match: Option<String>,
    /// Require an opaque generation from this backend's scope.
    pub version: Option<SourceVersion>,
}
/// Write preconditions and replacement attributes. Defaults to an unconditional
/// write without custom attributes or an explicit content type.
#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    /// Require the destination's entity tag; a mismatch is a precondition error.
    pub if_match: Option<String>,
    /// Use `Some("*".into())` for create-only publication where supported.
    pub if_none_match: Option<String>,
    /// Require the destination generation from this backend's scope.
    /// Providers reject unsupported generation conditions before mutation.
    pub version: Option<SourceVersion>,
    /// Custom attributes to store with the new object.
    pub attributes: BTreeMap<String, serde_json::Value>,
    /// Explicit MIME content type, when supported by the provider.
    pub content_type: Option<String>,
}
/// Optional deletion preconditions, enforced or explicitly rejected by the adapter.
#[derive(Clone, Debug, Default)]
pub struct DeleteOptions {
    /// Delete only a matching entity tag where supported.
    pub if_match: Option<String>,
    /// Delete only a non-matching entity tag where supported.
    pub if_none_match: Option<String>,
    /// Delete only this backend-scoped object generation.
    pub version: Option<SourceVersion>,
}
/// Source read conditions and destination write settings for a copy.
#[derive(Clone, Debug, Default)]
pub struct CopyOptions {
    /// Conditions protecting the source generation.
    pub source_conditions: GetConditions,
    /// Destination preconditions, replacement attributes, and content type.
    pub destination: WriteOptions,
}
/// Read limits. Defaults to a 64 MiB in-memory limit and a 1 MiB reader buffer.
#[derive(Clone, Debug)]
pub struct ReadOptions {
    /// Conditions passed to the selected backend.
    pub conditions: GetConditions,
    /// Maximum bytes materialized by byte/range reads; streams do not use this
    /// as a total object-size limit.
    pub max_in_memory_bytes: u64,
    /// Seekable reader buffer size in bytes; must be positive when supplied.
    /// `None` inherits the client builder's configured size (1 MiB by default).
    pub buffer_size: Option<usize>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            conditions: GetConditions::default(),
            max_in_memory_bytes: 64 * 1024 * 1024,
            buffer_size: None,
        }
    }
}
/// Metadata lookup options. Symlinks are followed by default.
#[derive(Clone, Copy, Debug)]
pub struct InfoOptions {
    /// Follow relative links with an eight-hop bound and cycle detection.
    pub follow_symlinks: bool,
}
impl Default for InfoOptions {
    fn default() -> Self {
        Self {
            follow_symlinks: true,
        }
    }
}
/// Provider publication result used by the client to verify the new generation.
#[derive(Clone, Debug, Default)]
pub struct PutResult {
    /// Published object length when known without an additional metadata request.
    /// Native copy can report this from its generation-pinned source.
    pub content_length: Option<u64>,
    /// Entity tag of the newly published object, when returned by the backend.
    pub etag: Option<String>,
    /// Scoped generation of the newly published object.
    pub source_version: Option<SourceVersion>,
    /// Authoritative metadata returned by a mapping provider after staging.
    pub metadata: Option<ObjectMetadata>,
}
/// One failed batch item, retaining its original index and typed cause.
#[derive(Clone, Debug)]
pub struct BatchFailure {
    /// Zero-based position in the original input slice.
    pub index: usize,
    /// Logical remote key associated with the failed operation.
    pub source: ObjectPath,
    /// Remote destination for operations that have one; local paths are not
    /// represented by this field.
    pub destination: Option<ObjectPath>,
    /// Operation failure with provider context.
    pub error: MscError,
}
/// Independent batch successes and failures, sorted by original input index.
/// A successful outer `Result` does not imply every item succeeded.
#[derive(Clone, Debug, Default)]
pub struct BatchResult {
    /// Original input indices that completed successfully.
    pub succeeded: Vec<usize>,
    /// Failed items and their typed causes.
    pub failed: Vec<BatchFailure>,
}
/// Every failed item keeps its original input index and its typed cause.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{} storage items failed", .failures.len())]
pub struct BatchError {
    /// All failed items with original indices.
    pub failures: Vec<BatchFailure>,
}
/// Scope within which a provider advertises native server-side copying.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CopyScope {
    /// No native copy; the client may use a bounded streaming fallback.
    #[default]
    None,
    /// Both keys must belong to the same provider instance.
    SameProvider,
    /// Copies can span providers sharing the same backend endpoint.
    SameEndpoint,
}
/// Immutable feature flags. Optional operations return [`ErrorKind::Unsupported`]
/// when unavailable; clients may provide explicit streaming fallbacks.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
// These independent backend features are deliberately individual flags.
#[allow(clippy::struct_excessive_bools)]
pub struct ProviderCapabilities {
    /// Supports half-open byte-range reads.
    pub ranged_reads: bool,
    /// Supports reading incrementally without materializing the whole object.
    pub streaming_reads: bool,
    /// Supports publishing content supplied as a byte stream.
    pub streaming_writes: bool,
    /// Supports backend-enforced read preconditions.
    pub conditional_get: bool,
    /// Supports backend-enforced write preconditions.
    pub conditional_put: bool,
    /// Supports backend-enforced deletion preconditions.
    pub conditional_delete: bool,
    /// Supports native batch deletion beyond the bounded fallback.
    pub batch_delete: bool,
    /// Advertised native copy scope.
    pub server_side_copy: CopyScope,
    /// Supports multipart publication for large objects.
    pub multipart_upload: bool,
    /// Provider listings already satisfy global lexical ordering.
    pub native_lexical_listing: bool,
    /// Represents symbolic links directly or through logical metadata.
    pub symlinks: bool,
    /// Can sign a time-limited GET URL.
    pub presigned_get: bool,
    /// Can sign a time-limited PUT URL.
    pub presigned_put: bool,
    /// Supports custom string attributes.
    pub user_metadata: bool,
    /// Returns generation tokens suitable for conditional reads and validation.
    pub stable_source_versions: bool,
}
/// Operation authorized by a presigned URL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PresignMethod {
    /// Download an object.
    Get,
    /// Upload an object where the provider supports signed writes.
    Put,
}
/// The attempt limit and backoff for client operations and safely replayable
/// multipart parts. Provider SDK request retries must be disabled to avoid
/// multiplying these budgets.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RetryPolicy {
    /// Total attempts, including the first; defaults to three.
    pub max_attempts: u32,
    /// Initial retry delay in milliseconds; defaults to 1,000.
    pub initial_delay_ms: u64,
    /// Exponential delay multiplier, at least one; defaults to two.
    pub multiplier: f64,
    /// Delay ceiling in milliseconds; defaults to 30,000.
    pub max_delay_ms: u64,
    /// Apply bounded jitter to calculated delays; enabled by default.
    pub jitter: bool,
}
impl<'de> Deserialize<'de> for RetryPolicy {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RetryFields {
            #[serde(default, alias = "attempts")]
            max_attempts: Option<u32>,
            #[serde(default)]
            initial_delay_ms: Option<u64>,
            #[serde(default)]
            delay: Option<f64>,
            #[serde(default, alias = "backoff_multiplier")]
            multiplier: Option<f64>,
            #[serde(default)]
            max_delay_ms: Option<u64>,
            #[serde(default)]
            jitter: Option<bool>,
        }
        let fields = RetryFields::deserialize(deserializer)?;
        let defaults = Self::default();
        if fields.initial_delay_ms.is_some() && fields.delay.is_some() {
            return Err(serde::de::Error::custom(
                "retry specifies both delay and initial_delay_ms",
            ));
        }
        let delay = if let Some(seconds) = fields.delay {
            if !seconds.is_finite() || seconds < 0.0 {
                return Err(serde::de::Error::custom(
                    "retry.delay must be finite and nonnegative",
                ));
            }
            let duration =
                Duration::try_from_secs_f64(seconds).map_err(serde::de::Error::custom)?;
            u64::try_from(duration.as_millis()).map_err(serde::de::Error::custom)?
        } else {
            fields.initial_delay_ms.unwrap_or(defaults.initial_delay_ms)
        };
        let policy = Self {
            max_attempts: fields.max_attempts.unwrap_or(defaults.max_attempts),
            initial_delay_ms: delay,
            multiplier: fields.multiplier.unwrap_or(defaults.multiplier),
            max_delay_ms: fields.max_delay_ms.unwrap_or(defaults.max_delay_ms),
            jitter: fields.jitter.unwrap_or(defaults.jitter),
        };
        policy.validate().map_err(serde::de::Error::custom)?;
        Ok(policy)
    }
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_ms: 1000,
            multiplier: 2.0,
            max_delay_ms: 30000,
            jitter: true,
        }
    }
}
impl RetryPolicy {
    /// Check the retry budget and exponential-delay settings.
    ///
    /// # Errors
    /// Returns `InvalidConfig` for zero attempts, invalid multipliers, or delay bounds.
    pub fn validate(&self) -> Result<()> {
        if self.max_attempts == 0
            || !self.multiplier.is_finite()
            || self.multiplier < 1.0
            || self.initial_delay_ms > self.max_delay_ms
        {
            return Err(MscError::new(
                ErrorKind::InvalidConfig,
                "retry requires positive attempts, a finite multiplier >= 1, and initial_delay_ms <= max_delay_ms",
            ));
        }
        Ok(())
    }
    /// Calculate the delay after a one-based attempt, respecting the ceiling
    /// and optionally applying jitter.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn delay(&self, attempt: u32) -> Duration {
        if self.initial_delay_ms == 0 {
            return Duration::ZERO;
        }
        let millis = (self.initial_delay_ms as f64
            * self.multiplier.powf(f64::from(attempt.saturating_sub(1))))
        .min(self.max_delay_ms as f64);
        let factor = if self.jitter {
            0.5 + f64::from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos()
                    % 500_000_000,
            ) / 1_000_000_000.0
        } else {
            1.0
        };
        Duration::from_millis((millis * factor) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ranges_and_bounds_validate() {
        assert!(ByteRange::new(u64::MAX, 1).is_err());
        assert_eq!(ByteRange::new(3, 0).unwrap().end().unwrap(), 3);
        let options = ListOptions {
            start_after: Some(ObjectPath::new("b").unwrap()),
            end_at: Some(ObjectPath::new("b").unwrap()),
            ..ListOptions::default()
        };
        assert!(options.validate().is_err());
        assert!(
            RetryPolicy {
                max_attempts: 0,
                ..RetryPolicy::default()
            }
            .validate()
            .is_err()
        );
    }
    #[test]
    fn python_retry_fields_preserve_units() {
        let policy: RetryPolicy =
            serde_json::from_str(r#"{"attempts":4,"delay":0.25,"backoff_multiplier":3}"#).unwrap();
        assert_eq!(policy.max_attempts, 4);
        assert_eq!(policy.initial_delay_ms, 250);
        assert!(policy.delay(30).as_millis() <= 30_000);
        assert!(serde_json::from_str::<RetryPolicy>(r#"{"delay":"1"}"#).is_err());
        assert!(
            serde_json::from_str::<RetryPolicy>(r#"{"delay":1,"initial_delay_ms":1000}"#).is_err()
        );
    }
}
