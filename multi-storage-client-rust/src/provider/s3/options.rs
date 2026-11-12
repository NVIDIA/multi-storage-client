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

//! Typed settings and validation before requests are constructed.
use super::upload::{MAX_PART_SIZE, MIN_PART_SIZE};
use crate::{
    error::{ErrorKind, MscError, Result},
    path::ObjectPath,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Provider options. `base_path` is `bucket[/prefix]` when `bucket` is omitted.
/// Defaults configure the transport and multipart limits, but a bucket must be
/// supplied before construction succeeds.
///
/// ```no_run
/// use multi_storage_client::provider::s3::{S3Options, S3Provider};
/// let provider = S3Provider::new(S3Options {
///     base_path: "my-bucket/project".into(),
///     ..S3Options::default()
/// }, None)?; // Standard AWS credential discovery.
/// # Ok::<(), multi_storage_client::MscError>(())
/// ```
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct S3Options {
    /// `bucket[/prefix]`, or just the relative prefix when `bucket` is explicit.
    pub base_path: String,
    /// Optional explicit bucket name, overriding the bucket parsed from `base_path`.
    #[serde(default)]
    pub bucket: Option<String>,
    /// Signing region; defaults to `us-east-1` and accepts `region_name` in config.
    #[serde(default = "default_region", alias = "region_name")]
    pub region: String,
    /// S3-compatible endpoint; explicit settings override inherited AWS endpoints.
    #[serde(default, alias = "endpoint")]
    pub endpoint_url: Option<String>,
    /// Use path-style bucket addressing; true by default.
    #[serde(default = "default_true")]
    pub path_style: bool,
    /// Verify HTTPS server certificates; true by default. HTTP emulators do not
    /// require disabling this setting.
    #[serde(default = "default_true", alias = "verify")]
    pub verify_ssl: bool,
    /// Optional PEM CA certificate file for custom HTTPS trust.
    #[serde(default)]
    pub ca_certificate: Option<PathBuf>,
    /// Multipart selection threshold in bytes; defaults to 8 MiB. Objects up to
    /// and including the threshold use a single PUT, as in MSC Python; larger
    /// unconditional objects use multipart.
    #[serde(default = "default_threshold")]
    pub multipart_threshold: u64,
    /// Multipart part size in bytes; defaults to 8 MiB. Each non-final part must
    /// satisfy S3's minimum 5 MiB size.
    #[serde(default = "default_part_size", alias = "multipart_chunksize")]
    pub part_size: u64,
    /// Maximum simultaneous multipart part uploads; defaults to four.
    #[serde(default = "default_concurrency", alias = "max_concurrency")]
    pub multipart_concurrency: usize,
    /// Connection timeout in milliseconds; defaults to 10,000.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
    /// Total request timeout in milliseconds; defaults to 600,000 (10 minutes).
    /// Unlike an idle-read timeout, this includes transferring the request and response bodies.
    #[serde(default = "default_request_timeout")]
    pub read_timeout_ms: u64,
    /// Maximum response read wait for GET and HEAD requests; defaults to 60,000
    /// milliseconds. All mutations, including multipart parts, use the total request timeout.
    /// Receiving bytes resets this timeout without extending the total request budget.
    /// This also bounds waiting for GET and HEAD response headers.
    #[serde(default = "default_idle_read_timeout")]
    pub idle_read_timeout_ms: u64,
    /// Throughput floor in bytes per second for a single connection; defaults to
    /// 524,288 (512 KiB/s), and 0 disables it. A GET body slower than this over
    /// `low_speed_time_ms` continues its remaining range, pinned to the same object
    /// version, on another connection. A multipart part still uploading after
    /// `low_speed_time_ms` plus its size at this rate is sent again on another
    /// connection, and the first successful attempt wins. Single PUTs are never
    /// duplicated.
    #[serde(default = "default_low_speed_limit")]
    pub low_speed_limit: u64,
    /// Measurement window for `low_speed_limit` in milliseconds; defaults to 10,000.
    #[serde(default = "default_low_speed_time")]
    pub low_speed_time_ms: u64,
    /// Idle connection-pool timeout in milliseconds; defaults to 90,000.
    #[serde(default = "default_pool_timeout")]
    pub pool_timeout_ms: u64,
    /// Optional checksum tag; only `sha256` is currently supported.
    #[serde(default)]
    pub checksum_algorithm: Option<String>,
    /// Optional HTTP user-agent value; validated before requests are constructed.
    #[serde(default)]
    pub user_agent: Option<String>,
    /// Optional AWS shared configuration profile; default discovery includes the default profile.
    #[serde(default)]
    pub profile_name: Option<String>,
    /// Infer MIME content type from the destination name when none is supplied.
    #[serde(default)]
    pub infer_content_type: bool,
    /// Omit signing for public buckets; omission inherits `AWS_SKIP_SIGNATURE`.
    #[serde(default, alias = "anonymous")]
    pub skip_signature: Option<bool>,
    /// Limit used for each listing request, including emulator pagination tests.
    #[serde(default = "default_page_size")]
    pub list_page_size: usize,
}
fn default_region() -> String {
    "us-east-1".into()
}
fn default_true() -> bool {
    true
}
fn default_threshold() -> u64 {
    8 * 1024 * 1024
}
fn default_part_size() -> u64 {
    8 * 1024 * 1024
}
fn default_concurrency() -> usize {
    4
}
fn default_connect_timeout() -> u64 {
    10_000
}
fn default_request_timeout() -> u64 {
    600_000
}
fn default_idle_read_timeout() -> u64 {
    60_000
}
fn default_low_speed_limit() -> u64 {
    512 * 1024
}
fn default_low_speed_time() -> u64 {
    10_000
}
fn default_pool_timeout() -> u64 {
    90_000
}
fn default_page_size() -> usize {
    1000
}
impl Default for S3Options {
    fn default() -> Self {
        Self {
            base_path: String::new(),
            bucket: None,
            region: default_region(),
            endpoint_url: None,
            path_style: true,
            verify_ssl: true,
            ca_certificate: None,
            multipart_threshold: default_threshold(),
            part_size: default_part_size(),
            multipart_concurrency: default_concurrency(),
            connect_timeout_ms: default_connect_timeout(),
            read_timeout_ms: default_request_timeout(),
            idle_read_timeout_ms: default_idle_read_timeout(),
            low_speed_limit: default_low_speed_limit(),
            low_speed_time_ms: default_low_speed_time(),
            pool_timeout_ms: default_pool_timeout(),
            checksum_algorithm: None,
            user_agent: None,
            profile_name: None,
            infer_content_type: false,
            skip_signature: None,
            list_page_size: default_page_size(),
        }
    }
}
impl S3Options {
    pub(crate) fn from_config(config: &crate::config::ProviderConfig) -> Result<Self> {
        let defaults = serde_json::to_value(Self::default()).expect("S3 option schema");
        let aliases = [
            "region_name",
            "endpoint",
            "verify",
            "multipart_chunksize",
            "max_concurrency",
            "anonymous",
        ];
        for key in config.options.keys() {
            if defaults.get(key).is_none() && !aliases.contains(&key.as_str()) {
                let name = if key.len() <= 64
                    && key
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                {
                    key.as_str()
                } else {
                    "[invalid name]"
                };
                return Err(config_error(&format!("unknown S3 option: {name}")));
            }
        }
        serde_json::from_value(config.options_value())
            .map_err(|_| config_error("invalid S3 option type or value"))
    }

    /// Validate all adapter settings without performing network I/O.
    ///
    /// # Errors
    /// Returns `InvalidConfig` for unsupported or inconsistent settings.
    pub fn validate(&self) -> Result<()> {
        self.location()?;
        reqwest::header::HeaderValue::from_str(&self.region)
            .map_err(|_| config_error("S3 region is not a valid request signing scope"))?;
        if self.region.trim().is_empty()
            || !(MIN_PART_SIZE..=MAX_PART_SIZE).contains(&self.part_size)
            || self.multipart_threshold == 0
            || self.multipart_threshold > MAX_PART_SIZE
            || self.multipart_concurrency == 0
            || self.multipart_concurrency > 64
            || self.connect_timeout_ms == 0
            || self.read_timeout_ms == 0
            || self.idle_read_timeout_ms == 0
            || self.low_speed_time_ms == 0
            || self.pool_timeout_ms == 0
            || !(1..=1000).contains(&self.list_page_size)
        {
            return Err(config_error(
                "S3 requires a region, 5 MiB–5 GiB parts, a positive threshold <= 5 GiB, concurrency 1–64, positive timeouts, and page size 1–1000",
            ));
        }
        usize::try_from(self.part_size)
            .map_err(|_| config_error("S3 part size exceeds this platform's address space"))?;
        usize::try_from(self.multipart_threshold).map_err(|_| {
            config_error("S3 multipart threshold exceeds this platform's address space")
        })?;
        if self
            .checksum_algorithm
            .as_ref()
            .is_some_and(|value| !value.eq_ignore_ascii_case("sha256"))
        {
            return Err(config_error(
                "the S3 adapter supports checksum_algorithm: sha256",
            ));
        }
        if let Some(endpoint) = &self.endpoint_url {
            validate_endpoint(endpoint)?;
        }
        if let Some(agent) = &self.user_agent {
            reqwest::header::HeaderValue::from_str(agent)
                .map_err(|_| config_error("S3 user_agent is not a valid HTTP header"))?;
        }
        Ok(())
    }
    pub(super) fn location(&self) -> Result<(String, ObjectPath)> {
        let (bucket, base) = if let Some(bucket) = &self.bucket {
            (bucket.clone(), self.base_path.as_str())
        } else {
            let (bucket, base) = self
                .base_path
                .split_once('/')
                .unwrap_or((&self.base_path, ""));
            (bucket.to_string(), base)
        };
        if bucket.is_empty()
            || bucket.contains(['/', '\\'])
            || bucket.bytes().any(|byte| byte.is_ascii_control())
            || bucket.trim() != bucket
        {
            return Err(config_error(
                "S3 requires bucket or a base_path beginning with a bucket name",
            ));
        }
        if bucket.ends_with("--x-s3") {
            return Err(config_error(
                "S3 directory buckets are unsupported because their listings are not lexically ordered",
            ));
        }
        let base = ObjectPath::new(base).map_err(|_| {
            config_error("S3 base_path must contain a normalized relative key prefix")
        })?;
        Ok((bucket, base))
    }
}
pub(super) fn config_error(message: &str) -> MscError {
    MscError::new(ErrorKind::InvalidConfig, message)
}
pub(super) fn validate_endpoint(endpoint: &str) -> Result<url::Url> {
    if endpoint.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(config_error(
            "S3 endpoint_url must not contain control characters",
        ));
    }
    let url = url::Url::parse(endpoint)
        .map_err(|_| config_error("S3 endpoint_url must be an absolute HTTP(S) URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(config_error(
            "S3 endpoint_url requires HTTP(S), a hostname, and no credentials, query, or fragment",
        ));
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn options_validate_before_io_and_accept_python_names() {
        let options: S3Options = serde_json::from_value(serde_json::json!({
            "base_path": "bucket/a//b", "region_name": "us-west-2",
            "endpoint_url": "http://localhost:7070", "multipart_chunksize": MIN_PART_SIZE,
            "max_concurrency": 2, "verify": false
        }))
        .unwrap();
        options.validate().unwrap();
        assert_eq!(options.read_timeout_ms, 600_000);
        assert_eq!(options.low_speed_limit, 512 * 1024);
        assert_eq!(options.low_speed_time_ms, 10_000);
        assert_eq!(
            S3Options::default().read_timeout_ms,
            options.read_timeout_ms
        );
        let custom_timeout: S3Options = serde_json::from_value(serde_json::json!({
            "base_path": "bucket", "read_timeout_ms": 120_000
        }))
        .unwrap();
        custom_timeout.validate().unwrap();
        assert_eq!(custom_timeout.read_timeout_ms, 120_000);
        assert_eq!(
            options.location().unwrap(),
            ("bucket".into(), ObjectPath::new("a/b").unwrap())
        );
        for value in [
            serde_json::json!({"base_path":"bucket", "secret_access_key":"must-not-be-here"}),
            serde_json::json!({"base_path":"bucket", "unsupported_option":true}),
            serde_json::json!({"region":"us-east-1"}),
        ] {
            assert!(serde_json::from_value::<S3Options>(value).is_err());
        }
        for options in [
            S3Options {
                base_path: "buc\nket/project".into(),
                ..Default::default()
            },
            S3Options {
                base_path: "project".into(),
                bucket: Some("buc\rket".into()),
                ..Default::default()
            },
            S3Options {
                base_path: "project".into(),
                bucket: Some("buc\tket".into()),
                ..Default::default()
            },
            S3Options {
                base_path: "bucket".into(),
                endpoint_url: Some("http://local\nhost:7070".into()),
                ..Default::default()
            },
            S3Options {
                base_path: "bucket".into(),
                region: "us-east-1\r\nsecret".into(),
                ..Default::default()
            },
            S3Options {
                base_path: "bucket--usw2-az1--x-s3".into(),
                ..Default::default()
            },
            S3Options {
                base_path: "bucket".into(),
                part_size: MIN_PART_SIZE - 1,
                ..Default::default()
            },
            S3Options {
                base_path: "bucket".into(),
                multipart_concurrency: 0,
                ..Default::default()
            },
            S3Options {
                base_path: "bucket".into(),
                low_speed_time_ms: 0,
                ..Default::default()
            },
            S3Options {
                base_path: "bucket/../escape".into(),
                ..Default::default()
            },
            S3Options {
                base_path: "bucket".into(),
                endpoint_url: Some("http://access:secret@localhost".into()),
                ..Default::default()
            },
            S3Options {
                base_path: "bucket".into(),
                checksum_algorithm: Some("crc32".into()),
                ..Default::default()
            },
        ] {
            assert_eq!(
                options.validate().unwrap_err().kind,
                ErrorKind::InvalidConfig
            );
        }
    }
}
