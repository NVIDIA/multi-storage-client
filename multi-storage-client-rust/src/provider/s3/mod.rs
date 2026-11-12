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

//! AWS S3 and AWS-compatible endpoints. SDK types stay inside this adapter.
mod listing;
mod native;
mod options;
mod resume;
mod transport;
mod upload;

pub use options::S3Options;

use crate::{
    credentials::AwsCredentials,
    error::{ErrorKind, MscError, Result},
    path::ObjectPath,
    provider::StorageProvider,
    types::{
        ByteRange, ByteStream, CopyOptions, CopyScope, DeleteOptions, GetConditions, ListOptions,
        MetadataStream, ObjectMetadata, PresignMethod, ProviderCapabilities, PutResult,
        SourceVersion, WriteOptions,
    },
};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, stream};
use object_store::{
    Attribute, Attributes, ClientOptions, ObjectStore, PutMode, PutOptions, RetryConfig,
    UpdateVersion,
    aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, Checksum, S3ConditionalPut},
    path::Path,
    signer::Signer,
};
use options::{config_error, validate_endpoint};
use std::{sync::Arc, time::Duration};
use transport::{CredentialBridge, LowSpeed, StatusConnector, translate};
use upload::{Cleanup, MAX_OBJECT_SIZE, PartReader, check_size};

/// The S3 backend supports signed requests through explicit or standard AWS credentials.
#[derive(Clone)]
#[allow(clippy::struct_excessive_bools)] // Independent SDK configuration switches.
pub struct S3Provider {
    store: Arc<AmazonS3>,
    base: ObjectPath,
    scope: String,
    options: S3Options,
    retry_policy: crate::RetryPolicy,
    cleanup: Arc<Cleanup>,
    name: &'static str,
    http: object_store::client::HttpClient,
    endpoint: String,
    bucket: String,
    skip_signature: bool,
    request_payer: bool,
    sign_payload: bool,
    encryption_configured: bool,
}
impl std::fmt::Debug for S3Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Provider")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}
fn validate_header_value(value: &str) -> Result<()> {
    reqwest::header::HeaderValue::from_str(value)
        .map(|_| ())
        .map_err(|_| config_error("S3 request option is not a valid HTTP header value"))
}
impl S3Provider {
    /// Construct with explicit credentials or SDK environment/role discovery.
    /// For shared profiles and the complete AWS chain use
    /// [`Self::new_with_default_credentials`] or a configured `StorageClient`.
    ///
    /// # Errors
    /// Returns `InvalidConfig` for invalid settings or `Unauthenticated` for expired credentials.
    pub fn new(options: S3Options, credentials: Option<AwsCredentials>) -> Result<Self> {
        Self::new_internal(options, credentials, None)
    }
    /// Construct with the complete renewable AWS credential chain, including profiles.
    ///
    /// Anonymous profiles omit credential discovery. Discovery creates no hidden runtime.
    ///
    /// # Errors
    /// Returns invalid configuration or credential-chain initialization errors.
    pub async fn new_with_default_credentials(options: S3Options) -> Result<Self> {
        options.validate()?;
        let anonymous = match options.skip_signature {
            Some(value) => value,
            None => configured_bool(
                &AmazonS3Builder::from_env(),
                AmazonS3ConfigKey::SkipSignature,
            )?,
        };
        if anonymous {
            return Self::new(options, None);
        }
        let credentials = crate::credentials::AwsDefaultCredentialsProvider::new(
            options.profile_name.as_deref(),
            &options.region,
        )
        .await?;
        Self::new_with_credentials_provider(
            options,
            Arc::new(crate::credentials::RefreshingCredentialsProvider::new(
                Arc::new(credentials),
            )),
        )
    }
    /// Keep a refreshable application credential provider live for every SDK request.
    ///
    /// # Errors
    /// Returns `InvalidConfig` when options or TLS configuration cannot be used.
    pub fn new_with_credentials_provider(
        options: S3Options,
        credentials: Arc<dyn crate::credentials::CredentialsProvider>,
    ) -> Result<Self> {
        Self::new_internal(options, None, Some(credentials))
    }
    fn new_internal(
        options: S3Options,
        credentials: Option<AwsCredentials>,
        dynamic: Option<Arc<dyn crate::credentials::CredentialsProvider>>,
    ) -> Result<Self> {
        options.validate()?;
        let (bucket, base) = options.location()?;
        let mut builder = AmazonS3Builder::from_env();
        if let Some(skip) = options.skip_signature {
            builder = builder.with_config(AmazonS3ConfigKey::SkipSignature, skip.to_string());
        }
        let skip_signature = configured_bool(&builder, AmazonS3ConfigKey::SkipSignature)?;
        let request_payer = configured_bool(&builder, AmazonS3ConfigKey::RequestPayer)?;
        let sign_payload = !configured_bool(&builder, AmazonS3ConfigKey::UnsignedPayload)?;
        let encryption_configured = [
            "aws_server_side_encryption",
            "aws_sse_kms_key_id",
            "aws_sse_bucket_key_enabled",
            "aws_sse_customer_key_base64",
        ]
        .iter()
        .any(|key| {
            builder
                .get_config_value(&key.parse().expect("SDK encryption key"))
                .is_some()
        });
        if configured_bool(&builder, AmazonS3ConfigKey::S3Express)? {
            return Err(config_error(
                "S3 Express is unsupported because its listings are not lexically ordered; unset AWS_S3_EXPRESS",
            ));
        }
        if let Some(endpoint) = &options.endpoint_url {
            // The SDK gives the service-specific environment endpoint precedence
            // over with_endpoint(), so override that same configuration slot.
            builder = builder.with_config(AmazonS3ConfigKey::S3Endpoint, endpoint);
        }
        if !skip_signature && credentials.is_none() && dynamic.is_none() {
            validate_environment_credentials(&builder)?;
        }
        let endpoint = builder
            .get_config_value(&AmazonS3ConfigKey::S3Endpoint)
            .or_else(|| builder.get_config_value(&AmazonS3ConfigKey::Endpoint))
            .map(|endpoint| endpoint.trim_end_matches('/').to_owned());
        if let Some(endpoint) = &endpoint {
            builder = builder.with_config(AmazonS3ConfigKey::S3Endpoint, endpoint);
        }
        let endpoint_url = endpoint.as_deref().map(validate_endpoint).transpose()?;
        let client_options = client_options(&options, endpoint_url.as_ref())?;
        let connector = StatusConnector::new(
            &client_options,
            Duration::from_millis(options.idle_read_timeout_ms.min(options.read_timeout_ms)),
            low_speed(&options),
        )
        .map_err(|_| config_error("unable to initialize S3 HTTP transport"))?;
        let http = object_store::client::HttpConnector::connect(&connector, &client_options)
            .map_err(|_| config_error("unable to initialize S3 HTTP transport"))?;
        builder = builder
            .with_bucket_name(&bucket)
            .with_region(&options.region)
            .with_virtual_hosted_style_request(!options.path_style)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_http_connector(connector)
            .with_retry(RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            })
            .with_client_options(client_options);
        if options.checksum_algorithm.is_some() {
            builder = builder.with_checksum_algorithm(Checksum::SHA256);
        }
        if let Some(credentials) = credentials {
            crate::credentials::validate_credentials(&credentials)?;
            builder = builder.with_credentials(Arc::new(CredentialBridge(Arc::new(
                crate::credentials::StaticCredentialsProvider(credentials),
            ))));
        }
        if let Some(credentials) = dynamic {
            builder = builder.with_credentials(Arc::new(CredentialBridge(credentials)));
        }
        let store = builder.build().map_err(|_| {
            config_error(
                "unable to build S3 provider; check endpoint, region, and credentials settings",
            )
        })?;
        let scope = format!(
            "s3:{}:{bucket}:{}",
            endpoint.as_deref().unwrap_or("aws"),
            options.region
        );
        let cleanup = Arc::new(Cleanup::new(options.multipart_concurrency));
        let endpoint = bucket_endpoint(endpoint, &bucket, &options);
        Ok(Self {
            store: Arc::new(store),
            base,
            scope,
            options,
            retry_policy: crate::RetryPolicy::default(),
            cleanup,
            name: "s3",
            http,
            endpoint,
            bucket,
            skip_signature,
            request_payer,
            sign_payload,
            encryption_configured,
        })
    }
    /// Set the provider identity used by S8K MIME inference.
    #[must_use]
    pub fn with_name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }
    pub(crate) fn with_retry_policy(mut self, policy: crate::RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }
    fn resumable(
        &self,
        path: &Path,
        options: object_store::GetOptions,
        result: object_store::GetResult,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Bytes>> {
        resume::body(&self.store, path, options, result, low_speed(&self.options))
    }
    fn path(&self, key: &ObjectPath) -> Result<Path> {
        Path::parse(self.base.join(key)?.as_str()).map_err(|_| {
            MscError::new(
                ErrorKind::InvalidPath,
                "S3 key contains unsupported control characters",
            )
        })
    }
    fn relative(&self, path: &Path) -> Result<ObjectPath> {
        let raw = if self.base.is_root() {
            path.as_ref()
        } else {
            path.as_ref()
                .strip_prefix(&format!("{}/", self.base))
                .ok_or_else(|| {
                    MscError::new(
                        ErrorKind::Integrity,
                        "S3 returned an object outside its configured base path",
                    )
                })?
        };
        let key = ObjectPath::new(raw)?;
        if key.as_str() != raw {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "S3 listing returned a noncanonical key",
            ));
        }
        Ok(key)
    }
    fn version(&self, version: Option<String>, etag: Option<&str>) -> Option<SourceVersion> {
        version
            .filter(|value| !value.is_empty() && value != "null")
            .map(|value| format!("version:{value}"))
            .or_else(|| {
                etag.filter(|value| concrete_etag(value))
                    .map(|value| format!("etag:{value}"))
            })
            .map(|value| SourceVersion {
                scope: self.scope.clone(),
                value,
            })
    }
    fn metadata(
        &self,
        meta: object_store::ObjectMeta,
        attributes: &Attributes,
    ) -> Result<ObjectMetadata> {
        let mut result = ObjectMetadata::file(self.relative(&meta.location)?, meta.size);
        result.last_modified = Some(meta.last_modified);
        result.source_version = self.version(meta.version, meta.e_tag.as_deref());
        result.etag = meta.e_tag;
        for (key, value) in attributes {
            match key {
                Attribute::Metadata(key) => {
                    if key.eq_ignore_ascii_case("msc-symlink-target") {
                        result.kind = crate::ObjectKind::Symlink;
                        result.symlink_target = Some(value.to_string());
                    } else {
                        result
                            .attributes
                            .insert(key.to_string(), value.to_string().into());
                    }
                }
                Attribute::ContentType => {
                    result
                        .provider_fields
                        .insert("content_type".into(), value.to_string());
                }
                _ => {}
            }
        }
        if let Some(target) = &result.symlink_target
            && (result.content_length != 0 || result.key.resolve_link(target).is_err())
        {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "invalid S3 logical link metadata",
            ));
        }
        Ok(result)
    }
    fn put_result(&self, result: object_store::PutResult) -> PutResult {
        PutResult {
            metadata: None,
            content_length: None,
            source_version: self.version(result.version, result.e_tag.as_deref()),
            etag: result.e_tag,
        }
    }
    fn completed_put(
        key: &ObjectPath,
        mut result: PutResult,
        length: u64,
        options: &WriteOptions,
    ) -> PutResult {
        let mut metadata = ObjectMetadata::file(key.clone(), length);
        metadata.etag.clone_from(&result.etag);
        metadata.source_version.clone_from(&result.source_version);
        metadata.attributes = options
            .attributes
            .iter()
            .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
            .collect();
        if let Some(value) = &options.content_type {
            metadata
                .provider_fields
                .insert("content_type".into(), value.clone());
        }
        if let Some(target) = metadata.attributes.remove("msc-symlink-target") {
            metadata.kind = crate::ObjectKind::Symlink;
            metadata.symlink_target = target.as_str().map(str::to_owned);
        }
        result.content_length = Some(length);
        result.metadata = Some(metadata);
        result
    }

    fn get_options(&self, conditions: &GetConditions) -> Result<object_store::GetOptions> {
        for etag in [&conditions.if_match, &conditions.if_none_match]
            .into_iter()
            .flatten()
        {
            validate_header_value(etag)?;
        }
        let mut result = object_store::GetOptions {
            if_match: conditions.if_match.clone(),
            if_none_match: conditions.if_none_match.clone(),
            ..Default::default()
        };
        if let Some(version) = &conditions.version {
            if version.scope != self.scope {
                return Err(MscError::new(
                    ErrorKind::PreconditionFailed,
                    "source version belongs to a different S3 backend",
                ));
            }
            if let Some(value) = version.value.strip_prefix("version:") {
                if value.is_empty() || value == "null" {
                    return Err(MscError::new(
                        ErrorKind::PreconditionFailed,
                        "source version is not an immutable S3 generation",
                    ));
                }
                result.version = Some(value.into());
            } else if let Some(value) = version.value.strip_prefix("etag:") {
                if !concrete_etag(value) {
                    return Err(MscError::new(
                        ErrorKind::PreconditionFailed,
                        "source version must contain a concrete ETag",
                    ));
                }
                validate_header_value(value)?;
                if result
                    .if_match
                    .as_ref()
                    .is_some_and(|etag| etag != "*" && etag != value)
                {
                    return Err(MscError::new(
                        ErrorKind::PreconditionFailed,
                        "ETag and source version conditions conflict",
                    ));
                }
                result.if_match = Some(value.into());
            } else {
                return Err(MscError::new(
                    ErrorKind::PreconditionFailed,
                    "source version is not an S3 generation",
                ));
            }
        }
        Ok(result)
    }
    fn attributes(options: &WriteOptions) -> Result<Attributes> {
        let mut result = Attributes::new();
        let mut names = std::collections::BTreeSet::new();
        for (key, value) in &options.attributes {
            if !names.insert(key.to_ascii_lowercase()) {
                return Err(config_error(
                    "S3 metadata names collide after HTTP case normalization",
                ));
            }
            let value = value
                .as_str()
                .ok_or_else(|| config_error("S3 native metadata values must be strings"))?;
            reqwest::header::HeaderName::from_bytes(format!("x-amz-meta-{key}").as_bytes())
                .map_err(|_| config_error("S3 metadata key is not a valid HTTP header name"))?;
            validate_header_value(value)?;
            result.insert(
                Attribute::Metadata(key.clone().into()),
                value.to_owned().into(),
            );
        }
        if let Some(content_type) = &options.content_type {
            validate_header_value(content_type)?;
            result.insert(Attribute::ContentType, content_type.clone().into());
        }
        Ok(result)
    }
    fn validate_link_metadata(
        key: &ObjectPath,
        options: &WriteOptions,
        size: Option<u64>,
    ) -> Result<bool> {
        let target = options
            .attributes
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("msc-symlink-target"))
            .and_then(|(_, value)| value.as_str());
        if let Some(target) = target {
            key.resolve_link(target)?;
            if size.is_some_and(|size| size != 0) {
                return Err(config_error(
                    "S3 logical links require an empty object body",
                ));
            }
        }
        Ok(target.is_some())
    }
    fn put_mode(&self, options: &WriteOptions) -> Result<PutMode> {
        let mut options = options.clone();
        if let Some(version) = &options.version {
            if version.scope != self.scope {
                return Err(MscError::new(
                    ErrorKind::PreconditionFailed,
                    "destination version belongs to a different backend",
                ));
            }
            let etag = version.value.strip_prefix("etag:").ok_or_else(|| {
                MscError::unsupported("S3 write conditions require an ETag generation")
            })?;
            if !concrete_etag(etag) {
                return Err(MscError::new(
                    ErrorKind::PreconditionFailed,
                    "destination version must contain a concrete ETag",
                ));
            }
            if options.if_match.as_ref().is_some_and(|value| value != etag) {
                return Err(MscError::new(
                    ErrorKind::PreconditionFailed,
                    "destination conditions conflict",
                ));
            }
            options.if_match = Some(etag.into());
        }
        if let Some(etag) = &options.if_match {
            validate_header_value(etag)?;
        }
        match (&options.if_match, &options.if_none_match) {
            (None, None) => Ok(PutMode::Overwrite),
            (Some(etag), None) => Ok(PutMode::Update(UpdateVersion {
                e_tag: Some(etag.clone()),
                version: None,
            })),
            (None, Some(etag)) if etag == "*" => Ok(PutMode::Create),
            _ => Err(MscError::unsupported(
                "S3 PUT with combined conditions or if_none_match other than '*'",
            )),
        }
    }
}

#[async_trait]
impl StorageProvider for S3Provider {
    fn backend_fingerprint(&self) -> String {
        self.scope.clone()
    }
    fn physical_root(&self) -> ObjectPath {
        self.base.clone()
    }
    fn name(&self) -> &'static str {
        self.name
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            ranged_reads: true,
            streaming_reads: true,
            streaming_writes: true,
            conditional_get: true,
            conditional_put: true,
            conditional_delete: true,
            batch_delete: true,
            server_side_copy: if self.encryption_configured {
                CopyScope::None
            } else {
                CopyScope::SameProvider
            },
            multipart_upload: true,
            native_lexical_listing: true,
            presigned_get: !self.skip_signature,
            presigned_put: !self.skip_signature,
            user_metadata: true,
            stable_source_versions: true,
            symlinks: true,
        }
    }
    async fn head(&self, key: &ObjectPath) -> Result<ObjectMetadata> {
        if key.is_root() {
            return Ok(ObjectMetadata::directory(key.clone()));
        }
        let result = self
            .store
            .get_opts(
                &self.path(key)?,
                object_store::GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| translate(error, "head", key.as_str()))?;
        self.metadata(result.meta, &result.attributes)
    }
    async fn head_version(
        &self,
        key: &ObjectPath,
        version: &SourceVersion,
    ) -> Result<ObjectMetadata> {
        let mut options = self.get_options(&GetConditions {
            version: Some(version.clone()),
            ..Default::default()
        })?;
        options.head = true;
        let result = self
            .store
            .get_opts(&self.path(key)?, options)
            .await
            .map_err(|error| translate(error, "head", key.as_str()))?;
        self.metadata(result.meta, &result.attributes)
    }
    async fn get(&self, key: &ObjectPath, conditions: &GetConditions) -> Result<ByteStream> {
        let path = self.path(key)?;
        let options = self.get_options(conditions)?;
        let result = self
            .store
            .get_opts(&path, options.clone())
            .await
            .map_err(|error| translate(error, "get", key.as_str()))?;
        let expected = result.range.end - result.range.start;
        let key = key.clone();
        Ok(Box::pin(stream::try_unfold(
            (self.resumable(&path, options, result), 0u64, key),
            move |(mut stream, count, key)| async move {
                if let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|error| translate(error, "get", key.as_str()))?;
                    let count = count.checked_add(chunk.len() as u64).ok_or_else(|| {
                        MscError::new(ErrorKind::Integrity, "S3 read length overflow")
                    })?;
                    if count > expected {
                        return Err(MscError::new(
                            ErrorKind::Integrity,
                            "S3 GET returned more bytes than its response length",
                        ));
                    }
                    Ok(Some((chunk, (stream, count, key))))
                } else {
                    if count != expected {
                        return Err(MscError::new(
                            ErrorKind::Integrity,
                            "S3 GET returned fewer bytes than its response length",
                        ));
                    }
                    Ok(None)
                }
            },
        )))
    }
    async fn get_range(
        &self,
        key: &ObjectPath,
        range: ByteRange,
        conditions: &GetConditions,
    ) -> Result<Bytes> {
        range.end()?;
        if range.length == 0 {
            return Ok(Bytes::new());
        }
        let path = self.path(key)?;
        let mut options = self.get_options(conditions)?;
        options.range = Some((range.offset..range.end()?).into());
        let result = match self
            .store
            .get_opts(&path, options.clone())
            .await
            .map_err(|error| translate(error, "get_range", key.as_str()))
        {
            Ok(result) => result,
            Err(error) if error.http_status == Some(416) => return Ok(Bytes::new()),
            Err(error) => return Err(error),
        };
        let expected = range
            .end()?
            .min(result.meta.size)
            .saturating_sub(range.offset);
        if result.range != (range.offset..range.offset + expected) {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "S3 range response differs from requested bounds",
            ));
        }
        let mut input = self.resumable(&path, options, result);
        let mut bytes = BytesMut::new();
        while let Some(chunk) = input.next().await {
            let chunk = chunk.map_err(|error| translate(error, "get_range", key.as_str()))?;
            if chunk.len() as u64 > expected - bytes.len() as u64 {
                return Err(MscError::new(
                    ErrorKind::Integrity,
                    "S3 range response exceeds its requested length",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != expected {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "S3 range response length differs from its requested length",
            ));
        }
        Ok(bytes.freeze())
    }
    async fn get_suffix(
        &self,
        key: &ObjectPath,
        range: crate::SuffixRange,
        conditions: &GetConditions,
    ) -> Result<Bytes> {
        let mut options = self.get_options(conditions)?;
        options.range = Some(object_store::GetRange::Suffix(range.length()));
        let result = match self
            .store
            .get_opts(&self.path(key)?, options)
            .await
            .map_err(|error| translate(error, "get_suffix", key.as_str()))
        {
            Ok(result) => result,
            Err(error) if error.http_status == Some(416) => {
                return crate::provider::suffix_fallback(self, key, range, conditions).await;
            }
            Err(error) => return Err(error),
        };
        let metadata = self.metadata(result.meta.clone(), &result.attributes)?;
        if metadata.symlink_target.is_some() {
            return crate::provider::suffix_fallback(self, key, range, conditions).await;
        }
        let size = result.meta.size;
        let expected = range.length().min(size);
        if result.range != (size - expected..size) {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "invalid suffix response range",
            ));
        }
        let key = key.clone();
        let body = result
            .into_stream()
            .map(move |chunk| chunk.map_err(|error| translate(error, "get_suffix", key.as_str())));
        crate::provider::range_bytes(Box::pin(body), expected).await
    }
    async fn put(
        &self,
        key: &ObjectPath,
        input: ByteStream,
        size: Option<u64>,
        options: &WriteOptions,
    ) -> Result<PutResult> {
        if key.is_root() {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "an S3 object key must not be empty",
            ));
        }
        if size.is_some_and(|size| size > MAX_OBJECT_SIZE) {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "S3 object exceeds 5 TiB",
            ));
        }
        let mode = self.put_mode(options)?;
        let path = self.path(key)?;
        let mut options = options.clone();
        if self.options.infer_content_type && options.content_type.is_none() {
            let inferred = mime_guess::from_path(key.as_str()).first_or_octet_stream();
            let content_type = if self.name == "s8k" && inferred.essence_str() == "audio/wav" {
                "audio/x-wav"
            } else {
                inferred.essence_str()
            };
            options.content_type = Some(content_type.into());
        }
        let attributes = Self::attributes(&options)?;
        let link = Self::validate_link_metadata(key, &options, size)?;
        let threshold = usize::try_from(self.options.multipart_threshold)
            .map_err(|_| config_error("multipart threshold exceeds address space"))?;
        let mut reader = PartReader {
            input,
            pending: Bytes::new(),
            total: 0,
            finished: false,
        };
        let lookahead = threshold
            .checked_add(1)
            .ok_or_else(|| config_error("multipart threshold exceeds address space"))?;
        let first = reader.next(lookahead).await?.unwrap_or_default();
        if link && !first.is_empty() {
            return Err(config_error(
                "S3 logical links require an empty object body",
            ));
        }
        // Match MSC Python: objects up to and including the threshold use one PUT.
        // Swift multipart initiation and completion add seconds per object.
        if first.len() <= threshold {
            check_size(size, reader.total)?;
            let create_only = mode == PutMode::Create;
            let conditional_update = matches!(mode, PutMode::Update(_));
            let result = self
                .store
                .put_opts(
                    &path,
                    first.into(),
                    PutOptions {
                        mode,
                        attributes,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| {
                    let mut error = translate(error, "put", key.as_str());
                    if (create_only && error.kind == ErrorKind::AlreadyExists)
                        || (conditional_update && error.kind == ErrorKind::NotFound)
                    {
                        error.kind = ErrorKind::PreconditionFailed;
                    }
                    error
                })?;
            Ok(Self::completed_put(
                key,
                self.put_result(result),
                reader.total,
                &options,
            ))
        } else {
            if mode != PutMode::Overwrite {
                return Err(MscError::unsupported("conditional multipart S3 PUT"));
            }
            let input = Box::pin(stream::once(async move { Ok(first) }).chain(reader.remainder()));
            let result = self
                .multipart(&path, input, size, attributes)
                .await
                .map_err(|mut error| {
                    error.retryable = false;
                    error
                })?;
            let length = result.content_length.expect("completed upload length");
            Ok(Self::completed_put(key, result, length, &options))
        }
    }
    async fn delete(&self, key: &ObjectPath, conditions: &DeleteOptions) -> Result<()> {
        if key.is_root() {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "an S3 object key must not be empty",
            ));
        }
        self.native_delete(key, conditions).await
    }
    async fn delete_many(
        &self,
        keys: &[ObjectPath],
        options: &DeleteOptions,
    ) -> crate::BatchResult {
        self.native_delete_many(keys, options).await
    }

    async fn copy(
        &self,
        source: &ObjectPath,
        destination: &ObjectPath,
        options: &CopyOptions,
    ) -> Result<PutResult> {
        self.native_copy(source, destination, options).await
    }
    async fn list(&self, options: &ListOptions) -> Result<MetadataStream> {
        listing::list(self.clone(), options)
    }
    async fn make_symlink(&self, key: &ObjectPath, target: &str) -> Result<()> {
        key.resolve_link(target)?;
        validate_header_value(target)?;
        let options = WriteOptions {
            attributes: std::collections::BTreeMap::from([(
                "msc-symlink-target".into(),
                target.into(),
            )]),
            ..Default::default()
        };
        self.put(key, Box::pin(stream::empty()), Some(0), &options)
            .await?;
        Ok(())
    }
    async fn presigned_url(
        &self,
        key: &ObjectPath,
        method: PresignMethod,
        expires: Duration,
    ) -> Result<String> {
        if self.skip_signature {
            return Err(MscError::unsupported("anonymous S3 presigning"));
        }
        if expires.as_secs() == 0 || expires > Duration::from_secs(7 * 24 * 60 * 60) {
            return Err(MscError::new(
                ErrorKind::InvalidConfig,
                "S3 presigned expiration must be positive and at most seven days",
            ));
        }
        let method = match method {
            PresignMethod::Get => reqwest::Method::GET,
            PresignMethod::Put => reqwest::Method::PUT,
        };
        self.store
            .signed_url(method, &self.path(key)?, expires)
            .await
            .map(|url| url.to_string())
            .map_err(|error| translate(error, "presign", key.as_str()))
    }
    async fn shutdown(&self) -> Result<()> {
        self.cleanup.shutdown().await
    }
}

fn low_speed(options: &S3Options) -> Option<LowSpeed> {
    LowSpeed::new(
        options.low_speed_limit,
        Duration::from_millis(options.low_speed_time_ms),
    )
}

fn configured_bool(builder: &AmazonS3Builder, key: AmazonS3ConfigKey) -> Result<bool> {
    match builder
        .get_config_value(&key)
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        None | Some("0" | "false" | "off" | "no" | "n") => Ok(false),
        Some("1" | "true" | "on" | "yes" | "y") => Ok(true),
        Some(_) => Err(config_error("invalid S3 boolean configuration")),
    }
}
fn client_options(options: &S3Options, endpoint_url: Option<&url::Url>) -> Result<ClientOptions> {
    let mut client_options = ClientOptions::new()
        .with_allow_http(endpoint_url.is_some_and(|url| url.scheme() == "http"))
        .with_allow_invalid_certificates(!options.verify_ssl)
        .with_connect_timeout(Duration::from_millis(options.connect_timeout_ms))
        .with_timeout(Duration::from_millis(options.read_timeout_ms))
        .with_read_timeout(Duration::from_millis(options.idle_read_timeout_ms))
        .with_pool_idle_timeout(Duration::from_millis(options.pool_timeout_ms));
    if let Some(agent) = &options.user_agent {
        client_options = client_options.with_user_agent(
            reqwest::header::HeaderValue::from_str(agent)
                .map_err(|_| config_error("invalid S3 user_agent"))?,
        );
    }
    if let Some(path) = &options.ca_certificate {
        let pem =
            std::fs::read(path).map_err(|_| config_error("unable to read S3 CA certificate"))?;
        let certificate = object_store::Certificate::from_pem(&pem)
            .map_err(|_| config_error("invalid S3 CA certificate"))?;
        client_options = client_options.with_root_certificate(certificate);
    }
    Ok(client_options)
}

fn validate_environment_credentials(builder: &AmazonS3Builder) -> Result<()> {
    for key in [AmazonS3ConfigKey::AccessKeyId, AmazonS3ConfigKey::Token] {
        if let Some(value) = builder.get_config_value(&key) {
            reqwest::header::HeaderValue::from_str(&value).map_err(|_| {
                MscError::new(
                    ErrorKind::Unauthenticated,
                    "S3 environment credentials contain invalid header characters",
                )
            })?;
        }
    }
    Ok(())
}

fn bucket_endpoint(endpoint: Option<String>, bucket: &str, options: &S3Options) -> String {
    match (endpoint, options.path_style) {
        (Some(endpoint), true) => format!("{}/{bucket}", endpoint.trim_end_matches('/')),
        (Some(endpoint), false) => endpoint,
        (None, true) => format!("https://s3.{}.amazonaws.com/{bucket}", options.region),
        (None, false) => format!("https://{bucket}.s3.{}.amazonaws.com", options.region),
    }
}

fn concrete_etag(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && value != "*" && !value.contains(',')
}
