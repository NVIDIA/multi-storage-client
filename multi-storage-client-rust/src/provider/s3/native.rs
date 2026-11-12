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

//! Native requests retaining conditional-delete and copy publication information.
use super::{S3Provider, config_error, translate, validate_header_value};
use crate::{
    CopyOptions, DeleteOptions, ErrorKind, GetConditions, MscError, ObjectPath, PutResult, Result,
};
use base64::Engine as _;
use futures::StreamExt as _;
use md5::Digest as _;
use object_store::ObjectStore;
use object_store::client::{HttpRequest, HttpResponse};
use serde::Deserialize;
use std::collections::{BTreeMap, VecDeque};

impl S3Provider {
    pub(super) fn bucket_url(&self) -> Result<url::Url> {
        url::Url::parse(&self.endpoint).map_err(|_| config_error("invalid S3 endpoint"))
    }
    fn object_url(&self, key: &ObjectPath) -> Result<url::Url> {
        self.path(key)?;
        let mut url = self.bucket_url()?;
        let path = format!(
            "{}/{}",
            url.path().trim_end_matches('/'),
            encode_path(self.base.join(key)?.as_str())
        );
        url.set_path(&path);
        Ok(url)
    }

    pub(super) async fn send_native(
        &self,
        mut request: HttpRequest,
        operation: &str,
        key: &ObjectPath,
    ) -> Result<HttpResponse> {
        if !self.skip_signature {
            let credentials = self
                .store
                .credentials()
                .get_credential()
                .await
                .map_err(|error| translate(error, operation, key.as_str()))?;
            // Header validation is required because the SDK authorizer assumes valid credentials.
            validate_header_value(&credentials.key_id)?;
            if let Some(token) = &credentials.token {
                validate_header_value(token)?;
            }
            object_store::aws::AwsAuthorizer::new(&credentials, "s3", &self.options.region)
                .with_request_payer(self.request_payer)
                .with_sign_payload(self.sign_payload)
                .try_authorize(&mut request, None)
                .map_err(|error| translate(error, operation, key.as_str()))?;
        }
        self.http.execute(request).await.map_err(|error| {
            translate(
                object_store::Error::Generic {
                    store: "S3",
                    source: Box::new(error),
                },
                operation,
                key.as_str(),
            )
        })
    }

    pub(super) async fn native_delete(
        &self,
        key: &ObjectPath,
        conditions: &DeleteOptions,
    ) -> Result<()> {
        if key.is_root() {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "an S3 object key must not be empty",
            ));
        }
        if conditions.if_none_match.is_some() {
            return Err(MscError::unsupported("S3 DELETE if_none_match"));
        }
        let mut url = self.object_url(key)?;
        let mut etag = conditions.if_match.clone();
        if let Some(version) = &conditions.version {
            let parsed = self.get_options(&GetConditions {
                version: Some(version.clone()),
                ..Default::default()
            })?;
            if let Some(value) = parsed.version {
                url.query_pairs_mut().append_pair("versionId", &value);
            }
            if let Some(value) = parsed.if_match {
                if etag.as_ref().is_some_and(|etag| etag != &value) {
                    return Err(MscError::new(
                        ErrorKind::PreconditionFailed,
                        "S3 delete conditions conflict",
                    ));
                }
                etag = Some(value);
            }
        }
        let mut request = http::Request::builder().method("DELETE").uri(url.as_str());
        if let Some(etag) = etag {
            validate_header_value(&etag)?;
            request = request.header("if-match", etag);
        }
        let request = request
            .body(bytes::Bytes::new().into())
            .map_err(|_| config_error("invalid S3 delete request"))?;
        self.send_native(request, "delete", key).await?;
        Ok(())
    }

    pub(super) async fn native_delete_many(
        &self,
        keys: &[ObjectPath],
        options: &DeleteOptions,
    ) -> crate::BatchResult {
        let mut result = crate::BatchResult::default();
        // Conditional deletes require individual requests: DeleteObjects has no
        // per-object ETag condition on general-purpose buckets.
        if options.version.is_some()
            || options.if_match.is_some()
            || options.if_none_match.is_some()
        {
            for (index, key) in keys.iter().enumerate() {
                match self.native_delete(key, options).await {
                    Ok(()) => result.succeeded.push(index),
                    Err(error) if error.kind == ErrorKind::NotFound => result.succeeded.push(index),
                    Err(error) => result.failed.push(crate::BatchFailure {
                        index,
                        source: key.clone(),
                        destination: None,
                        error,
                    }),
                }
            }
            return result;
        }
        let mut valid = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let physical = if key.is_root() {
                Err(MscError::new(
                    ErrorKind::InvalidPath,
                    "an S3 object key must not be empty",
                ))
            } else {
                self.path(key)
            };
            match physical {
                Ok(path) => valid.push((index, key, path.to_string())),
                Err(error) => result.failed.push(crate::BatchFailure {
                    index,
                    source: key.clone(),
                    destination: None,
                    error,
                }),
            }
        }
        for chunk in valid.chunks(1000) {
            let outcomes = self.delete_chunk(chunk).await;
            for (offset, (index, key, _)) in chunk.iter().enumerate() {
                let error = match &outcomes {
                    Ok(outcomes) => outcomes[offset].clone(),
                    Err(error) => Some(error.clone()),
                };
                if let Some(error) = error {
                    result.failed.push(crate::BatchFailure {
                        index: *index,
                        source: (*key).clone(),
                        destination: None,
                        error,
                    });
                } else {
                    result.succeeded.push(*index);
                }
            }
        }
        result.succeeded.sort_unstable();
        result.failed.sort_by_key(|failure| failure.index);
        result
    }

    async fn delete_chunk(
        &self,
        keys: &[(usize, &ObjectPath, String)],
    ) -> Result<Vec<Option<MscError>>> {
        let mut body = String::from(
            "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Quiet>false</Quiet>",
        );
        for (_, _, path) in keys {
            body.push_str("<Object><Key>");
            body.push_str(&quick_xml::escape::escape(path));
            body.push_str("</Key></Object>");
        }
        body.push_str("</Delete>");
        let mut url = self.bucket_url()?;
        url.query_pairs_mut().append_pair("delete", "");
        let digest =
            base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body.as_bytes()));
        let request = http::Request::builder()
            .method("POST")
            .uri(url.as_str())
            .header("content-type", "application/xml")
            .header("content-md5", digest)
            .body(bytes::Bytes::from(body).into())
            .map_err(|_| config_error("invalid S3 batch request"))?;
        let response = self
            .send_native(request, "delete_many", &ObjectPath::default())
            .await?;
        let mut input = response.into_body().bytes_stream();
        let mut bytes = bytes::BytesMut::new();
        while let Some(chunk) = input.next().await {
            let chunk = chunk.map_err(|_| batch_integrity())?;
            if bytes.len().saturating_add(chunk.len()) > 4 * 1024 * 1024 {
                return Err(batch_integrity());
            }
            bytes.extend_from_slice(&chunk);
        }
        validate_xml_root(&bytes, b"DeleteResult")?;
        let response: DeleteResult =
            quick_xml::de::from_reader(bytes.as_ref()).map_err(|_| batch_integrity())?;
        let mut pending: BTreeMap<&str, VecDeque<usize>> = BTreeMap::new();
        for (offset, (_, _, path)) in keys.iter().enumerate() {
            pending.entry(path).or_default().push_back(offset);
        }
        let mut outcomes = vec![None; keys.len()];
        for item in response.items {
            let (key, error) = match item {
                DeleteItem::Deleted(deleted) => (deleted.key, None),
                DeleteItem::Error(error) => {
                    let failure = delete_error(&error.code);
                    (error.key, failure)
                }
            };
            let offset = pending
                .get_mut(key.as_str())
                .and_then(VecDeque::pop_front)
                .ok_or_else(batch_integrity)?;
            outcomes[offset] = error;
        }
        if pending.values().any(|offsets| !offsets.is_empty()) {
            return Err(batch_integrity());
        }
        Ok(outcomes)
    }

    pub(super) async fn native_copy(
        &self,
        source: &ObjectPath,
        destination: &ObjectPath,
        options: &CopyOptions,
    ) -> Result<PutResult> {
        if source.is_root() || destination.is_root() {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "S3 copy requires two object keys",
            ));
        }
        if options.destination.if_match.is_some()
            || options.destination.if_none_match.is_some()
            || options.destination.version.is_some()
        {
            return Err(MscError::unsupported(
                "S3 COPY destination conditions; use streaming copy",
            ));
        }
        if self.encryption_configured {
            return Err(MscError::unsupported(
                "S3 native copy with configured encryption; use streaming copy",
            ));
        }
        self.path(source)?;
        self.path(destination)?;
        Self::attributes(&options.destination)?;
        Self::validate_link_metadata(destination, &options.destination, None)?;
        let mut source_options = self.get_options(&options.source_conditions)?;
        source_options.head = true;
        let observed = self
            .store
            .get_opts(&self.path(source)?, source_options.clone())
            .await
            .map_err(|error| translate(error, "copy", source.as_str()))?;
        let metadata = self.metadata(observed.meta, &observed.attributes)?;
        if metadata.kind != crate::ObjectKind::File {
            return Err(MscError::unsupported("native S3 copy of a non-file"));
        }
        if metadata.content_length > 5 * 1024 * 1024 * 1024 {
            return Err(MscError::unsupported(
                "multipart S3 copy; use streaming copy",
            ));
        }
        let copy_source = self.copy_source(source, source_options.version.as_deref())?;
        let etag = metadata
            .etag
            .clone()
            .or(source_options.if_match)
            .filter(|etag| super::concrete_etag(etag))
            .ok_or_else(|| MscError::unsupported("native S3 copy requires a source generation"))?;
        validate_header_value(&etag)?;
        let url = self.object_url(destination)?;
        let mut request = http::Request::builder()
            .method("PUT")
            .uri(url.as_str())
            .header("x-amz-copy-source", copy_source)
            .header("x-amz-copy-source-if-match", etag);
        request = copy_metadata(
            request,
            destination,
            &options.destination,
            &metadata,
            &observed.attributes,
        )?;
        if let Some(value) = source_options.if_none_match {
            validate_header_value(&value)?;
            request = request.header("x-amz-copy-source-if-none-match", value);
        }
        let request = request
            .body(bytes::Bytes::new().into())
            .map_err(|_| config_error("invalid S3 copy request"))?;
        let response = self.send_native(request, "copy", destination).await?;
        let result = self.copy_publication(response).await?;
        let mut destination_options = options.destination.clone();
        if destination_options.attributes.is_empty() {
            destination_options.attributes = metadata.attributes;
        }
        if destination_options.content_type.is_none() {
            destination_options.content_type =
                metadata.provider_fields.get("content_type").cloned();
        }
        Ok(Self::completed_put(
            destination,
            result,
            metadata.content_length,
            &destination_options,
        ))
    }

    fn copy_source(&self, source: &ObjectPath, version: Option<&str>) -> Result<String> {
        let path = encode_path(&format!("{}/{}", self.bucket, self.base.join(source)?));
        let query = version.map(|version| {
            url::form_urlencoded::Serializer::new(String::new())
                .append_pair("versionId", version)
                .finish()
        });
        Ok(query.map_or_else(|| format!("/{path}"), |query| format!("/{path}?{query}")))
    }

    async fn copy_publication(&self, response: HttpResponse) -> Result<PutResult> {
        let version = response
            .headers()
            .get("x-amz-version-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut input = response.into_body().bytes_stream();
        let mut bytes = bytes::BytesMut::new();
        while let Some(chunk) = input.next().await {
            let chunk = chunk.map_err(|_| {
                MscError::new(ErrorKind::Integrity, "invalid S3 copy response body")
            })?;
            if bytes.len().saturating_add(chunk.len()) > 64 * 1024 {
                return Err(MscError::new(
                    ErrorKind::Integrity,
                    "S3 copy response exceeds its bounded envelope",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if validate_xml_root(&bytes, b"Error").is_ok() {
            let failure: ServiceFailure =
                quick_xml::de::from_reader(bytes.as_ref()).map_err(|_| batch_integrity())?;
            if failure.code.is_empty() {
                return Err(batch_integrity());
            }
            let mut error = match failure.code.as_str() {
                "NoSuchKey" | "NoSuchVersion" => {
                    MscError::new(ErrorKind::NotFound, "S3 copy source does not exist")
                }
                "RequestTimeout" => MscError::new(ErrorKind::Timeout, "S3 copy timed out"),
                code => delete_error(code)
                    .unwrap_or_else(|| MscError::new(ErrorKind::Provider, "S3 copy failed")),
            };
            // CopyObject may report a service failure in a successful HTTP response.
            error.http_status = Some(200);
            return Err(error);
        }
        validate_xml_root(&bytes, b"CopyObjectResult")?;
        let result: CopyResult = quick_xml::de::from_reader(bytes.as_ref()).map_err(|_| {
            MscError::new(
                ErrorKind::Integrity,
                "S3 copy did not return a completed publication",
            )
        })?;
        if !super::concrete_etag(&result.etag) {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "S3 copy response has no generation",
            ));
        }
        validate_header_value(&result.etag)?;
        Ok(PutResult {
            source_version: self.version(version, Some(&result.etag)),
            etag: Some(result.etag),
            metadata: None,
            content_length: None,
        })
    }
}

fn copy_metadata(
    mut request: http::request::Builder,
    destination: &ObjectPath,
    options: &crate::WriteOptions,
    metadata: &crate::ObjectMetadata,
    attributes: &object_store::Attributes,
) -> Result<http::request::Builder> {
    if options.attributes.is_empty() && options.content_type.is_none() {
        // COPY retains headers the SDK does not expose, including Expires and
        // website redirects, along with user metadata and content properties.
        return Ok(request.header("x-amz-metadata-directive", "COPY"));
    }
    let mut options = options.clone();
    if options.attributes.is_empty() {
        options.attributes.clone_from(&metadata.attributes);
    }
    if options.content_type.is_none() {
        options.content_type = metadata.provider_fields.get("content_type").cloned();
    }
    S3Provider::attributes(&options)?;
    S3Provider::validate_link_metadata(destination, &options, Some(metadata.content_length))?;
    request = request.header("x-amz-metadata-directive", "REPLACE");
    for (key, value) in &options.attributes {
        request = request.header(
            format!("x-amz-meta-{key}"),
            value.as_str().expect("validated metadata"),
        );
    }
    for (attribute, name) in [
        (object_store::Attribute::CacheControl, "cache-control"),
        (
            object_store::Attribute::ContentDisposition,
            "content-disposition",
        ),
        (object_store::Attribute::ContentEncoding, "content-encoding"),
        (object_store::Attribute::ContentLanguage, "content-language"),
    ] {
        if let Some(value) = attributes.get(&attribute) {
            request = request.header(name, value.as_ref());
        }
    }
    if let Some(content_type) = &options.content_type {
        request = request.header("content-type", content_type);
    }
    Ok(request)
}

#[derive(Deserialize)]
struct ServiceFailure {
    #[serde(rename = "Code")]
    code: String,
}
#[derive(Deserialize)]
struct CopyResult {
    #[serde(rename = "ETag")]
    etag: String,
}
#[derive(Deserialize)]
struct DeleteResult {
    #[serde(rename = "$value", default)]
    items: Vec<DeleteItem>,
}
#[derive(Deserialize)]
enum DeleteItem {
    Deleted(DeletedKey),
    Error(DeleteFailure),
}
#[derive(Deserialize)]
struct DeletedKey {
    #[serde(rename = "Key")]
    key: String,
}
#[derive(Deserialize)]
struct DeleteFailure {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Code")]
    code: String,
}
fn batch_integrity() -> MscError {
    MscError::new(
        ErrorKind::Integrity,
        "S3 batch DELETE returned invalid or incomplete outcomes",
    )
}
fn encode_path(path: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::new();
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~/".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }
    encoded
}
fn delete_error(code: &str) -> Option<MscError> {
    let (kind, status) = match code {
        "NoSuchKey" | "NoSuchVersion" => return None,
        "AccessDenied" => (ErrorKind::PermissionDenied, Some(403)),
        "InvalidAccessKeyId" | "SignatureDoesNotMatch" | "ExpiredToken" => {
            (ErrorKind::Unauthenticated, Some(403))
        }
        "PreconditionFailed" => (ErrorKind::PreconditionFailed, Some(412)),
        "SlowDown" | "Throttling" => (ErrorKind::RateLimited, Some(429)),
        "InternalError" | "ServiceUnavailable" => (ErrorKind::Unavailable, Some(503)),
        _ => (ErrorKind::Provider, None),
    };
    let mut error = MscError::new(kind, "S3 batch DELETE failed for an object");
    error.http_status = status;
    Some(error)
}

fn validate_xml_root(bytes: &[u8], root: &[u8]) -> Result<()> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(bytes);
    let mut depth = 0usize;
    let mut seen = false;
    loop {
        match reader.read_event().map_err(|_| batch_integrity())? {
            Event::Start(element) => {
                if depth == 0 {
                    if seen || element.name().as_ref() != root {
                        return Err(batch_integrity());
                    }
                    seen = true;
                }
                depth += 1;
                if depth > 64 {
                    return Err(batch_integrity());
                }
            }
            Event::Empty(element) if depth == 0 => {
                if seen || element.name().as_ref() != root {
                    return Err(batch_integrity());
                }
                seen = true;
            }
            Event::End(_) => {
                depth = depth.checked_sub(1).ok_or_else(batch_integrity)?;
            }
            Event::Eof if seen && depth == 0 => return Ok(()),
            Event::Eof | Event::DocType(_) => return Err(batch_integrity()),
            Event::Decl(_) if seen => return Err(batch_integrity()),
            Event::Text(text) if depth == 0 && !text.iter().all(u8::is_ascii_whitespace) => {
                return Err(batch_integrity());
            }
            Event::CData(_) | Event::GeneralRef(_) if depth == 0 => return Err(batch_integrity()),
            _ => {}
        }
    }
}
