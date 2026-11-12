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

//! Credentials and typed, redacted transport failures at the SDK boundary.
use crate::error::{ErrorKind, MscError};
use async_trait::async_trait;
use futures::StreamExt as _;
use http_body_util::BodyExt as _;
use object_store::{
    ClientOptions,
    client::{
        HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
        HttpResponseBody, HttpService, ReqwestConnector,
    },
};
use std::{sync::Arc, time::Duration};

/// Keep SDK response bodies and credentials out of messages and error sources.
#[allow(clippy::needless_pass_by_value)] // Consume and discard the unsanitized SDK error.
pub(super) fn translate(error: object_store::Error, operation: &str, key: &str) -> MscError {
    let mut kind = match &error {
        object_store::Error::NotFound { .. } => ErrorKind::NotFound,
        object_store::Error::AlreadyExists { .. } => ErrorKind::AlreadyExists,
        object_store::Error::Precondition { .. } => ErrorKind::PreconditionFailed,
        object_store::Error::NotModified { .. } => ErrorKind::NotModified,
        object_store::Error::PermissionDenied { .. } => ErrorKind::PermissionDenied,
        object_store::Error::Unauthenticated { .. } => ErrorKind::Unauthenticated,
        object_store::Error::InvalidPath { .. } => ErrorKind::InvalidPath,
        object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented { .. } => {
            ErrorKind::Unsupported
        }
        _ => ErrorKind::Provider,
    };
    let mut status = None;
    let mut request_id = None;
    let mut retry_after = None;
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(error) = source {
        if let Some(error) = error.downcast_ref::<MscError>() {
            return error.clone().context(operation, "s3", key);
        }
        if let Some(response) = error.downcast_ref::<ResponseFailure>() {
            status = Some(response.status);
            request_id.clone_from(&response.request_id);
            retry_after = response.retry_after;
        }
        if let Some(http) = error.downcast_ref::<HttpError>() {
            kind = match http.kind() {
                HttpErrorKind::Timeout => ErrorKind::Timeout,
                HttpErrorKind::Connect | HttpErrorKind::Request | HttpErrorKind::Interrupted => {
                    ErrorKind::Unavailable
                }
                HttpErrorKind::Decode => ErrorKind::Integrity,
                _ => kind,
            };
        }
        source = error.source();
    }
    if let Some(status) = status {
        kind = match status {
            304 => ErrorKind::NotModified,
            401 => ErrorKind::Unauthenticated,
            403 => ErrorKind::PermissionDenied,
            404 => ErrorKind::NotFound,
            408 => ErrorKind::Timeout,
            409 | 412 => ErrorKind::PreconditionFailed,
            429 => ErrorKind::RateLimited,
            500..=599 => ErrorKind::Unavailable,
            _ => kind,
        };
    }
    let mut result =
        MscError::new(kind, format!("S3 {operation} failed")).context(operation, "s3", key);
    result.http_status = status;
    result.request_id = request_id.map(String::into_boxed_str);
    result.retry_after = retry_after;
    result
}

/// Per-connection throughput floor. Some network paths throttle individual TCP
/// connections for their whole lifetime while new connections run at full
/// speed, so slow transfers move their remaining work to another connection.
#[derive(Debug, Clone, Copy)]
pub(super) struct LowSpeed {
    /// Bytes per second.
    limit: u64,
    window: Duration,
}
impl LowSpeed {
    pub(super) fn new(limit: u64, window: Duration) -> Option<Self> {
        (limit != 0).then_some(Self { limit, window })
    }
    pub(super) fn window(self) -> Duration {
        self.window
    }
    /// Whether `bytes` transferred over `elapsed` falls below the floor.
    pub(super) fn slow(self, bytes: u64, elapsed: Duration) -> bool {
        u128::from(bytes) * 1000 < u128::from(self.limit) * elapsed.as_millis()
    }
    /// Time allowed for a request body before another connection sends it too.
    fn hedge_delay(self, length: u64) -> Duration {
        self.window.saturating_add(Duration::from_millis(
            length.saturating_mul(1000) / self.limit,
        ))
    }
}

// The SDK's HTTP status error is private. Preserve typed status information at
// its public HTTP boundary, without retaining a response body or signed URL.
#[derive(Debug, Clone)]
pub(super) struct StatusConnector(HttpClient);
impl StatusConnector {
    pub(super) fn new(
        options: &ClientOptions,
        idle_read_timeout: Duration,
        low_speed: Option<LowSpeed>,
    ) -> object_store::Result<Self> {
        Ok(Self(HttpClient::new(StatusService {
            client: ReqwestConnector::default()
                .connect(&options.clone().with_read_timeout_disabled())?,
            idle_read_timeout,
            low_speed,
        })))
    }
}
impl HttpConnector for StatusConnector {
    fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(self.0.clone())
    }
}
#[derive(Debug)]
struct StatusService {
    client: HttpClient,
    idle_read_timeout: Duration,
    low_speed: Option<LowSpeed>,
}
impl StatusService {
    /// Send a multipart part again on another connection once it exceeds its
    /// low-speed allowance. Parts are replaceable until completion, and the
    /// signed request and payload are identical, so the first success wins.
    async fn hedged(
        &self,
        request: HttpRequest,
        delay: Duration,
    ) -> std::result::Result<HttpResponse, HttpError> {
        let Some(duplicate) = duplicate_request(&request) else {
            return self.client.execute(request).await;
        };
        let mut original = Box::pin(self.client.execute(request));
        tokio::select! {
            result = &mut original => return result,
            () = tokio::time::sleep(delay) => {}
        }
        let hedge = Box::pin(self.client.execute(duplicate));
        let (first, other) = match futures::future::select(original, hedge).await {
            futures::future::Either::Left(done) | futures::future::Either::Right(done) => done,
        };
        if first
            .as_ref()
            .is_ok_and(|response| response.status().is_success())
        {
            return first;
        }
        let second = other.await;
        if second
            .as_ref()
            .is_ok_and(|response| response.status().is_success())
        {
            second
        } else {
            first
        }
    }
}

/// Only multipart parts are replaceable; a duplicated single PUT could publish
/// again after the caller has observed success.
fn part_hedge_delay(request: &HttpRequest, low_speed: Option<LowSpeed>) -> Option<Duration> {
    let low_speed = low_speed?;
    if request.method() != http::Method::PUT || request.headers().contains_key("x-amz-copy-source")
    {
        return None;
    }
    let query = request.uri().query()?;
    let (mut part, mut upload) = (false, false);
    for (key, _) in url::form_urlencoded::parse(query.as_bytes()) {
        part |= key == "partNumber";
        upload |= key == "uploadId";
    }
    let length = request.body().as_bytes()?.len();
    (part && upload).then(|| low_speed.hedge_delay(length as u64))
}

fn duplicate_request(request: &HttpRequest) -> Option<HttpRequest> {
    let body = request.body().as_bytes()?.clone();
    let mut duplicate = http::Request::builder()
        .method(request.method().clone())
        .uri(request.uri().clone())
        .version(request.version())
        .body(body.into())
        .ok()?;
    duplicate.headers_mut().clone_from(request.headers());
    Some(duplicate)
}
#[async_trait]
impl HttpService for StatusService {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        let range = request
            .headers()
            .get("range")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let reading =
            request.method() == http::Method::GET || request.method() == http::Method::HEAD;
        let request = reusable_put_body(request).await?;
        // Share connections across SDK and native requests. Apply the read idle
        // timer per response so uploads retain their full request deadline.
        let response = if reading {
            tokio::time::timeout(self.idle_read_timeout, self.client.execute(request))
                .await
                .map_err(|_| idle_read_failure())??
        } else if let Some(delay) = part_hedge_delay(&request, self.low_speed) {
            self.hedged(request, delay).await?
        } else {
            self.client.execute(request).await?
        };
        let response = if reading {
            idle_read_response(response, self.idle_read_timeout)
        } else {
            response
        };
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let request_id = response
                .headers()
                .get("x-amz-request-id")
                .and_then(|value| value.to_str().ok())
                .filter(|value| {
                    value.len() <= 256
                        && value
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || b"+/-_=".contains(&byte))
                })
                .map(str::to_owned);
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs);
            // Let an early error response settle before dropping the upload connection.
            // Discard the body without retaining provider error text or unbounded data.
            let _ =
                tokio::time::timeout(self.idle_read_timeout, discard_error_body(response)).await;
            return Err(HttpError::new(
                match status {
                    408 => HttpErrorKind::Timeout,
                    429 | 500..=599 => HttpErrorKind::Request,
                    _ => HttpErrorKind::Unknown,
                },
                ResponseFailure {
                    status,
                    request_id,
                    retry_after,
                },
            ));
        }
        validate_range_response(range.as_deref(), &response)?;
        Ok(response)
    }
}

fn idle_read_failure() -> HttpError {
    HttpError::new(
        HttpErrorKind::Timeout,
        MscError::new(
            ErrorKind::Timeout,
            "S3 response read exceeded its inactivity timeout",
        ),
    )
}

fn idle_read_response(response: HttpResponse, timeout: Duration) -> HttpResponse {
    let (parts, body) = response.into_parts();
    let frames = futures::stream::unfold(Some(body), move |body| async move {
        let mut body = body?;
        match tokio::time::timeout(timeout, body.frame()).await {
            Ok(Some(frame)) => Some((frame, Some(body))),
            Ok(None) => None,
            Err(_) => Some((Err(idle_read_failure()), None)),
        }
    });
    HttpResponse::from_parts(
        parts,
        HttpResponseBody::new(http_body_util::StreamBody::new(frames)),
    )
}

async fn reusable_put_body(request: HttpRequest) -> std::result::Result<HttpRequest, HttpError> {
    if request.method() != http::Method::PUT || request.body().as_bytes().is_some() {
        return Ok(request);
    }
    let Some(length) = request
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return Ok(request);
    };
    if request.body().content_length() != length {
        return Err(HttpError::new(
            HttpErrorKind::Decode,
            MscError::new(ErrorKind::Integrity, "S3 upload payload length mismatch"),
        ));
    }
    // The SDK payload is already buffered and bounded by the upload threshold or
    // part size. A single chunk stays zero-copy; several chunks are coalesced.
    // Bytes lets reqwest replay only its guaranteed-unprocessed protocol failures.
    let (parts, body) = request.into_parts();
    let bytes = body.collect().await?.to_bytes();
    Ok(HttpRequest::from_parts(parts, bytes.into()))
}

async fn discard_error_body(response: HttpResponse) {
    const MAX_BYTES: usize = 64 * 1024;
    let mut input = response.into_body().bytes_stream();
    let mut consumed = 0usize;
    while let Some(Ok(chunk)) = input.next().await {
        consumed = consumed.saturating_add(chunk.len());
        if consumed >= MAX_BYTES {
            break;
        }
    }
}

// Validate before the SDK converts the inclusive end to an exclusive bound.
// Its parser otherwise adds one unchecked, and a full GET must not accept 206.
fn validate_range_response(
    requested: Option<&str>,
    response: &HttpResponse,
) -> std::result::Result<(), HttpError> {
    if requested.is_none() && response.status().as_u16() != 206 {
        return Ok(());
    }
    let actual = response
        .headers()
        .get("content-range")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("bytes "))
        .and_then(|value| value.split_once('/'))
        .and_then(|(bounds, total)| Some((parse_bounds(bounds)?, total.parse::<u64>().ok()?)));
    let expected = requested.and_then(|value| {
        actual.and_then(|(_, total)| crate::provider::requested_bounds(value, total))
    });
    if !matches!((expected, actual), (Some(expected), Some((actual, total)))
        if response.status().as_u16() == 206 && expected == actual
            && actual.0 <= actual.1 && actual.1 < total)
    {
        let mut error = MscError::new(
            ErrorKind::Integrity,
            "S3 range response does not match requested bounds",
        );
        error.http_status = Some(response.status().as_u16());
        return Err(HttpError::new(HttpErrorKind::Decode, error));
    }
    Ok(())
}
fn parse_bounds(bounds: &str) -> Option<(u64, u64)> {
    let (start, end) = bounds.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

#[derive(Debug, thiserror::Error)]
#[error("S3 returned HTTP {status}")]
struct ResponseFailure {
    status: u16,
    request_id: Option<String>,
    retry_after: Option<Duration>,
}

pub(super) struct CredentialBridge(pub(super) Arc<dyn crate::credentials::CredentialsProvider>);
impl std::fmt::Debug for CredentialBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CredentialBridge([REDACTED])")
    }
}

#[async_trait]
impl object_store::CredentialProvider for CredentialBridge {
    type Credential = object_store::aws::AwsCredential;
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let crate::credentials::Credentials::Aws(credentials) = self
            .0
            .credentials()
            .await
            .map_err(|_| object_store::Error::Unauthenticated {
                path: String::new(),
                source: "credential refresh failed".into(),
            })?;
        crate::credentials::validate_credentials(&credentials).map_err(|_| {
            object_store::Error::Unauthenticated {
                path: String::new(),
                source: "credentials are invalid or expired".into(),
            }
        })?;
        Ok(Arc::new(object_store::aws::AwsCredential {
            key_id: credentials.access_key_id.expose_secret().to_string(),
            secret_key: credentials.secret_access_key.expose_secret().to_string(),
            token: credentials
                .session_token
                .map(|value| value.expose_secret().to_string()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn prepared_uploads_reuse_bytes_without_changing_signed_requests() {
        let bytes = bytes::Bytes::from(vec![0x5a; 1024]);
        let pointer = bytes.as_ptr();
        let mut request = http::Request::builder()
            .method(http::Method::PUT)
            .uri("https://example.invalid/bucket/object?partNumber=1&uploadId=example")
            .header(http::header::CONTENT_LENGTH, bytes.len())
            .header("x-amz-content-sha256", "example-checksum")
            .header(http::header::AUTHORIZATION, "example-signature")
            .body(object_store::PutPayload::from(bytes).into())
            .unwrap();
        request.extensions_mut().insert(17usize);
        let headers = request.headers().clone();
        let uri = request.uri().clone();
        let request = reusable_put_body(request).await.unwrap();
        let bytes = request.body().as_bytes().unwrap();
        assert_eq!(bytes.as_ptr(), pointer);
        assert_eq!(bytes.as_ref(), &[0x5a; 1024]);
        assert_eq!(request.method(), http::Method::PUT);
        assert_eq!(request.uri(), &uri);
        assert_eq!(request.headers(), &headers);
        assert_eq!(request.extensions().get::<usize>(), Some(&17));

        let payload = [
            bytes::Bytes::from_static(b"part"),
            bytes::Bytes::from_static(b"two"),
        ]
        .into_iter()
        .collect::<object_store::PutPayload>();
        let request = http::Request::builder()
            .method(http::Method::PUT)
            .header(http::header::CONTENT_LENGTH, 7)
            .body(payload.clone().into())
            .unwrap();
        let request = reusable_put_body(request).await.unwrap();
        assert_eq!(request.body().as_bytes().unwrap().as_ref(), b"parttwo");

        let request = http::Request::builder()
            .method(http::Method::POST)
            .header(http::header::CONTENT_LENGTH, 7)
            .body(payload.clone().into())
            .unwrap();
        assert!(
            reusable_put_body(request)
                .await
                .unwrap()
                .body()
                .as_bytes()
                .is_none()
        );
        let request = http::Request::builder()
            .method(http::Method::PUT)
            .header(http::header::CONTENT_LENGTH, 8)
            .body(payload.into())
            .unwrap();
        let error = reusable_put_body(request).await.unwrap_err();
        assert!(matches!(error.kind(), HttpErrorKind::Decode));
    }

    #[test]
    fn low_speed_hedges_only_replaceable_multipart_parts() {
        assert!(LowSpeed::new(0, Duration::from_secs(1)).is_none());
        let low = LowSpeed::new(1024, Duration::from_secs(1)).unwrap();
        assert!(low.slow(1023, Duration::from_secs(1)));
        assert!(!low.slow(1024, Duration::from_secs(1)));
        assert!(low.slow(2047, Duration::from_secs(2)));
        let request = |method: http::Method, uri: &str, copy: bool| {
            let mut request = http::Request::builder()
                .method(method)
                .uri(uri)
                .header(http::header::AUTHORIZATION, "example-signature")
                .body(bytes::Bytes::from(vec![0; 2048]).into())
                .unwrap();
            if copy {
                request
                    .headers_mut()
                    .insert("x-amz-copy-source", "bucket/source".parse().unwrap());
            }
            request
        };
        let part = "https://example.invalid/bucket/key?partNumber=2&uploadId=upload";
        let hedged = request(http::Method::PUT, part, false);
        assert_eq!(
            part_hedge_delay(&hedged, Some(low)),
            Some(Duration::from_secs(3)),
            "the window plus 2048 bytes at 1024 bytes per second"
        );
        let duplicate = duplicate_request(&hedged).unwrap();
        assert_eq!(duplicate.method(), hedged.method());
        assert_eq!(duplicate.uri(), hedged.uri());
        assert_eq!(duplicate.headers(), hedged.headers());
        assert_eq!(duplicate.body().as_bytes(), hedged.body().as_bytes());
        for (method, uri, copy) in [
            (
                http::Method::PUT,
                "https://example.invalid/bucket/key",
                false,
            ),
            (http::Method::PUT, part, true),
            (http::Method::POST, part, false),
        ] {
            assert_eq!(
                part_hedge_delay(&request(method, uri, copy), Some(low)),
                None
            );
        }
        assert_eq!(part_hedge_delay(&hedged, None), None);
    }

    #[test]
    fn typed_statuses_preserve_context_without_secret_bodies() {
        for (status, kind, retryable) in [
            (304, ErrorKind::NotModified, false),
            (401, ErrorKind::Unauthenticated, false),
            (403, ErrorKind::PermissionDenied, false),
            (404, ErrorKind::NotFound, false),
            (408, ErrorKind::Timeout, true),
            (409, ErrorKind::PreconditionFailed, false),
            (412, ErrorKind::PreconditionFailed, false),
            (429, ErrorKind::RateLimited, true),
            (500, ErrorKind::Unavailable, true),
            (503, ErrorKind::Unavailable, true),
        ] {
            let error = object_store::Error::Generic {
                store: "S3",
                source: Box::new(HttpError::new(
                    HttpErrorKind::Unknown,
                    ResponseFailure {
                        status,
                        request_id: Some("request-123".into()),
                        retry_after: Some(Duration::from_secs(2)),
                    },
                )),
            };
            let error = translate(error, "get", "key");
            assert_eq!(error.kind, kind);
            assert_eq!(error.retryable, retryable);
            assert_eq!(error.http_status, Some(status));
            assert_eq!(error.request_id.as_deref(), Some("request-123"));
            assert_eq!(error.retry_after, Some(Duration::from_secs(2)));
        }
        let error = translate(
            object_store::Error::Generic {
                store: "S3",
                source: "secret-access-key and session-token".into(),
            },
            "get",
            "key",
        );
        assert!(!format!("{error:?} {error}").contains("secret-access-key"));
        for (http, kind) in [
            (HttpErrorKind::Timeout, ErrorKind::Timeout),
            (HttpErrorKind::Connect, ErrorKind::Unavailable),
            (HttpErrorKind::Decode, ErrorKind::Integrity),
        ] {
            let error = translate(
                object_store::Error::Generic {
                    store: "S3",
                    source: Box::new(HttpError::new(
                        http,
                        std::io::Error::other("credential-secret"),
                    )),
                },
                "get",
                "key",
            );
            assert_eq!(error.kind, kind);
            assert!(!format!("{error:?}").contains("credential-secret"));
        }
    }
}
