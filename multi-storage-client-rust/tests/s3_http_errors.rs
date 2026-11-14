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

//! Controlled HTTP failures exercised through the actual S3 SDK transport.
#![cfg(feature = "s3")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use multi_storage_client::{
    ByteRange, CopyOptions, CopyScope, DeleteOptions, ErrorKind, GetConditions, InfoOptions,
    MscError, ObjectPath, ReadOptions, RetryPolicy, StorageClient, StorageProvider, WriteOptions,
    credentials::{AwsCredentials, SecretString},
    provider::s3::{S3Options, S3Provider},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

const ACCESS: &str = "test-access-do-not-log";
const SECRET: &str = "test-secret-do-not-log";
const TOKEN: &str = "test-session-do-not-log";

#[derive(Clone, Copy)]
enum Behavior {
    SharedConnection,
    Success,
    Failure(u16),
    RecoverAfterTwoFailures,
    FailureThenPrecondition,
    TruncatedGet,
    OversizedRange,
    MultipartPartAndAbortFailure,
    MultipartPartDisconnected { failures: usize },
    ThresholdBoundaryPartDisconnected,
    MultipartPartProgress,
    MultipartPartStalled,
    MultipartPartThrottled,
    MultipartPartForbidden,
    MultipartPartStatus { status: u16, failures: usize },
    MultipartCompletionTimeout,
    EarlyPreconditionWithTruncatedBody,
    DelayedPutResponse,
    StalledGetBody,
    SlowSteadyGetBody,
    ThrottledGets { slow: usize },
    Timeout,
}

struct FailureServer {
    endpoint: String,
    requests: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl FailureServer {
    async fn start(behavior: Behavior, retry_after: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&requests);
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted_connections = Arc::clone(&connections);
        let gets = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        accepted_connections.fetch_add(1, Ordering::SeqCst);
                        let observed = Arc::clone(&observed);
                        connections.spawn(serve(stream, observed, Arc::clone(&gets), behavior, retry_after));
                    }
                    result = connections.join_next(), if !connections.is_empty() => {
                        result.unwrap().unwrap();
                    }
                }
            }
        });
        Self {
            endpoint,
            requests,
            connections,
            task,
        }
    }

    fn provider(&self, timeout_ms: u64) -> S3Provider {
        self.provider_timeouts(timeout_ms, S3Options::default().idle_read_timeout_ms)
    }

    fn provider_timeouts(&self, total_ms: u64, idle_ms: u64) -> S3Provider {
        S3Provider::new(
            S3Options {
                base_path: "test-bucket".into(),
                endpoint_url: Some(self.endpoint.clone()),
                multipart_threshold: 5 * 1024 * 1024,
                connect_timeout_ms: 1000,
                read_timeout_ms: total_ms,
                idle_read_timeout_ms: idle_ms,
                ..Default::default()
            },
            Some(AwsCredentials {
                access_key_id: SecretString::new(ACCESS),
                secret_access_key: SecretString::new(SECRET),
                session_token: Some(SecretString::new(TOKEN)),
                expires_at: None,
            }),
        )
        .unwrap()
    }

    fn low_speed_provider(&self, limit: u64, window_ms: u64) -> S3Provider {
        S3Provider::new(
            S3Options {
                base_path: "test-bucket".into(),
                endpoint_url: Some(self.endpoint.clone()),
                connect_timeout_ms: 1000,
                read_timeout_ms: 5000,
                idle_read_timeout_ms: 2000,
                low_speed_limit: limit,
                low_speed_time_ms: window_ms,
                ..Default::default()
            },
            Some(AwsCredentials {
                access_key_id: SecretString::new(ACCESS),
                secret_access_key: SecretString::new(SECRET),
                session_token: Some(SecretString::new(TOKEN)),
                expires_at: None,
            }),
        )
        .unwrap()
    }

    fn count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    async fn multipart_provider(
        &self,
        total_ms: u64,
        idle_ms: u64,
        retry: RetryPolicy,
    ) -> Arc<dyn StorageProvider> {
        self.multipart_provider_with_threshold(total_ms, idle_ms, retry, 5 * 1024 * 1024)
            .await
    }

    async fn multipart_provider_with_threshold(
        &self,
        total_ms: u64,
        idle_ms: u64,
        retry: RetryPolicy,
        threshold: u64,
    ) -> Arc<dyn StorageProvider> {
        self.multipart_provider_with_options(
            retry,
            serde_json::json!({
                "multipart_threshold": threshold,
                "read_timeout_ms": total_ms,
                "idle_read_timeout_ms": idle_ms
            }),
        )
        .await
    }

    async fn multipart_provider_with_options(
        &self,
        retry: RetryPolicy,
        extra: serde_json::Value,
    ) -> Arc<dyn StorageProvider> {
        let mut options = serde_json::json!({
            "base_path": "test-bucket", "endpoint_url": self.endpoint,
            "multipart_threshold": 5 * 1024 * 1024, "connect_timeout_ms": 1000
        });
        options
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let profile = serde_json::from_value(serde_json::json!({
            "storage_provider": {"type": "s3", "options": options},
            "credentials_provider": {"type": "S3Credentials", "options": {
                "access_key": ACCESS, "secret_key": SECRET, "session_token": TOKEN
            }},
            "retry": retry
        }))
        .unwrap();
        multi_storage_client::provider::ProviderRegistry::new()
            .build(&profile)
            .await
            .unwrap()
    }
}

impl Drop for FailureServer {
    fn drop(&mut self) {
        // Abort is synchronous; dropping the server task also aborts its JoinSet.
        self.task.abort();
    }
}

async fn serve(
    mut socket: TcpStream,
    requests: Arc<AtomicUsize>,
    gets: Arc<AtomicUsize>,
    behavior: Behavior,
    retry_after: u64,
) {
    if matches!(behavior, Behavior::SharedConnection) {
        serve_shared_connection(&mut socket, &requests).await;
        return;
    }
    let headers = read_headers(&mut socket).await;
    if headers.is_empty() {
        return;
    }
    let attempt = requests.fetch_add(1, Ordering::SeqCst) + 1;
    if serve_mutation_response(&mut socket, &headers, behavior).await {
        return;
    }
    let get = if headers.starts_with(b"GET ") {
        gets.fetch_add(1, Ordering::SeqCst) + 1
    } else {
        0
    };
    if serve_slow_get(&mut socket, &headers, get, behavior).await {
        return;
    }
    if matches!(behavior, Behavior::MultipartPartAndAbortFailure) {
        serve_multipart_failure(&mut socket, &headers, attempt).await;
        return;
    }
    if matches!(
        behavior,
        Behavior::MultipartPartDisconnected { .. }
            | Behavior::ThresholdBoundaryPartDisconnected
            | Behavior::MultipartPartProgress
            | Behavior::MultipartPartStalled
            | Behavior::MultipartPartThrottled
            | Behavior::MultipartPartForbidden
            | Behavior::MultipartPartStatus { .. }
            | Behavior::MultipartCompletionTimeout
    ) {
        serve_multipart_retry(&mut socket, &headers, attempt, behavior).await;
        return;
    }
    if matches!(behavior, Behavior::Timeout) {
        tokio::time::sleep(Duration::from_secs(5)).await;
        return;
    }
    let status = match behavior {
        Behavior::Failure(status) => status,
        Behavior::FailureThenPrecondition => {
            if attempt == 1 {
                503
            } else {
                412
            }
        }
        Behavior::RecoverAfterTwoFailures if attempt <= 2 => 503,
        Behavior::Success | Behavior::RecoverAfterTwoFailures | Behavior::TruncatedGet => 200,
        Behavior::OversizedRange => {
            if headers.starts_with(b"HEAD ") {
                200
            } else {
                206
            }
        }
        _ => unreachable!("special fixture responses must be handled before this match"),
    };
    let reason = match status {
        200 => "OK",
        403 => "Forbidden",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => "Failure",
    };
    let body = format!("<Error><Message>{ACCESS} {SECRET} {TOKEN}</Message></Error>");
    let response = if matches!(behavior, Behavior::OversizedRange) {
        if headers.starts_with(b"HEAD ") {
            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 8\r\nETag: \"test-etag\"\r\nLast-Modified: Tue, 06 Oct 2026 12:00:00 GMT\r\n\r\n".to_owned()
        } else {
            // Valid requested Content-Range, but the body never ends. The adapter
            // must reject the first excess byte instead of waiting for EOF.
            socket.write_all(b"HTTP/1.1 206 Partial Content\r\nConnection: close\r\nContent-Length: 1048576\r\nContent-Range: bytes 0-0/8\r\nETag: \"test-etag\"\r\nLast-Modified: Tue, 06 Oct 2026 12:00:00 GMT\r\n\r\nxx").await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
            return;
        }
    } else if matches!(behavior, Behavior::TruncatedGet) {
        let body = if headers.starts_with(b"HEAD ") {
            ""
        } else {
            "part"
        };
        format!(
            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 8\r\nETag: \"test-etag\"\r\nLast-Modified: Tue, 06 Oct 2026 12:00:00 GMT\r\n\r\n{body}"
        )
    } else if status == 200 {
        successful_response(&headers)
    } else {
        format!(
            "HTTP/1.1 {status} {reason}\r\nConnection: close\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nx-amz-request-id: request-{attempt}\r\nRetry-After: {retry_after}\r\n\r\n{body}",
            body.len()
        )
    };
    // A timed-out/cancelled client may close the socket before our response.
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.shutdown().await;
}

fn successful_response(headers: &[u8]) -> String {
    let length = if headers.starts_with(b"GET ") || headers.starts_with(b"HEAD ") {
        4
    } else {
        0
    };
    let data = if headers.starts_with(b"GET ") {
        "data"
    } else {
        ""
    };
    format!(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {length}\r\nETag: \"test-etag\"\r\nLast-Modified: Tue, 06 Oct 2026 12:00:00 GMT\r\n\r\n{data}"
    )
}

async fn serve_shared_connection(socket: &mut TcpStream, requests: &AtomicUsize) {
    loop {
        let input = read_headers(socket).await;
        if input.is_empty() {
            return;
        }
        requests.fetch_add(1, Ordering::SeqCst);
        let header_end = input
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap()
            + 4;
        let headers = std::str::from_utf8(&input[..header_end]).unwrap();
        let length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut remaining = length - (input.len() - header_end);
        while remaining != 0 {
            let mut buffer = [0; 1024];
            let limit = remaining.min(buffer.len());
            let count = socket.read(&mut buffer[..limit]).await.unwrap();
            assert_ne!(count, 0);
            remaining -= count;
        }
        let response = if input.starts_with(b"PUT ") {
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"test-etag\"\r\n\r\n"
        } else if input.starts_with(b"DELETE ") {
            "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"
        } else if input.starts_with(b"HEAD ") {
            "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"test-etag\"\r\nLast-Modified: Tue, 06 Oct 2026 12:00:00 GMT\r\n\r\n"
        } else {
            assert!(input.starts_with(b"GET "));
            "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"test-etag\"\r\nLast-Modified: Tue, 06 Oct 2026 12:00:00 GMT\r\n\r\ndata"
        };
        socket.write_all(response.as_bytes()).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn sdk_reads_uploads_and_native_deletes_share_one_connection() {
    use futures::{TryStreamExt, stream};
    let server = FailureServer::start(Behavior::SharedConnection, 0).await;
    let provider = server.provider_timeouts(2000, 500);
    let key = ObjectPath::new("shared.bin").unwrap();
    provider
        .put(
            &key,
            Box::pin(stream::once(async {
                Ok(bytes::Bytes::from_static(b"data"))
            })),
            Some(4),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(provider.head(&key).await.unwrap().content_length, 4);
    let bytes = provider
        .get(&key, &GetConditions::default())
        .await
        .unwrap()
        .try_fold(Vec::new(), |mut output, chunk| async move {
            output.extend_from_slice(&chunk);
            Ok(output)
        })
        .await
        .unwrap();
    assert_eq!(bytes, b"data");
    provider
        .delete(&key, &DeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(server.count(), 4);
    assert_eq!(server.connections.load(Ordering::SeqCst), 1);
}

async fn read_headers(socket: &mut TcpStream) -> Vec<u8> {
    let mut headers = Vec::new();
    let mut buffer = [0; 1024];
    while !headers.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
        let length = socket.read(&mut buffer).await.unwrap();
        if length == 0 {
            return Vec::new();
        }
        headers.extend_from_slice(&buffer[..length]);
        assert!(headers.len() <= 16 * 1024, "bounded HTTP request headers");
    }
    headers
}

async fn serve_mutation_response(
    socket: &mut TcpStream,
    headers: &[u8],
    behavior: Behavior,
) -> bool {
    if matches!(behavior, Behavior::DelayedPutResponse) {
        assert!(headers.starts_with(b"PUT "));
        tokio::time::sleep(Duration::from_millis(200)).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\nETag: \"test-etag\"\r\n\r\n").await.unwrap();
        socket.shutdown().await.unwrap();
        return true;
    }
    if matches!(behavior, Behavior::EarlyPreconditionWithTruncatedBody) {
        assert!(headers.starts_with(b"PUT "));
        assert!(String::from_utf8_lossy(headers).contains("if-none-match: *"));
        socket.write_all(b"HTTP/1.1 412 Precondition Failed\r\nConnection: close\r\nContent-Length: 128\r\nx-amz-request-id: early-precondition\r\n\r\n<Error>").await.unwrap();
        // Close the response cleanly, then drain any already in-flight upload
        // bytes so a TCP reset cannot discard the known HTTP failure status.
        socket.shutdown().await.unwrap();
        let mut buffer = [0; 1024];
        let _ = tokio::time::timeout(Duration::from_secs(2), async {
            while socket.read(&mut buffer).await.unwrap_or(0) != 0 {}
        })
        .await;
        return true;
    }
    false
}

async fn serve_slow_get(
    socket: &mut TcpStream,
    headers: &[u8],
    attempt: usize,
    behavior: Behavior,
) -> bool {
    match behavior {
        Behavior::StalledGetBody | Behavior::SlowSteadyGetBody => {
            serve_timed_get_body(socket, headers, behavior).await;
        }
        // GET attempts are counted separately from optional metadata requests.
        Behavior::ThrottledGets { slow } => {
            serve_throttled_gets(socket, headers, attempt, slow).await;
        }
        _ => return false,
    }
    true
}

async fn serve_timed_get_body(socket: &mut TcpStream, headers: &[u8], behavior: Behavior) {
    socket.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\nETag: \"test-etag\"\r\nLast-Modified: Tue, 06 Oct 2026 12:00:00 GMT\r\n\r\n").await.unwrap();
    if headers.starts_with(b"GET ") {
        socket.write_all(b"d").await.unwrap();
        if matches!(behavior, Behavior::StalledGetBody) {
            tokio::time::sleep(Duration::from_secs(5)).await;
            return;
        }
        for byte in b"ata" {
            tokio::time::sleep(Duration::from_millis(20)).await;
            socket.write_all(&[*byte]).await.unwrap();
        }
    } else {
        assert!(headers.starts_with(b"HEAD "));
    }
    socket.shutdown().await.unwrap();
}

const THROTTLED_LENGTH: usize = 64 * 1024;

fn throttled_object() -> Vec<u8> {
    (0..THROTTLED_LENGTH)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect()
}

/// The first `slow` GETs trickle their body; every GET after the first must
/// resume the remaining range of the same object version.
async fn serve_throttled_gets(socket: &mut TcpStream, headers: &[u8], get: usize, slow: usize) {
    let object = throttled_object();
    let text = String::from_utf8_lossy(headers).to_ascii_lowercase();
    let metadata = "Connection: close\r\nETag: \"resume-etag\"\r\nLast-Modified: Tue, 06 Oct 2026 12:00:00 GMT\r\n";
    if headers.starts_with(b"HEAD ") {
        let response =
            format!("HTTP/1.1 200 OK\r\n{metadata}Content-Length: {THROTTLED_LENGTH}\r\n\r\n");
        socket.write_all(response.as_bytes()).await.unwrap();
        return;
    }
    assert!(headers.starts_with(b"GET "));
    let range = text.lines().find_map(|line| {
        let (start, end) = line.strip_prefix("range: bytes=")?.split_once('-')?;
        Some((
            start.parse::<usize>().ok()?,
            end.trim().parse::<usize>().ok()?,
        ))
    });
    let (start, end) = range.unwrap_or((0, THROTTLED_LENGTH - 1));
    let status = if range.is_some() {
        format!("206 Partial Content\r\nContent-Range: bytes {start}-{end}/{THROTTLED_LENGTH}")
    } else {
        "200 OK".to_owned()
    };
    let response = format!(
        "HTTP/1.1 {status}\r\n{metadata}Content-Length: {}\r\n\r\n",
        end + 1 - start
    );
    socket.write_all(response.as_bytes()).await.unwrap();
    if get > 1 {
        assert!(
            range.is_some(),
            "a resumed GET must request its remaining range"
        );
        assert!(
            text.contains("if-match: \"resume-etag\""),
            "a resumed GET must be pinned to the first response's ETag"
        );
    }
    if get <= slow {
        for byte in &object[start..=end] {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if socket.write_all(&[*byte]).await.is_err() {
                return; // The client moved the transfer to another connection.
            }
        }
        return;
    }
    socket.write_all(&object[start..=end]).await.unwrap();
    socket.shutdown().await.unwrap();
}

async fn serve_multipart_failure(socket: &mut TcpStream, input: &[u8], attempt: usize) {
    let header_end = input
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .unwrap()
        + 4;
    let headers = std::str::from_utf8(&input[..header_end]).unwrap();
    let request = headers.lines().next().unwrap();
    let mut request = request.split_whitespace();
    let method = request.next().unwrap();
    let target = request.next().unwrap();
    let url = url::Url::parse(&format!("http://localhost{target}")).unwrap();
    let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
    let length = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, value)| value.trim().parse::<usize>().unwrap());
    let mut remaining = length.checked_sub(input.len() - header_end).unwrap();
    let mut buffer = vec![0; 16 * 1024];
    // Read the entire part before rejecting it so the client observes our 503,
    // rather than a connection reset while it is still transmitting the body.
    while remaining > 0 {
        let wanted = remaining.min(buffer.len());
        let received = socket.read(&mut buffer[..wanted]).await.unwrap();
        assert_ne!(received, 0, "multipart request body must be complete");
        remaining -= received;
    }
    let (status, reason, body, request_id) = match method {
        "POST" => {
            assert_eq!(attempt, 1);
            assert!(query.contains_key("uploads"));
            assert_eq!(length, 0);
            (
                200,
                "OK",
                "<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>",
                "multipart-initiation",
            )
        }
        "PUT" => {
            assert_eq!(attempt, 2);
            assert_eq!(
                query.get("uploadId").map(AsRef::as_ref),
                Some("test-upload")
            );
            assert_eq!(query.get("partNumber").map(AsRef::as_ref), Some("1"));
            assert_eq!(length, 8 * 1024 * 1024);
            (
                503,
                "Service Unavailable",
                "<Error><Code>ServiceUnavailable</Code></Error>",
                "multipart-part-failure",
            )
        }
        "DELETE" => {
            assert_eq!(
                query.get("uploadId").map(AsRef::as_ref),
                Some("test-upload")
            );
            assert_eq!(length, 0);
            if attempt == 3 {
                (
                    403,
                    "Forbidden",
                    "<Error><Code>AccessDenied</Code></Error>",
                    "multipart-abort-failure",
                )
            } else {
                assert_eq!(attempt, 4, "shutdown must retry the failed abort once");
                (204, "No Content", "", "multipart-abort-retry")
            }
        }
        _ => panic!("unexpected multipart request method"),
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nConnection: close\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nx-amz-request-id: {request_id}\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await.unwrap();
    socket.shutdown().await.unwrap();
}

async fn serve_multipart_retry(
    socket: &mut TcpStream,
    input: &[u8],
    attempt: usize,
    behavior: Behavior,
) {
    let header_end = input
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .unwrap()
        + 4;
    let headers = std::str::from_utf8(&input[..header_end]).unwrap();
    let mut request = headers.lines().next().unwrap().split_whitespace();
    let method = request.next().unwrap();
    let target = request.next().unwrap();
    let url = url::Url::parse(&format!("http://localhost{target}")).unwrap();
    let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
    let length = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, value)| value.trim().parse::<usize>().unwrap());
    if matches!(behavior, Behavior::MultipartPartThrottled) && method == "PUT" && attempt == 2 {
        // A throttled connection: the first part attempt never drains its body.
        tokio::time::sleep(Duration::from_secs(5)).await;
        return;
    }
    let Some(body) = read_request_body(
        socket,
        &input[header_end..],
        length,
        matches!(behavior, Behavior::MultipartPartProgress),
    )
    .await
    else {
        return;
    };
    if matches!(behavior, Behavior::ThresholdBoundaryPartDisconnected)
        && method == "PUT"
        && !query.contains_key("uploadId")
    {
        // Objects at the threshold use one PUT, as in MSC Python.
        assert_eq!(body, [42; 2]);
        socket.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\nETag: \"single-put\"\r\n\r\n").await.unwrap();
        socket.shutdown().await.unwrap();
        return;
    }
    let (status, response_body) = match method {
        "POST" if query.contains_key("uploads") => {
            assert!(body.is_empty());
            (
                200,
                "<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>",
            )
        }
        "PUT" => {
            assert_eq!(
                query.get("uploadId").map(AsRef::as_ref),
                Some("test-upload")
            );
            assert_eq!(query.get("partNumber").map(AsRef::as_ref), Some("1"));
            let Some(response) = multipart_part_response(socket, &body, attempt, behavior).await
            else {
                return;
            };
            response
        }
        "POST" => {
            assert_eq!(
                query.get("uploadId").map(AsRef::as_ref),
                Some("test-upload")
            );
            let completed = std::str::from_utf8(&body).unwrap();
            assert!(completed.contains("<PartNumber>1</PartNumber>"));
            assert!(completed.contains("part-etag"));
            assert!(!completed.contains("<PartNumber>2</PartNumber>"));
            if matches!(behavior, Behavior::MultipartCompletionTimeout) {
                tokio::time::sleep(Duration::from_secs(5)).await;
                return;
            }
            (
                200,
                "<CompleteMultipartUploadResult><ETag>&quot;complete-etag&quot;</ETag></CompleteMultipartUploadResult>",
            )
        }
        "DELETE" => {
            assert_eq!(
                query.get("uploadId").map(AsRef::as_ref),
                Some("test-upload")
            );
            assert!(body.is_empty());
            (204, "")
        }
        _ => panic!("unexpected multipart request method"),
    };
    let response = format!(
        "HTTP/1.1 {status} Response\r\nConnection: close\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nETag: \"part-etag\"\r\nx-amz-request-id: multipart-{attempt}\r\n\r\n{response_body}",
        response_body.len()
    );
    socket.write_all(response.as_bytes()).await.unwrap();
    socket.shutdown().await.unwrap();
}

async fn multipart_part_response(
    socket: &mut TcpStream,
    body: &[u8],
    attempt: usize,
    behavior: Behavior,
) -> Option<(u16, &'static str)> {
    let expected_length = match behavior {
        Behavior::ThresholdBoundaryPartDisconnected => 3,
        _ => 8 * 1024 * 1024,
    };
    assert!(
        body == vec![42; expected_length],
        "every part attempt must resend the complete, identical payload"
    );
    if matches!(behavior, Behavior::MultipartPartStalled) {
        tokio::time::sleep(Duration::from_secs(5)).await;
        return None;
    }
    let disconnected = match behavior {
        Behavior::MultipartPartDisconnected { failures } => attempt - 1 <= failures,
        Behavior::ThresholdBoundaryPartDisconnected => attempt == 2,
        _ => false,
    };
    if disconnected {
        socket.shutdown().await.unwrap();
        return None;
    }
    if let Behavior::MultipartPartStatus { status, failures } = behavior
        && attempt - 1 <= failures
    {
        return Some((status, "<Error><Code>SlowDown</Code></Error>"));
    }
    Some(if matches!(behavior, Behavior::MultipartPartForbidden) {
        (403, "<Error><Code>AccessDenied</Code></Error>")
    } else {
        (200, "")
    })
}

async fn read_request_body(
    socket: &mut TcpStream,
    input: &[u8],
    length: usize,
    progress: bool,
) -> Option<Vec<u8>> {
    let mut body = Vec::with_capacity(length);
    body.extend_from_slice(input);
    let mut buffer = vec![0; 64 * 1024];
    while body.len() < length {
        let wanted = (length - body.len()).min(buffer.len());
        let received = socket.read(&mut buffer[..wanted]).await.unwrap();
        if received == 0 && progress {
            return None; // A premature idle timeout is the regression's red case.
        }
        assert_ne!(received, 0, "multipart request body must be complete");
        body.extend_from_slice(&buffer[..received]);
        if progress {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    assert_eq!(body.len(), length);
    Some(body)
}

fn client(provider: S3Provider) -> StorageClient {
    StorageClient::from_provider(Arc::new(provider))
        .with_retry_policy(RetryPolicy {
            max_attempts: 3,
            initial_delay_ms: 0,
            max_delay_ms: 0,
            multiplier: 2.0,
            jitter: false,
        })
        .unwrap()
}

fn assert_redacted(error: &MscError) {
    let text = format!("{error} {error:?}");
    for secret in [ACCESS, SECRET, TOKEN] {
        assert!(
            !text.contains(secret),
            "HTTP errors must not expose credential material or response bodies"
        );
    }
}

struct UncheckedCredentials(AwsCredentials);
#[async_trait::async_trait]
impl multi_storage_client::credentials::CredentialsProvider for UncheckedCredentials {
    async fn credentials(
        &self,
    ) -> multi_storage_client::error::Result<multi_storage_client::credentials::Credentials> {
        Ok(multi_storage_client::credentials::Credentials::Aws(
            self.0.clone(),
        ))
    }
}

// The environment is set only on a fresh process, so parallel tests cannot
// inherit another test's endpoints or S3 Express setting.
async fn endpoint_probe(case: &str, configured: &str, inherited: &str) {
    let executable = std::env::current_exe().unwrap();
    let case = case.to_owned();
    let configured = configured.to_owned();
    let inherited = inherited.to_owned();
    let output = tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new(executable);
        command
            .args([
                "--exact",
                "endpoint_environment_child_probe",
                "--ignored",
                "--nocapture",
            ])
            .env("MSC_S3_PROBE_CASE", &case)
            .env("MSC_S3_PROBE_ENDPOINT", configured)
            .env("AWS_ENDPOINT_URL_S3", inherited)
            .env_remove("AWS_ENDPOINT_URL")
            .env_remove("AWS_S3_EXPRESS");
        if case == "express" {
            command.env("AWS_S3_EXPRESS", "true");
        }
        if case.starts_with("invalid_env") || case == "safe_explicit" {
            command.env(
                "AWS_ACCESS_KEY_ID",
                if case == "invalid_env_token" {
                    ACCESS
                } else {
                    "access\nsecret"
                },
            );
            command.env("AWS_SECRET_ACCESS_KEY", SECRET);
            command.env(
                "AWS_SESSION_TOKEN",
                if case == "invalid_env_token" {
                    "token\nsecret"
                } else {
                    TOKEN
                },
            );
        }
        command.output().unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "child probe failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
#[ignore = "child probe for isolated environment tests; run just run-integration-tests"]
async fn endpoint_environment_child_probe() {
    let Ok(case) = std::env::var("MSC_S3_PROBE_CASE") else {
        return;
    };
    let endpoint = std::env::var("MSC_S3_PROBE_ENDPOINT").unwrap();
    let provider = S3Provider::new(
        S3Options {
            base_path: "test-bucket".into(),
            endpoint_url: (case != "inherited").then_some(endpoint.clone()),
            ..Default::default()
        },
        (!case.starts_with("invalid_env")).then(|| AwsCredentials {
            access_key_id: SecretString::new(ACCESS),
            secret_access_key: SecretString::new(SECRET),
            session_token: None,
            expires_at: None,
        }),
    );
    if case == "express" {
        let error = provider.unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidConfig);
        assert!(error.message.contains("S3 Express"));
        return;
    }
    if case.starts_with("invalid_env") {
        let error = provider.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unauthenticated);
        assert!(!format!("{error:?} {error}").contains("secret"));
        return;
    }
    let provider = provider.unwrap();
    let key = ObjectPath::new("object").unwrap();
    if case == "explicit" {
        provider
            .put(
                &key,
                Box::pin(futures::stream::once(async {
                    Ok(bytes::Bytes::from_static(b"data"))
                })),
                Some(4),
                &WriteOptions::default(),
            )
            .await
            .unwrap();
    } else {
        let metadata = provider.head(&key).await.unwrap();
        let scope = metadata.source_version.unwrap().scope;
        let expected = if case == "inherited" {
            std::env::var("AWS_ENDPOINT_URL_S3").unwrap()
        } else {
            endpoint
        };
        assert!(
            scope.contains(&expected),
            "source version must identify the endpoint actually used"
        );
    }
    provider.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn explicit_endpoint_wins_and_inherited_endpoint_scopes_versions() {
    let configured = FailureServer::start(Behavior::Success, 0).await;
    let inherited = FailureServer::start(Behavior::Success, 0).await;
    endpoint_probe("explicit", &configured.endpoint, &inherited.endpoint).await;
    assert_eq!(
        configured.count(),
        1,
        "configured endpoint must receive the PUT"
    );
    assert_eq!(
        inherited.count(),
        0,
        "inherited endpoint must not receive the PUT"
    );
    endpoint_probe("inherited", &configured.endpoint, &inherited.endpoint).await;
    assert_eq!(inherited.count(), 1);
    endpoint_probe("express", &configured.endpoint, &inherited.endpoint).await;
    assert_eq!(configured.count(), 1, "S3 Express must fail before I/O");
    assert_eq!(inherited.count(), 1, "S3 Express must fail before I/O");
    endpoint_probe("invalid_env_key", &configured.endpoint, &inherited.endpoint).await;
    endpoint_probe(
        "invalid_env_token",
        &configured.endpoint,
        &inherited.endpoint,
    )
    .await;
    assert_eq!(configured.count(), 1);
    assert_eq!(inherited.count(), 1);
    endpoint_probe("safe_explicit", &configured.endpoint, &inherited.endpoint).await;
    assert_eq!(
        configured.count(),
        2,
        "safe explicit credentials must override unused invalid environment credentials"
    );
    assert_eq!(inherited.count(), 1);
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn native_copy_declines_before_mutating_destination() {
    let server = FailureServer::start(Behavior::Success, 0).await;
    let provider = server.provider(1000);
    assert_eq!(
        provider.capabilities().server_side_copy,
        CopyScope::SameProvider
    );
    assert_eq!(
        provider
            .copy(
                &ObjectPath::new("source").unwrap(),
                &ObjectPath::new("destination").unwrap(),
                &CopyOptions {
                    destination: WriteOptions {
                        if_none_match: Some("*".into()),
                        ..Default::default()
                    },
                    ..Default::default()
                }
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        server.count(),
        0,
        "copy must not issue an unverifiable mutation"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn malformed_request_headers_return_errors_before_network_io() {
    let server = FailureServer::start(Behavior::Success, 0).await;
    let provider = server.provider(1000);
    for options in [
        WriteOptions {
            content_type: Some("text/plain\nsecret".into()),
            ..Default::default()
        },
        WriteOptions {
            attributes: [("invalid header".into(), "value".into())].into(),
            ..Default::default()
        },
        WriteOptions {
            attributes: [("owner".into(), "value\nsecret".into())].into(),
            ..Default::default()
        },
        WriteOptions {
            if_match: Some("etag\nsecret".into()),
            ..Default::default()
        },
    ] {
        let error = provider
            .put(
                &ObjectPath::new("object").unwrap(),
                Box::pin(futures::stream::empty()),
                Some(0),
                &options,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidConfig);
        assert!(!format!("{error:?} {error}").contains("secret"));
    }
    let error = provider
        .get(
            &ObjectPath::new("object").unwrap(),
            &GetConditions {
                if_none_match: Some("etag\nsecret".into()),
                ..Default::default()
            },
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::InvalidConfig);
    assert_eq!(server.count(), 0);
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn malformed_explicit_and_refreshed_credentials_fail_before_signing() {
    let server = FailureServer::start(Behavior::Success, 0).await;
    let options = S3Options {
        base_path: "test-bucket".into(),
        endpoint_url: Some(server.endpoint.clone()),
        ..Default::default()
    };
    for token_is_invalid in [false, true] {
        let credentials = AwsCredentials {
            access_key_id: SecretString::new(if token_is_invalid {
                ACCESS.to_owned()
            } else {
                format!("{ACCESS}\n")
            }),
            secret_access_key: SecretString::new(SECRET),
            session_token: Some(SecretString::new(if token_is_invalid {
                format!("{TOKEN}\r\n")
            } else {
                TOKEN.to_owned()
            })),
            expires_at: None,
        };
        let error = S3Provider::new(options.clone(), Some(credentials.clone())).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unauthenticated);
        assert_redacted(&error);
        let provider = S3Provider::new_with_credentials_provider(
            options.clone(),
            Arc::new(UncheckedCredentials(credentials)),
        )
        .unwrap();
        let error = provider
            .head(&ObjectPath::new("object").unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unauthenticated);
        assert_redacted(&error);
    }
    assert_eq!(server.count(), 0);
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn oversized_range_fails_before_waiting_for_the_response_to_finish() {
    let server = FailureServer::start(Behavior::OversizedRange, 0).await;
    let provider = server.provider(2000);
    let error = tokio::time::timeout(
        Duration::from_millis(500),
        provider.get_range(
            &ObjectPath::new("object").unwrap(),
            ByteRange::new(0, 1).unwrap(),
            &GetConditions::default(),
        ),
    )
    .await
    .expect("excess range bytes must fail without waiting for EOF")
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Integrity);
    assert_eq!(server.count(), 1);
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn multipart_preserves_part_failure_when_abort_fails_and_shutdown_retries() {
    let server = FailureServer::start(Behavior::MultipartPartAndAbortFailure, 0).await;
    let provider = server
        .multipart_provider(2000, 200, part_retry_policy(1))
        .await;
    let body = bytes::Bytes::from(vec![42; 8 * 1024 * 1024]);
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        provider.put(
            &ObjectPath::new("object").unwrap(),
            Box::pin(futures::stream::once(async move { Ok(body) })),
            Some(8 * 1024 * 1024),
            &WriteOptions::default(),
        ),
    )
    .await
    .expect("local multipart failure must finish within ten seconds")
    .unwrap_err();
    assert_eq!(
        server.count(),
        3,
        "initiation, failed part, and failed abort"
    );
    provider.shutdown().await.unwrap();
    assert_eq!(server.count(), 4, "shutdown must retry the queued abort");
    provider.shutdown().await.unwrap();
    assert_eq!(
        server.count(),
        4,
        "successful cleanup must leave no queued upload"
    );
    assert_eq!(error.http_status, Some(503));
    assert_eq!(error.kind, ErrorKind::Unavailable);
    assert_eq!(error.operation.as_deref(), Some("put"));
    assert_eq!(error.request_id.as_deref(), Some("multipart-part-failure"));
    assert_redacted(&error);
}

fn part_retry_policy(max_attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        initial_delay_ms: 0,
        max_delay_ms: 0,
        multiplier: 2.0,
        jitter: false,
    }
}

async fn upload_one_part(
    provider: &dyn StorageProvider,
) -> multi_storage_client::Result<multi_storage_client::PutResult> {
    let body = bytes::Bytes::from(vec![42; 8 * 1024 * 1024]);
    tokio::time::timeout(
        Duration::from_secs(10),
        provider.put(
            &ObjectPath::new("object").unwrap(),
            Box::pin(futures::stream::once(async move { Ok(body) })),
            Some(8 * 1024 * 1024),
            &WriteOptions::default(),
        ),
    )
    .await
    .expect("the local multipart operation must finish within ten seconds")
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn throttled_multipart_part_is_hedged_on_another_connection() {
    let server = FailureServer::start(Behavior::MultipartPartThrottled, 0).await;
    let provider = server
        .multipart_provider_with_options(
            part_retry_policy(1),
            serde_json::json!({
                "read_timeout_ms": 8000, "idle_read_timeout_ms": 200,
                "low_speed_limit": 1024 * 1024 * 1024, "low_speed_time_ms": 100
            }),
        )
        .await;
    let started = std::time::Instant::now();
    let result = upload_one_part(provider.as_ref()).await;
    let elapsed = started.elapsed();
    provider.shutdown().await.unwrap();
    assert_eq!(result.unwrap().etag.as_deref(), Some("\"complete-etag\""));
    assert!(
        elapsed < Duration::from_secs(3),
        "the hedged part must finish before the throttled attempt: {elapsed:?}"
    );
    assert_eq!(
        server.count(),
        4,
        "initiation, throttled part, hedged identical part, one completion"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn single_put_is_never_duplicated_by_low_speed_hedging() {
    let server = FailureServer::start(Behavior::DelayedPutResponse, 0).await;
    let provider = server.low_speed_provider(1024 * 1024 * 1024, 20);
    let result = provider
        .put(
            &ObjectPath::new("object").unwrap(),
            Box::pin(futures::stream::once(async {
                Ok(bytes::Bytes::from_static(b"data"))
            })),
            Some(4),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
    provider.shutdown().await.unwrap();
    assert_eq!(result.etag.as_deref(), Some("\"test-etag\""));
    assert_eq!(
        server.count(),
        1,
        "a single PUT must not be published twice"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn throttled_get_resumes_remaining_range_on_another_connection() {
    let server = FailureServer::start(Behavior::ThrottledGets { slow: 1 }, 0).await;
    let client = client(server.low_speed_provider(1024, 100));
    let started = std::time::Instant::now();
    let bytes = client.read("object").await.unwrap();
    let elapsed = started.elapsed();
    client.shutdown().await.unwrap();
    assert_eq!(bytes.as_ref(), throttled_object().as_slice());
    assert!(
        elapsed < Duration::from_secs(3),
        "the remaining range must move off the throttled connection: {elapsed:?}"
    );
    assert_eq!(
        server.count(),
        2,
        "one throttled GET and one resumed range GET"
    );
    assert_eq!(server.connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn repeatedly_throttled_get_keeps_resuming_with_backoff() {
    let server = FailureServer::start(Behavior::ThrottledGets { slow: 4 }, 0).await;
    let client = client(server.low_speed_provider(1024, 100));
    let started = std::time::Instant::now();
    let bytes = client.read("object").await.unwrap();
    let elapsed = started.elapsed();
    client.shutdown().await.unwrap();
    assert_eq!(bytes.as_ref(), throttled_object().as_slice());
    // Resumes follow windows of 1, 1, 2, and 4 windows: about 0.8 seconds.
    assert!(
        elapsed >= Duration::from_millis(700) && elapsed < Duration::from_secs(3),
        "resumes must back off yet continue until a fast connection: {elapsed:?}"
    );
    assert_eq!(
        server.count(),
        5,
        "four throttled GETs and the fast resumed range"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn throttled_range_read_resumes_within_its_requested_range() {
    let server = FailureServer::start(Behavior::ThrottledGets { slow: 1 }, 0).await;
    let provider = server.low_speed_provider(1024, 100);
    let bytes = provider
        .get_range(
            &ObjectPath::new("object").unwrap(),
            ByteRange {
                offset: 1000,
                length: 20_000,
            },
            &GetConditions::default(),
        )
        .await
        .unwrap();
    provider.shutdown().await.unwrap();
    assert_eq!(bytes.as_ref(), &throttled_object()[1000..21_000]);
    assert_eq!(server.count(), 2, "throttled range and resumed range");
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn multipart_part_disconnect_retries_identical_part_and_completes() {
    let server = FailureServer::start(Behavior::MultipartPartDisconnected { failures: 1 }, 0).await;
    let provider = server
        .multipart_provider(2000, 200, part_retry_policy(2))
        .await;
    let result = upload_one_part(provider.as_ref()).await;
    provider.shutdown().await.unwrap();
    assert_eq!(result.unwrap().etag.as_deref(), Some("\"complete-etag\""));
    assert_eq!(
        server.count(),
        4,
        "initiation, two identical part attempts, completion"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn threshold_sized_upload_uses_one_put_and_larger_upload_retries_parts() {
    for (length, requests, etag) in [(2, 1, "\"single-put\""), (3, 4, "\"complete-etag\"")] {
        let server = FailureServer::start(Behavior::ThresholdBoundaryPartDisconnected, 0).await;
        let provider = server
            .multipart_provider_with_threshold(2000, 50, part_retry_policy(2), 2)
            .await;
        let result = provider
            .put(
                &ObjectPath::new("object").unwrap(),
                Box::pin(futures::stream::once(async move {
                    Ok(bytes::Bytes::from(vec![42; length]))
                })),
                Some(length as u64),
                &WriteOptions::default(),
            )
            .await;
        provider.shutdown().await.unwrap();
        assert_eq!(
            server.count(),
            requests,
            "a threshold-sized object uses one PUT; a larger one uses one initiation, \
             two identical part attempts, and one completion"
        );
        assert_eq!(result.unwrap().etag.as_deref(), Some(etag));
    }
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn progressing_multipart_upload_exceeds_read_idle_window_and_completes_once() {
    let server = FailureServer::start(Behavior::MultipartPartProgress, 0).await;
    let provider = server
        .multipart_provider(2000, 50, part_retry_policy(2))
        .await;
    let started = std::time::Instant::now();
    let result = upload_one_part(provider.as_ref()).await;
    provider.shutdown().await.unwrap();
    assert_eq!(result.unwrap().etag.as_deref(), Some("\"complete-etag\""));
    assert!(
        started.elapsed() > Duration::from_millis(200),
        "the part body must keep progressing beyond multiple idle windows"
    );
    assert_eq!(
        server.count(),
        3,
        "initiate, one progressing part, one completion, with no retry"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn multipart_total_request_timeout_exhausts_elapsed_budget_and_aborts() {
    let server = FailureServer::start(Behavior::MultipartPartStalled, 0).await;
    let provider = server
        .multipart_provider(300, 50, part_retry_policy(3))
        .await;
    let started = std::time::Instant::now();
    let error = upload_one_part(provider.as_ref()).await.unwrap_err();
    provider.shutdown().await.unwrap();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert_redacted(&error);
    assert!(
        started.elapsed() >= Duration::from_millis(250),
        "upload failure must follow the total deadline rather than the read idle window"
    );
    assert_eq!(
        server.count(),
        3,
        "initiation, one total-timeout part, abort; the elapsed budget forbids another attempt"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn multipart_part_disconnect_obeys_profile_attempt_limit_and_aborts() {
    let server = FailureServer::start(
        Behavior::MultipartPartDisconnected {
            failures: usize::MAX,
        },
        0,
    )
    .await;
    let provider = server
        .multipart_provider(2000, 200, part_retry_policy(2))
        .await;
    let error = upload_one_part(provider.as_ref()).await.unwrap_err();
    provider.shutdown().await.unwrap();
    assert_eq!(error.kind, ErrorKind::Unavailable);
    assert!(!error.retryable, "the part budget is exhausted");
    assert_redacted(&error);
    assert_eq!(
        server.count(),
        4,
        "profile's two part attempts and one abort, with no completion"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn multipart_parts_use_each_cloned_clients_programmatic_retry_budget() {
    let server = FailureServer::start(
        Behavior::MultipartPartDisconnected {
            failures: usize::MAX,
        },
        0,
    )
    .await;
    let provider = server
        .multipart_provider(2000, 200, part_retry_policy(3))
        .await;
    let first = StorageClient::from_provider(provider)
        .with_retry_policy(part_retry_policy(1))
        .unwrap();
    let second = first
        .clone()
        .with_retry_policy(part_retry_policy(2))
        .unwrap();
    let mut previous = 0;
    for (client, expected_requests) in [(&first, 3), (&second, 4), (&first, 3)] {
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            client.write(
                "object",
                bytes::Bytes::from(vec![42; 8 * 1024 * 1024]),
                WriteOptions::default(),
            ),
        )
        .await
        .expect("local multipart client write must finish promptly")
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unavailable);
        assert_redacted(&error);
        assert_eq!(
            server.count() - previous,
            expected_requests,
            "each client must use its own part budget without replaying whole publication"
        );
        previous = server.count();
    }
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    assert_eq!(
        server.count(),
        10,
        "all failed uploads must already be aborted"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn multipart_part_retry_does_not_start_when_backoff_exceeds_elapsed_budget() {
    let server = FailureServer::start(
        Behavior::MultipartPartDisconnected {
            failures: usize::MAX,
        },
        0,
    )
    .await;
    let provider = server
        .multipart_provider(
            1000,
            200,
            RetryPolicy {
                initial_delay_ms: 1000,
                max_delay_ms: 1000,
                ..part_retry_policy(3)
            },
        )
        .await;
    let error = upload_one_part(provider.as_ref()).await.unwrap_err();
    provider.shutdown().await.unwrap();
    assert_eq!(error.kind, ErrorKind::Unavailable);
    assert_eq!(
        server.count(),
        3,
        "do not schedule a retry outside the part's elapsed-time budget"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn multipart_part_permission_failure_is_not_replayed() {
    let server = FailureServer::start(Behavior::MultipartPartForbidden, 0).await;
    let provider = server
        .multipart_provider(2000, 200, part_retry_policy(3))
        .await;
    let error = upload_one_part(provider.as_ref()).await.unwrap_err();
    provider.shutdown().await.unwrap();
    assert_eq!(error.kind, ErrorKind::PermissionDenied);
    assert_eq!(error.http_status, Some(403));
    assert_redacted(&error);
    assert_eq!(
        server.count(),
        3,
        "initiation, one forbidden part attempt, abort"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn multipart_uncertain_completion_is_not_replayed() {
    let server = FailureServer::start(Behavior::MultipartCompletionTimeout, 0).await;
    let provider = server
        .multipart_provider(2000, 200, part_retry_policy(3))
        .await;
    let error = upload_one_part(provider.as_ref()).await.unwrap_err();
    provider.shutdown().await.unwrap();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert_redacted(&error);
    assert_eq!(
        server.count(),
        4,
        "initiation, part, one completion attempt, abort"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn early_precondition_status_survives_truncated_error_body_without_put_replay() {
    let server = FailureServer::start(Behavior::EarlyPreconditionWithTruncatedBody, 0).await;
    let provider = server.provider_timeouts(2000, 200);
    let body = bytes::Bytes::from(vec![42; 5 * 1024 * 1024]);
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        provider.put(
            &ObjectPath::new("object").unwrap(),
            Box::pin(futures::stream::once(async move { Ok(body) })),
            Some(5 * 1024 * 1024),
            &WriteOptions {
                if_none_match: Some("*".into()),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("an early HTTP error must return without finishing the upload")
    .unwrap_err();
    provider.shutdown().await.unwrap();
    assert_eq!(error.kind, ErrorKind::PreconditionFailed);
    assert_eq!(error.http_status, Some(412));
    assert_eq!(error.request_id.as_deref(), Some("early-precondition"));
    assert_redacted(&error);
    assert_eq!(
        server.count(),
        1,
        "conditional PUT must not be explicitly replayed"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn ordinary_put_waits_for_response_beyond_read_idle_window_without_replay() {
    let server = FailureServer::start(Behavior::DelayedPutResponse, 0).await;
    let provider = server.provider_timeouts(1000, 50);
    let started = std::time::Instant::now();
    let result = provider
        .put(
            &ObjectPath::new("object").unwrap(),
            Box::pin(futures::stream::once(async {
                Ok(bytes::Bytes::from_static(b"data"))
            })),
            Some(4),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
    provider.shutdown().await.unwrap();
    assert_eq!(result.etag.as_deref(), Some("\"test-etag\""));
    assert!(started.elapsed() >= Duration::from_millis(200));
    assert_eq!(
        server.count(),
        1,
        "ordinary PUT uses its total deadline without an explicit replay"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn idle_read_timeout_retries_stalled_body_without_publishing_partial_data() {
    let server = FailureServer::start(Behavior::StalledGetBody, 0).await;
    let client = client(server.provider_timeouts(1000, 50));
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("existing.bin");
    tokio::fs::write(&destination, b"original").await.unwrap();
    let started = std::time::Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        client.download_to("object", &destination, ReadOptions::default()),
    )
    .await
    .expect("three local GET attempts must finish within five seconds")
    .unwrap_err();
    let elapsed = started.elapsed();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert!(error.retryable);
    assert_eq!(error.operation.as_deref(), Some("get"));
    assert_redacted(&error);
    assert_eq!(server.count(), 4, "one HEAD and exactly three GET attempts");
    assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"original");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    client.shutdown().await.unwrap();
    assert!(
        elapsed < Duration::from_millis(750),
        "stalled reads must exhaust idle timeouts rather than wait for total timeouts: {elapsed:?}"
    );
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn idle_read_timeout_resets_when_response_body_keeps_progressing() {
    let server = FailureServer::start(Behavior::SlowSteadyGetBody, 0).await;
    let client = client(server.provider_timeouts(1000, 50));
    let started = std::time::Instant::now();
    let bytes = client.read("object").await.unwrap();
    assert_eq!(bytes.as_ref(), b"data");
    assert!(
        started.elapsed() > Duration::from_millis(50),
        "the complete transfer must exceed one idle timeout window"
    );
    assert_eq!(server.count(), 1, "steady progress must use one GET");
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn total_request_timeout_still_bounds_progressing_response_bodies() {
    use futures::TryStreamExt;
    let server = FailureServer::start(Behavior::SlowSteadyGetBody, 0).await;
    let provider = server.provider_timeouts(45, 50);
    let error = provider
        .get(
            &ObjectPath::new("object").unwrap(),
            &GetConditions::default(),
        )
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert!(error.retryable);
    assert_eq!(server.count(), 1);
    assert_redacted(&error);
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn real_http_transients_preserve_typed_context_without_sdk_retries() {
    for (status, kind) in [(429, ErrorKind::RateLimited), (503, ErrorKind::Unavailable)] {
        let server = FailureServer::start(Behavior::Failure(status), 2).await;
        let provider = server.provider(1000);
        let error = provider
            .get(
                &ObjectPath::new("object").unwrap(),
                &GetConditions::default(),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind, kind);
        assert!(error.retryable);
        assert_eq!(error.http_status, Some(status));
        assert_eq!(error.request_id.as_deref(), Some("request-1"));
        assert_eq!(error.retry_after, Some(Duration::from_secs(2)));
        assert_eq!(error.operation.as_deref(), Some("get"));
        assert_eq!(error.provider.as_deref(), Some("s3"));
        assert_eq!(error.key.as_deref(), Some("object"));
        assert_redacted(&error);
        assert_eq!(
            server.count(),
            1,
            "one provider call must perform exactly one SDK request"
        );
        provider.shutdown().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn real_http_client_owns_the_three_attempt_budget_and_stops_on_success() {
    for (status, kind, attempts) in [
        (429, ErrorKind::RateLimited, 3),
        (503, ErrorKind::Unavailable, 3),
        (403, ErrorKind::PermissionDenied, 1),
    ] {
        let server = FailureServer::start(Behavior::Failure(status), 0).await;
        let client = client(server.provider(1000));
        let error = client
            .info("object", InfoOptions::default())
            .await
            .unwrap_err();
        assert_eq!(error.kind, kind);
        assert_eq!(error.http_status, Some(status));
        assert_eq!(
            error.request_id.as_deref(),
            Some(format!("request-{attempts}").as_str())
        );
        assert_redacted(&error);
        assert_eq!(
            server.count(),
            attempts,
            "client retry attempts must not multiply with SDK retry loops"
        );
        client.shutdown().await.unwrap();
    }
    let server = FailureServer::start(Behavior::RecoverAfterTwoFailures, 0).await;
    let client = client(server.provider(1000));
    let metadata = client.info("object", InfoOptions::default()).await.unwrap();
    assert_eq!(metadata.content_length, 4);
    assert_eq!(metadata.etag.as_deref(), Some("\"test-etag\""));
    assert_eq!(server.count(), 3);
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn python_s8k_profile_retries_http_429_with_the_client_attempt_budget() {
    let server = FailureServer::start(Behavior::Failure(429), 0).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("s8k.yaml");
    let document = serde_json::json!({"profiles": {"data": {
        "storage_provider": {"type": "s8k", "options": {
            "base_path": "test-bucket", "endpoint_url": server.endpoint,
            "region_name": "us-east-1"
        }},
        "credentials_provider": {"type": "S3Credentials", "options": {
            "access_key": ACCESS, "secret_key": SECRET, "session_token": TOKEN
        }},
        "retry": {"attempts": 3, "delay": 0.0, "max_delay_ms": 0, "jitter": false}
    }}});
    std::fs::write(&path, serde_yaml::to_string(&document).unwrap()).unwrap();
    let config = multi_storage_client::ConfigLoader::new()
        .with_explicit_paths([path])
        .load()
        .unwrap();
    let client = StorageClient::builder(config, "data")
        .build()
        .await
        .unwrap();
    let error = client
        .info("object", InfoOptions::default())
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::RateLimited);
    assert_eq!(error.http_status, Some(429));
    assert_eq!(server.count(), 3);
    assert_redacted(&error);
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn real_transport_timeouts_are_retryable_and_exhaust_the_same_budget() {
    let server = FailureServer::start(Behavior::Timeout, 0).await;
    let client = client(server.provider(50));
    let error = tokio::time::timeout(
        Duration::from_secs(3),
        client.info("object", InfoOptions::default()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert!(error.retryable);
    assert_eq!(error.http_status, None);
    assert_eq!(error.request_id, None);
    assert_redacted(&error);
    assert_eq!(server.count(), 3);
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn truncated_http_get_is_retried_without_publishing_partial_data() {
    let server = FailureServer::start(Behavior::TruncatedGet, 0).await;
    let client = client(server.provider(1000));
    let error = client.read("object").await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unavailable);
    assert!(error.retryable);
    assert_redacted(&error);
    assert_eq!(server.count(), 3, "exactly three interrupted GET attempts");

    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("download");
    tokio::fs::write(&destination, b"original destination")
        .await
        .unwrap();
    let error = client
        .download_to("object", &destination, ReadOptions::default())
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unavailable);
    assert_eq!(
        server.count(),
        7,
        "three reads, then one HEAD and three download GET attempts"
    );
    assert_eq!(
        tokio::fs::read(&destination).await.unwrap(),
        b"original destination"
    );
    assert_eq!(
        std::fs::read_dir(directory.path()).unwrap().count(),
        1,
        "failed download must remove its temporary file"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn multipart_parts_retry_transient_http_statuses_with_one_upload_id() {
    for status in [429, 500, 503] {
        let server = FailureServer::start(
            Behavior::MultipartPartStatus {
                status,
                failures: 1,
            },
            0,
        )
        .await;
        let client = StorageClient::from_provider(
            server
                .multipart_provider(2000, 200, part_retry_policy(3))
                .await,
        )
        .with_retry_policy(part_retry_policy(3))
        .unwrap();
        let result = client
            .write(
                "object",
                bytes::Bytes::from(vec![42; 8 * 1024 * 1024]),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.content_length, 8 * 1024 * 1024);
        assert_eq!(
            server.count(),
            4,
            "one initiation, two identical part attempts, one completion"
        );
    }
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn unconditional_mutations_use_the_profile_budget_without_metadata_heads() {
    for operation in ["write", "upload", "delete"] {
        let server = FailureServer::start(Behavior::RecoverAfterTwoFailures, 0).await;
        let client = client(server.provider(1000));
        match operation {
            "write" => {
                let metadata = client
                    .write(
                        "object",
                        bytes::Bytes::from_static(b"data"),
                        WriteOptions {
                            attributes: std::collections::BTreeMap::from([(
                                "OWNER".into(),
                                "test".into(),
                            )]),
                            ..WriteOptions::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(metadata.attributes["owner"], "test");
            }
            "upload" => {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("source");
                std::fs::write(&path, b"data").unwrap();
                client
                    .upload_from("object", path, WriteOptions::default())
                    .await
                    .unwrap();
            }
            _ => client
                .delete("object", false, DeleteOptions::default())
                .await
                .unwrap(),
        }
        assert_eq!(
            server.count(),
            3,
            "{operation} must retry twice and avoid a post-write HEAD"
        );
    }
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn conditional_retry_conflicts_report_an_ambiguous_outcome() {
    let server = FailureServer::start(Behavior::FailureThenPrecondition, 0).await;
    let client = client(server.provider(1000));
    let error = client
        .write(
            "object",
            bytes::Bytes::from_static(b"data"),
            WriteOptions {
                if_none_match: Some("*".into()),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::AmbiguousOutcome);
    assert!(!error.retryable);
    assert_eq!(server.count(), 2);
}

#[tokio::test]
#[ignore = "requires local TCP binding; run just run-integration-tests"]
async fn ordinary_reads_and_streams_use_one_get_each() {
    use futures::TryStreamExt;
    let server = FailureServer::start(Behavior::Success, 0).await;
    let client = client(server.provider(1000));
    assert_eq!(client.read("object").await.unwrap(), "data");
    assert_eq!(server.count(), 1);
    let chunks = client
        .read_stream("object", ReadOptions::default())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(chunks.concat(), b"data");
    assert_eq!(server.count(), 2);
}
