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

//! Native S3 copy/delete publication evidence exercised over the actual HTTP adapter.
#![cfg(feature = "s3")]
use multi_storage_client::{
    CopyOptions, DeleteOptions, ErrorKind, GetConditions, ObjectPath, StorageClient,
    StorageProvider, WriteOptions,
    credentials::{AwsCredentials, SecretString},
    provider::s3::{S3Options, S3Provider},
};
use std::fmt::Write as _;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

#[derive(Debug)]
struct Request {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
    body: String,
}
struct Response {
    status: u16,
    headers: String,
    body: String,
}
impl Response {
    fn xml(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            headers: String::new(),
            body: body.into(),
        }
    }
    fn head(etag: &str, version: &str, tag: &str) -> Self {
        Self {
            status: 200,
            headers: format!(
                "ETag: {etag}\r\nx-amz-version-id: {version}\r\nx-amz-meta-tag: {tag}\r\nContent-Type: text/plain\r\nLast-Modified: Tue, 06 Oct 2026 12:00:00 GMT\r\n"
            ),
            body: String::new(),
        }
    }
}
struct Server {
    endpoint: String,
    requests: Arc<Mutex<Vec<Request>>>,
    task: JoinHandle<()>,
}
impl Server {
    async fn new(responses: Vec<Response>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let task = tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request(&mut socket).await;
                let head = request.method == "HEAD";
                recorded.lock().unwrap().push(request);
                let length = if head {
                    response.body.len().max(4)
                } else {
                    response.body.len()
                };
                let wire = format!(
                    "HTTP/1.1 {} Test\r\nConnection: close\r\nContent-Length: {length}\r\n{}\r\n{}",
                    response.status,
                    response.headers,
                    if head { "" } else { &response.body }
                );
                socket.write_all(wire.as_bytes()).await.unwrap();
            }
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }
    fn provider(&self) -> S3Provider {
        S3Provider::new(
            S3Options {
                base_path: "bucket/base".into(),
                endpoint_url: Some(self.endpoint.clone()),
                read_timeout_ms: 1000,
                ..Default::default()
            },
            Some(AwsCredentials {
                access_key_id: SecretString::new("test-access"),
                secret_access_key: SecretString::new("test-secret"),
                session_token: None,
                expires_at: None,
            }),
        )
        .unwrap()
    }
    async fn done(&self) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !self.task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn read_request(socket: &mut TcpStream) -> Request {
    let mut bytes = Vec::new();
    let mut buf = [0; 8192];
    let end = loop {
        if let Some(pos) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
        assert!(bytes.len() < 64 * 1024);
    };
    let head = std::str::from_utf8(&bytes[..end]).unwrap();
    let mut lines = head.lines();
    let mut first = lines.next().unwrap().split_whitespace();
    let method = first.next().unwrap().to_owned();
    let target = first.next().unwrap().to_owned();
    let headers: BTreeMap<_, _> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    let length = headers
        .get("content-length")
        .map_or(0, |v| v.parse::<usize>().unwrap());
    while bytes.len() - end < length {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
        assert!(bytes.len() < 4 * 1024 * 1024);
    }
    Request {
        method,
        target,
        headers,
        body: String::from_utf8(bytes[end..end + length].to_vec()).unwrap(),
    }
}
fn key(k: &str) -> ObjectPath {
    ObjectPath::new(k).unwrap()
}

#[tokio::test]
async fn native_copy_pins_historical_source_and_returns_response_generation() {
    let server = Server::new(vec![
        Response::head("\"old\"", "v1", "old-tag"),
        Response {
            status: 200,
            headers: "x-amz-version-id: dest-v2\r\n".into(),
            body: "<CopyObjectResult><ETag>&quot;published&quot;</ETag></CopyObjectResult>".into(),
        },
    ])
    .await;
    let provider = server.provider();
    let info = multi_storage_client::SourceVersion {
        scope: provider.backend_fingerprint(),
        value: "version:v1".into(),
    };
    let result = provider
        .copy(
            &key("source +%☃"),
            &key("destination"),
            &CopyOptions {
                source_conditions: GetConditions {
                    version: Some(info),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.etag.as_deref(), Some("\"published\""));
    assert_eq!(result.content_length, Some(4));
    assert_eq!(result.source_version.unwrap().value, "version:dest-v2");
    server.done().await;
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "HEAD");
    assert!(requests[0].target.contains("versionId=v1"));
    let copy = &requests[1];
    assert_eq!(copy.method, "PUT");
    assert_eq!(copy.headers["x-amz-copy-source-if-match"], "\"old\"");
    assert_eq!(copy.headers["x-amz-metadata-directive"], "COPY");
    assert!(!copy.headers.contains_key("x-amz-meta-tag"));
    assert!(copy.headers["x-amz-copy-source"].contains("versionId=v1"));
    assert!(copy.headers["x-amz-copy-source"].contains("%2B%25"));
    assert!(copy.headers["authorization"].starts_with("AWS4-HMAC-SHA256"));
}

#[tokio::test]
async fn copy_http_success_requires_completed_xml_publication() {
    for body in [
        "<CopyObjectResult/>",
        "garbage",
        "<Unexpected><ETag>&quot;x&quot;</ETag></Unexpected>",
        "<CopyObjectResult><ETag>&quot;x&quot;</ETag></CopyObjectResult><Error/>",
    ] {
        let server = Server::new(vec![
            Response::head("\"old\"", "v1", "old"),
            Response::xml(body),
        ])
        .await;
        assert_eq!(
            server
                .provider()
                .copy(&key("source"), &key("dest"), &CopyOptions::default())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Integrity
        );
    }
}

#[tokio::test]
async fn conditional_delete_uses_etag_and_version_and_rejects_conflicting_scope() {
    let server = Server::new(vec![Response {
        status: 204,
        headers: String::new(),
        body: String::new(),
    }])
    .await;
    let provider = server.provider();
    provider
        .delete(
            &key("日本語/%28name%29+ space?#.txt"),
            &DeleteOptions {
                if_match: Some("\"expected\"".into()),
                version: Some(multi_storage_client::SourceVersion {
                    scope: provider.backend_fingerprint(),
                    value: "version:v1".into(),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    server.done().await;
    {
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests[0].method, "DELETE");
        assert_eq!(requests[0].headers["if-match"], "\"expected\"");
        assert_eq!(
            requests[0].target,
            "/bucket/base/%E6%97%A5%E6%9C%AC%E8%AA%9E/%2528name%2529%2B%20space%3F%23.txt?versionId=v1"
        );
    }
    for options in [
        DeleteOptions {
            if_none_match: Some("*".into()),
            ..Default::default()
        },
        DeleteOptions {
            version: Some(multi_storage_client::SourceVersion {
                scope: "other".into(),
                value: "etag:x".into(),
            }),
            ..Default::default()
        },
    ] {
        assert!(provider.delete(&key("object"), &options).await.is_err());
    }
    assert_eq!(
        provider
            .delete(&ObjectPath::default(), &DeleteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidPath
    );
}

#[tokio::test]
async fn batch_deletion_preserves_input_indices_and_partial_failure_details() {
    let server=Server::new(vec![Response::xml("<DeleteResult><Deleted><Key>base/third</Key></Deleted><Error><Key>base/second</Key><Code>AccessDenied</Code><Message>secret</Message></Error><Deleted><Key>base/first&amp;☃</Key></Deleted></DeleteResult>")]).await;
    let client = StorageClient::from_provider(Arc::new(server.provider()));
    let keys = vec![key("first&☃"), key("second"), key("third")];
    let result = client
        .delete_many(&keys, DeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(result.succeeded, vec![0, 2]);
    assert_eq!(result.failed.len(), 1);
    assert_eq!(result.failed[0].index, 1);
    assert_eq!(result.failed[0].error.kind, ErrorKind::PermissionDenied);
    assert!(!format!("{:?}", result.failed).contains("secret"));
    server.done().await;
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert!(requests[0].target.contains("delete="));
    assert!(requests[0].body.contains("first&amp;☃"));
    assert!(requests[0].headers.contains_key("content-md5"));
}

#[tokio::test]
async fn batch_delete_reports_a_precondition_after_a_transient_attempt_as_ambiguous() {
    let server = Server::new(vec![
        Response {
            status: 503,
            headers: String::new(),
            body: "<Error><Code>SlowDown</Code></Error>".into(),
        },
        Response {
            status: 412,
            headers: String::new(),
            body: "<Error><Code>PreconditionFailed</Code></Error>".into(),
        },
    ])
    .await;
    let client = StorageClient::from_provider(Arc::new(server.provider()))
        .with_retry_policy(multi_storage_client::RetryPolicy {
            max_attempts: 2,
            initial_delay_ms: 1,
            max_delay_ms: 1,
            jitter: false,
            ..Default::default()
        })
        .unwrap();
    let result = client
        .delete_many(
            &[key("a")],
            DeleteOptions {
                if_match: Some("\"old\"".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(result.succeeded.is_empty());
    assert_eq!(result.failed[0].error.kind, ErrorKind::AmbiguousOutcome);
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn historical_reads_obtain_size_and_etag_from_the_requested_version() {
    use futures::StreamExt as _;
    let mut current = Response::head("\"current\"", "v2", "current");
    current.body = "a longer current object".into();
    let mut responses = vec![current];
    for operation in 0..5 {
        responses.push(Response::head("\"old\"", "v1", "old"));
        let ranged = matches!(operation, 2 | 4);
        let mut response = Response::head("\"old\"", "v1", "old");
        response.status = if ranged { 206 } else { 200 };
        if ranged {
            response.headers.push_str("Content-Range: bytes 0-3/4\r\n");
        }
        response.body = "old!".into();
        responses.push(response);
    }
    let server = Server::new(responses).await;
    let provider = Arc::new(server.provider());
    let mut version = provider
        .head(&key("a"))
        .await
        .unwrap()
        .source_version
        .unwrap();
    version.value = "version:v1".into();
    let client = StorageClient::from_provider(provider);
    let options = multi_storage_client::ReadOptions {
        conditions: GetConditions {
            version: Some(version),
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        client
            .read_with_options("a", options.clone())
            .await
            .unwrap(),
        "old!"
    );
    let mut stream = client.read_stream("a", options.clone()).await.unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(bytes, b"old!");
    let mut reader = client.reader("a", options.clone()).await.unwrap();
    assert_eq!(reader.len(), 4);
    bytes.clear();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"old!");
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("download");
    let metadata = client
        .download_to("a", &output, options.clone())
        .await
        .unwrap();
    assert_eq!(metadata.content_length, 4);
    assert_eq!(std::fs::read(output).unwrap(), b"old!");
    assert_eq!(
        client
            .read_range(
                "a",
                multi_storage_client::ByteRange::new(0, 4).unwrap(),
                options
            )
            .await
            .unwrap(),
        "old!"
    );
    server.done().await;
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 11);
    for request in &requests[1..] {
        assert!(
            request.target.contains("versionId=v1"),
            "{}",
            request.target
        );
        if request.method == "GET" {
            assert!(
                request
                    .headers
                    .get("if-match")
                    .is_none_or(|etag| etag == "\"old\"")
            );
        }
    }
}

#[tokio::test]
async fn malformed_batch_outcomes_never_become_success_or_panic() {
    for body in [
        "<DeleteResult/>",
        "<DeleteResult><Error><Key>unknown</Key><Code>AccessDenied</Code></Error></DeleteResult>",
        "<DeleteResult><Deleted><Key>base/a</Key></Deleted><Deleted><Key>base/a</Key></Deleted></DeleteResult>",
        "<Error/>",
        "broken",
        "<Unexpected><Deleted><Key>base/a</Key></Deleted></Unexpected>",
        "<DeleteResult><Deleted><Key>base/a</Key></Deleted></DeleteResult><Error/>",
    ] {
        let server = Server::new(vec![Response::xml(body)]).await;
        let result = server
            .provider()
            .delete_many(&[key("a")], &DeleteOptions::default())
            .await;
        assert!(result.succeeded.is_empty(), "{body}");
        assert_eq!(result.failed[0].error.kind, ErrorKind::Integrity, "{body}");
    }
}

#[tokio::test]
async fn batch_requests_are_bounded_and_invalid_inputs_keep_their_indices() {
    let response = |start: usize, end: usize| {
        let mut body = String::from("<DeleteResult>");
        for index in start..end {
            write!(body, "<Deleted><Key>base/key-{index}</Key></Deleted>").unwrap();
        }
        body.push_str("</DeleteResult>");
        Response::xml(body)
    };
    let server = Server::new(vec![response(0, 1000), response(1000, 1001)]).await;
    let mut keys = (0..1001)
        .map(|i| key(&format!("key-{i}")))
        .collect::<Vec<_>>();
    keys.insert(500, ObjectPath::default());
    let result = server
        .provider()
        .delete_many(&keys, &DeleteOptions::default())
        .await;
    assert_eq!(result.succeeded.len(), 1001);
    assert_eq!(result.failed.len(), 1);
    assert_eq!(result.failed[0].index, 500);
    assert_eq!(result.failed[0].error.kind, ErrorKind::InvalidPath);
    server.done().await;
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body.matches("<Object>").count(), 1000);
    assert_eq!(requests[1].body.matches("<Object>").count(), 1);
}

#[tokio::test]
async fn destination_copy_conditions_decline_before_network_mutation() {
    let server = Server::new(vec![]).await;
    let error = server
        .provider()
        .copy(
            &key("src"),
            &key("dst"),
            &CopyOptions {
                destination: WriteOptions {
                    if_none_match: Some("*".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unsupported);
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn native_copy_metadata_override_retains_standard_content_properties() {
    let mut head = Response::head("\"old\"", "v1", "inherited");
    head.headers.push_str("Cache-Control: max-age=3600\r\nContent-Encoding: gzip\r\nContent-Language: en\r\nContent-Disposition: attachment\r\n");
    let server = Server::new(vec![
        head,
        Response::xml("<CopyObjectResult><ETag>&quot;new&quot;</ETag></CopyObjectResult>"),
    ])
    .await;
    server
        .provider()
        .copy(
            &key("source"),
            &key("destination"),
            &CopyOptions {
                destination: WriteOptions {
                    attributes: BTreeMap::from([("new".into(), "value".into())]),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
    server.done().await;
    let requests = server.requests.lock().unwrap();
    let copy = &requests[1];
    assert_eq!(copy.headers["x-amz-metadata-directive"], "REPLACE");
    assert_eq!(copy.headers["x-amz-meta-new"], "value");
    assert!(!copy.headers.contains_key("x-amz-meta-tag"));
    assert_eq!(copy.headers["content-type"], "text/plain");
    assert_eq!(copy.headers["cache-control"], "max-age=3600");
    assert_eq!(copy.headers["content-encoding"], "gzip");
    assert_eq!(copy.headers["content-language"], "en");
    assert_eq!(copy.headers["content-disposition"], "attachment");
}

#[tokio::test]
async fn unstable_versions_invalid_keys_and_metadata_reject_before_requests() {
    let server = Server::new(vec![]).await;
    let provider = server.provider();
    for token in ["version:null", "version:", "etag:*", "etag:  ", "etag:a,b"] {
        let version = multi_storage_client::SourceVersion {
            scope: provider.backend_fingerprint(),
            value: token.into(),
        };
        assert_eq!(
            provider
                .delete(
                    &key("object"),
                    &DeleteOptions {
                        version: Some(version.clone()),
                        ..Default::default()
                    }
                )
                .await
                .unwrap_err()
                .kind,
            ErrorKind::PreconditionFailed
        );
        assert_eq!(
            provider
                .get(
                    &key("object"),
                    &GetConditions {
                        version: Some(version),
                        ..Default::default()
                    }
                )
                .await
                .err()
                .unwrap()
                .kind,
            ErrorKind::PreconditionFailed
        );
    }
    assert_eq!(
        provider
            .delete(&key("bad\nkey"), &DeleteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidPath
    );
    for attributes in [
        BTreeMap::from([("Name".into(), "one".into()), ("name".into(), "two".into())]),
        BTreeMap::from([("MSC-SYMLINK-TARGET".into(), "../escape".into())]),
    ] {
        assert!(
            provider
                .copy(
                    &key("source"),
                    &key("destination"),
                    &CopyOptions {
                        destination: WriteOptions {
                            attributes,
                            ..Default::default()
                        },
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
    }
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn null_version_uses_etag_and_poisoned_link_metadata_is_rejected() {
    let mut link = Response::head("\"link\"", "null", "tag");
    link.headers
        .push_str("x-amz-meta-MSC-SYMLINK-TARGET: destination\r\n");
    let mut ranged = Response::head("\"stable\"", "null", "tag");
    ranged.status = 206;
    ranged.headers.push_str("Content-Range: bytes 0-3/4\r\n");
    ranged.body = "data".into();
    let server = Server::new(vec![
        Response::head("\"stable\"", "null", "tag"),
        ranged,
        link,
    ])
    .await;
    let provider = server.provider();
    let version = provider
        .head(&key("object"))
        .await
        .unwrap()
        .source_version
        .unwrap();
    assert_eq!(version.value, "etag:\"stable\"");
    let bytes = provider
        .get_range(
            &key("object"),
            multi_storage_client::ByteRange::new(0, 4).unwrap(),
            &GetConditions {
                version: Some(version),
                if_match: Some("*".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(bytes, "data");
    assert_eq!(
        server.requests.lock().unwrap()[1].headers["if-match"],
        "\"stable\""
    );
    assert_eq!(
        provider.head(&key("link")).await.unwrap_err().kind,
        ErrorKind::Integrity
    );
}

#[test]
fn native_environment_settings_are_checked_in_isolated_processes() {
    for scenario in ["anonymous", "express", "encrypted", "requester"] {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "native_environment_child_probe",
            "--ignored",
            "--nocapture",
        ]);
        for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("AWS_")) {
            command.env_remove(name);
        }
        command.env("MSC_S3_NATIVE_SCENARIO", scenario);
        match scenario {
            "anonymous" => {
                command.env("AWS_SKIP_SIGNATURE", "1");
            }
            "express" => {
                command.env("AWS_S3_EXPRESS", "yes");
            }
            "encrypted" => {
                command
                    .env("AWS_SERVER_SIDE_ENCRYPTION", "aws:kms")
                    .env("AWS_SSE_KMS_KEY_ID", "test-kms-key");
            }
            "requester" => {
                command
                    .env("AWS_REQUEST_PAYER", "on")
                    .env("AWS_UNSIGNED_PAYLOAD", "1");
            }
            _ => unreachable!(),
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{scenario}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
#[ignore = "subprocess probe invoked by isolated environment regression"]
async fn native_environment_child_probe() {
    let scenario = std::env::var("MSC_S3_NATIVE_SCENARIO").unwrap();
    if scenario == "express" {
        let error = S3Provider::new(
            S3Options {
                base_path: "bucket".into(),
                ..Default::default()
            },
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidConfig);
        return;
    }
    let responses = if scenario == "requester" {
        vec![Response {
            status: 204,
            headers: String::new(),
            body: String::new(),
        }]
    } else {
        vec![]
    };
    let server = Server::new(responses).await;
    let provider = server.provider();
    match scenario.as_str() {
        "anonymous" => {
            assert!(!provider.capabilities().presigned_get);
            assert_eq!(
                provider
                    .presigned_url(
                        &key("object"),
                        multi_storage_client::PresignMethod::Get,
                        Duration::from_secs(60)
                    )
                    .await
                    .unwrap_err()
                    .kind,
                ErrorKind::Unsupported
            );
            provider.delete_many(&[], &DeleteOptions::default()).await;
        }
        "encrypted" => {
            assert_eq!(
                provider.capabilities().server_side_copy,
                multi_storage_client::CopyScope::None
            );
            assert_eq!(
                provider
                    .copy(&key("source"), &key("destination"), &CopyOptions::default())
                    .await
                    .unwrap_err()
                    .kind,
                ErrorKind::Unsupported
            );
            assert!(server.requests.lock().unwrap().is_empty());
        }
        "requester" => {
            provider
                .delete(&key("object"), &DeleteOptions::default())
                .await
                .unwrap();
            server.done().await;
            let requests = server.requests.lock().unwrap();
            assert_eq!(requests[0].headers["x-amz-request-payer"], "requester");
            assert_eq!(
                requests[0].headers["x-amz-content-sha256"],
                "UNSIGNED-PAYLOAD"
            );
            assert!(requests[0].headers["authorization"].contains("x-amz-request-payer"));
        }
        _ => panic!("unexpected scenario"),
    }
}

#[tokio::test]
async fn copy_http_success_with_service_error_is_typed_and_redacted() {
    for (code, kind) in [
        ("InternalError", ErrorKind::Unavailable),
        ("SlowDown", ErrorKind::RateLimited),
        ("AccessDenied", ErrorKind::PermissionDenied),
        ("NoSuchKey", ErrorKind::NotFound),
    ] {
        let server = Server::new(vec![
            Response::head("\"source\"", "v1", "tag"),
            Response::xml(format!(
                "<Error><Code>{code}</Code><Message>secret-value</Message></Error>"
            )),
        ])
        .await;
        let error = server
            .provider()
            .copy(&key("source"), &key("target"), &CopyOptions::default())
            .await
            .unwrap_err();
        assert_eq!(error.kind, kind);
        assert_eq!(error.http_status, Some(200));
        assert!(!format!("{error:?}").contains("secret-value"));
    }
}

#[tokio::test]
async fn suffix_read_uses_one_get_with_native_suffix_header() {
    let server = Server::new(vec![Response { status: 206,
        headers: "Content-Range: bytes 7-9/10\r\nETag: \"v1\"\r\nLast-Modified: Tue, 06 Oct 2026 12:00:00 GMT\r\n".into(), body: "789".into() }]).await;
    let client = StorageClient::from_provider(Arc::new(server.provider()));
    let result = client
        .read_range(
            "tail",
            multi_storage_client::SuffixRange::new(3).unwrap(),
            multi_storage_client::ReadOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(result, "789");
    server.done().await;
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].headers["range"], "bytes=-3");
}
