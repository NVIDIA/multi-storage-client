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

//! Raw S3 listings and range responses must survive SDK validation without aliasing.
#![cfg(feature = "s3")]

use futures::{StreamExt, TryStreamExt};
use multi_storage_client::{
    ByteRange, ErrorKind, ListOptions, ObjectKind, ObjectPath, ReadOptions, RetryPolicy,
    StorageClient,
    credentials::{AwsCredentials, SecretString},
    provider::s3::{S3Options, S3Provider},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

#[derive(Clone)]
struct Request {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
}
struct Reply {
    status: u16,
    content_range: Option<&'static str>,
    bytes: Vec<u8>,
    length: usize,
    headers: String,
}
impl Reply {
    fn bytes(status: u16, bytes: &[u8], content_range: Option<&'static str>) -> Self {
        Self {
            status,
            content_range,
            bytes: bytes.to_vec(),
            length: bytes.len(),
            headers: String::new(),
        }
    }
}
struct Server {
    endpoint: String,
    requests: Arc<Mutex<Vec<Request>>>,
    task: JoinHandle<()>,
}
impl Server {
    async fn start(pages: Vec<String>) -> Self {
        Self::start_responses(
            pages
                .into_iter()
                .map(|page| Reply::bytes(200, page.as_bytes(), None))
                .collect(),
        )
        .await
    }
    async fn start_responses(responses: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut input = Vec::new();
                let mut buffer = [0; 8192];
                while !input.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let read = socket.read(&mut buffer).await.unwrap();
                    assert!(read > 0);
                    input.extend_from_slice(&buffer[..read]);
                    assert!(input.len() < 64 * 1024);
                }
                let head = std::str::from_utf8(&input).unwrap();
                let mut lines = head.lines();
                let mut first = lines.next().unwrap().split_whitespace();
                let method = first.next().unwrap().into();
                let target = first.next().unwrap().into();
                let headers = lines
                    .filter_map(|line| line.split_once(':'))
                    .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
                    .collect();
                observed.lock().unwrap().push(Request {
                    method,
                    target,
                    headers,
                });
                let content_range = response
                    .content_range
                    .map_or_else(String::new, |value| format!("Content-Range: {value}\r\n"));
                let headers = format!(
                    "HTTP/1.1 {} Test\r\nConnection: close\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nETag: \"etag\"\r\nLast-Modified: Wed, 07 Oct 2026 00:00:00 GMT\r\n{content_range}{}\r\n",
                    response.status, response.length, response.headers
                );
                if socket.write_all(headers.as_bytes()).await.is_ok() {
                    let _ = socket.write_all(&response.bytes).await;
                }
            }
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }
    fn client(&self) -> StorageClient {
        let provider = S3Provider::new(
            S3Options {
                base_path: "bucket/base".into(),
                endpoint_url: Some(self.endpoint.clone()),
                list_page_size: 2,
                read_timeout_ms: 5000,
                ..Default::default()
            },
            Some(AwsCredentials {
                access_key_id: SecretString::new("test-access"),
                secret_access_key: SecretString::new("test-secret"),
                session_token: None,
                expires_at: None,
            }),
        )
        .unwrap();
        StorageClient::from_provider(Arc::new(provider))
            .with_retry_policy(RetryPolicy {
                max_attempts: 3,
                initial_delay_ms: 1,
                max_delay_ms: 1,
                jitter: false,
                ..Default::default()
            })
            .unwrap()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn object(key: &str) -> String {
    format!(
        "<Contents><Key>{}</Key><Size>3</Size><LastModified>2026-10-07T00:00:00Z</LastModified><ETag>&quot;etag&quot;</ETag></Contents>",
        quick_xml::escape::escape(key)
    )
}
fn page(contents: &str) -> String {
    format!(
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{contents}</ListBucketResult>"
    )
}

#[tokio::test]
async fn unsupported_object_keys_fail_before_any_normalized_page_is_exposed() {
    for key in [
        "/base/a",
        "/base/a/",
        "base/a//b",
        "base/a\\b",
        "base/a/./b",
        "base/a/../b",
        "base/control\tkey",
    ] {
        let server =
            Server::start(vec![page(&format!("{}{}", object("base/a"), object(key)))]).await;
        let client = server.client();
        let mut listing = client.list(ListOptions::default()).await.unwrap();
        let error = listing.next().await.unwrap().unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidPath, "{key}");
        assert_eq!(error.http_status, Some(200));
        assert!(!error.retryable);
        assert!(!format!("{error:?}").contains(key));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn directory_prefixes_keep_one_delimiter_and_invalid_aliases_fail() {
    let server = Server::start(vec![page(&format!(
        "<CommonPrefixes><Prefix>base/a/</Prefix></CommonPrefixes>{}",
        object("base/z")
    ))])
    .await;
    let entries = server
        .client()
        .list(ListOptions {
            recursive: false,
            include_directories: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>(),
        ["a", "z"]
    );
    assert_eq!(entries[0].kind, ObjectKind::Directory);
    assert_eq!(entries[1].kind, ObjectKind::File);
    for prefix in [
        "base/a",
        "/base/a/",
        "base/a//",
        "base/a/./b/",
        "base/a/../b/",
        "",
    ] {
        let server = Server::start(vec![page(&format!(
            "<CommonPrefixes><Prefix>{prefix}</Prefix></CommonPrefixes>"
        ))])
        .await;
        let error = server
            .client()
            .list(ListOptions {
                recursive: false,
                include_directories: true,
                ..Default::default()
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidPath, "{prefix}");
    }
}

#[tokio::test]
async fn native_pagination_preserves_encoded_utf8_keys() {
    let token = "token+%&";
    let server = Server::start(vec![
        page(&format!(
            "{}<NextContinuationToken>{}</NextContinuationToken>",
            object("base/a +%☃&"),
            quick_xml::escape::escape(token)
        )),
        page(&object("base/z")),
    ])
    .await;
    let entries = server
        .client()
        .list(ListOptions::default())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>(),
        [
            ObjectPath::new("a +%☃&").unwrap(),
            ObjectPath::new("z").unwrap()
        ]
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for request in &*requests {
        assert!(request.headers["authorization"].starts_with("AWS4-HMAC-SHA256"));
        let url = url::Url::parse(&format!("{}{}", server.endpoint, request.target)).unwrap();
        let query = url.query_pairs().collect::<BTreeMap<_, _>>();
        assert_eq!(query.get("list-type").unwrap(), "2");
        assert_eq!(query.get("prefix").unwrap(), "base/");
        assert_eq!(query.get("max-keys").unwrap(), "2");
    }
    let url = url::Url::parse(&format!("{}{}", server.endpoint, requests[1].target)).unwrap();
    assert!(
        url.query_pairs()
            .any(|(key, value)| key == "continuation-token" && value == token)
    );
}

#[tokio::test]
async fn malformed_structures_missing_fields_and_trailing_payloads_are_integrity_errors() {
    for body in [
        "<Unexpected/>",
        "<ListBucketResult/>secret",
        "<ListBucketResult/><Error/>",
        "<ListBucketResult><Contents></ListBucketResult>",
        "<ListBucketResult><Contents><Key>base/a</Key><Key>base/b</Key><Size>3</Size><LastModified>2026-10-07T00:00:00Z</LastModified></Contents></ListBucketResult>",
        "<ListBucketResult><Contents><Size>3</Size><LastModified>2026-10-07T00:00:00Z</LastModified></Contents></ListBucketResult>",
        "<ListBucketResult><Contents><Key>base/a</Key><Size>-1</Size><LastModified>2026-10-07T00:00:00Z</LastModified></Contents></ListBucketResult>",
        "<ListBucketResult><Contents><Key>base/a</Key><Size>3</Size><LastModified>secret</LastModified></Contents></ListBucketResult>",
        "<ListBucketResult><CommonPrefixes/></ListBucketResult>",
        "<!DOCTYPE ListBucketResult><ListBucketResult/>",
        "<ListBucketResult><NextContinuationToken/></ListBucketResult>",
        "<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>",
    ] {
        let server = Server::start(vec![body.into()]).await;
        let error = server
            .client()
            .list(ListOptions::default())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Integrity, "{body}");
        assert_eq!(error.http_status, Some(200));
        assert!(!format!("{error:?}").contains("secret"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn listing_page_counts_tokens_envelope_depth_and_bytes_are_bounded() {
    let objects = (0..1000)
        .map(|index| object(&format!("base/key-{index:04}")))
        .collect::<String>();
    let server = Server::start(vec![page(&objects)]).await;
    assert_eq!(
        server
            .client()
            .list(ListOptions::default())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        1000
    );
    for contents in [
        format!("{objects}{}", object("base/key-1000")),
        format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            "x".repeat(16 * 1024 + 1)
        ),
        format!("{}{}", "<nested>".repeat(65), "</nested>".repeat(65)),
        format!("<ignored>{}</ignored>", "x".repeat(16 * 1024 * 1024)),
    ] {
        let server = Server::start(vec![page(&contents)]).await;
        let error = server
            .client()
            .list(ListOptions::default())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Integrity);
        assert!(!error.retryable);
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn malformed_partial_response_bounds_fail_without_sdk_overflow_or_range_aliasing() {
    for (status, header) in [
        (
            206,
            Some("bytes 1-18446744073709551615/18446744073709551615"),
        ),
        (206, Some("bytes 3-1/8")),
        (206, Some("bytes secret-3/8")),
        (206, Some("bytes 0-2/8")),
        (206, Some("bytes 1-3/3")),
        (206, Some("bytes 1-3/secret")),
        (206, None),
        (200, Some("bytes 1-3/8")),
    ] {
        let server = Server::start_responses(vec![Reply::bytes(status, b"bcd", header)]).await;
        let error = server
            .client()
            .read_range(
                "range",
                ByteRange::new(1, 3).unwrap(),
                ReadOptions::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Integrity);
        assert_eq!(error.http_status, Some(status));
        assert!(!error.retryable);
        assert!(!error.to_string().contains("secret"));
        let requests = server.requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.method.as_str())
                .collect::<Vec<_>>(),
            ["GET"]
        );
        assert_eq!(
            requests.last().unwrap().headers.get("range").unwrap(),
            "bytes=1-3"
        );
    }
    let server =
        Server::start_responses(vec![Reply::bytes(206, b"bcd", Some("bytes 1-3/8"))]).await;
    assert_eq!(
        server.client().read("range").await.unwrap_err().kind,
        ErrorKind::Integrity
    );
    let server =
        Server::start_responses(vec![Reply::bytes(206, b"bcdefgh", Some("bytes 1-7/8"))]).await;
    assert_eq!(
        server
            .client()
            .read_range(
                "range",
                ByteRange::new(1, 100).unwrap(),
                ReadOptions::default()
            )
            .await
            .unwrap(),
        "bcdefgh"
    );
    let server = Server::start_responses(vec![Reply::bytes(200, b"abcdefgh", None)]).await;
    assert_eq!(server.client().read("range").await.unwrap(), "abcdefgh");
}

#[tokio::test]
async fn zero_byte_folder_markers_are_ignored_without_aliasing_files() {
    for recursive in [true, false] {
        let marker = object("base/folder/").replace("<Size>3</Size>", "<Size>0</Size>");
        let server = Server::start(vec![page(&format!(
            "{marker}{}",
            object("base/folder/file.txt")
        ))])
        .await;
        let entries = server
            .client()
            .list(ListOptions {
                recursive,
                include_directories: true,
                ..ListOptions::default()
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(
            entries
                .iter()
                .all(|entry| entry.key.as_str() != "folder" || entry.kind == ObjectKind::Directory)
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.key.as_str() == "folder/file.txt")
        );
    }
}

#[tokio::test]
async fn recursive_markers_synthesize_ancestors_without_duplicates_across_pages() {
    for include_child in [false, true] {
        let marker = object("base/a/b/").replace("<Size>3</Size>", "<Size>0</Size>");
        let pages = if include_child {
            vec![
                page(&format!(
                    "{marker}<NextContinuationToken>next</NextContinuationToken>"
                )),
                page(&object("base/a/b/file")),
            ]
        } else {
            vec![page(&marker)]
        };
        let server = Server::start(pages).await;
        let entries = server
            .client()
            .list(ListOptions {
                include_directories: true,
                ..Default::default()
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let keys: Vec<_> = entries.iter().map(|entry| entry.key.as_str()).collect();
        assert_eq!(
            keys,
            if include_child {
                vec!["a", "a/b", "a/b/file"]
            } else {
                vec!["a", "a/b"]
            }
        );
        assert!(
            entries[..2]
                .iter()
                .all(|entry| entry.kind == ObjectKind::Directory)
        );
    }
}

#[tokio::test]
async fn a_zero_byte_probe_never_hides_invalid_logical_link_metadata() {
    let listing = page(&object("base/link").replace("<Size>3</Size>", "<Size>0</Size>"));
    let mut head = Reply::bytes(200, b"", None);
    head.headers = "x-amz-meta-msc-symlink-target: ../escape\r\n".into();
    let server =
        Server::start_responses(vec![Reply::bytes(200, listing.as_bytes(), None), head]).await;
    let error = server
        .client()
        .list(ListOptions::default())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Integrity);
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn repeated_continuation_tokens_fail_for_recursive_and_shallow_lists() {
    for recursive in [true, false] {
        let server = Server::start(vec![
            page("<NextContinuationToken>same</NextContinuationToken>"),
            page("<NextContinuationToken>same</NextContinuationToken>"),
        ])
        .await;
        let error = server
            .client()
            .list(ListOptions {
                recursive,
                ..ListOptions::default()
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Integrity);
        assert_eq!(server.requests.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn recursive_directories_share_pages_without_per_file_probes_and_bounds_skip_head() {
    let server = Server::start(vec![page(&format!(
        "{}{}{}",
        object("base/a.txt"),
        object("base/a/file"),
        object("base/z").replace("<Size>3</Size>", "<Size>0</Size>")
    ))])
    .await;
    let entries = server
        .client()
        .list(ListOptions {
            include_directories: true,
            start_after: Some(ObjectPath::new("0").unwrap()),
            end_at: Some(ObjectPath::new("b").unwrap()),
            ..ListOptions::default()
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>(),
        ["a.txt", "a", "a/file"]
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let url = url::Url::parse(&format!("{}{}", server.endpoint, requests[0].target)).unwrap();
    assert!(
        url.query_pairs()
            .any(|(key, value)| key == "start-after" && value == "base/0")
    );
}

#[tokio::test]
async fn directory_bounds_stop_before_fetching_an_out_of_range_punctuation_group() {
    let server = Server::start(vec![page(&format!(
        "{}<NextContinuationToken>unused</NextContinuationToken>",
        object("base/c.txt")
    ))])
    .await;
    let entries = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        server
            .client()
            .list(ListOptions {
                include_directories: true,
                end_at: Some(ObjectPath::new("b").unwrap()),
                ..ListOptions::default()
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(entries.is_empty());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn recursive_upper_bound_retains_a_directory_after_an_out_of_range_file() {
    let server = Server::start(vec![page(&format!(
        "{}{}",
        object("base/a.txt"),
        object("base/a/file"),
    ))])
    .await;
    let entries = server
        .client()
        .list(ListOptions {
            include_directories: true,
            end_at: Some(ObjectPath::new("a").unwrap()),
            ..Default::default()
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key.as_str(), "a");
    assert_eq!(entries[0].kind, ObjectKind::Directory);
}

#[tokio::test]
async fn disconnected_listing_bodies_are_retried_before_emitting_a_partial_page() {
    let valid = page(&object("base/a"));
    let mut disconnected = Reply::bytes(200, b"<ListBucketResult>", None);
    disconnected.length = valid.len() + 100;
    let server = Server::start_responses(vec![
        disconnected,
        Reply::bytes(200, valid.as_bytes(), None),
    ])
    .await;
    let entries = server
        .client()
        .list(ListOptions::default())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key.as_str(), "a");
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn unsupported_keys_can_be_skipped_without_hiding_valid_neighbors() {
    let long_key = object(&format!("base/{}", "x".repeat(1025)));
    let server = Server::start(vec![page(&format!(
        "{}{}{}{long_key}",
        object("base/a"),
        object("base/a//bad"),
        object("base/z")
    ))])
    .await;
    let entries = server
        .client()
        .list(ListOptions {
            skip_unsupported_keys: true,
            ..ListOptions::default()
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>(),
        ["a", "z"]
    );
}

#[tokio::test]
async fn shallow_listing_matches_python_prefixes_before_page_objects() {
    let server = Server::start(vec![page(&format!("<CommonPrefixes><Prefix>base/z/</Prefix></CommonPrefixes>{}<NextContinuationToken>next</NextContinuationToken>", object("base/a"))), page(&format!("<CommonPrefixes><Prefix>base/y/</Prefix></CommonPrefixes>{}", object("base/b")))]).await;
    let entries = server
        .client()
        .list(ListOptions {
            recursive: false,
            include_directories: true,
            ..ListOptions::default()
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>(),
        ["z", "a", "y", "b"]
    );
}
