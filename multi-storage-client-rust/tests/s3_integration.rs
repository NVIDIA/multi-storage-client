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

//! Run through `just run-integration-tests`; no credentials or external cloud are required.
#![cfg(feature = "s3")]
#![allow(clippy::too_many_lines)] // Keep each provider contract scenario together.

use std::{collections::BTreeMap, io::SeekFrom, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use multi_storage_client::{
    ConfigLoader, StorageClient,
    credentials::{AwsCredentials, FileCredentialsProvider, SecretString},
    error::{ErrorKind, MscError},
    path::ObjectPath,
    provider::{
        StorageProvider,
        s3::{S3Options, S3Provider},
    },
    types::{
        ByteRange, ByteStream, CopyOptions, DeleteOptions, GetConditions, InfoOptions, ListOptions,
        ObjectKind, ObjectMetadata, PresignMethod, PutResult, ReadOptions, WriteOptions,
    },
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

fn key(value: &str) -> ObjectPath {
    ObjectPath::new(value).unwrap()
}
fn input(bytes: Bytes) -> ByteStream {
    Box::pin(stream::once(async move { Ok(bytes) }))
}
fn test_provider(prefix: &str) -> S3Provider {
    test_provider_concurrency(prefix, 2)
}
fn test_provider_concurrency(prefix: &str, concurrency: usize) -> S3Provider {
    S3Provider::new(
        S3Options {
            bucket: Some(
                std::env::var("MSC_S3_TEST_BUCKET").expect("run just run-integration-tests"),
            ),
            base_path: prefix.into(),
            endpoint_url: Some(
                std::env::var("MSC_S3_TEST_ENDPOINT").expect("run just run-integration-tests"),
            ),
            multipart_threshold: 5 * 1024 * 1024,
            part_size: 5 * 1024 * 1024,
            multipart_concurrency: concurrency,
            list_page_size: 2,
            ..S3Options::default()
        },
        Some(credentials()),
    )
    .unwrap()
}
fn credentials() -> AwsCredentials {
    AwsCredentials {
        access_key_id: SecretString::new(std::env::var("MSC_S3_TEST_ACCESS_KEY").unwrap()),
        secret_access_key: SecretString::new(std::env::var("MSC_S3_TEST_SECRET_KEY").unwrap()),
        session_token: None,
        expires_at: None,
    }
}
async fn read(provider: &S3Provider, key: &ObjectPath) -> Bytes {
    let chunks: Vec<_> = provider
        .get(key, &GetConditions::default())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    Bytes::from(chunks.concat())
}
async fn put(
    provider: &S3Provider,
    key: &ObjectPath,
    value: Bytes,
    options: &WriteOptions,
) -> PutResult {
    let size = value.len() as u64;
    provider
        .put(key, input(value), Some(size), options)
        .await
        .unwrap()
}

async fn configured_client(prefix: &str) -> StorageClient {
    configured_client_with_tags(prefix, "s3", "StaticS3CredentialsProvider").await
}

async fn configured_client_with_tags(
    prefix: &str,
    storage_tag: &str,
    credentials_tag: &str,
) -> StorageClient {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("config.yaml");
    let configuration = serde_json::json!({
        "profiles": { "data": {
            "storage_provider": { "type": storage_tag, "options": {
                "base_path": format!("${{MSC_S3_TEST_BUCKET}}/{prefix}"),
                "region_name": "us-east-1",
                "endpoint_url": "${MSC_S3_TEST_ENDPOINT}",
                "multipart_threshold": 5 * 1024 * 1024,
                "multipart_chunksize": 5 * 1024 * 1024,
                "max_concurrency": 2,
                "list_page_size": 2
            }},
            "credentials_provider": { "type": credentials_tag, "options": {
                "access_key": "${MSC_S3_TEST_ACCESS_KEY}",
                "secret_key": "${MSC_S3_TEST_SECRET_KEY}"
            }},
            "retry": { "attempts": 3, "delay": 0.0, "backoff_multiplier": 2.0,
                "max_delay_ms": 0, "jitter": false }
        }}
    });
    tokio::fs::write(&file, serde_yaml::to_string(&configuration).unwrap())
        .await
        .unwrap();
    let config = ConfigLoader::new()
        .with_explicit_paths([file])
        .load()
        .unwrap();
    StorageClient::builder(config, "data")
        .with_read_buffer_size(16 * 1024)
        .build()
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn object_contract_and_conditions() {
    let provider = test_provider("contract");
    let object = key("Unicode//日本語 and spaces.txt");
    let payload = Bytes::from_static(b"0123456789");
    let options = WriteOptions {
        attributes: BTreeMap::from([("owner".into(), "msc".into())]),
        content_type: Some("text/plain".into()),
        ..Default::default()
    };
    let created = put(&provider, &object, payload.clone(), &options).await;
    let info = provider.head(&object).await.unwrap();
    assert_eq!(info.key, object);
    assert_eq!(info.content_length, 10);
    assert_eq!(info.attributes, options.attributes);
    assert!(info.last_modified.is_some());
    assert_eq!(created.etag, info.etag);
    assert_eq!(read(&provider, &object).await, payload);
    assert_eq!(
        provider
            .get_range(
                &object,
                ByteRange::new(2, 4).unwrap(),
                &GetConditions::default()
            )
            .await
            .unwrap(),
        Bytes::from_static(b"2345")
    );
    assert_eq!(
        provider
            .get_range(
                &object,
                ByteRange::new(8, 99).unwrap(),
                &GetConditions::default()
            )
            .await
            .unwrap(),
        Bytes::from_static(b"89")
    );
    assert!(
        provider
            .get_range(
                &object,
                ByteRange::new(99, 4).unwrap(),
                &GetConditions::default()
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        provider
            .get_range(
                &key("missing"),
                ByteRange::new(0, 0).unwrap(),
                &GetConditions::default()
            )
            .await
            .unwrap()
            .is_empty()
    );

    let matching = GetConditions {
        if_match: info.etag.clone(),
        ..Default::default()
    };
    assert!(provider.get(&object, &matching).await.is_ok());
    let source_version = GetConditions {
        version: info.source_version.clone(),
        ..Default::default()
    };
    assert!(provider.get(&object, &source_version).await.is_ok());
    let mismatching = GetConditions {
        if_match: Some("\"wrong-etag\"".into()),
        ..Default::default()
    };
    assert_eq!(
        provider
            .get(&object, &mismatching)
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::PreconditionFailed
    );
    assert_eq!(
        provider
            .get_range(&object, ByteRange::new(99, 1).unwrap(), &mismatching)
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::PreconditionFailed
    );
    let unchanged = GetConditions {
        if_none_match: info.etag.clone(),
        ..Default::default()
    };
    assert_eq!(
        provider.get(&object, &unchanged).await.err().unwrap().kind,
        ErrorKind::NotModified
    );
    let create_only = WriteOptions {
        if_none_match: Some("*".into()),
        ..Default::default()
    };
    assert_eq!(
        provider
            .put(&object, input(Bytes::new()), Some(0), &create_only)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PreconditionFailed
    );
    let update = WriteOptions {
        if_match: info.etag.clone(),
        ..Default::default()
    };
    put(&provider, &object, Bytes::from_static(b"new"), &update).await;
    assert_eq!(
        provider
            .put(&object, input(Bytes::new()), Some(0), &update)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PreconditionFailed
    );
    let update_missing = WriteOptions {
        if_match: Some("\"wrong\"".into()),
        ..Default::default()
    };
    assert_eq!(
        provider
            .put(
                &key("absent"),
                input(Bytes::new()),
                Some(0),
                &update_missing
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PreconditionFailed
    );

    let copy = key("copy");
    let publication = provider
        .copy(&object, &copy, &CopyOptions::default())
        .await
        .unwrap();
    assert_eq!(publication.etag, provider.head(&copy).await.unwrap().etag);
    StorageClient::from_provider(Arc::new(provider.clone()))
        .copy(&object, &copy, CopyOptions::default())
        .await
        .unwrap();
    assert_eq!(read(&provider, &copy).await, Bytes::from_static(b"new"));
    let signed = provider
        .presigned_url(&copy, PresignMethod::Get, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(
        reqwest::get(signed).await.unwrap().bytes().await.unwrap(),
        Bytes::from_static(b"new")
    );
    let signed_put = provider
        .presigned_url(
            &key("signed-put"),
            PresignMethod::Put,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert!(
        reqwest::Client::new()
            .put(signed_put)
            .body("signed upload")
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    assert_eq!(
        read(&provider, &key("signed-put")).await,
        Bytes::from_static(b"signed upload")
    );
    let batch = provider
        .delete_many(
            &[object.clone(), copy.clone(), key("missing")],
            &DeleteOptions::default(),
        )
        .await;
    assert_eq!(batch.succeeded, vec![0, 1, 2]);
    assert!(batch.failed.is_empty());
    assert_eq!(
        provider.head(&object).await.unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert!(provider.head(&ObjectPath::default()).await.unwrap().kind == ObjectKind::Directory);
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn listing_pagination_boundaries_and_directory_order() {
    let provider = test_provider("lists/base");
    let names = [
        "a-000",
        "a-001",
        "a-002",
        "a/b",
        "a/c",
        "a0",
        "ab/item",
        "z",
        "日本語/a",
    ];
    for name in names {
        put(
            &provider,
            &key(name),
            Bytes::new(),
            &WriteOptions::default(),
        )
        .await;
    }
    // A neighboring provider scope must never leak into this listing.
    let neighbor = test_provider("lists/base-collision");
    put(
        &neighbor,
        &key("leak"),
        Bytes::new(),
        &WriteOptions::default(),
    )
    .await;
    let recursive: Vec<ObjectMetadata> = provider
        .list(&ListOptions::default())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut expected: Vec<_> = names.iter().map(|value| key(value)).collect();
    expected.sort();
    assert_eq!(
        recursive
            .iter()
            .map(|meta| meta.key.clone())
            .collect::<Vec<_>>(),
        expected
    );
    let options = ListOptions {
        include_directories: true,
        ..Default::default()
    };
    let synthesized: Vec<ObjectMetadata> = provider
        .list(&options)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut expected = expected.clone();
    let directories = [key("a"), key("ab"), key("日本語")];
    expected.extend(directories.clone());
    expected.sort_by_cached_key(|key| {
        if directories.contains(key) {
            format!("{key}/")
        } else {
            key.to_string()
        }
    });
    assert_eq!(
        synthesized
            .iter()
            .map(|meta| meta.key.clone())
            .collect::<Vec<_>>(),
        expected
    );
    let options = ListOptions {
        recursive: false,
        include_directories: true,
        ..Default::default()
    };
    let delimited: Vec<ObjectMetadata> = provider
        .list(&options)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let expected = ["a", "a-000", "a-001", "a-002", "a0", "ab", "z", "日本語"].map(key);
    assert_eq!(
        {
            let mut keys = delimited
                .iter()
                .map(|meta| meta.key.clone())
                .collect::<Vec<_>>();
            keys.sort();
            keys
        },
        expected
    );
    assert!(
        delimited
            .iter()
            .any(|metadata| metadata.key == key("a") && metadata.kind == ObjectKind::Directory)
    );
    let bounded = ListOptions {
        start_after: Some(key("a-001")),
        end_at: Some(key("a/c")),
        ..Default::default()
    };
    let bounded: Vec<ObjectMetadata> = provider
        .list(&bounded)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        bounded
            .iter()
            .map(|meta| meta.key.clone())
            .collect::<Vec<_>>(),
        vec![key("a-002"), key("a/b"), key("a/c")]
    );
    let prefixed = ListOptions {
        prefix: key("a"),
        ..Default::default()
    };
    let prefixed: Vec<ObjectMetadata> = provider
        .list(&prefixed)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(prefixed.len(), 7);
    let attributes = ListOptions {
        include_attributes: true,
        ..Default::default()
    };
    assert_eq!(
        provider
            .list(&attributes)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        names.len()
    );
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn zero_multipart_boundaries_length_failure_and_cancellation() {
    let provider = Arc::new(test_provider("multipart"));
    for size in [
        0,
        5 * 1024 * 1024 - 1,
        5 * 1024 * 1024,
        5 * 1024 * 1024 + 1,
        10 * 1024 * 1024 + 1,
    ] {
        let object = key(&format!("size-{size}"));
        let value = Bytes::from(vec![42; size]);
        put(&provider, &object, value.clone(), &WriteOptions::default()).await;
        assert_eq!(read(&provider, &object).await, value);
    }
    let mismatch = key("wrong-length");
    let error = provider
        .put(
            &mismatch,
            input(Bytes::from(vec![0; 6 * 1024 * 1024])),
            Some(5),
            &WriteOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Integrity);
    assert_eq!(
        provider.head(&mismatch).await.unwrap_err().kind,
        ErrorKind::NotFound
    );
    let failed = key("failed-source");
    let body = stream::iter([
        Ok(Bytes::from(vec![0; 6 * 1024 * 1024])),
        Err(MscError::new(ErrorKind::Io, "simulated source failure")),
    ]);
    assert_eq!(
        provider
            .put(&failed, Box::pin(body), None, &WriteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Io
    );
    assert_eq!(
        provider.head(&failed).await.unwrap_err().kind,
        ErrorKind::NotFound
    );

    let (sender, receiver) = tokio::sync::oneshot::channel();
    let body = stream::once(async move { Ok(Bytes::from(vec![7; 6 * 1024 * 1024])) }).chain(
        stream::once(async move {
            let _ = sender.send(());
            futures::future::pending::<Result<Bytes, MscError>>().await
        }),
    );
    let worker_provider = Arc::clone(&provider);
    let worker = tokio::spawn(async move {
        worker_provider
            .put(
                &key("cancelled"),
                Box::pin(body),
                None,
                &WriteOptions::default(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), receiver)
        .await
        .unwrap()
        .unwrap();
    assert!(
        multipart_upload_count().await > 0,
        "the cancellation test must observe an active multipart upload"
    );
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert_eq!(
        provider.head(&key("cancelled")).await.unwrap_err().kind,
        ErrorKind::NotFound
    );
    provider.shutdown().await.unwrap();
    assert_eq!(
        multipart_upload_count().await,
        0,
        "failed and cancelled uploads must be aborted"
    );
}
async fn multipart_upload_count() -> usize {
    let credentials = credentials();
    let credentials = object_store::aws::AwsCredential {
        key_id: credentials.access_key_id.expose_secret().into(),
        secret_key: credentials.secret_access_key.expose_secret().into(),
        token: None,
    };
    let endpoint = std::env::var("MSC_S3_TEST_ENDPOINT").unwrap();
    let bucket = std::env::var("MSC_S3_TEST_BUCKET").unwrap();
    let url = format!("{endpoint}/{bucket}?uploads");
    let mut request =
        object_store::client::HttpRequest::new(object_store::client::HttpRequestBody::empty());
    *request.uri_mut() = url.parse().unwrap();
    object_store::aws::AwsAuthorizer::new(&credentials, "s3", "us-east-1")
        .try_authorize(&mut request, None)
        .unwrap();
    let response = reqwest::Client::new()
        .get(&url)
        .headers(request.headers().clone())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap();
    response.matches("<Upload>").count()
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn waiting_upload_drains_cancelled_upload_without_deadlock() {
    let provider = Arc::new(test_provider_concurrency("cancel-wakeup", 1));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let body = stream::once(async { Ok(Bytes::from(vec![0; 6 * 1024 * 1024])) }).chain(
        stream::once(async move {
            let _ = sender.send(());
            futures::future::pending::<Result<Bytes, MscError>>().await
        }),
    );
    let first_provider = Arc::clone(&provider);
    let first = tokio::spawn(async move {
        first_provider
            .put(
                &key("cancelled"),
                Box::pin(body),
                None,
                &WriteOptions::default(),
            )
            .await
    });
    receiver.await.unwrap();
    let second_provider = Arc::clone(&provider);
    let mut second = tokio::spawn(async move {
        second_provider
            .put(
                &key("waiting"),
                input(Bytes::from(vec![1; 6 * 1024 * 1024])),
                None,
                &WriteOptions::default(),
            )
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut second)
            .await
            .is_err()
    );
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(10), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    provider.shutdown().await.unwrap();
    assert_eq!(multipart_upload_count().await, 0);
    assert_eq!(
        provider.head(&key("waiting")).await.unwrap().content_length,
        6 * 1024 * 1024
    );
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn synthesized_directories_stay_inside_requested_prefix() {
    let provider = test_provider("prefixes");
    for name in [
        ".hidden",
        "..prefix/file",
        "dot/.file",
        "folder/nested/file",
        "folder/nested/child/file",
        "folder/plain.txt",
        "folder-copy/leak",
        "a/x",
        "a-1/x",
    ] {
        put(
            &provider,
            &key(name),
            Bytes::new(),
            &WriteOptions::default(),
        )
        .await;
    }
    let all: Vec<ObjectMetadata> = provider
        .list(&ListOptions {
            include_directories: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(all.iter().any(|meta| meta.key == key(".hidden")));
    assert!(all.iter().any(|meta| meta.key == key("dot/.file")));
    assert!(all.iter().any(|meta| meta.key == key("..prefix")));
    let options = ListOptions {
        prefix: key("folder/nested"),
        include_directories: true,
        ..Default::default()
    };
    let result: Vec<ObjectMetadata> = provider
        .list(&options)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        result
            .iter()
            .map(|meta| meta.key.clone())
            .collect::<Vec<_>>(),
        [
            "folder/nested/child",
            "folder/nested/child/file",
            "folder/nested/file"
        ]
        .map(key)
    );
    let options = ListOptions {
        prefix: key("folder"),
        recursive: false,
        include_directories: true,
        ..Default::default()
    };
    let result: Vec<ObjectMetadata> = provider
        .list(&options)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        result
            .iter()
            .map(|meta| meta.key.clone())
            .collect::<Vec<_>>(),
        vec![key("folder/nested"), key("folder/plain.txt")]
    );
    let result: Vec<ObjectMetadata> = provider
        .list(&ListOptions {
            prefix: key("folder"),
            recursive: false,
            include_directories: false,
            ..Default::default()
        })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        result
            .iter()
            .map(|meta| meta.key.clone())
            .collect::<Vec<_>>(),
        vec![key("folder/plain.txt")]
    );
    let options = ListOptions {
        prefix: key("a-"),
        include_directories: true,
        ..Default::default()
    };
    let result: Vec<ObjectMetadata> = provider
        .list(&options)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        result
            .iter()
            .map(|meta| meta.key.clone())
            .collect::<Vec<_>>(),
        vec![key("a-1"), key("a-1/x")]
    );
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn live_file_credentials_and_error_redaction() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("credentials.json");
    let credentials = credentials();
    let data = serde_json::json!({ "access_key_id": credentials.access_key_id.expose_secret(), "secret_access_key": credentials.secret_access_key.expose_secret() });
    tokio::fs::write(&file, serde_json::to_vec(&data).unwrap())
        .await
        .unwrap();
    let options = S3Options {
        bucket: Some(std::env::var("MSC_S3_TEST_BUCKET").unwrap()),
        base_path: "credentials".into(),
        endpoint_url: Some(std::env::var("MSC_S3_TEST_ENDPOINT").unwrap()),
        ..Default::default()
    };
    let provider = S3Provider::new_with_credentials_provider(
        options,
        Arc::new(FileCredentialsProvider::new(&file)),
    )
    .unwrap();
    put(
        &provider,
        &key("object"),
        Bytes::new(),
        &WriteOptions::default(),
    )
    .await;
    let rotated = directory.path().join("rotated.json");
    tokio::fs::write(
        &rotated,
        br#"{"access_key_id":"invalid-access-secret","secret_access_key":"invalid-key-secret"}"#,
    )
    .await
    .unwrap();
    tokio::fs::rename(&rotated, &file).await.unwrap();
    let error = provider.head(&key("object")).await.unwrap_err();
    assert!(matches!(
        error.kind,
        ErrorKind::PermissionDenied | ErrorKind::Unauthenticated
    ));
    let formatted = format!("{error:?} {error}");
    assert!(!formatted.contains("invalid-access-secret"));
    assert!(!formatted.contains("invalid-key-secret"));
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn configured_storage_client_crud_listing_and_seekable_reader() {
    let prefix = "facade/objects";
    let client = configured_client(prefix).await;
    configured_object_contract(client, prefix).await;
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn configured_s8k_client_crud_listing_and_seekable_reader() {
    let prefix = "facade/s8k-objects";
    let client = configured_client_with_tags(prefix, "s8k", "S3Credentials").await;
    let body = Bytes::from_static(b"RIFF\x00\x00\x00\x00WAVE");
    let directory = tempfile::tempdir().unwrap();
    let local = directory.path().join("audio.wav");
    tokio::fs::write(&local, &body).await.unwrap();
    for (path, options, expected) in [
        ("inferred.wav", WriteOptions::default(), "audio/x-wav"),
        (
            "explicit.wav",
            WriteOptions {
                content_type: Some("audio/wav".into()),
                ..WriteOptions::default()
            },
            "audio/wav",
        ),
    ] {
        for metadata in [
            client
                .write(path, body.clone(), options.clone())
                .await
                .unwrap(),
            client.upload_from(path, &local, options).await.unwrap(),
        ] {
            assert_eq!(
                metadata
                    .provider_fields
                    .get("content_type")
                    .map(String::as_str),
                Some(expected)
            );
        }
        assert_eq!(client.read(path).await.unwrap(), body);
        client
            .delete(path, false, DeleteOptions::default())
            .await
            .unwrap();
    }
    let standard = StorageClient::from_provider(Arc::new(
        S3Provider::new(
            S3Options {
                bucket: Some(std::env::var("MSC_S3_TEST_BUCKET").unwrap()),
                base_path: "facade/s3-mime".into(),
                endpoint_url: Some(std::env::var("MSC_S3_TEST_ENDPOINT").unwrap()),
                infer_content_type: true,
                ..S3Options::default()
            },
            Some(credentials()),
        )
        .unwrap(),
    ));
    let metadata = standard
        .write("inferred.wav", body, WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(
        metadata
            .provider_fields
            .get("content_type")
            .map(String::as_str),
        Some("audio/wav")
    );
    standard
        .delete("inferred.wav", false, DeleteOptions::default())
        .await
        .unwrap();
    standard.shutdown().await.unwrap();
    configured_object_contract(client, prefix).await;
}

async fn configured_object_contract(client: StorageClient, prefix: &str) {
    assert_eq!(
        client.info("", InfoOptions::default()).await.unwrap().kind,
        ObjectKind::Directory
    );
    assert!(client.is_empty("").await.unwrap());
    let attributes = BTreeMap::from([("owner".into(), "facade".into())]);
    let options = WriteOptions {
        if_none_match: Some("*".into()),
        attributes: attributes.clone(),
        ..WriteOptions::default()
    };
    let created = client
        .write(
            "nested//hello.txt",
            Bytes::from_static(b"0123456789"),
            options,
        )
        .await
        .unwrap();
    assert_eq!(created.key, key("nested/hello.txt"));
    assert_eq!(created.attributes, attributes);
    assert!(created.etag.is_some());
    assert!(client.is_file("nested/hello.txt").await.unwrap());
    assert_eq!(
        client.read("nested/hello.txt").await.unwrap(),
        Bytes::from_static(b"0123456789")
    );
    let physical = test_provider("");
    assert_eq!(
        physical
            .head(&key(&format!("{prefix}/nested/hello.txt")))
            .await
            .unwrap()
            .content_length,
        10
    );
    assert_eq!(
        client
            .read_range(
                "nested/hello.txt",
                ByteRange::new(2, 4).unwrap(),
                ReadOptions::default()
            )
            .await
            .unwrap(),
        Bytes::from_static(b"2345")
    );
    assert_eq!(
        client
            .read_range(
                "nested/hello.txt",
                ByteRange::new(8, 99).unwrap(),
                ReadOptions::default()
            )
            .await
            .unwrap(),
        Bytes::from_static(b"89")
    );
    assert_eq!(
        client
            .read_with_options(
                "nested/hello.txt",
                ReadOptions {
                    max_in_memory_bytes: 5,
                    ..ReadOptions::default()
                }
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    let mut reader = client
        .reader(
            "nested/hello.txt",
            ReadOptions {
                buffer_size: Some(3),
                ..ReadOptions::default()
            },
        )
        .await
        .unwrap();
    reader.seek(SeekFrom::Start(4)).await.unwrap();
    let mut two = [0; 2];
    reader.read_exact(&mut two).await.unwrap();
    assert_eq!(&two, b"45");
    reader.seek(SeekFrom::End(-2)).await.unwrap();
    reader.read_exact(&mut two).await.unwrap();
    assert_eq!(&two, b"89");
    let updated = client
        .write(
            "nested/hello.txt",
            Bytes::from_static(b"abcdefghij"),
            WriteOptions {
                if_match: created.etag,
                attributes: attributes.clone(),
                content_type: Some("text/plain".into()),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap();
    reader.seek(SeekFrom::Start(0)).await.unwrap();
    let error = reader.read(&mut two).await.unwrap_err();
    assert_eq!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<MscError>()
            .unwrap()
            .kind,
        ErrorKind::PreconditionFailed
    );
    let copied = client
        .copy("nested/hello.txt", "copy.txt", CopyOptions::default())
        .await
        .unwrap();
    assert_eq!(copied.attributes, attributes);
    assert_eq!(
        copied
            .provider_fields
            .get("content_type")
            .map(String::as_str),
        Some("text/plain")
    );
    let replaced_attributes = BTreeMap::from([("owner".into(), "replacement".into())]);
    let streamed = client
        .copy(
            "nested/hello.txt",
            "z-conditional-copy.txt",
            CopyOptions {
                source_conditions: GetConditions {
                    if_match: updated.etag,
                    ..GetConditions::default()
                },
                destination: WriteOptions {
                    if_none_match: Some("*".into()),
                    attributes: replaced_attributes.clone(),
                    content_type: Some("application/json".into()),
                    ..WriteOptions::default()
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(streamed.attributes, replaced_attributes);
    assert_eq!(
        streamed
            .provider_fields
            .get("content_type")
            .map(String::as_str),
        Some("application/json")
    );
    assert_eq!(
        client.read("z-conditional-copy.txt").await.unwrap(),
        Bytes::from_static(b"abcdefghij")
    );
    let listed: Vec<ObjectMetadata> = client
        .list_recursive(ListOptions::default())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let expected = ["copy.txt", "nested/hello.txt", "z-conditional-copy.txt"].map(key);
    assert_eq!(
        listed.into_iter().map(|meta| meta.key).collect::<Vec<_>>(),
        expected
    );
    let matching: Vec<ObjectMetadata> = client
        .glob("*.txt", ListOptions::default())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        matching
            .into_iter()
            .map(|meta| meta.key)
            .collect::<Vec<_>>(),
        expected
    );
    let bounded: Vec<ObjectMetadata> = client
        .list(ListOptions {
            start_after: Some(key("copy.txt")),
            end_at: Some(key("nested/hello.txt")),
            ..ListOptions::default()
        })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(bounded.len(), 1);
    assert_eq!(bounded[0].key, key("nested/hello.txt"));
    let signed = client
        .presigned_url("copy.txt", PresignMethod::Get, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(
        reqwest::get(signed).await.unwrap().bytes().await.unwrap(),
        Bytes::from_static(b"abcdefghij")
    );
    assert_eq!(
        client
            .delete("nested", true, DeleteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    assert!(client.is_file("nested/hello.txt").await.unwrap());
    client
        .delete("nested/hello.txt", false, DeleteOptions::default())
        .await
        .unwrap();
    assert!(!client.is_file("nested/hello.txt").await.unwrap());
    let deleted = client
        .delete_many(
            &[
                key("copy.txt"),
                key("z-conditional-copy.txt"),
                key("missing"),
            ],
            DeleteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.succeeded, [0, 1, 2]);
    assert!(deleted.failed.is_empty());
    assert!(client.is_empty("").await.unwrap());
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn configured_storage_client_writer_file_transfers_and_batches() {
    let client = configured_client("facade/files").await;
    let mut writer = client
        .writer("finished", Some(6 * 1024 * 1024), WriteOptions::default())
        .await
        .unwrap();
    let chunk = vec![42_u8; 64 * 1024];
    for _ in 0..96 {
        writer.write_all(&chunk).await.unwrap();
    }
    writer.flush().await.unwrap();
    assert!(!client.is_file("finished").await.unwrap());
    writer.shutdown().await.unwrap();
    assert!(!client.is_file("finished").await.unwrap());
    assert_eq!(
        writer.finish().await.unwrap().content_length,
        6 * 1024 * 1024
    );
    assert_eq!(
        client
            .read_range(
                "finished",
                ByteRange::new(0, 16).unwrap(),
                ReadOptions::default()
            )
            .await
            .unwrap(),
        Bytes::from(vec![42; 16])
    );
    let mut dropped = client
        .writer("dropped", None, WriteOptions::default())
        .await
        .unwrap();
    dropped.write_all(b"unpublished").await.unwrap();
    drop(dropped);
    assert!(!client.is_file("dropped").await.unwrap());
    let mut aborted = client
        .writer("aborted", None, WriteOptions::default())
        .await
        .unwrap();
    aborted.write_all(b"unpublished").await.unwrap();
    aborted.abort().await.unwrap();
    assert!(!client.is_file("aborted").await.unwrap());

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let mut file = tokio::fs::File::create(&source).await.unwrap();
    for _ in 0..96 {
        file.write_all(&chunk).await.unwrap();
    }
    file.flush().await.unwrap();
    drop(file);
    assert_eq!(
        client
            .upload_from("uploaded", &source, WriteOptions::default())
            .await
            .unwrap()
            .content_length,
        6 * 1024 * 1024
    );
    let destination = directory.path().join("destination");
    tokio::fs::write(&destination, b"original").await.unwrap();
    assert_eq!(
        client
            .download_to("missing", &destination, ReadOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"original");
    client
        .download_to("uploaded", &destination, ReadOptions::default())
        .await
        .unwrap();
    let downloaded = tokio::fs::read(&destination).await.unwrap();
    assert_eq!(downloaded.len(), 6 * 1024 * 1024);
    assert!(downloaded.iter().all(|byte| *byte == 42));
    drop(downloaded);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 2);

    let small = directory.path().join("small");
    tokio::fs::write(&small, b"batch-content").await.unwrap();
    let uploads = [
        (key("batch/one"), small.clone()),
        (key("batch/missing"), directory.path().join("absent-source")),
        (key("batch/three"), small),
    ];
    let uploaded = client
        .upload_many(&uploads, WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(uploaded.succeeded, [0, 2]);
    assert_eq!(uploaded.failed.len(), 1);
    assert_eq!(uploaded.failed[0].index, 1);
    assert_eq!(uploaded.failed[0].destination, Some(key("batch/missing")));
    let downloads = [
        (key("batch/one"), directory.path().join("one")),
        (key("batch/missing"), directory.path().join("missing")),
        (key("batch/three"), directory.path().join("three")),
    ];
    let downloaded = client
        .download_many(&downloads, ReadOptions::default())
        .await
        .unwrap();
    assert_eq!(downloaded.succeeded, [0, 2]);
    assert_eq!(downloaded.failed.len(), 1);
    assert_eq!(downloaded.failed[0].index, 1);
    assert_eq!(downloaded.failed[0].source, key("batch/missing"));
    assert_eq!(
        tokio::fs::read(&downloads[0].1).await.unwrap(),
        b"batch-content"
    );
    assert_eq!(
        tokio::fs::read(&downloads[2].1).await.unwrap(),
        b"batch-content"
    );
    assert!(!downloads[1].1.exists());
    let cloned = client.clone();
    let (first, third) = tokio::join!(client.read("batch/one"), cloned.read("batch/three"));
    assert_eq!(first.unwrap(), third.unwrap());
    let deleted = client
        .delete_many(
            &[key("batch/one"), key("batch/three")],
            DeleteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.succeeded, [0, 1]);
    assert!(deleted.failed.is_empty());
    assert!(client.is_empty("batch").await.unwrap());
    assert!(client.is_file("uploaded").await.unwrap());
    client.shutdown().await.unwrap();
    assert_eq!(multipart_upload_count().await, 0);
}

async fn client_listing(client: &StorageClient, options: ListOptions) -> Vec<ObjectMetadata> {
    client
        .list(options)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn configured_storage_client_stream_conditions_and_literal_url_keys() {
    let client = configured_client("facade/stream-conditions").await;
    let path = "日本語//%28sici%29+ spaces?#✅.bin";
    let value = Bytes::from_static("Unicode payload ✅\n0123456789".as_bytes());
    let metadata = client
        .write(path, value.clone(), WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(metadata.key, key(path));
    assert_eq!(metadata.kind, ObjectKind::File);
    assert_eq!(metadata.content_length, value.len() as u64);
    assert!(metadata.last_modified.is_none());
    assert!(
        client
            .info(path, InfoOptions::default())
            .await
            .unwrap()
            .last_modified
            .is_some()
    );
    assert!(metadata.source_version.is_some());
    assert!(
        !client
            .is_file("日本語/(sici)+ spaces?#✅.bin")
            .await
            .unwrap()
    );
    let stored = client.info(path, InfoOptions::default()).await.unwrap();
    assert_eq!(stored.etag, metadata.etag);
    assert_eq!(stored.source_version, metadata.source_version);
    assert_eq!(stored.content_length, metadata.content_length);
    assert_eq!(stored.attributes, metadata.attributes);

    {
        let options = ReadOptions {
            max_in_memory_bytes: 1,
            conditions: GetConditions {
                version: metadata.source_version.clone(),
                ..Default::default()
            },
            ..Default::default()
        };
        let body = client.read_stream(path, options).await.unwrap();
        let chunks: Vec<_> = body.try_collect().await.unwrap();
        assert_eq!(Bytes::from(chunks.concat()), value);
    }
    assert_eq!(
        client
            .read_with_options(
                path,
                ReadOptions {
                    max_in_memory_bytes: value.len() as u64,
                    ..Default::default()
                }
            )
            .await
            .unwrap(),
        value
    );
    assert_eq!(
        client
            .read_range(
                path,
                ByteRange::new(value.len() as u64 - 3, 99).unwrap(),
                ReadOptions {
                    conditions: GetConditions {
                        version: metadata.source_version.clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                }
            )
            .await
            .unwrap(),
        Bytes::from_static(b"789")
    );

    let options = ReadOptions {
        conditions: GetConditions {
            if_none_match: metadata.etag.clone(),
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        client
            .read_with_options(path, options.clone())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotModified
    );
    assert_eq!(
        client
            .read_stream(path, options.clone())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::NotModified
    );
    assert_eq!(
        client
            .read_stream(path, options.clone())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::NotModified
    );
    assert_eq!(
        client
            .read_range(path, ByteRange::new(0, 1).unwrap(), options)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotModified
    );

    let mut wrong_scope = metadata.source_version.clone().unwrap();
    wrong_scope.scope = "s3:another-endpoint:another-bucket:us-east-1".into();
    let mut invalid_version = metadata.source_version.clone().unwrap();
    invalid_version.value = "not-an-s3-generation".into();
    for conditions in [
        GetConditions {
            version: Some(wrong_scope),
            ..Default::default()
        },
        GetConditions {
            version: Some(invalid_version),
            ..Default::default()
        },
        GetConditions {
            if_match: Some("\"different\"".into()),
            version: metadata.source_version.clone(),
            ..Default::default()
        },
    ] {
        let options = ReadOptions {
            conditions,
            ..Default::default()
        };
        assert_eq!(
            client
                .read_stream(path, options.clone())
                .await
                .err()
                .unwrap()
                .kind,
            ErrorKind::PreconditionFailed
        );
        assert_eq!(
            client
                .read_range(path, ByteRange::new(0, 1).unwrap(), options)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::PreconditionFailed
        );
    }
    client
        .write(
            path,
            Bytes::from_static(b"replacement"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        client
            .read_stream(
                path,
                ReadOptions {
                    conditions: GetConditions {
                        version: metadata.source_version,
                        ..Default::default()
                    },
                    ..Default::default()
                }
            )
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::PreconditionFailed
    );
    client
        .write("empty", Bytes::new(), WriteOptions::default())
        .await
        .unwrap();
    {
        let body = client
            .read_stream("empty", ReadOptions::default())
            .await
            .unwrap();
        assert!(
            body.try_collect::<Vec<_>>()
                .await
                .unwrap()
                .concat()
                .is_empty()
        );
    }
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn configured_storage_client_multipart_attributes_and_condition_boundaries() {
    let client = configured_client("facade/multipart-attributes").await;
    let attributes = BTreeMap::from([
        ("k".repeat(32), serde_json::Value::String("v".repeat(128))),
        ("etag".into(), "custom-etag-is-not-the-generation".into()),
        ("empty".into(), String::new().into()),
    ]);
    let options = WriteOptions {
        attributes: attributes.clone(),
        content_type: Some("application/octet-stream".into()),
        ..Default::default()
    };
    for size in [5 * 1024 * 1024 - 1, 5 * 1024 * 1024, 5 * 1024 * 1024 + 1] {
        let path = format!("size-{size}");
        let body = vec![42; size];
        let mut writer = client
            .writer(&path, Some(1), options.clone())
            .await
            .unwrap();
        writer.write_all(&body[..size / 2]).await.unwrap();
        writer.write_all(&body[size / 2..]).await.unwrap();
        assert_eq!(writer.bytes_written(), size as u64);
        let metadata = writer.finish().await.unwrap();
        assert_eq!(metadata.content_length, size as u64);
        assert_eq!(metadata.attributes, attributes);
        assert_eq!(
            metadata
                .provider_fields
                .get("content_type")
                .map(String::as_str),
            Some("application/octet-stream")
        );
        assert_ne!(
            metadata.etag.as_deref(),
            Some("custom-etag-is-not-the-generation")
        );
        let chunks: Vec<_> = client
            .read_stream(&path, ReadOptions::default())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.concat(), body);
    }
    let listed = client_listing(
        &client,
        ListOptions {
            include_attributes: true,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(listed.len(), 3);
    assert!(
        listed
            .iter()
            .all(|metadata| metadata.attributes == attributes)
    );

    let threshold = 5 * 1024 * 1024;
    let exact_body = Bytes::from(vec![7; threshold]);
    let exact_path = "exact-threshold-conditional";
    let created = client
        .write(
            exact_path,
            exact_body.clone(),
            WriteOptions {
                if_none_match: Some("*".into()),
                ..options.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(created.content_length, threshold as u64);
    assert_eq!(created.attributes, attributes);
    assert_eq!(client.read(exact_path).await.unwrap(), exact_body);
    let conflict = client
        .write(
            exact_path,
            exact_body.clone(),
            WriteOptions {
                if_none_match: Some("*".into()),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        conflict.kind,
        ErrorKind::PreconditionFailed | ErrorKind::AmbiguousOutcome
    ));
    assert_eq!(client.read(exact_path).await.unwrap(), exact_body);
    let original_etag = created.etag.expect("conditional creation returns an ETag");
    let replacement = Bytes::from(vec![9; threshold]);
    let overwritten = client
        .write(
            exact_path,
            replacement.clone(),
            WriteOptions {
                if_match: Some(original_etag.clone()),
                ..options.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(overwritten.content_length, threshold as u64);
    assert_eq!(overwritten.attributes, attributes);
    assert_ne!(overwritten.etag.as_deref(), Some(original_etag.as_str()));
    assert_eq!(client.read(exact_path).await.unwrap(), replacement);
    let conflict = client
        .write(
            exact_path,
            exact_body,
            WriteOptions {
                if_match: Some(original_etag),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        conflict.kind,
        ErrorKind::PreconditionFailed | ErrorKind::AmbiguousOutcome
    ));
    assert_eq!(client.read(exact_path).await.unwrap(), replacement);

    let original = client
        .write("conditional", Bytes::from_static(b"original"), options)
        .await
        .unwrap();
    let payload = Bytes::from(vec![7; threshold + 1]);
    for (path, options) in [
        (
            "conditional",
            WriteOptions {
                if_match: original.etag.clone(),
                ..Default::default()
            },
        ),
        (
            "create-only",
            WriteOptions {
                if_none_match: Some("*".into()),
                ..Default::default()
            },
        ),
    ] {
        assert_eq!(
            client
                .write(path, payload.clone(), options)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Unsupported
        );
    }
    assert_eq!(
        client.read("conditional").await.unwrap(),
        Bytes::from_static(b"original")
    );
    assert!(!client.is_file("create-only").await.unwrap());
    assert_eq!(multipart_upload_count().await, 0);

    for invalid in [
        WriteOptions {
            attributes: BTreeMap::from([("invalid header name".into(), "value".into())]),
            ..Default::default()
        },
        WriteOptions {
            attributes: BTreeMap::from([("key".into(), "value\r\ninjected: header".into())]),
            ..Default::default()
        },
        WriteOptions {
            content_type: Some("text/plain\nheader".into()),
            ..Default::default()
        },
    ] {
        assert_eq!(
            client
                .write("conditional", Bytes::from_static(b"bad"), invalid)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::InvalidConfig
        );
        assert_eq!(
            client.read("conditional").await.unwrap(),
            Bytes::from_static(b"original")
        );
    }
    let overwritten = client
        .write("conditional", Bytes::new(), WriteOptions::default())
        .await
        .unwrap();
    assert!(overwritten.attributes.is_empty());
    client
        .delete(exact_path, false, DeleteOptions::default())
        .await
        .unwrap();
    assert!(!client.is_file(exact_path).await.unwrap());
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn configured_storage_client_presigned_put_get_and_expiration_boundaries() {
    let client = configured_client("facade/presigned").await;
    let path = "日本語/%28name%29+ space?#.txt";
    let signed = client
        .presigned_url(path, PresignMethod::Put, Duration::from_secs(60))
        .await
        .unwrap();
    let response = reqwest::Client::new()
        .put(signed)
        .header("content-type", "text/plain")
        .body("Unicode ✅")
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let metadata = client.info(path, InfoOptions::default()).await.unwrap();
    assert!(metadata.attributes.is_empty());
    assert_eq!(
        metadata
            .provider_fields
            .get("content_type")
            .map(String::as_str),
        Some("text/plain")
    );
    assert_eq!(
        client.read(path).await.unwrap(),
        Bytes::from_static("Unicode ✅".as_bytes())
    );
    for expires in [
        Duration::from_secs(1),
        Duration::from_secs(60),
        Duration::from_secs(7 * 24 * 60 * 60),
    ] {
        let signed = client
            .presigned_url(path, PresignMethod::Get, expires)
            .await
            .unwrap();
        let url = url::Url::parse(&signed).unwrap();
        assert_eq!(
            url.query_pairs()
                .find(|(key, _)| key.eq_ignore_ascii_case("x-amz-expires"))
                .unwrap()
                .1,
            expires.as_secs().to_string()
        );
        if expires.as_secs() > 1 {
            assert_eq!(
                reqwest::get(signed)
                    .await
                    .unwrap()
                    .error_for_status()
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                Bytes::from_static("Unicode ✅".as_bytes())
            );
        }
    }
    for expires in [
        Duration::ZERO,
        Duration::from_millis(999),
        Duration::from_secs(7 * 24 * 60 * 60 + 1),
    ] {
        for method in [PresignMethod::Get, PresignMethod::Put] {
            assert_eq!(
                client
                    .presigned_url(path, method, expires)
                    .await
                    .unwrap_err()
                    .kind,
                ErrorKind::InvalidConfig
            );
        }
    }
    client
        .delete(path, false, DeleteOptions::default())
        .await
        .unwrap();
    let signed = client
        .presigned_url(path, PresignMethod::Get, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(
        reqwest::get(signed).await.unwrap().status(),
        reqwest::StatusCode::NOT_FOUND
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn configured_storage_client_batch_transfers_and_delete_chunk_indices() {
    let client = configured_client("facade/batch-boundaries").await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    tokio::fs::write(&source, b"batch content").await.unwrap();
    let paths: Vec<_> = (0..100)
        .map(|index| key(&format!("batch/file-{index:04}")))
        .collect();
    let uploads: Vec<_> = paths
        .iter()
        .map(|path| (path.clone(), source.clone()))
        .collect();
    let attributes = BTreeMap::from([("batch".into(), "100-files".into())]);
    let uploaded = client
        .upload_many(
            &uploads,
            WriteOptions {
                attributes: attributes.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(uploaded.succeeded, (0..100).collect::<Vec<_>>());
    assert!(uploaded.failed.is_empty());
    let listed = client_listing(
        &client,
        ListOptions {
            include_attributes: true,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        listed
            .iter()
            .map(|item| item.key.clone())
            .collect::<Vec<_>>(),
        paths
    );
    assert!(
        listed
            .iter()
            .all(|item| item.attributes == attributes && item.content_length == 13)
    );
    let downloads: Vec<_> = paths
        .iter()
        .enumerate()
        .map(|(index, path)| {
            (
                path.clone(),
                directory.path().join(format!("download-{index:04}")),
            )
        })
        .collect();
    let downloaded = client
        .download_many(
            &downloads,
            ReadOptions {
                max_in_memory_bytes: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(downloaded.succeeded, (0..100).collect::<Vec<_>>());
    assert!(downloaded.failed.is_empty());
    for (_, local) in &downloads {
        assert_eq!(tokio::fs::read(local).await.unwrap(), b"batch content");
    }
    let mut deletion_keys: Vec<_> = (0..1002)
        .map(|index| key(&format!("missing-{index:04}")))
        .collect();
    deletion_keys[..100].clone_from_slice(&paths);
    deletion_keys[999] = paths[0].clone();
    deletion_keys[1000] = paths[99].clone();
    deletion_keys[1001] = ObjectPath::default();
    for _ in 0..2 {
        let deleted = client
            .delete_many(&deletion_keys, DeleteOptions::default())
            .await
            .unwrap();
        assert_eq!(deleted.succeeded, (0..1001).collect::<Vec<_>>());
        assert_eq!(deleted.failed.len(), 1);
        assert_eq!(deleted.failed[0].index, 1001);
        assert_eq!(deleted.failed[0].source, ObjectPath::default());
        assert!(deleted.failed[0].destination.is_none());
        assert_eq!(deleted.failed[0].error.kind, ErrorKind::InvalidPath);
        assert!(client.is_empty("").await.unwrap());
    }
    assert!(
        client
            .upload_many(&[], WriteOptions::default())
            .await
            .unwrap()
            .succeeded
            .is_empty()
    );
    assert!(
        client
            .download_many(&[], ReadOptions::default())
            .await
            .unwrap()
            .succeeded
            .is_empty()
    );
    assert!(
        client
            .delete_many(&[], DeleteOptions::default())
            .await
            .unwrap()
            .succeeded
            .is_empty()
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires local storage simulators; run just run-integration-tests"]
async fn configured_storage_client_unsupported_conditions_preserve_objects() {
    let client = configured_client("facade/unsupported-conditions").await;
    let metadata = client
        .write(
            "existing",
            Bytes::from_static(b"original"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    for options in [
        WriteOptions {
            if_none_match: metadata.etag.clone(),
            ..Default::default()
        },
        WriteOptions {
            if_match: metadata.etag.clone(),
            if_none_match: Some("*".into()),
            ..Default::default()
        },
    ] {
        assert_eq!(
            client
                .write("existing", Bytes::from_static(b"replacement"), options)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Unsupported
        );
    }
    for options in [
        DeleteOptions {
            if_none_match: metadata.etag.clone(),
            ..Default::default()
        },
        DeleteOptions {
            if_match: metadata.etag.clone(),
            if_none_match: Some("*".into()),
            ..Default::default()
        },
    ] {
        assert_eq!(
            client
                .delete("existing", false, options.clone())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Unsupported
        );
        let paths = [key("existing"), key("missing")];
        let batch = client.delete_many(&paths, options).await.unwrap();
        assert!(batch.succeeded.is_empty());
        assert_eq!(batch.failed.len(), 2);
        for (index, failure) in batch.failed.iter().enumerate() {
            assert_eq!(failure.index, index);
            assert_eq!(failure.source, paths[index]);
            assert_eq!(failure.error.kind, ErrorKind::Unsupported);
        }
        assert_eq!(
            client.read("existing").await.unwrap(),
            Bytes::from_static(b"original")
        );
    }
    let link = client.make_symlink("link", "existing").await.unwrap();
    assert_eq!(link.kind, ObjectKind::Symlink);
    assert_eq!(client.read("link").await.unwrap(), "original");
    let empty_attributes = BTreeMap::from([("owner".into(), "empty-file".into())]);
    client
        .write(
            "empty",
            Bytes::new(),
            WriteOptions {
                attributes: empty_attributes.clone(),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap();
    let provider = test_provider("facade/unsupported-conditions");
    for recursive in [true, false] {
        for include_attributes in [false, true] {
            let entries = provider
                .list(&ListOptions {
                    recursive,
                    include_attributes,
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
                ["empty", "existing", "link"]
            );
            assert_eq!(entries[0].kind, ObjectKind::File);
            assert_eq!(entries[0].content_length, 0);
            assert!(entries[0].symlink_target.is_none());
            if include_attributes {
                assert_eq!(entries[0].attributes, empty_attributes);
            } else {
                assert!(entries[0].attributes.is_empty());
            }
            assert_eq!(entries[2].kind, ObjectKind::Symlink);
            assert_eq!(entries[2].content_length, 0);
            assert_eq!(entries[2].symlink_target.as_deref(), Some("existing"));
            assert!(entries[2].attributes.is_empty());
        }
    }
    let signed = client
        .presigned_url("link", PresignMethod::Get, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(
        reqwest::get(signed)
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "original"
    );
    client
        .delete("link", false, DeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(client.read("existing").await.unwrap(), "original");
    client
        .delete("existing", false, DeleteOptions::default())
        .await
        .unwrap();
    client
        .delete("empty", false, DeleteOptions::default())
        .await
        .unwrap();
    client
        .delete("missing", false, DeleteOptions::default())
        .await
        .unwrap();
    assert!(client.is_empty("").await.unwrap());
    client.shutdown().await.unwrap();
}
