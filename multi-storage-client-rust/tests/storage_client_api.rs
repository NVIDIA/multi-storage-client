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

//! Public API contracts adapted from the Python client's shared end-to-end suite.
#![cfg(unix)]
#![allow(clippy::too_many_lines)] // Keep each public API contract scenario together.

#[path = "common/provider_contract.rs"]
mod provider_contract;

use bytes::Bytes;
use futures::TryStreamExt;
use multi_storage_client::{
    ByteRange, ByteStream, ConfigLoader, DeleteOptions, ErrorKind, GetConditions, InfoOptions,
    ListOptions, MetadataStream, ObjectKind, ObjectMetadata, ObjectPath, ProviderCapabilities,
    PutResult, ReadOptions, Result, StorageBackend, StorageClient, StorageClientBuilder,
    WriteOptions,
};
use serde_json::json;
use std::{collections::BTreeMap, path::Path};

fn key(value: &str) -> ObjectPath {
    ObjectPath::new(value).unwrap()
}

async fn client(root: &Path) -> StorageClient {
    let config_file = root.join("../config.json");
    std::fs::write(
        &config_file,
        serde_json::to_vec(&json!({"profiles":{"local":{"storage_provider":{
            "type":"file","options":{"base_path":root}
        }}}}))
        .unwrap(),
    )
    .unwrap();
    StorageClient::builder(
        ConfigLoader::new()
            .with_explicit_paths([config_file])
            .load()
            .unwrap(),
        "local",
    )
    .build()
    .await
    .unwrap()
}

async fn fixture() -> (tempfile::TempDir, StorageClient) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("storage");
    std::fs::create_dir(&root).unwrap();
    let client = client(&root).await;
    (directory, client)
}

async fn keys(client: &StorageClient, options: ListOptions) -> Vec<String> {
    client
        .list(options)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|item| item.key.to_string())
        .collect()
}

#[tokio::test]
async fn empty_missing_and_directory_objects_have_distinct_public_behavior() {
    let (directory, client) = fixture().await;
    assert_eq!(
        client.info("", InfoOptions::default()).await.unwrap().kind,
        ObjectKind::Directory
    );
    assert!(!client.is_file("").await.unwrap());
    assert!(client.is_empty("").await.unwrap());
    for path in ["missing", "missing/", "missing/deep/file"] {
        assert!(!client.is_file(path).await.unwrap());
        assert!(client.is_empty(path).await.unwrap());
        assert_eq!(
            client
                .info(path, InfoOptions::default())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::NotFound
        );
        assert_eq!(
            client
                .read(path, ReadOptions::default())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::NotFound
        );
    }
    assert!(!directory.path().join("storage/missing").exists());
    client
        .write("nested/empty", Bytes::new(), WriteOptions::default())
        .await
        .unwrap();
    let metadata = client
        .info("nested/empty", InfoOptions::default())
        .await
        .unwrap();
    assert_eq!(metadata.content_length, 0);
    assert_eq!(metadata.kind, ObjectKind::File);
    assert!(metadata.source_version.is_some());
    assert!(client.is_file("nested/empty/").await.unwrap());
    assert!(!client.is_file("nested").await.unwrap());
    assert_eq!(
        client
            .info("nested", InfoOptions { strict: false })
            .await
            .unwrap()
            .kind,
        ObjectKind::Directory
    );
    assert!(!client.is_empty("nested/empty").await.unwrap());
    assert!(
        client
            .read("nested/empty", ReadOptions::default())
            .await
            .unwrap()
            .is_empty()
    );
    for path in ["", "nested"] {
        assert_eq!(
            client
                .read(path, ReadOptions::default())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::InvalidPath
        );
    }
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn normalized_unicode_keys_and_replacement_attributes_round_trip() {
    let (_directory, client) = fixture().await;
    let body = Bytes::from("研究 🌍\nline two\0");
    let attrs = if cfg!(any(target_os = "linux", target_os = "macos")) {
        BTreeMap::from([
            ("dataset".into(), "研究".into()),
            ("empty".into(), String::new().into()),
        ])
    } else {
        BTreeMap::new()
    };
    client
        .write(
            "α//sp ace%?#.txt/",
            body.clone(),
            WriteOptions {
                attributes: attrs.clone(),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap();
    let written = client
        .info("α//sp ace%?#.txt/", InfoOptions::default())
        .await
        .unwrap();
    assert_eq!(written.key, key("α/sp ace%?#.txt"));
    assert_eq!(written.attributes, attrs);
    assert_eq!(
        client
            .read("α/sp ace%?#.txt", ReadOptions::default())
            .await
            .unwrap(),
        body
    );
    let listing = client
        .list(ListOptions {
            include_attributes: true,
            ..ListOptions::default()
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(listing.len(), 1);
    assert_eq!(listing[0].attributes, attrs);
    let replacement_attrs = if cfg!(any(target_os = "linux", target_os = "macos")) {
        BTreeMap::from([("replacement".into(), "yes".into())])
    } else {
        BTreeMap::new()
    };
    client
        .write(
            "α/sp ace%?#.txt",
            Bytes::from_static(b"new"),
            WriteOptions {
                attributes: replacement_attrs.clone(),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap();
    let replacement = client
        .info("α/sp ace%?#.txt", InfoOptions::default())
        .await
        .unwrap();
    assert_ne!(written.source_version, replacement.source_version);
    assert_eq!(replacement.attributes, replacement_attrs);
    assert_eq!(
        client
            .read("α/sp ace%?#.txt", ReadOptions::default())
            .await
            .unwrap(),
        "new"
    );
    client
        .write("α/sp ace%?#.txt", Bytes::new(), WriteOptions::default())
        .await
        .unwrap();
    assert!(
        client
            .info("α/sp ace%?#.txt", InfoOptions::default())
            .await
            .unwrap()
            .attributes
            .is_empty()
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn ranges_clip_at_eof_and_enforce_memory_and_generation_conditions() {
    let (_directory, client) = fixture().await;
    client
        .write(
            "file",
            Bytes::from_static(b"0123456789"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let metadata = client.info("file", InfoOptions::default()).await.unwrap();
    for (offset, length, expected) in [
        (0, 1, "0"),
        (3, 4, "3456"),
        (9, 10, "9"),
        (10, 1, ""),
        (30, 1, ""),
        (5, 0, ""),
    ] {
        assert_eq!(
            client
                .read(
                    "file",
                    ReadOptions {
                        range: Some((ByteRange::new(offset, length).unwrap()).into()),
                        ..ReadOptions::default()
                    }
                )
                .await
                .unwrap(),
            expected
        );
    }
    assert!(
        client
            .read(
                "missing",
                ReadOptions {
                    range: Some((ByteRange::new(0, 0).unwrap()).into()),
                    ..ReadOptions::default()
                }
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        client
            .read(
                "file",
                ReadOptions {
                    range: Some(
                        (ByteRange {
                            offset: u64::MAX,
                            length: 1
                        })
                        .into()
                    ),
                    ..ReadOptions::default()
                }
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidPath
    );
    assert_eq!(
        client
            .read(
                "file",
                ReadOptions {
                    range: Some((ByteRange::new(0, 4).unwrap()).into()),
                    ..ReadOptions {
                        max_in_memory_bytes: Some(3),
                        ..ReadOptions::default()
                    }
                }
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        client
            .read(
                "file",
                ReadOptions {
                    max_in_memory_bytes: Some(9),
                    ..ReadOptions::default()
                }
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        client
            .read(
                "file",
                ReadOptions {
                    max_in_memory_bytes: Some(10),
                    ..ReadOptions::default()
                }
            )
            .await
            .unwrap(),
        "0123456789"
    );
    let pinned = ReadOptions {
        conditions: GetConditions {
            version: metadata.source_version,
            ..GetConditions::default()
        },
        ..ReadOptions::default()
    };
    assert_eq!(
        client
            .read(
                "file",
                ReadOptions {
                    range: Some((ByteRange::new(1, 2).unwrap()).into()),
                    ..pinned.clone()
                }
            )
            .await
            .unwrap(),
        "12"
    );
    client
        .write(
            "file",
            Bytes::from_static(b"replacement"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        client
            .read(
                "file",
                ReadOptions {
                    range: Some((ByteRange::new(1, 2).unwrap()).into()),
                    ..pinned.clone()
                }
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PreconditionFailed
    );
    assert_eq!(
        client.read("file", pinned).await.unwrap_err().kind,
        ErrorKind::PreconditionFailed
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn builder_memory_limits_apply_to_default_reads_and_allow_explicit_overrides() {
    for injected in [false, true] {
        let (directory, original) = fixture().await;
        for (path, bytes) in [("empty", ""), ("boundary", "123"), ("larger", "1234")] {
            original
                .write(path, Bytes::from(bytes.to_owned()), WriteOptions::default())
                .await
                .unwrap();
        }
        original.shutdown().await.unwrap();
        let config = ConfigLoader::new()
            .with_explicit_paths([directory.path().join("config.json")])
            .load()
            .unwrap();
        let builder = || {
            if injected {
                StorageClientBuilder::from_provider(std::sync::Arc::new(
                    multi_storage_client::provider::posix::PosixProvider::new(
                        multi_storage_client::provider::posix::PosixOptions::new(
                            directory.path().join("storage"),
                        ),
                    )
                    .unwrap(),
                ))
            } else {
                StorageClient::builder(config.clone(), "local")
            }
        };
        assert!(
            builder()
                .with_retry_policy(multi_storage_client::RetryPolicy {
                    max_attempts: 0,
                    ..Default::default()
                })
                .is_err()
        );
        let bounded = builder().with_max_in_memory_bytes(3).build().await.unwrap();
        assert_eq!(
            bounded
                .read("boundary", ReadOptions::default())
                .await
                .unwrap(),
            "123"
        );
        assert!(
            bounded
                .read("empty", ReadOptions::default())
                .await
                .unwrap()
                .is_empty()
        );
        for client in [&bounded, &bounded.clone()] {
            assert_eq!(
                client
                    .read("larger", ReadOptions::default())
                    .await
                    .unwrap_err()
                    .kind,
                ErrorKind::Unsupported
            );
        }
        assert_eq!(
            bounded
                .read(
                    "larger",
                    ReadOptions {
                        max_in_memory_bytes: Some(4),
                        ..ReadOptions::default()
                    }
                )
                .await
                .unwrap(),
            "1234"
        );
        assert_eq!(
            bounded
                .read(
                    "larger",
                    ReadOptions {
                        range: Some(ByteRange::new(0, 4).unwrap().into()),
                        max_in_memory_bytes: Some(4),
                        ..ReadOptions::default()
                    }
                )
                .await
                .unwrap(),
            "1234"
        );
        assert_eq!(
            bounded
                .read(
                    "larger",
                    ReadOptions {
                        range: Some(ByteRange::new(0, 4).unwrap().into()),
                        ..ReadOptions::default()
                    }
                )
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Unsupported
        );
        let zero = builder().with_max_in_memory_bytes(0).build().await.unwrap();
        assert!(
            zero.read("empty", ReadOptions::default())
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            zero.read("boundary", ReadOptions::default())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Unsupported
        );
        let unbounded = builder()
            .with_max_in_memory_bytes(u64::MAX)
            .build()
            .await
            .unwrap();
        assert_eq!(
            unbounded
                .read("larger", ReadOptions::default())
                .await
                .unwrap(),
            "1234"
        );
        bounded.shutdown().await.unwrap();
        zero.shutdown().await.unwrap();
        unbounded.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn file_downloads_create_missing_parents_and_preserve_destinations_on_failure() {
    let (directory, client) = fixture().await;
    let body = Bytes::from(vec![42; 131_073]);
    client
        .write("file", body.clone(), WriteOptions::default())
        .await
        .unwrap();
    let target = directory.path().join("downloads/data/downloaded.bin");
    client
        .download_file(
            "file",
            &target,
            ReadOptions {
                max_in_memory_bytes: Some(1),
                ..ReadOptions::default()
            },
        )
        .await
        .unwrap();
    let metadata = client.info("file", InfoOptions::default()).await.unwrap();
    assert_eq!(metadata.content_length, body.len() as u64);
    assert_eq!(std::fs::read(&target).unwrap(), body);
    assert_eq!(
        client
            .download_file("missing", &target, ReadOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(std::fs::read(&target).unwrap(), body);
    let mut wrong_version = metadata.source_version.unwrap();
    wrong_version.value.push_str("-stale");
    assert_eq!(
        client
            .download_file(
                "file",
                &target,
                ReadOptions {
                    conditions: GetConditions {
                        version: Some(wrong_version),
                        ..GetConditions::default()
                    },
                    ..ReadOptions::default()
                }
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PreconditionFailed
    );
    assert_eq!(std::fs::read(&target).unwrap(), body);
    assert_eq!(
        std::fs::read_dir(target.parent().unwrap()).unwrap().count(),
        1
    );
    let missing_target = directory.path().join("uncreated/deep/file");
    assert_eq!(
        client
            .download_file("missing", &missing_target, ReadOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert!(!directory.path().join("uncreated").exists());
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_download_destinations_preserve_existing_data_and_remove_temporary_files() {
    let (directory, client) = fixture().await;
    client
        .write(
            "file",
            Bytes::from_static(b"downloaded"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let parent_file = directory.path().join("parent-file");
    std::fs::write(&parent_file, b"retained parent").unwrap();
    assert!(
        client
            .download_file("file", parent_file.join("child"), ReadOptions::default())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&parent_file).unwrap(), b"retained parent");
    let downloads = directory.path().join("downloads");
    let target = downloads.join("existing-directory");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(target.join("retained"), b"original child").unwrap();
    assert!(
        client
            .download_file("file", &target, ReadOptions::default())
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(target.join("retained")).unwrap(),
        b"original child"
    );
    assert_eq!(std::fs::read_dir(&downloads).unwrap().count(), 1);
    assert_eq!(std::fs::read_dir(&target).unwrap().count(), 1);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn uploads_reject_missing_directory_and_symlink_sources_without_mutation() {
    let (directory, client) = fixture().await;
    let source = directory.path().join("source");
    std::fs::write(&source, b"uploaded").unwrap();
    let attrs = if cfg!(any(target_os = "linux", target_os = "macos")) {
        BTreeMap::from([("owner".into(), "test".into())])
    } else {
        BTreeMap::new()
    };
    client
        .upload_file(
            "nested/uploaded",
            &source,
            WriteOptions {
                attributes: attrs.clone(),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap();
    let uploaded = client
        .info("nested/uploaded", InfoOptions::default())
        .await
        .unwrap();
    assert_eq!(uploaded.content_length, 8);
    assert_eq!(uploaded.attributes, attrs);
    assert_eq!(
        client
            .read("nested/uploaded", ReadOptions::default())
            .await
            .unwrap(),
        "uploaded"
    );
    assert_eq!(
        client
            .upload_file(
                "nested/uploaded",
                directory.path().join("missing"),
                WriteOptions::default()
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        client
            .upload_file("nested/uploaded", directory.path(), WriteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidPath
    );
    let link = directory.path().join("source-link");
    std::os::unix::fs::symlink(&source, &link).unwrap();
    assert_eq!(
        client
            .upload_file("nested/uploaded", &link, WriteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    assert_eq!(
        client
            .read("nested/uploaded", ReadOptions::default())
            .await
            .unwrap(),
        "uploaded"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn batch_delete_preserves_indices_and_treats_missing_objects_as_success() {
    let (_directory, client) = fixture().await;
    let mut paths = (0..40)
        .map(|index| key(&format!("batch/{index:03}")))
        .collect::<Vec<_>>();
    for path in [&paths[0], &paths[39]] {
        client
            .write(
                path,
                Bytes::from_static(b"payload"),
                WriteOptions::default(),
            )
            .await
            .unwrap();
    }
    paths.insert(0, key("missing"));
    for _ in 0..2 {
        let result = client
            .delete_many(&paths, DeleteOptions::default())
            .await
            .unwrap();
        assert_eq!(result.succeeded, (0..paths.len()).collect::<Vec<_>>());
        assert!(result.failed.is_empty());
    }
    assert!(
        client
            .delete_many(&[], DeleteOptions::default())
            .await
            .unwrap()
            .succeeded
            .is_empty()
    );
    assert!(client.is_empty("batch").await.unwrap());
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn listings_respect_exact_paths_bounds_and_directory_options() {
    let (directory, client) = fixture().await;
    for path in [
        "data/a.txt",
        "data/a.txt.bak",
        "data/b.bin",
        "data/sub/c.txt",
        "data-copy/keep.txt",
    ] {
        client
            .write(path, Bytes::new(), WriteOptions::default())
            .await
            .unwrap();
    }
    std::fs::create_dir(directory.path().join("storage/data/empty")).unwrap();
    assert_eq!(
        keys(
            &client,
            ListOptions {
                prefix: key("data/a.txt"),
                ..ListOptions::default()
            }
        )
        .await,
        ["data/a.txt"]
    );
    assert!(
        keys(
            &client,
            ListOptions {
                prefix: key("dat"),
                ..ListOptions::default()
            }
        )
        .await
        .is_empty()
    );
    let immediate = client
        .list(ListOptions {
            prefix: key("data"),
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
        immediate
            .iter()
            .map(|item| item.key.as_str())
            .collect::<Vec<_>>(),
        [
            "data/a.txt",
            "data/a.txt.bak",
            "data/b.bin",
            "data/empty",
            "data/sub"
        ]
    );
    assert_eq!(
        immediate
            .iter()
            .filter(|item| item.kind == ObjectKind::Directory)
            .count(),
        2
    );
    let recursive = client
        .list_recursive(ListOptions {
            prefix: key("data"),
            recursive: false,
            ..ListOptions::default()
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        recursive
            .iter()
            .map(|item| item.key.as_str())
            .collect::<Vec<_>>(),
        [
            "data/a.txt",
            "data/a.txt.bak",
            "data/b.bin",
            "data/sub/c.txt"
        ]
    );
    assert_eq!(
        keys(
            &client,
            ListOptions {
                prefix: key("data"),
                start_after: Some(key("data/a.txt")),
                end_at: Some(key("data/b.bin")),
                ..ListOptions::default()
            }
        )
        .await,
        ["data/a.txt.bak", "data/b.bin"]
    );
    for end in ["data/a.txt", "data/0"] {
        assert_eq!(
            client
                .list(ListOptions {
                    start_after: Some(key("data/a.txt")),
                    end_at: Some(key(end)),
                    ..ListOptions::default()
                })
                .await
                .err()
                .unwrap()
                .kind,
            ErrorKind::InvalidPath
        );
    }
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn symlinks_follow_relative_targets_and_deleting_links_retains_targets() {
    let (directory, client) = fixture().await;
    client
        .write(
            "target",
            Bytes::from_static(b"target payload"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    client.make_symlink("links/alias", "target").await.unwrap();
    assert_eq!(
        std::fs::read_link(directory.path().join("storage/links/alias")).unwrap(),
        Path::new("../target")
    );
    assert!(client.is_file("links/alias").await.unwrap());
    assert_eq!(
        client
            .read("links/alias", ReadOptions::default())
            .await
            .unwrap(),
        "target payload"
    );
    assert_eq!(
        client
            .read(
                "links/alias",
                ReadOptions {
                    range: Some((ByteRange::new(7, 7).unwrap()).into()),
                    ..ReadOptions::default()
                }
            )
            .await
            .unwrap(),
        "payload"
    );
    let listed = client
        .list(ListOptions::default())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let alias = listed
        .iter()
        .find(|item| item.key == key("links/alias"))
        .unwrap();
    assert_eq!(alias.symlink_target.as_deref(), Some("../target"));
    client.make_symlink("dangling", "missing").await.unwrap();
    assert!(!client.is_file("dangling").await.unwrap());
    assert_eq!(
        client
            .read("dangling", ReadOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        client
            .make_symlink("escape", "../outside")
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidPath
    );
    client
        .delete("links/alias", false, DeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(
        client.read("target", ReadOptions::default()).await.unwrap(),
        "target payload"
    );
    client
        .delete("dangling", false, DeleteOptions::default())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_paths_and_unsupported_conditions_never_mutate_existing_objects() {
    let (directory, client) = fixture().await;
    client
        .write(
            "stable",
            Bytes::from_static(b"original"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    for path in [
        "../outside",
        "/absolute",
        "a/../stable",
        "a/./stable",
        "a\\stable",
        "a\0stable",
    ] {
        assert_eq!(
            client
                .read(path, ReadOptions::default())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::InvalidPath
        );
        assert_eq!(
            client
                .write(path, Bytes::new(), WriteOptions::default())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::InvalidPath
        );
        assert_eq!(
            client
                .delete(path, false, DeleteOptions::default())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::InvalidPath
        );
    }
    assert_eq!(
        client
            .delete("", false, DeleteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidPath
    );
    assert_eq!(
        client
            .write("", Bytes::new(), WriteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidPath
    );
    for options in [
        WriteOptions {
            if_match: Some("etag".into()),
            ..WriteOptions::default()
        },
        WriteOptions {
            if_none_match: Some("etag".into()),
            ..WriteOptions::default()
        },
    ] {
        assert_eq!(
            client
                .write("stable", Bytes::new(), options)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Unsupported
        );
    }
    for options in [
        DeleteOptions {
            if_match: Some("etag".into()),
            ..DeleteOptions::default()
        },
        DeleteOptions {
            if_none_match: Some("etag".into()),
            ..DeleteOptions::default()
        },
    ] {
        assert_eq!(
            client
                .delete("stable", false, options)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Unsupported
        );
    }

    assert_eq!(
        client.read("stable", ReadOptions::default()).await.unwrap(),
        "original"
    );
    assert_eq!(
        std::fs::read_dir(directory.path().join("storage"))
            .unwrap()
            .count(),
        1
    );
    client.shutdown().await.unwrap();
}

struct SourceChangingProvider {
    inner: multi_storage_client::provider::posix::PosixProvider,
    source: std::path::PathBuf,
    replace: bool,
}

#[async_trait::async_trait]
impl StorageBackend for SourceChangingProvider {
    fn name(&self) -> &'static str {
        "source-changing-test"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        self.inner.capabilities()
    }
    async fn get_object_metadata(&self, key: &ObjectPath) -> Result<ObjectMetadata> {
        self.inner.get_object_metadata(key).await
    }
    async fn get_object(&self, key: &ObjectPath, conditions: &GetConditions) -> Result<ByteStream> {
        self.inner.get_object(key, conditions).await
    }
    async fn get_object_range(
        &self,
        key: &ObjectPath,
        range: ByteRange,
        conditions: &GetConditions,
    ) -> Result<Bytes> {
        self.inner.get_object_range(key, range, conditions).await
    }
    async fn put_object(
        &self,
        key: &ObjectPath,
        body: ByteStream,
        size: Option<u64>,
        options: &WriteOptions,
    ) -> Result<PutResult> {
        if self.replace {
            std::fs::remove_file(&self.source)?;
            std::fs::write(&self.source, b"new")?;
        } else {
            std::fs::write(&self.source, b"changed size")?;
        }
        self.inner.put_object(key, body, size, options).await
    }
    async fn delete_object(&self, key: &ObjectPath, options: &DeleteOptions) -> Result<()> {
        self.inner.delete_object(key, options).await
    }
    async fn list_objects(&self, options: &ListOptions) -> Result<MetadataStream> {
        self.inner.list_objects(options).await
    }
}

#[tokio::test]
async fn direct_file_upload_retains_its_open_source_and_rejects_in_place_changes() {
    use multi_storage_client::provider::posix::{PosixOptions, PosixProvider};
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::write(&source, b"old").unwrap();
    let client = StorageClientBuilder::from_provider(std::sync::Arc::new(SourceChangingProvider {
        inner: PosixProvider::new(PosixOptions::new(directory.path())).unwrap(),
        source: source.clone(),
        replace: true,
    }))
    .build()
    .await
    .unwrap();
    client
        .upload_file("anchored", &source, WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(
        client
            .read("anchored", ReadOptions::default())
            .await
            .unwrap(),
        "old"
    );
    assert_eq!(std::fs::read(&source).unwrap(), b"new");
    let client = StorageClientBuilder::from_provider(std::sync::Arc::new(SourceChangingProvider {
        inner: PosixProvider::new(PosixOptions::new(directory.path())).unwrap(),
        source: source.clone(),
        replace: false,
    }))
    .build()
    .await
    .unwrap();
    let error = client
        .upload_file("changed", source, WriteOptions::default())
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Integrity);
    assert!(!client.is_file("changed").await.unwrap());
}

struct ExternalBackend {
    inner: multi_storage_client::provider::posix::PosixProvider,
    get_attempts: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl StorageBackend for ExternalBackend {
    fn name(&self) -> &'static str {
        "external-contract"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            server_side_copy: multi_storage_client::CopyScope::None,
            symlinks: false,
            ..self.inner.capabilities()
        }
    }
    async fn get_object_metadata(&self, key: &ObjectPath) -> Result<ObjectMetadata> {
        self.inner.get_object_metadata(key).await
    }
    async fn get_object(&self, key: &ObjectPath, conditions: &GetConditions) -> Result<ByteStream> {
        if self
            .get_attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            < 2
        {
            return Err(multi_storage_client::MscError::new(
                ErrorKind::Unavailable,
                "injected transient read failure",
            ));
        }
        self.inner.get_object(key, conditions).await
    }
    async fn get_object_range(
        &self,
        key: &ObjectPath,
        range: ByteRange,
        conditions: &GetConditions,
    ) -> Result<Bytes> {
        self.inner.get_object_range(key, range, conditions).await
    }
    async fn put_object(
        &self,
        key: &ObjectPath,
        body: ByteStream,
        size: Option<u64>,
        options: &WriteOptions,
    ) -> Result<PutResult> {
        self.inner.put_object(key, body, size, options).await
    }
    async fn delete_object(&self, key: &ObjectPath, options: &DeleteOptions) -> Result<()> {
        self.inner.delete_object(key, options).await
    }
    async fn list_objects(&self, options: &ListOptions) -> Result<MetadataStream> {
        self.inner.list_objects(options).await
    }
}

fn provider_retry(attempts: u32) -> multi_storage_client::RetryPolicy {
    multi_storage_client::RetryPolicy {
        max_attempts: attempts,
        initial_delay_ms: 0,
        max_delay_ms: 0,
        jitter: false,
        ..multi_storage_client::RetryPolicy::default()
    }
}

#[tokio::test]
async fn public_provider_contract_is_shared_by_posix_and_external_backends() {
    use multi_storage_client::{
        BaseStorageProvider, StorageProvider,
        provider::posix::{PosixOptions, PosixProvider},
    };
    for external in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let backend = PosixProvider::new(PosixOptions::new(directory.path())).unwrap();
        let provider: Box<dyn StorageProvider> = if external {
            Box::new(
                BaseStorageProvider::new(std::sync::Arc::new(ExternalBackend {
                    inner: backend,
                    get_attempts: 0.into(),
                }))
                .with_max_in_memory_bytes(16)
                .with_retry_policy(provider_retry(3))
                .unwrap(),
            )
        } else {
            Box::new(
                BaseStorageProvider::new(std::sync::Arc::new(backend)).with_max_in_memory_bytes(16),
            )
        };
        provider_contract::assert_contract(provider.as_ref(), local.path()).await;
        if !external {
            provider_contract::assert_symlink_contract(provider.as_ref()).await;
        }
        provider.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn external_factory_receives_shared_policy_without_nested_retries() {
    use multi_storage_client::{
        ProviderRegistry,
        provider::{
            ProviderFactory,
            posix::{PosixOptions, PosixProvider},
        },
    };
    use std::sync::{Arc, atomic::Ordering};
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(ExternalBackend {
        inner: PosixProvider::new(PosixOptions::new(directory.path())).unwrap(),
        get_attempts: 0.into(),
    });
    let factory_backend = backend.clone();
    let factory: ProviderFactory = Arc::new(move |_| {
        let backend: Arc<dyn StorageBackend> = factory_backend.clone();
        Box::pin(async move { Ok(backend) })
    });
    let mut registry = ProviderRegistry::new();
    registry.register("external-contract", factory).unwrap();
    let config_file = directory.path().join("config.json");
    std::fs::write(&config_file, serde_json::to_vec(&json!({"profiles":{"data":{
        "storage_provider":{"type":"external-contract","options":{"base_path":directory.path()}},
        "retry":{"attempts":2,"delay":0.0,"max_delay_ms":0,"jitter":false}
    }}})).unwrap()).unwrap();
    let config = ConfigLoader::new()
        .with_registry(registry.clone())
        .with_explicit_paths([config_file])
        .load()
        .unwrap();
    for attempts in [1, 3] {
        backend.get_attempts.store(0, Ordering::SeqCst);
        let client = StorageClient::builder(config.clone(), "data")
            .with_provider_registry(registry.clone())
            .with_retry_policy(provider_retry(attempts))
            .unwrap()
            .with_max_in_memory_bytes(3)
            .build()
            .await
            .unwrap();
        client
            .write(
                "object",
                Bytes::from_static(b"abc"),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        let result = client.read("object", ReadOptions::default()).await;
        assert_eq!(
            backend.get_attempts.load(Ordering::SeqCst),
            attempts as usize
        );
        if attempts == 1 {
            assert_eq!(result.unwrap_err().kind, ErrorKind::Unavailable);
        } else {
            assert_eq!(result.unwrap(), "abc");
        }
        backend.get_attempts.store(2, Ordering::SeqCst);
        client
            .write(
                "large",
                Bytes::from_static(b"abcd"),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .read("large", ReadOptions::default())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Unsupported
        );
        let local = tempfile::tempdir().unwrap();
        let destination = local.path().join("download");
        client
            .download_file("large", &destination, ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(std::fs::read(destination).unwrap(), b"abcd");
        client.shutdown().await.unwrap();
    }
}
