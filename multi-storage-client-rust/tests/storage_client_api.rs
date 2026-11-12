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

use bytes::Bytes;
use futures::TryStreamExt;
use multi_storage_client::{
    ByteRange, ConfigLoader, DeleteOptions, ErrorKind, GetConditions, InfoOptions, ListOptions,
    MscError, ObjectKind, ObjectPath, PresignMethod, ReadOptions, StorageClient, WriteOptions,
};
use serde_json::json;
use std::{collections::BTreeMap, io::SeekFrom, path::Path, time::Duration};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

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
    assert!(client.is_posix());
    assert!(!client.is_read_only());
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
            client.read(path).await.unwrap_err().kind,
            ErrorKind::NotFound
        );
    }
    assert!(!directory.path().join("storage/missing").exists());
    let metadata = client
        .write("nested/empty", Bytes::new(), WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(metadata.content_length, 0);
    assert_eq!(metadata.kind, ObjectKind::File);
    assert!(metadata.source_version.is_some());
    assert!(client.is_file("nested/empty/").await.unwrap());
    assert!(!client.is_file("nested").await.unwrap());
    assert!(!client.is_empty("nested/empty").await.unwrap());
    assert!(client.read("nested/empty").await.unwrap().is_empty());
    let mut reader = client
        .reader("nested/empty", ReadOptions::default())
        .await
        .unwrap();
    assert!(reader.is_empty());
    assert_eq!(reader.len(), 0);
    assert_eq!(reader.read(&mut [0; 1]).await.unwrap(), 0);
    for path in ["", "nested"] {
        assert_eq!(
            client.read(path).await.unwrap_err().kind,
            ErrorKind::InvalidPath
        );
        assert_eq!(
            client
                .reader(path, ReadOptions::default())
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
    let written = client
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
    assert_eq!(written.key, key("α/sp ace%?#.txt"));
    assert_eq!(written.attributes, attrs);
    assert_eq!(client.read("α/sp ace%?#.txt").await.unwrap(), body);
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
    let replacement = client
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
    assert_ne!(written.source_version, replacement.source_version);
    assert_eq!(replacement.attributes, replacement_attrs);
    assert_eq!(client.read("α/sp ace%?#.txt").await.unwrap(), "new");
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
    let metadata = client
        .write(
            "file",
            Bytes::from_static(b"0123456789"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
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
                .read_range(
                    "file",
                    ByteRange::new(offset, length).unwrap(),
                    ReadOptions::default()
                )
                .await
                .unwrap(),
            expected
        );
    }
    assert!(
        client
            .read_range(
                "missing",
                ByteRange::new(0, 0).unwrap(),
                ReadOptions::default()
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        client
            .read_range(
                "file",
                ByteRange {
                    offset: u64::MAX,
                    length: 1
                },
                ReadOptions::default()
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidPath
    );
    assert_eq!(
        client
            .read_range(
                "file",
                ByteRange::new(0, 4).unwrap(),
                ReadOptions {
                    max_in_memory_bytes: 3,
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
            .read_with_options(
                "file",
                ReadOptions {
                    max_in_memory_bytes: 9,
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
            .read_with_options(
                "file",
                ReadOptions {
                    max_in_memory_bytes: 10,
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
            .read_range("file", ByteRange::new(1, 2).unwrap(), pinned.clone())
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
            .read_range("file", ByteRange::new(1, 2).unwrap(), pinned.clone())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PreconditionFailed
    );
    assert_eq!(
        client
            .read_with_options("file", pinned)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PreconditionFailed
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn builder_memory_limits_apply_to_default_reads_and_allow_explicit_overrides() {
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
    let bounded = StorageClient::builder(config.clone(), "local")
        .with_max_in_memory_bytes(3)
        .build()
        .await
        .unwrap();
    assert_eq!(bounded.read("boundary").await.unwrap(), "123");
    assert!(bounded.read("empty").await.unwrap().is_empty());
    for client in [&bounded, &bounded.clone()] {
        assert_eq!(
            client.read("larger").await.unwrap_err().kind,
            ErrorKind::Unsupported
        );
    }
    assert_eq!(
        bounded
            .read_with_options(
                "larger",
                ReadOptions {
                    max_in_memory_bytes: 4,
                    ..ReadOptions::default()
                }
            )
            .await
            .unwrap(),
        "1234"
    );
    assert_eq!(
        bounded
            .read_range(
                "larger",
                ByteRange::new(0, 4).unwrap(),
                ReadOptions::default()
            )
            .await
            .unwrap(),
        "1234"
    );
    let chunks = bounded
        .read_stream("larger", ReadOptions::default())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(chunks.concat(), b"1234");
    let zero = StorageClient::builder(config.clone(), "local")
        .with_max_in_memory_bytes(0)
        .build()
        .await
        .unwrap();
    assert!(zero.read("empty").await.unwrap().is_empty());
    assert_eq!(
        zero.read("boundary").await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    let unbounded = StorageClient::builder(config, "local")
        .with_max_in_memory_bytes(u64::MAX)
        .build()
        .await
        .unwrap();
    assert_eq!(unbounded.read("larger").await.unwrap(), "1234");
    bounded.shutdown().await.unwrap();
    zero.shutdown().await.unwrap();
    unbounded.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_one_mib_reader_buffer_overrides_a_different_builder_default() {
    let (directory, _) = fixture().await;
    let config = ConfigLoader::new()
        .with_explicit_paths([directory.path().join("config.json")])
        .load()
        .unwrap();
    let client = StorageClient::builder(config, "local")
        .with_read_buffer_size(2 * 1024 * 1024)
        .build()
        .await
        .unwrap();
    for buffer_size in [None, Some(1024 * 1024)] {
        client
            .write(
                "large",
                Bytes::from(vec![1; 2 * 1024 * 1024]),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        let mut reader = client
            .reader(
                "large",
                ReadOptions {
                    buffer_size,
                    ..ReadOptions::default()
                },
            )
            .await
            .unwrap();
        reader.read_exact(&mut vec![0; 1024 * 1024]).await.unwrap();
        client
            .write(
                "large",
                Bytes::from(vec![2; 2 * 1024 * 1024]),
                WriteOptions::default(),
            )
            .await
            .unwrap();
        let result = reader.read_exact(&mut [0; 1]).await;
        assert_eq!(result.is_ok(), buffer_size.is_none());
    }
}

#[tokio::test]
async fn seekable_reader_handles_buffer_boundaries_eof_and_invalid_seeks() {
    let (_directory, client) = fixture().await;
    client
        .write(
            "file",
            Bytes::from_static(b"0123456789"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let mut reader = client
        .reader(
            "file",
            ReadOptions {
                buffer_size: Some(3),
                max_in_memory_bytes: 1,
                ..ReadOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(reader.len(), 10);
    assert!(!reader.is_empty());
    assert_eq!(reader.position(), 0);
    assert_eq!(reader.read(&mut []).await.unwrap(), 0);
    let mut first = [0; 4];
    reader.read_exact(&mut first).await.unwrap();
    assert_eq!(&first, b"0123");
    assert_eq!(reader.position(), 4);
    assert_eq!(reader.seek(SeekFrom::Current(-2)).await.unwrap(), 2);
    let mut middle = [0; 3];
    reader.read_exact(&mut middle).await.unwrap();
    assert_eq!(&middle, b"234");
    assert_eq!(reader.seek(SeekFrom::End(-2)).await.unwrap(), 8);
    let mut tail = Vec::new();
    reader.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, b"89");
    assert_eq!(reader.seek(SeekFrom::Start(100)).await.unwrap(), 100);
    assert_eq!(reader.read(&mut [0; 1]).await.unwrap(), 0);
    assert_eq!(
        reader.seek(SeekFrom::End(-11)).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(reader.position(), 100);
    reader.seek(SeekFrom::Start(u64::MAX)).await.unwrap();
    assert_eq!(
        reader.seek(SeekFrom::Current(1)).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    reader.rewind().await.unwrap();
    let mut all = Vec::new();
    reader.read_to_end(&mut all).await.unwrap();
    assert_eq!(all, b"0123456789");
    assert_eq!(
        client
            .reader(
                "file",
                ReadOptions {
                    buffer_size: Some(0),
                    ..ReadOptions::default()
                }
            )
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::InvalidConfig
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn readers_and_streams_propagate_stale_generation_errors() {
    let (_directory, client) = fixture().await;
    let metadata = client
        .write(
            "file",
            Bytes::from_static(b"0123456789"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let mut reader = client
        .reader(
            "file",
            ReadOptions {
                buffer_size: Some(2),
                ..ReadOptions::default()
            },
        )
        .await
        .unwrap();
    let mut first = [0; 2];
    reader.read_exact(&mut first).await.unwrap();
    client
        .write(
            "file",
            Bytes::from_static(b"abcdefghij"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let error = reader.read_exact(&mut first).await.unwrap_err();
    assert_eq!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<MscError>()
            .unwrap()
            .kind,
        ErrorKind::PreconditionFailed
    );
    let options = ReadOptions {
        conditions: GetConditions {
            version: metadata.source_version,
            ..GetConditions::default()
        },
        ..ReadOptions::default()
    };
    assert_eq!(
        client
            .read_stream("file", options)
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::PreconditionFailed
    );
    {
        let options = ReadOptions {
            max_in_memory_bytes: 1,
            ..ReadOptions::default()
        };
        let stream = client.read_stream("file", options).await.unwrap();
        let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(chunks.concat(), b"abcdefghij");
    }
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn writer_only_publishes_on_finish_and_honors_create_only_conditions() {
    let (_directory, client) = fixture().await;
    client
        .write(
            "stable",
            Bytes::from_static(b"old"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    for action in ["drop", "abort"] {
        let mut writer = client
            .writer("stable", Some(100), WriteOptions::default())
            .await
            .unwrap();
        writer.write_all(b"unpublished").await.unwrap();
        writer.flush().await.unwrap();
        assert_eq!(client.read("stable").await.unwrap(), "old");
        if action == "abort" {
            writer.abort().await.unwrap();
        } else {
            drop(writer);
        }
        assert_eq!(client.read("stable").await.unwrap(), "old");
    }
    let mut writer = client
        .writer("finished", Some(100), WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(writer.bytes_written(), 0);
    writer.write_all(b"new").await.unwrap();
    assert_eq!(writer.bytes_written(), 3);
    writer.shutdown().await.unwrap();
    assert!(!client.is_file("finished").await.unwrap());
    assert_eq!(
        writer.write_all(b"closed").await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert_eq!(writer.finish().await.unwrap().content_length, 3);
    assert_eq!(client.read("finished").await.unwrap(), "new");
    assert_eq!(
        client
            .writer("empty", Some(99), WriteOptions::default())
            .await
            .unwrap()
            .finish()
            .await
            .unwrap()
            .content_length,
        0
    );
    let mut writer = client
        .writer(
            "stable",
            None,
            WriteOptions {
                if_none_match: Some("*".into()),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap();
    writer.write_all(b"rejected").await.unwrap();
    assert_eq!(
        writer.finish().await.unwrap_err().kind,
        ErrorKind::PreconditionFailed
    );
    assert_eq!(client.read("stable").await.unwrap(), "old");
    assert_eq!(
        keys(&client, ListOptions::default()).await,
        ["empty", "finished", "stable"]
    );
    client.shutdown().await.unwrap();
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
    let metadata = client
        .download_to(
            "file",
            &target,
            ReadOptions {
                max_in_memory_bytes: 1,
                ..ReadOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(metadata.content_length, body.len() as u64);
    assert_eq!(std::fs::read(&target).unwrap(), body);
    assert_eq!(
        client
            .download_to("missing", &target, ReadOptions::default())
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
            .download_to(
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
            .download_to("missing", &missing_target, ReadOptions::default())
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
            .download_to("file", parent_file.join("child"), ReadOptions::default())
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
            .download_to("file", &target, ReadOptions::default())
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
    let uploaded = client
        .upload_from(
            "nested/uploaded",
            &source,
            WriteOptions {
                attributes: attrs.clone(),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(uploaded.content_length, 8);
    assert_eq!(uploaded.attributes, attrs);
    assert_eq!(client.read("nested/uploaded").await.unwrap(), "uploaded");
    assert_eq!(
        client
            .upload_from(
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
            .upload_from("nested/uploaded", directory.path(), WriteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidPath
    );
    let link = directory.path().join("source-link");
    std::os::unix::fs::symlink(&source, &link).unwrap();
    assert_eq!(
        client
            .upload_from("nested/uploaded", &link, WriteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    assert_eq!(client.read("nested/uploaded").await.unwrap(), "uploaded");
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn batches_keep_original_indices_and_allow_independent_partial_success() {
    let (directory, client) = fixture().await;
    let source = directory.path().join("source");
    std::fs::write(&source, b"payload").unwrap();
    let missing = directory.path().join("missing");
    let mut uploads = (0..40)
        .map(|index| (key(&format!("batch/{index:03}")), source.clone()))
        .collect::<Vec<_>>();
    uploads[1].1.clone_from(&missing);
    uploads[35].1.clone_from(&missing);
    let result = client
        .upload_many(&uploads, WriteOptions::default())
        .await
        .unwrap();
    let expected = (0..40)
        .filter(|index| ![1, 35].contains(index))
        .collect::<Vec<_>>();
    assert_eq!(result.succeeded, expected);
    assert_eq!(
        result
            .failed
            .iter()
            .map(|item| item.index)
            .collect::<Vec<_>>(),
        [1, 35]
    );
    for failed in &result.failed {
        assert_eq!(failed.source, uploads[failed.index].0);
        assert_eq!(failed.destination.as_ref(), Some(&failed.source));
        assert_eq!(failed.error.kind, ErrorKind::NotFound);
    }
    let downloads = uploads
        .iter()
        .map(|(remote, _)| {
            (
                remote.clone(),
                directory
                    .path()
                    .join("downloads/deep")
                    .join(remote.as_str()),
            )
        })
        .collect::<Vec<_>>();
    let downloaded = client
        .download_many(&downloads, ReadOptions::default())
        .await
        .unwrap();
    assert_eq!(downloaded.succeeded, expected);
    assert_eq!(
        downloaded
            .failed
            .iter()
            .map(|item| item.index)
            .collect::<Vec<_>>(),
        [1, 35]
    );
    for index in downloaded.succeeded {
        assert_eq!(std::fs::read(&downloads[index].1).unwrap(), b"payload");
    }
    let mut deleted = uploads
        .into_iter()
        .map(|(remote, _)| remote)
        .collect::<Vec<_>>();
    deleted.insert(0, key("missing"));
    for _ in 0..2 {
        let result = client
            .delete_many(&deleted, DeleteOptions::default())
            .await
            .unwrap();
        assert_eq!(result.succeeded, (0..deleted.len()).collect::<Vec<_>>());
        assert!(result.failed.is_empty());
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
            .failed
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
    assert!(client.is_empty("batch").await.unwrap());
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn listings_and_globs_respect_exact_paths_bounds_and_directory_options() {
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
    let matches = client
        .glob("data/*.[tb][xi][tn]", ListOptions::default())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        matches
            .iter()
            .map(|item| item.key.as_str())
            .collect::<Vec<_>>(),
        ["data/a.txt", "data/b.bin", "data/sub/c.txt"]
    );
    assert!(
        client
            .glob("*.absent", ListOptions::default())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        client
            .glob("[", ListOptions::default())
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::InvalidPath
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
    let (_directory, client) = fixture().await;
    client
        .write(
            "target",
            Bytes::from_static(b"target payload"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let made = client
        .make_symlink("links/alias", "../target")
        .await
        .unwrap();
    assert_eq!(made.kind, ObjectKind::Symlink);
    assert_eq!(made.symlink_target.as_deref(), Some("../target"));
    assert!(client.is_file("links/alias").await.unwrap());
    assert_eq!(client.read("links/alias").await.unwrap(), "target payload");
    assert_eq!(
        client
            .read_range(
                "links/alias",
                ByteRange::new(7, 7).unwrap(),
                ReadOptions::default()
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
        client.read("dangling").await.unwrap_err().kind,
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
    assert_eq!(client.read("target").await.unwrap(), "target payload");
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
            client.read(path).await.unwrap_err().kind,
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
        assert_eq!(
            client
                .writer(path, None, WriteOptions::default())
                .await
                .err()
                .unwrap()
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
    for method in [PresignMethod::Get, PresignMethod::Put] {
        assert_eq!(
            client
                .presigned_url("stable", method, Duration::from_secs(60))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Unsupported
        );
    }
    assert_eq!(client.read("stable").await.unwrap(), "original");
    assert_eq!(
        std::fs::read_dir(directory.path().join("storage"))
            .unwrap()
            .count(),
        1
    );
    client.shutdown().await.unwrap();
}

#[test]
fn replacing_named_spools_cannot_change_writer_or_snapshot_uploads() {
    fn replace_named_spools() {
        for entry in std::fs::read_dir(std::env::temp_dir()).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                std::fs::remove_file(&path).unwrap();
                std::fs::write(path, b"bad").unwrap();
            }
        }
    }
    if std::env::var_os("MSC_SPOOL_CHILD").is_none() {
        let directory = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "replacing_named_spools_cannot_change_writer_or_snapshot_uploads",
            ])
            .env("MSC_SPOOL_CHILD", "1")
            .env("TMPDIR", directory.path())
            .env("TMP", directory.path())
            .env("TEMP", directory.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (directory, client) = fixture().await;
        let mut writer = client
            .writer("written", None, WriteOptions::default())
            .await
            .unwrap();
        writer.write_all(b"old").await.unwrap();
        writer.flush().await.unwrap();
        replace_named_spools();
        writer.finish().await.unwrap();
        assert_eq!(client.read("written").await.unwrap(), "old");
        let source = directory.path().join("source");
        std::fs::write(&source, b"old").unwrap();
        let callback =
            std::sync::Arc::new(|_: multi_storage_client::TransferProgress| replace_named_spools());
        client
            .upload_from_with_callback("snapshot", source, WriteOptions::default(), Some(callback))
            .await
            .unwrap();
        assert_eq!(client.read("snapshot").await.unwrap(), "old");
    });
}

#[tokio::test]
async fn direct_file_upload_retains_its_open_source_and_rejects_in_place_changes() {
    let (directory, client) = fixture().await;
    let source = directory.path().join("source");
    std::fs::write(&source, b"old").unwrap();
    let path = source.clone();
    let once = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback = std::sync::Arc::new(move |_: multi_storage_client::TransferProgress| {
        if !once.swap(true, std::sync::atomic::Ordering::SeqCst) {
            std::fs::remove_file(&path).unwrap();
            std::fs::write(&path, b"new").unwrap();
        }
    });
    client
        .upload_from_with_callback("anchored", &source, WriteOptions::default(), Some(callback))
        .await
        .unwrap();
    assert_eq!(client.read("anchored").await.unwrap(), "old");
    let path = source.clone();
    let callback = std::sync::Arc::new(move |_: multi_storage_client::TransferProgress| {
        std::fs::write(&path, b"changed size").unwrap();
    });
    let error = client
        .upload_from_with_callback("changed", source, WriteOptions::default(), Some(callback))
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Integrity);
    assert!(!client.is_file("changed").await.unwrap());
}
