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

//! POSIX provider and public-client contracts using isolated temporary directories.
#![cfg(unix)]
use bytes::Bytes;
use futures::{StreamExt, stream};
use multi_storage_client::{
    ByteRange, DeleteOptions, ErrorKind, GetConditions, InfoOptions, ListOptions, MscError,
    ObjectKind, ObjectPath, ReadOptions, StorageClient, StorageProvider, WriteOptions,
    provider::posix::{PosixOptions, PosixProvider},
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::io::AsyncReadExt;

fn client(directory: &tempfile::TempDir) -> (Arc<PosixProvider>, StorageClient) {
    let provider = Arc::new(PosixProvider::new(PosixOptions::new(directory.path())).unwrap());
    let client = StorageClient::from_provider(provider.clone());
    (provider, client)
}

#[tokio::test]
async fn posix_contract_full_range_attributes_reader_and_generation() {
    let directory = tempfile::tempdir().unwrap();
    let (_, client) = client(&directory);
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let options = WriteOptions {
        attributes: BTreeMap::from([("owner".into(), "研究".into())]),
        ..WriteOptions::default()
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let options = WriteOptions::default();
    let metadata = client
        .write("α/sp ace.txt", Bytes::from_static(b"0123456789"), options)
        .await
        .unwrap();
    assert_eq!(metadata.content_length, 10);
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert_eq!(metadata.attributes["owner"], "研究");
    assert_eq!(client.read("α/sp ace.txt").await.unwrap(), "0123456789");
    assert_eq!(
        client
            .read_range(
                "α/sp ace.txt",
                ByteRange::new(8, 10).unwrap(),
                ReadOptions::default()
            )
            .await
            .unwrap(),
        "89"
    );
    let mut reader = client
        .reader(
            "α/sp ace.txt",
            ReadOptions {
                buffer_size: Some(3),
                ..ReadOptions::default()
            },
        )
        .await
        .unwrap();
    let mut first = [0; 3];
    reader.read_exact(&mut first).await.unwrap();
    client
        .write(
            "α/sp ace.txt",
            Bytes::from_static(b"abcdefghij"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let mut next = [0; 3];
    assert!(reader.read_exact(&mut next).await.is_err());
    assert_eq!(client.read("α/sp ace.txt").await.unwrap(), "abcdefghij");
    assert_eq!(
        client.info("α", InfoOptions::default()).await.unwrap().kind,
        ObjectKind::Directory
    );
}

#[tokio::test]
async fn posix_atomic_create_is_exclusive_between_concurrent_writers() {
    let directory = tempfile::tempdir().unwrap();
    let (_, client) = client(&directory);
    let mut writers = futures::stream::iter(0..24)
        .map(|index| {
            let client = client.clone();
            async move {
                client
                    .write(
                        "unique",
                        Bytes::from(index.to_string()),
                        WriteOptions {
                            if_none_match: Some("*".into()),
                            ..WriteOptions::default()
                        },
                    )
                    .await
            }
        })
        .buffer_unordered(24);
    let mut successes = 0;
    while let Some(result) = writers.next().await {
        match result {
            Ok(_) => successes += 1,
            Err(error) => assert_eq!(error.kind, ErrorKind::PreconditionFailed),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn posix_failed_and_cancelled_writes_leave_destination_and_temps_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let (provider, client) = client(&directory);
    client
        .write(
            "stable",
            Bytes::from_static(b"old"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let key = ObjectPath::new("stable").unwrap();
    let body = Box::pin(stream::iter([
        Ok(Bytes::from_static(b"new")),
        Err(MscError::new(ErrorKind::Integrity, "broken source")),
    ]));
    assert_eq!(
        provider
            .put(&key, body, Some(3), &WriteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Integrity
    );
    let body = Box::pin(
        stream::once(async { Ok(Bytes::from_static(b"partial")) }).chain(stream::pending()),
    );
    let options = WriteOptions::default();
    let mut future = Box::pin(provider.put(&key, body, None, &options));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut future)
            .await
            .is_err()
    );
    let observer = StorageClient::from_provider(Arc::new(
        PosixProvider::new(PosixOptions::new(directory.path())).unwrap(),
    ));
    let listed: Vec<_> = observer
        .list(ListOptions::default())
        .await
        .unwrap()
        .map(|row| row.unwrap().key)
        .collect()
        .await;
    assert_eq!(listed.as_slice(), std::slice::from_ref(&key));
    drop(future);
    assert_eq!(client.read("stable").await.unwrap(), "old");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn posix_symlink_escape_is_rejected_for_reads_and_mutations() {
    let directory = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret"), b"private").unwrap();
    std::os::unix::fs::symlink(outside.path(), directory.path().join("escape")).unwrap();
    std::os::unix::fs::symlink(
        outside.path().join("secret"),
        directory.path().join("absolute"),
    )
    .unwrap();
    let (_, client) = client(&directory);
    assert_eq!(
        client.read("escape/secret").await.unwrap_err().kind,
        ErrorKind::InvalidPath
    );
    assert_eq!(
        client.read("absolute").await.unwrap_err().kind,
        ErrorKind::InvalidPath
    );
    assert_eq!(
        client
            .write(
                "escape/new",
                Bytes::from_static(b"bad"),
                WriteOptions::default()
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidPath
    );
    assert_eq!(
        client
            .delete("escape/secret", false, DeleteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidPath
    );
    assert_eq!(
        std::fs::read(outside.path().join("secret")).unwrap(),
        b"private"
    );
    assert!(!outside.path().join("new").exists());
}

#[tokio::test]
async fn posix_internal_symlinks_and_cycles_have_consistent_metadata() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("real")).unwrap();
    std::fs::write(directory.path().join("real/data"), b"inside").unwrap();
    std::os::unix::fs::symlink("real", directory.path().join("alias")).unwrap();
    std::os::unix::fs::symlink("real/data", directory.path().join("link")).unwrap();
    std::os::unix::fs::symlink("cycle2", directory.path().join("cycle1")).unwrap();
    std::os::unix::fs::symlink("cycle1", directory.path().join("cycle2")).unwrap();
    let (_, client) = client(&directory);
    assert_eq!(client.read("alias/data").await.unwrap(), "inside");
    assert_eq!(client.read("link").await.unwrap(), "inside");
    let version = client
        .info("link", InfoOptions::default())
        .await
        .unwrap()
        .source_version;
    assert_eq!(
        client
            .read_with_options(
                "link",
                ReadOptions {
                    conditions: GetConditions {
                        version: version.clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                }
            )
            .await
            .unwrap(),
        "inside"
    );
    assert_eq!(
        client
            .read_suffix(
                "link",
                multi_storage_client::SuffixRange::new(4).unwrap(),
                ReadOptions {
                    conditions: GetConditions {
                        version,
                        ..Default::default()
                    },
                    ..Default::default()
                }
            )
            .await
            .unwrap(),
        "side"
    );
    let link = client
        .info(
            "link",
            InfoOptions {
                follow_symlinks: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(link.kind, ObjectKind::Symlink);
    assert_eq!(link.symlink_target.as_deref(), Some("real/data"));
    assert_eq!(
        client.read("cycle1").await.unwrap_err().kind,
        ErrorKind::Integrity
    );
    client.make_symlink("made", "real/data").await.unwrap();
    assert_eq!(client.read("made").await.unwrap(), "inside");
}

#[tokio::test]
async fn posix_listing_matches_python_directory_delimiter_order() {
    let directory = tempfile::tempdir().unwrap();
    let (_, client) = client(&directory);
    for key in ["a/file", "a!", "a-", "a.txt", "a0", "b/deep/file", "é"] {
        client
            .write(key, Bytes::new(), WriteOptions::default())
            .await
            .unwrap();
    }
    let records = client
        .list(ListOptions {
            include_directories: true,
            ..ListOptions::default()
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let keys: Vec<_> = records
        .into_iter()
        .map(|metadata| metadata.key.to_string())
        .collect();
    assert_eq!(
        keys,
        [
            "a!",
            "a-",
            "a.txt",
            "a",
            "a/file",
            "a0",
            "b",
            "b/deep",
            "b/deep/file",
            "é"
        ]
    );
    let keys = client
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
    assert_eq!(keys.len(), 7);
    let keys = client
        .list(ListOptions {
            start_after: Some(ObjectPath::new("a-").unwrap()),
            end_at: Some(ObjectPath::new("a0").unwrap()),
            ..ListOptions::default()
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        keys.into_iter()
            .map(|metadata| metadata.key.to_string())
            .collect::<Vec<_>>(),
        ["a.txt", "a/file", "a0"]
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn posix_listing_uses_exact_directories_and_visits_descendants_lazily() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(directory.path().join("data/sub")).unwrap();
    std::fs::create_dir(directory.path().join("data-copy")).unwrap();
    std::fs::write(directory.path().join("data/a.txt"), b"first").unwrap();
    std::fs::write(directory.path().join("data-copy/unrelated.txt"), b"other").unwrap();
    let nested_file = directory.path().join("data/sub/file.txt");
    std::fs::write(&nested_file, b"nested").unwrap();
    xattr::set(&nested_file, "user.json", b"invalid-json").unwrap();
    let (_, client) = client(&directory);
    let options = ListOptions {
        prefix: ObjectPath::new("data").unwrap(),
        recursive: false,
        include_directories: true,
        include_attributes: true,
        ..ListOptions::default()
    };
    let entries = client
        .list(options.clone())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|item| item.key.as_str())
            .collect::<Vec<_>>(),
        ["data/a.txt", "data/sub"]
    );
    for prefix in ["dat", "missing"] {
        let mut listing = client
            .list(ListOptions {
                prefix: ObjectPath::new(prefix).unwrap(),
                ..options.clone()
            })
            .await
            .unwrap();
        assert!(listing.next().await.is_none());
    }
    let mut listing = client
        .list(ListOptions {
            recursive: true,
            include_directories: false,
            ..options
        })
        .await
        .unwrap();
    assert_eq!(
        listing.next().await.unwrap().unwrap().key.as_str(),
        "data/a.txt"
    );
    assert_eq!(
        listing.next().await.unwrap().unwrap_err().kind,
        ErrorKind::Integrity
    );
    client.shutdown().await.unwrap();
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn posix_listing_reads_attributes_only_when_requested() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("a.txt"), b"first").unwrap();
    let file = directory.path().join("z.txt");
    std::fs::write(&file, b"content").unwrap();
    xattr::set(&file, "user.json", b"invalid-json").unwrap();
    let (_, client) = client(&directory);
    let entries = client
        .list(ListOptions::default())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|item| item.attributes.is_empty()));
    let mut listing = client
        .list(ListOptions {
            include_attributes: true,
            ..ListOptions::default()
        })
        .await
        .unwrap();
    assert_eq!(listing.next().await.unwrap().unwrap().key.as_str(), "a.txt");
    assert_eq!(
        listing.next().await.unwrap().unwrap_err().kind,
        ErrorKind::Integrity
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn posix_refuses_nonempty_directory_and_missing_deletes_are_successful() {
    let directory = tempfile::tempdir().unwrap();
    let (_, client) = client(&directory);
    client
        .write("directory/file", Bytes::new(), WriteOptions::default())
        .await
        .unwrap();
    assert!(
        client
            .delete("directory", false, DeleteOptions::default())
            .await
            .is_err()
    );
    client
        .delete("directory/file", false, DeleteOptions::default())
        .await
        .unwrap();
    client
        .delete("directory", false, DeleteOptions::default())
        .await
        .unwrap();
    client
        .delete("missing", false, DeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn posix_source_version_conditions_and_preserved_attributes() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let mut options = PosixOptions::new(directory.path());
    options.preserve_mtime = true;
    options.preserve_permissions = true;
    let provider = Arc::new(PosixProvider::new(options).unwrap());
    let client = StorageClient::from_provider(provider.clone());
    let mut metadata = client
        .write("file", Bytes::from_static(b"old"), WriteOptions::default())
        .await
        .unwrap();
    let old_version = metadata.source_version.clone();
    metadata.last_modified = chrono::DateTime::from_timestamp(1_500_000_000, 123_456_789);
    metadata
        .provider_fields
        .insert("posix_mode".into(), "416".into());
    provider
        .preserve_metadata(&metadata.key, &metadata, metadata.source_version.as_ref())
        .await
        .unwrap();
    assert_eq!(
        std::fs::metadata(directory.path().join("file"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o640
    );
    assert_eq!(
        client
            .info("file", InfoOptions::default())
            .await
            .unwrap()
            .last_modified,
        metadata.last_modified
    );
    assert_eq!(
        provider
            .get(
                &ObjectPath::new("file").unwrap(),
                &GetConditions {
                    version: old_version,
                    ..GetConditions::default()
                }
            )
            .await
            .err()
            .unwrap()
            .kind,
        ErrorKind::PreconditionFailed
    );
}

use futures::TryStreamExt;

#[tokio::test]
async fn symlinks_below_directory_aliases_resolve_and_validate_physical_parents() {
    use std::os::unix::fs::symlink;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(directory.path().join("real/sub")).unwrap();
    std::fs::write(directory.path().join("real/data"), b"correct").unwrap();
    std::fs::write(directory.path().join("data"), b"wrong").unwrap();
    symlink("real/sub", directory.path().join("alias")).unwrap();
    symlink("../data", directory.path().join("real/sub/link")).unwrap();
    symlink("alias/../data", directory.path().join("ambiguous")).unwrap();
    let (provider, client) = client(&directory);
    assert_eq!(client.read("alias/link").await.unwrap(), "correct");
    assert_eq!(
        std::fs::read(directory.path().join("ambiguous")).unwrap(),
        b"correct"
    );
    assert_eq!(
        client.read("ambiguous").await.unwrap_err().kind,
        ErrorKind::Unsupported
    );
    std::fs::create_dir(directory.path().join("nested")).unwrap();
    symlink("..", directory.path().join("nested/alias")).unwrap();
    let error = provider
        .make_symlink(&ObjectPath::new("nested/alias/link").unwrap(), "../secret")
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidPath);
    assert!(!directory.path().join("link").exists());
}

#[tokio::test]
async fn concurrent_copies_complete_with_one_provider_permit() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = PosixOptions::new(directory.path());
    options.max_concurrency = 1;
    let client = StorageClient::from_provider(Arc::new(PosixProvider::new(options).unwrap()));
    let bytes = Bytes::from(vec![42; 131_073]);
    client
        .write("source", bytes.clone(), WriteOptions::default())
        .await
        .unwrap();
    let copies = futures::future::join_all((0..8).map(|index| {
        client.copy(
            "source",
            format!("copy-{index}"),
            multi_storage_client::CopyOptions::default(),
        )
    }));
    for result in tokio::time::timeout(std::time::Duration::from_secs(5), copies)
        .await
        .unwrap()
    {
        result.unwrap();
    }
    assert_eq!(client.read("copy-7").await.unwrap(), bytes);
}

#[test]
fn listing_does_not_depend_on_blocking_pool_capacity_and_hides_cross_process_staging() {
    let directory = tempfile::tempdir().unwrap();
    for index in 0..2200 {
        std::fs::write(directory.path().join(format!("file-{index:04}")), b"x").unwrap();
    }
    std::fs::write(directory.path().join(".msc-tmp-other-process"), b"partial").unwrap();
    let (_, client) = client(&directory);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let listed = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            client
                .list(ListOptions::default())
                .await
                .unwrap()
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        assert_eq!(listed.len(), 2200);
        assert!(
            listed
                .into_iter()
                .all(|entry| entry.unwrap().key.as_str().starts_with("file-"))
        );
        assert_eq!(
            client
                .write(".msc-tmp-forbidden", Bytes::new(), WriteOptions::default())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::InvalidPath
        );
    });
}

#[test]
fn listing_system_temporary_directory_never_exposes_its_own_spool() {
    if std::env::var_os("MSC_LISTING_TMP_CHILD").is_none() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("file"), b"x").unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "listing_system_temporary_directory_never_exposes_its_own_spool",
            ])
            .env("MSC_LISTING_TMP_CHILD", "1")
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
    let client = StorageClient::from_provider(Arc::new(
        PosixProvider::new(PosixOptions::new(std::env::temp_dir())).unwrap(),
    ));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let entries = client
            .list(ListOptions {
                recursive: false,
                include_directories: true,
                ..ListOptions::default()
            })
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries.into_iter().next().unwrap().unwrap().key.as_str(),
            "file"
        );
    });
}

#[tokio::test]
async fn replacing_download_staging_cannot_truncate_or_publish_an_unrelated_file() {
    use std::os::unix::fs::symlink;
    let directory = tempfile::tempdir().unwrap();
    let downloads = tempfile::tempdir().unwrap();
    let mut options = PosixOptions::new(directory.path());
    options.max_concurrency = 1;
    let provider = Arc::new(PosixProvider::new(options).unwrap());
    let client = StorageClient::from_provider(provider.clone());
    client
        .write(
            "source",
            Bytes::from_static(b"new"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let victim = downloads.path().join("victim");
    let target = downloads.path().join("target");
    std::fs::write(&victim, b"retained victim").unwrap();
    std::fs::write(&target, b"retained destination").unwrap();
    let held = provider
        .get(
            &ObjectPath::new("source").unwrap(),
            &GetConditions::default(),
        )
        .await
        .unwrap();
    let mut download = Box::pin(client.download_to("source", &target, ReadOptions::default()));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut download)
            .await
            .is_err()
    );
    let staging = std::fs::read_dir(downloads.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.is_dir())
        .unwrap();
    let spool = std::fs::read_dir(staging)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_file(&spool).unwrap();
    symlink(&victim, &spool).unwrap();
    drop(held);
    assert_eq!(download.await.unwrap_err().kind, ErrorKind::Integrity);
    assert_eq!(std::fs::read(victim).unwrap(), b"retained victim");
    assert_eq!(std::fs::read(target).unwrap(), b"retained destination");
}
