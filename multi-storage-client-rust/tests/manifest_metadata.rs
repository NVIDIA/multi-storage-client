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

//! Python manifest compatibility and logical client integration.
#![cfg(unix)]

#[path = "common/metadata_catalog.rs"]
mod metadata_catalog;

use futures::TryStreamExt;
use metadata_catalog::WritableMetadata;
use multi_storage_client::{
    ConfigLoader, CopyOptions, DeleteOptions, ErrorKind, InfoOptions, ListOptions,
    ManifestMetadataProvider, MetadataProvider, MetadataProviderRegistry, MscError, ObjectKind,
    ObjectMetadata, ObjectPath, ReadOptions, ResolvedConfig, ResolvedPathState, StorageClient,
    StorageClientBuilder, WriteOptions,
    provider::posix::{PosixOptions, PosixProvider},
};
use std::{collections::BTreeMap, path::Path, sync::Arc};

const INDEX: &str = "msc_manifest_index.json";
const PART: &str = "parts/files.jsonl";

const ROWS: &str = concat!(
    r#"{"key":"docs/é.txt","size_bytes":5,"last_modified":"2026-10-01T00:00:00.000000Z","physical_path":"physical/file","etag":"abc","content_type":"text/plain","storage_class":"STANDARD","metadata":{"group":"blue","scores":[1,2],"verified":true}}"#,
    "\r\n\r\n",
    r#"{"key":"docs/link","type":"file","size_bytes":0,"last_modified":"2026-10-01T01:00:00+01:00","symlink_target":"é.txt"}"#,
    "\n",
    r#"{"key":"docs/leaf/a.txt","size_bytes":7,"last_modified":"2026-10-02T00:00:00Z","metadata":{"group":"blue"}}"#,
    "\n",
    r#"{"key":"docs/leaf/empty/","type":"directory","size_bytes":0,"last_modified":"2026-10-03T00:00:00Z"}"#,
    "\n",
    r#"{"key":"docs-copy/other","size_bytes":14,"last_modified":"2026-10-04T00:00:00Z"}"#,
);

fn builder(root: &Path) -> StorageClientBuilder {
    let provider = PosixProvider::new(PosixOptions::new(root)).unwrap();
    StorageClientBuilder::from_provider(Arc::new(provider))
}

async fn client(root: &Path) -> StorageClient {
    builder(root).build().await.unwrap()
}

fn snapshot(root: &Path, name: &str, index: &str, rows: &str) {
    let root = root.join(name);
    std::fs::create_dir_all(root.join("parts")).unwrap();
    std::fs::write(root.join(INDEX), index).unwrap();
    std::fs::write(root.join(PART), rows).unwrap();
}

fn key(value: &str) -> ObjectPath {
    ObjectPath::new(value).unwrap()
}

async fn keys(provider: &dyn MetadataProvider, options: ListOptions) -> Vec<ObjectPath> {
    provider
        .list_objects(options)
        .await
        .unwrap()
        .map_ok(|entry| entry.key)
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test]
async fn empty_manifest_exposes_both_root_spellings() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join(INDEX), r#"{"version":"1","parts":[]}"#).unwrap();
    let provider = Arc::new(
        ManifestMetadataProvider::load(&client(root.path()).await, INDEX)
            .await
            .unwrap(),
    );
    let mapped = builder(root.path())
        .with_metadata_provider(provider.clone())
        .build()
        .await
        .unwrap();
    for path in ["", "/"] {
        for entry in [
            provider.get_object_metadata(path, false).await.unwrap(),
            mapped.info(path, InfoOptions::default()).await.unwrap(),
        ] {
            assert_eq!(entry.kind, ObjectKind::Directory);
            assert!(entry.key.is_root());
        }
    }
    assert_eq!(
        provider
            .get_object_metadata("absent", false)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn python_manifest_discovery_listing_glob_and_logical_storage_reads() {
    let manifest_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let storage = builder(manifest_dir.path())
        .with_max_in_memory_bytes(128)
        .build()
        .await
        .unwrap();
    let index = r#"{"version":"1","format":"jsonl","parts":[{"path":"parts/files.jsonl"}]}"#;
    let legacy = r#"{"version":"1.0","parts":[{"path":"parts/files.jsonl"}]}"#;
    let old = "project/.msc_manifests/2026-10-01T00:00:00+00:00";
    let latest = "project/.msc_manifests/2026-10-02T00:00:00+00:00";
    snapshot(manifest_dir.path(), old, legacy, "");
    snapshot(manifest_dir.path(), latest, index, ROWS);
    snapshot(
        manifest_dir.path(),
        "project/.msc_manifests-sibling/later",
        index,
        "",
    );
    std::fs::create_dir_all(
        manifest_dir
            .path()
            .join("project/.msc_manifests/2026-10-03T00:00:00+00:00"),
    )
    .unwrap();

    assert!(
        keys(
            &ManifestMetadataProvider::load(&storage, format!("{old}/{INDEX}"))
                .await
                .unwrap(),
            ListOptions::default()
        )
        .await
        .is_empty()
    );
    let manifest = Arc::new(
        ManifestMetadataProvider::load(&storage, "project")
            .await
            .unwrap(),
    );
    let direct_dir = ManifestMetadataProvider::load(&storage, latest)
        .await
        .unwrap();
    assert_eq!(
        direct_dir
            .get_object_metadata("docs/é.txt", false)
            .await
            .unwrap()
            .content_length,
        5
    );
    for path in ["docs", "docs/", "/", "docs/leaf/empty"] {
        assert_eq!(
            manifest
                .get_object_metadata(path, false)
                .await
                .unwrap()
                .kind,
            ObjectKind::Directory
        );
    }
    assert_eq!(
        manifest
            .get_object_metadata("doc", false)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        manifest
            .get_object_metadata("docs/é.txt/", false)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        manifest
            .realpath("docs-copy/other")
            .await
            .unwrap()
            .physical_path,
        key("docs-copy/other")
    );
    assert_eq!(
        manifest.realpath("missing").await.unwrap().state,
        ResolvedPathState::Untracked
    );
    assert_eq!(
        manifest.realpath("docs/leaf/empty/").await.unwrap().state,
        ResolvedPathState::Untracked
    );

    let shallow = ListOptions {
        prefix: key("docs"),
        include_directories: true,
        recursive: false,
        ..Default::default()
    };
    assert_eq!(
        keys(manifest.as_ref(), shallow.clone()).await,
        [key("docs/leaf"), key("docs/link"), key("docs/é.txt")]
    );
    assert_eq!(
        keys(
            manifest.as_ref(),
            ListOptions {
                include_directories: true,
                ..Default::default()
            }
        )
        .await,
        [
            "docs",
            "docs-copy",
            "docs-copy/other",
            "docs/leaf",
            "docs/leaf/a.txt",
            "docs/leaf/empty",
            "docs/link",
            "docs/é.txt"
        ]
        .map(key)
    );
    assert_eq!(
        keys(
            manifest.as_ref(),
            ListOptions {
                start_after: Some(key("docs/leaf")),
                end_at: Some(key("docs/link")),
                ..shallow
            }
        )
        .await,
        [key("docs/link")]
    );
    assert_eq!(
        keys(
            manifest.as_ref(),
            ListOptions {
                prefix: key("docs"),
                end_at: Some(key("docs/leaf/a.txt")),
                attribute_filter_expression: Some(r#"group="blue""#.into()),
                ..Default::default()
            }
        )
        .await,
        [key("docs/leaf/a.txt")]
    );
    assert_eq!(
        manifest
            .glob("docs/**/*.txt", Some(r#"group="blue""#))
            .await
            .unwrap(),
        [key("docs/leaf/a.txt"), key("docs/é.txt")]
    );

    let physical = client(data_dir.path()).await;
    physical
        .write(
            "physical/file",
            b"value".as_slice().into(),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    physical
        .write(
            "untracked",
            b"hidden".as_slice().into(),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let logical = builder(data_dir.path())
        .with_metadata_provider(manifest.clone())
        .build()
        .await
        .unwrap();
    let metadata = logical
        .info("docs/é.txt", InfoOptions::default())
        .await
        .unwrap();
    assert_eq!(metadata.key, key("docs/é.txt"));
    assert_eq!(metadata.attributes["scores"], serde_json::json!([1, 2]));
    assert_eq!(metadata.etag.as_deref(), Some("abc"));
    assert_eq!(metadata.provider_fields["content_type"], "text/plain");
    assert_eq!(metadata.provider_fields["storage_class"], "STANDARD");
    assert_eq!(
        logical
            .read("docs/link", ReadOptions::default())
            .await
            .unwrap(),
        "value"
    );
    let download = data_dir.path().join("downloaded");
    logical
        .download_file("docs/é.txt", &download, ReadOptions::default())
        .await
        .unwrap();
    assert_eq!(std::fs::read(download).unwrap(), b"value");
    assert_eq!(
        logical
            .read("untracked", ReadOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert!(!logical.is_file("docs").await.unwrap());
    assert!(logical.is_file("docs/link").await.unwrap());
    assert!(!logical.is_empty("docs").await.unwrap());
    assert!(logical.is_empty("doc").await.unwrap());
    assert_eq!(
        logical
            .list_recursive(ListOptions {
                prefix: key("docs"),
                recursive: false,
                ..Default::default()
            })
            .await
            .unwrap()
            .map_ok(|entry| entry.key)
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
        [key("docs/leaf/a.txt"), key("docs/link"), key("docs/é.txt")]
    );
    for error in [
        logical
            .write(
                "untracked",
                b"new".as_slice().into(),
                WriteOptions::default(),
            )
            .await
            .unwrap_err(),
        logical
            .copy("docs/é.txt", "untracked", CopyOptions::default())
            .await
            .unwrap_err(),
        logical
            .delete("docs/é.txt", false, DeleteOptions::default())
            .await
            .unwrap_err(),
        logical
            .delete_many(&[key("docs/é.txt")], DeleteOptions::default())
            .await
            .unwrap_err(),
        logical
            .make_symlink("alias", "docs/é.txt")
            .await
            .unwrap_err(),
        logical.commit_updates().await.unwrap_err(),
        manifest.add_file("docs/é.txt", metadata).await.unwrap_err(),
        manifest.remove_file("docs/é.txt").await.unwrap_err(),
    ] {
        assert_eq!(error.kind, ErrorKind::Unsupported);
    }
    assert_eq!(
        physical
            .read("untracked", ReadOptions::default())
            .await
            .unwrap(),
        "hidden"
    );
}

#[tokio::test]
async fn manifest_rejects_invalid_snapshots_and_bounds_symlink_resolution() {
    let directory = tempfile::tempdir().unwrap();
    let storage = client(directory.path()).await;
    for (name, index, rows, expected) in [
        (
            "version",
            r#"{"version":"2","parts":[]}"#,
            "",
            ErrorKind::Unsupported,
        ),
        (
            "parquet",
            r#"{"version":"1","parts":[{"path":"part.parquet"}]}"#,
            "",
            ErrorKind::Unsupported,
        ),
        (
            "traversal",
            r#"{"version":"1","parts":[{"path":"../files.jsonl"}]}"#,
            "",
            ErrorKind::Integrity,
        ),
        (
            "absolute",
            r#"{"version":"1","parts":[{"path":"/outside.jsonl"}]}"#,
            "",
            ErrorKind::Integrity,
        ),
        (
            "invalid",
            r#"{"version":"1","parts":[{"path":"parts/files.jsonl"}]}"#,
            "not JSON",
            ErrorKind::Integrity,
        ),
        (
            "duplicate",
            r#"{"version":"1","parts":[{"path":"parts/files.jsonl"},{"path":"parts/files.jsonl"}]}"#,
            ROWS,
            ErrorKind::Integrity,
        ),
    ] {
        snapshot(directory.path(), name, index, rows);
        let Err(error) = ManifestMetadataProvider::load(&storage, name).await else {
            panic!("{name} accepted");
        };
        assert_eq!(error.kind, expected, "{name}: {error}");
    }
    let index = r#"{"version":"1","parts":[{"path":"parts/files.jsonl"}]}"#;
    snapshot(
        directory.path(),
        "long-record",
        index,
        &" ".repeat(1024 * 1024 + 2),
    );
    let Err(error) = ManifestMetadataProvider::load(&storage, "long-record").await else {
        panic!("oversized row accepted");
    };
    assert_eq!(error.kind, ErrorKind::Integrity);
    assert_eq!(error.key.as_deref(), Some("long-record/parts/files.jsonl"));
    let Err(error) = ManifestMetadataProvider::load(&storage, "missing").await else {
        panic!("missing manifest accepted");
    };
    assert_eq!(error.kind, ErrorKind::ManifestNotFound);

    snapshot(
        directory.path(),
        "cycles",
        index,
        concat!(
            r#"{"key":"a","size_bytes":0,"last_modified":"2026-10-01T00:00:00Z","symlink_target":"b"}"#,
            "\n",
            r#"{"key":"b","size_bytes":0,"last_modified":"2026-10-01T00:00:00Z","symlink_target":"a"}"#,
            "\n",
            r#"{"key":"nested/broken","size_bytes":0,"last_modified":"2026-10-01T00:00:00Z","symlink_target":"missing"}"#,
            "\n",
            r#"{"key":"unsafe","size_bytes":0,"last_modified":"2026-10-01T00:00:00Z","symlink_target":"../outside"}"#,
        ),
    );
    let manifest = ManifestMetadataProvider::load(&storage, "cycles")
        .await
        .unwrap();
    assert_eq!(
        manifest.realpath("a").await.unwrap_err().kind,
        ErrorKind::Integrity
    );
    assert_eq!(
        manifest.realpath("unsafe").await.unwrap_err().kind,
        ErrorKind::InvalidPath
    );
    assert_eq!(
        manifest.realpath("nested/broken").await.unwrap().state,
        ResolvedPathState::Untracked
    );
}

#[tokio::test]
async fn manifest_rejects_aliased_file_keys_physical_paths_and_part_paths() {
    let directory = tempfile::tempdir().unwrap();
    let storage = client(directory.path()).await;
    for (name, logical, physical) in [
        ("logical", "a//b.txt", None),
        ("physical", "file.txt", Some("a//b.txt")),
        ("trailing-slash", "a/", None),
    ] {
        let row = serde_json::json!({
            "key": logical,
            "physical_path": physical,
            "size_bytes": 1,
            "last_modified": "2026-10-01T00:00:00Z",
        });
        snapshot(
            directory.path(),
            name,
            r#"{"version":"1","parts":[{"path":"parts/files.jsonl"}]}"#,
            &row.to_string(),
        );
        let Err(error) = ManifestMetadataProvider::load(&storage, name).await else {
            panic!("accepted aliased {name}");
        };
        assert_eq!(error.kind, ErrorKind::Integrity);
        assert!(error.message.contains("noncanonical") && error.message.contains("line 1"));
    }

    snapshot(
        directory.path(),
        "aliased-part",
        r#"{"version":"1","parts":[{"path":"parts//files.jsonl"}]}"#,
        ROWS,
    );
    let Err(error) = ManifestMetadataProvider::load(&storage, "aliased-part").await else {
        panic!("accepted an aliased part reference");
    };
    assert_eq!(error.kind, ErrorKind::Integrity);
    assert_eq!(
        error.key.as_deref(),
        Some("aliased-part/msc_manifest_index.json")
    );
}

#[tokio::test]
async fn custom_writable_metadata_stages_mutations_until_commit() {
    let root = tempfile::tempdir().unwrap();
    let storage = builder(root.path())
        .with_metadata_provider(Arc::new(WritableMetadata::default()))
        .build()
        .await
        .unwrap();
    storage
        .write(
            "original",
            b"hello".as_slice().into(),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(root.path().join("physical/original")).unwrap(),
        b"hello"
    );
    assert_eq!(
        storage
            .info("original", InfoOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        storage
            .info("original", InfoOptions { strict: false })
            .await
            .unwrap()
            .key,
        key("original")
    );
    storage.commit_updates().await.unwrap();
    assert_eq!(
        storage
            .read("original", ReadOptions::default())
            .await
            .unwrap(),
        "hello"
    );
    assert_eq!(
        storage
            .write(
                "original",
                b"new".as_slice().into(),
                WriteOptions::default()
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );
    storage
        .copy("original", "copied", CopyOptions::default())
        .await
        .unwrap();
    storage.make_symlink("linked", "original").await.unwrap();
    storage.commit_updates().await.unwrap();
    assert_eq!(
        storage
            .read("linked", ReadOptions::default())
            .await
            .unwrap(),
        "hello"
    );

    let removed = storage
        .delete_many(
            &[key("linked"), key("copied"), key("missing")],
            DeleteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(removed.succeeded, [0, 1, 2]);
    assert!(removed.failed.is_empty());
    assert!(root.path().join("physical/original").exists());
    assert!(!root.path().join("physical/copied").exists());
    storage.commit_updates().await.unwrap();
    assert!(!storage.is_file("linked").await.unwrap());
}

#[tokio::test]
async fn mapped_writes_protect_pending_objects_and_dangling_symlinks() {
    let root = tempfile::tempdir().unwrap();
    let mapped = builder(root.path())
        .with_metadata_provider(Arc::new(WritableMetadata::default()))
        .build()
        .await
        .unwrap();
    let clone = mapped.clone();
    let bytes = bytes::Bytes::from_static(b"first");
    let (first, second) = tokio::join!(
        mapped.write("new/key", bytes.clone(), WriteOptions::default()),
        clone.write("new//key", bytes, WriteOptions::default()),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert_eq!(
        first.err().or_else(|| second.err()).unwrap().kind,
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        std::fs::read(root.path().join("physical/new/key")).unwrap(),
        b"first"
    );

    mapped.make_symlink("dangling", "missing").await.unwrap();
    mapped.commit_updates().await.unwrap();
    assert!(!mapped.is_file("dangling").await.unwrap());
    assert_eq!(
        mapped
            .write("dangling", bytes::Bytes::new(), WriteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );
}

#[tokio::test]
async fn mapped_symlink_to_a_directory_is_not_a_file() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("physical-dir")).unwrap();
    let catalog = WritableMetadata::with_entries(
        [("dir", ObjectMetadata::directory(key("physical-dir")))],
        None,
    );
    let mapped = builder(root.path())
        .with_metadata_provider(Arc::new(catalog))
        .build()
        .await
        .unwrap();
    mapped.make_symlink("link", "dir").await.unwrap();
    mapped.commit_updates().await.unwrap();
    assert!(!mapped.is_file("link").await.unwrap());
}

#[tokio::test]
async fn mapped_destinations_reject_conditions_before_publishing() {
    let root = tempfile::tempdir().unwrap();
    let mapped = builder(root.path())
        .with_metadata_provider(Arc::new(WritableMetadata::default()))
        .build()
        .await
        .unwrap();
    let bytes = bytes::Bytes::from_static(b"data");
    mapped
        .write("source", bytes.clone(), WriteOptions::default())
        .await
        .unwrap();
    mapped.commit_updates().await.unwrap();
    let conditional = WriteOptions {
        if_none_match: Some("*".into()),
        ..WriteOptions::default()
    };
    let source = root.path().join("physical/source");
    for error in [
        mapped
            .write("write", bytes, conditional.clone())
            .await
            .unwrap_err(),
        mapped
            .upload_file("upload", source, conditional.clone())
            .await
            .unwrap_err(),
        mapped
            .copy(
                "source",
                "copy",
                CopyOptions {
                    destination: conditional,
                    ..CopyOptions::default()
                },
            )
            .await
            .unwrap_err(),
    ] {
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert!(
            error
                .to_string()
                .contains("conditional logical destination")
        );
    }
    assert_eq!(
        std::fs::read_dir(root.path().join("physical"))
            .unwrap()
            .count(),
        1
    );
}

#[tokio::test]
async fn mapped_delete_retains_shared_bytes_and_preserves_missing_match_errors() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("shared"), b"data").unwrap();
    let catalog = WritableMetadata::with_entries(
        [
            ("alias", ObjectMetadata::file(key("shared"), 4)),
            ("other", ObjectMetadata::file(key("shared"), 4)),
            ("missing", ObjectMetadata::file(key("absent"), 4)),
        ],
        None,
    );
    let mapped = builder(root.path())
        .with_metadata_provider(Arc::new(catalog))
        .build()
        .await
        .unwrap();
    mapped
        .delete("alias", false, DeleteOptions::default())
        .await
        .unwrap();
    let conditional = DeleteOptions {
        if_match: Some("*".into()),
        ..DeleteOptions::default()
    };
    assert_eq!(
        mapped
            .delete("missing", false, conditional)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    mapped.commit_updates().await.unwrap();
    assert_eq!(
        mapped.read("other", ReadOptions::default()).await.unwrap(),
        "data"
    );
    assert!(!mapped.is_file("alias").await.unwrap());
    assert!(mapped.info("missing", InfoOptions::default()).await.is_ok());
    mapped
        .delete("missing", false, DeleteOptions::default())
        .await
        .unwrap();
    mapped.commit_updates().await.unwrap();
    assert!(!mapped.is_file("missing").await.unwrap());
}

#[tokio::test]
async fn mapped_upload_and_copy_keep_user_attributes_in_logical_metadata() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::write(&source, b"data").unwrap();
    let physical = client(root.path()).await;
    let mapped = builder(root.path())
        .with_metadata_provider(Arc::new(WritableMetadata::default()))
        .build()
        .await
        .unwrap();
    let original = BTreeMap::from([("scores".into(), serde_json::json!([1, 2]))]);
    mapped
        .upload_file(
            "uploaded",
            &source,
            WriteOptions {
                attributes: original.clone(),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap();
    mapped.commit_updates().await.unwrap();

    mapped
        .copy("uploaded", "inherited", CopyOptions::default())
        .await
        .unwrap();
    let replacement = BTreeMap::from([("reviewed".into(), serde_json::json!(true))]);
    mapped
        .copy(
            "uploaded",
            "replaced",
            CopyOptions {
                destination: WriteOptions {
                    attributes: replacement.clone(),
                    ..WriteOptions::default()
                },
                ..CopyOptions::default()
            },
        )
        .await
        .unwrap();
    mapped.commit_updates().await.unwrap();

    for (path, expected) in [
        ("uploaded", &original),
        ("inherited", &original),
        ("replaced", &replacement),
    ] {
        let logical = mapped.info(path, InfoOptions::default()).await.unwrap();
        assert_eq!(&logical.attributes, expected);
        let stored = physical
            .info(format!("physical/{path}"), InfoOptions::default())
            .await
            .unwrap();
        assert!(stored.attributes.is_empty());
    }
}

#[tokio::test]
async fn inherited_copy_rejects_changed_physical_generation() {
    let root = tempfile::tempdir().unwrap();
    let mapped = builder(root.path())
        .with_metadata_provider(Arc::new(WritableMetadata::default()))
        .build()
        .await
        .unwrap();
    mapped
        .write(
            "source",
            bytes::Bytes::from_static(b"old"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    mapped.commit_updates().await.unwrap();
    client(root.path())
        .await
        .write(
            "physical/source",
            bytes::Bytes::from_static(b"new bytes"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        mapped
            .copy("source", "inherited", CopyOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PreconditionFailed
    );
    assert!(!root.path().join("physical/inherited").exists());
    mapped
        .copy(
            "source",
            "explicit",
            CopyOptions {
                destination: WriteOptions {
                    attributes: BTreeMap::from([("reviewed".into(), serde_json::json!(true))]),
                    ..WriteOptions::default()
                },
                ..CopyOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(root.path().join("physical/explicit")).unwrap(),
        b"new bytes"
    );
}

fn catalog_config(root: &Path) -> ResolvedConfig {
    let path = root.join("msc_config.yaml");
    std::fs::write(
        &path,
        "\
profiles:
  reports:
    storage_provider:
      type: file
      options:
        base_path: ${STORAGE_ROOT}
    metadata_provider:
      type: report_catalog
      options:
        endpoint: https://catalog.example.com
        routing:
          prefix: physical
          revisions: [1, 2]
",
    )
    .unwrap();
    ConfigLoader::new()
        .with_explicit_paths([path])
        .with_environment(BTreeMap::from([(
            "STORAGE_ROOT".into(),
            root.to_str().unwrap().into(),
        )]))
        .load()
        .unwrap()
}

fn catalog_provider(physical_path: &str) -> Arc<dyn MetadataProvider> {
    Arc::new(WritableMetadata::with_entries(
        [("report.csv", ObjectMetadata::file(key(physical_path), 6))],
        None,
    ))
}

#[tokio::test]
async fn yaml_metadata_factory_receives_options_and_maps_reads() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("physical")).unwrap();
    std::fs::write(root.path().join("physical/report.csv"), "report").unwrap();
    let mut registry = MetadataProviderRegistry::new();
    registry
        .register(
            "report_catalog",
            Arc::new(|options| {
                Box::pin(async move {
                    assert_eq!(options["endpoint"], "https://catalog.example.com");
                    let routing = &options["routing"];
                    assert_eq!(routing["revisions"], serde_json::json!([1, 2]));
                    let prefix = routing["prefix"].as_str().unwrap();
                    Ok(catalog_provider(&format!("{prefix}/report.csv")))
                })
            }),
        )
        .unwrap();
    assert!(registry.contains("report_catalog"));
    let duplicate = registry
        .register("report_catalog", Arc::new(|_| panic!("replaced factory")))
        .unwrap_err();
    assert_eq!(duplicate.kind, ErrorKind::InvalidConfig);
    assert!(
        duplicate.message.contains("report_catalog")
            && duplicate.message.contains("already registered")
    );

    let client = StorageClient::builder(catalog_config(root.path()), "reports")
        .with_metadata_provider_registry(registry)
        .build()
        .await
        .unwrap();
    let report = client
        .read("report.csv", ReadOptions::default())
        .await
        .unwrap();
    assert_eq!(report, "report");
}

#[tokio::test]
async fn yaml_metadata_factory_reports_unregistered_tags_and_factory_errors() {
    let root = tempfile::tempdir().unwrap();
    let config = catalog_config(root.path());
    let error = StorageClient::builder(config.clone(), "reports")
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::InvalidConfig);
    assert!(
        error.message.contains("unknown metadata_provider.type")
            && error.message.contains("reports")
    );

    let mut registry = MetadataProviderRegistry::new();
    registry
        .register(
            "report_catalog",
            Arc::new(|_| {
                Box::pin(async { Err(MscError::new(ErrorKind::Unavailable, "catalog is offline")) })
            }),
        )
        .unwrap();
    let error = StorageClient::builder(config, "reports")
        .with_metadata_provider_registry(registry)
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Unavailable);
    assert!(error.retryable);
    assert!(
        error.message.contains("report_catalog") && error.message.contains("catalog is offline")
    );
}

#[tokio::test]
async fn explicit_metadata_provider_skips_factory_selection() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("injected.csv"), "report").unwrap();
    let config = catalog_config(root.path());
    let mut registered = MetadataProviderRegistry::new();
    registered
        .register(
            "report_catalog",
            Arc::new(|_| panic!("factory called despite override")),
        )
        .unwrap();
    for registry in [MetadataProviderRegistry::new(), registered] {
        let client = StorageClient::builder(config.clone(), "reports")
            .with_metadata_provider(catalog_provider("injected.csv"))
            .with_metadata_provider_registry(registry)
            .build()
            .await
            .unwrap();
        assert_eq!(
            client
                .read("report.csv", ReadOptions::default())
                .await
                .unwrap(),
            "report"
        );
    }
}
