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

use futures::TryStreamExt;
use multi_storage_client::{
    CopyOptions, DeleteOptions, ErrorKind, InfoOptions, ListOptions, ManifestMetadataProvider,
    MetadataProvider, MetadataStream, MscError, ObjectKind, ObjectMetadata, ObjectPath,
    ReadOptions, ResolvedPath, ResolvedPathState, Result, StorageClient, StorageClientBuilder,
    WriteOptions,
    provider::posix::{PosixOptions, PosixProvider},
};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};

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

#[derive(Default)]
struct WritableMetadata {
    state: Mutex<WritableState>,
}

#[derive(Default)]
struct WritableState {
    committed: BTreeMap<String, ObjectMetadata>,
    pending: BTreeMap<String, Option<ObjectMetadata>>,
}

#[async_trait::async_trait]
impl MetadataProvider for WritableMetadata {
    async fn list_objects(&self, _: ListOptions) -> Result<MetadataStream> {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn get_object_metadata(
        &self,
        path: &str,
        include_pending: bool,
    ) -> Result<ObjectMetadata> {
        let state = self.state.lock().unwrap();
        let entry = match state.pending.get(path).filter(|_| include_pending) {
            Some(entry) => entry.as_ref(),
            None => state.committed.get(path),
        };
        let mut entry = entry
            .cloned()
            .ok_or_else(|| MscError::new(ErrorKind::NotFound, "test key missing"))?;
        entry.key = key(path);
        Ok(entry)
    }

    async fn glob(&self, _: &str, _: Option<&str>) -> Result<Vec<ObjectPath>> {
        Ok(Vec::new())
    }

    async fn realpath(&self, path: &str) -> Result<ResolvedPath> {
        let state = self.state.lock().unwrap();
        let mut path = key(path);
        if let Some(link) = state
            .committed
            .get(path.as_str())
            .and_then(|entry| entry.symlink_target.as_deref())
        {
            path = path.resolve_link(link)?;
        }
        Ok(match state.committed.get(path.as_str()) {
            Some(entry) => ResolvedPath {
                physical_path: entry.key.clone(),
                state: ResolvedPathState::Exists,
                profile: None,
            },
            None => ResolvedPath {
                physical_path: path,
                state: ResolvedPathState::Untracked,
                profile: None,
            },
        })
    }

    async fn generate_physical_path(&self, path: &str, _: bool) -> Result<ResolvedPath> {
        Ok(ResolvedPath {
            physical_path: key(&format!("physical/{path}")),
            state: ResolvedPathState::Untracked,
            profile: None,
        })
    }

    async fn add_file(&self, path: &str, metadata: ObjectMetadata) -> Result<()> {
        self.state
            .lock()
            .unwrap()
            .pending
            .insert(path.into(), Some(metadata));
        Ok(())
    }

    async fn remove_file(&self, path: &str) -> Result<()> {
        self.state.lock().unwrap().pending.insert(path.into(), None);
        Ok(())
    }

    async fn commit_updates(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        for (path, entry) in std::mem::take(&mut state.pending) {
            if let Some(entry) = entry {
                state.committed.insert(path, entry);
            } else {
                state.committed.remove(&path);
            }
        }
        Ok(())
    }

    fn is_writable(&self) -> bool {
        true
    }
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
