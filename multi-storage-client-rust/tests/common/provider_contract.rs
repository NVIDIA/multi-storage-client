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

//! Shared public-provider assertions, exercised independently of `StorageClient`.
use bytes::Bytes;
use futures::TryStreamExt;
use multi_storage_client::{
    ByteRange, CopyOptions, DeleteOptions, ErrorKind, InfoOptions, ListOptions, ObjectKind,
    ObjectPath, ReadOptions, StorageProvider, SuffixRange, WriteOptions,
};
use std::{collections::BTreeMap, path::Path};

#[allow(clippy::too_many_lines)]
pub async fn assert_contract(provider: &dyn StorageProvider, local: &Path) {
    let options = WriteOptions {
        attributes: BTreeMap::from([("owner".into(), "msc".into())]),
        ..WriteOptions::default()
    };
    provider
        .put_object("data//file", Bytes::from_static(b"0123456789"), options)
        .await
        .unwrap();
    let metadata = provider
        .get_object_metadata("data/file", InfoOptions::default())
        .await
        .unwrap();
    assert_eq!(metadata.key.as_str(), "data/file");
    assert_eq!(metadata.content_length, 10);
    assert_eq!(metadata.attributes["owner"], "msc");
    assert!(provider.is_file("data/file").await.unwrap());
    assert!(!provider.is_file("missing").await.unwrap());
    assert_eq!(
        provider
            .get_object("data/file", ReadOptions::default())
            .await
            .unwrap(),
        "0123456789"
    );
    for (range, expected) in [
        (ByteRange::new(2, 3).unwrap().into(), "234"),
        (SuffixRange::new(3).unwrap().into(), "789"),
    ] {
        assert_eq!(
            provider
                .get_object(
                    "data/file",
                    ReadOptions {
                        range: Some(range),
                        ..ReadOptions::default()
                    }
                )
                .await
                .unwrap(),
            expected
        );
    }
    provider
        .copy_object("data/file", "copied", CopyOptions::default())
        .await
        .unwrap();
    assert_eq!(
        provider
            .get_object_metadata("copied", InfoOptions::default())
            .await
            .unwrap()
            .attributes["owner"],
        "msc"
    );
    provider
        .put_object("empty", Bytes::new(), WriteOptions::default())
        .await
        .unwrap();
    assert!(
        provider
            .get_object("empty", ReadOptions::default())
            .await
            .unwrap()
            .is_empty()
    );
    let source = local.join("source");
    let destination = local.join("destination");
    std::fs::write(&source, b"local bytes").unwrap();
    provider
        .upload_file("uploaded", &source, WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(
        provider
            .get_object(
                "uploaded",
                ReadOptions {
                    max_in_memory_bytes: Some(3),
                    ..ReadOptions::default()
                }
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    provider
        .download_file("uploaded", &destination, ReadOptions::default())
        .await
        .unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), b"local bytes");
    let objects: Vec<_> = provider
        .list_objects_recursive(ListOptions {
            recursive: false,
            start_after: Some(ObjectPath::new("copied").unwrap()),
            end_at: Some(ObjectPath::new("empty").unwrap()),
            attribute_filter_expression: Some(r#"owner = "msc""#.into()),
            ..ListOptions::default()
        })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key.as_str(), "data/file");
    assert!(objects[0].attributes.is_empty());
    for path in ["../escape", "/absolute", "data/../escape"] {
        assert_eq!(
            provider
                .get_object(path, ReadOptions::default())
                .await
                .unwrap_err()
                .kind,
            ErrorKind::InvalidPath
        );
    }
    assert_eq!(
        provider
            .delete_object("missing", DeleteOptions::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    let paths: Vec<_> = ["data/file", "copied", "empty", "uploaded", "missing"]
        .into_iter()
        .map(|key| ObjectPath::new(key).unwrap())
        .collect();
    let batch = provider
        .delete_objects(&paths, DeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(batch.succeeded, [0, 1, 2, 3, 4]);
    assert!(batch.failed.is_empty());
    assert!(!provider.is_file("data/file").await.unwrap());
}

pub async fn assert_symlink_contract(provider: &dyn StorageProvider) {
    provider
        .put_object(
            "target",
            Bytes::from_static(b"target bytes"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    provider
        .make_symlink("links/alias", "target")
        .await
        .unwrap();
    let link = provider
        .get_object_metadata("links/alias", InfoOptions::default())
        .await
        .unwrap();
    assert_eq!(link.key.as_str(), "links/alias");
    assert_eq!(link.kind, ObjectKind::Symlink);
    assert_eq!(link.symlink_target.as_deref(), Some("../target"));
    assert_eq!(
        provider
            .get_object("links/alias", ReadOptions::default())
            .await
            .unwrap(),
        "target bytes"
    );
    assert!(provider.is_file("links/alias").await.unwrap());
    provider
        .delete_object("links/alias", DeleteOptions::default())
        .await
        .unwrap();
    assert!(provider.is_file("target").await.unwrap());
    provider
        .delete_object("target", DeleteOptions::default())
        .await
        .unwrap();
}
