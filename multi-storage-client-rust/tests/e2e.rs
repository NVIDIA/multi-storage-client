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

//! Explicitly configured remote backend tests, outside the build gate.
#![cfg(feature = "s3")]

use bytes::Bytes;
use futures::{FutureExt, TryStreamExt};
use multi_storage_client::{
    ByteRange, ConfigLoader, CopyOptions, DeleteOptions, ListOptions, ObjectPath, ReadOptions,
    StorageClient, WriteOptions,
};
use std::panic::{AssertUnwindSafe, resume_unwind};

#[tokio::test]
#[ignore = "requires MSC_E2E_PROFILE and configured backend credentials; run just run-e2e-tests"]
async fn configured_backend_round_trip() {
    let profile = std::env::var("MSC_E2E_PROFILE")
        .expect("set MSC_E2E_PROFILE and configure that profile through MSC_CONFIG");
    let config = ConfigLoader::new().load().unwrap();
    let profile_config = config.profile(&profile).unwrap();
    let provider = profile_config.storage_provider.as_ref().unwrap();
    assert_ne!(
        provider.provider_type, "file",
        "E2E tests require a remote backend profile"
    );
    let client = StorageClient::builder(config, profile)
        .build()
        .await
        .unwrap();
    let prefix = format!("msc-native-e2e-{}", uuid::Uuid::new_v4());
    let source = ObjectPath::new(format!("{prefix}/source")).unwrap();
    let destination = ObjectPath::new(format!("{prefix}/copy")).unwrap();
    let outcome = AssertUnwindSafe(async {
        let payload = Bytes::from_static(b"rust native e2e");
        client
            .write(&source, payload.clone(), WriteOptions::default())
            .await
            .unwrap();
        assert_eq!(
            client.read(&source, ReadOptions::default()).await.unwrap(),
            payload
        );
        assert_eq!(
            client
                .read(
                    &source,
                    ReadOptions {
                        range: Some((ByteRange::new(5, 6).unwrap()).into()),
                        ..ReadOptions::default()
                    }
                )
                .await
                .unwrap(),
            "native"
        );
        let directory = tempfile::tempdir().unwrap();
        let local_source = directory.path().join("source");
        let local_destination = directory.path().join("downloaded");
        tokio::fs::write(&local_source, &payload).await.unwrap();
        client
            .upload_file(&source, &local_source, WriteOptions::default())
            .await
            .unwrap();
        client
            .copy(&source, &destination, CopyOptions::default())
            .await
            .unwrap();
        assert_eq!(
            client
                .read(&destination, ReadOptions::default())
                .await
                .unwrap(),
            payload
        );
        client
            .download_file(
                &destination,
                &local_destination,
                ReadOptions {
                    max_in_memory_bytes: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(local_destination).await.unwrap(), payload);
        let objects = client
            .list(ListOptions {
                prefix: ObjectPath::new(&prefix).unwrap(),
                ..Default::default()
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(objects.len(), 2);
        assert!(objects.iter().any(|metadata| metadata.key == source));
        assert!(objects.iter().any(|metadata| metadata.key == destination));
    })
    .catch_unwind()
    .await;
    let cleanup = client
        .delete_many(&[source, destination], DeleteOptions::default())
        .await;
    let shutdown = client.shutdown().await;
    assert!(
        cleanup.unwrap().failed.is_empty(),
        "E2E object cleanup failed"
    );
    shutdown.unwrap();
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
