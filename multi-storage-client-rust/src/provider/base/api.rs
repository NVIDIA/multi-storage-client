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

//! Python-named provider operations over the shared implementation.
use super::BaseStorageProvider;
use crate::{
    BatchResult, CopyOptions, DeleteOptions, InfoOptions, ListOptions, MetadataStream,
    ObjectMetadata, ObjectPath, ReadOptions, Result, StorageBackend, StorageProvider, WriteOptions,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;

#[async_trait]
impl<B: StorageBackend + ?Sized + 'static> StorageProvider for BaseStorageProvider<B> {
    async fn get_object_metadata(
        &self,
        path: &str,
        options: InfoOptions,
    ) -> Result<ObjectMetadata> {
        self.metadata_impl(path, options).await
    }

    async fn get_object(&self, path: &str, mut options: ReadOptions) -> Result<Bytes> {
        if let Some(range) = options.range.take() {
            return self.read_range_impl(path, range, options).await;
        }
        self.read_with_options_impl(path, options).await
    }

    async fn put_object(&self, path: &str, body: Bytes, options: WriteOptions) -> Result<()> {
        self.write_impl(path, body, options).await.map(drop)
    }

    async fn upload_file(&self, remote: &str, local: &Path, options: WriteOptions) -> Result<()> {
        self.upload_file_impl(remote, local, options)
            .await
            .map(drop)
    }

    async fn download_file(&self, remote: &str, local: &Path, options: ReadOptions) -> Result<()> {
        self.download_atomic(remote, local, options).await.map(drop)
    }

    async fn copy_object(
        &self,
        source: &str,
        destination: &str,
        options: CopyOptions,
    ) -> Result<()> {
        self.copy_impl(source, destination, options).await.map(drop)
    }

    async fn make_symlink(&self, path: &str, target: &str) -> Result<()> {
        self.make_symlink_impl(path, target).await.map(drop)
    }

    async fn delete_object(&self, path: &str, options: DeleteOptions) -> Result<()> {
        self.delete_impl(path, options).await
    }

    async fn delete_objects(
        &self,
        paths: &[ObjectPath],
        options: DeleteOptions,
    ) -> Result<BatchResult> {
        self.delete_many_impl(paths, options).await
    }

    async fn list_objects(&self, options: ListOptions) -> Result<MetadataStream> {
        self.list_impl(options).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.state.provider.shutdown().await
    }
}
