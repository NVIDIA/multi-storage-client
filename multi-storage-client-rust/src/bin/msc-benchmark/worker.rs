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

use bytes::Bytes;
use futures::{StreamExt, stream};
use multi_storage_client::{
    ConfigLoader, DeleteOptions, ErrorKind, MscError, ReadOptions, StorageClient, WriteOptions,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{fmt, path::PathBuf, sync::Arc, time::Instant};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};

use super::{Result, metrics::Measurements};

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Job {
    pub profile: String,
    pub storage_config: Option<PathBuf>,
    pub prefix: String,
    pub size: String,
    pub size_bytes: u64,
    pub start: usize,
    pub end: usize,
    pub processes: usize,
    pub threads: usize,
    pub payload: PathBuf,
    pub files_directory: Option<PathBuf>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Phase {
    Upload,
    Download,
    UploadFile,
    DownloadFile,
    Delete,
    Stop,
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Upload => "upload",
            Self::Download => "download",
            Self::UploadFile => "upload_file",
            Self::DownloadFile => "download_file",
            Self::Delete => "delete",
            Self::Stop => "stop",
        })
    }
}

pub(super) async fn send<T: Serialize>(
    output: &mut (impl AsyncWrite + Unpin),
    value: &T,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    output.write_all(&bytes).await?;
    output.flush().await?;
    Ok(())
}

pub(super) async fn receive<T: DeserializeOwned>(
    input: &mut (impl AsyncBufRead + Unpin),
) -> Result<T> {
    let mut line = String::new();
    if input.read_line(&mut line).await? == 0 {
        return Err("benchmark worker connection closed unexpectedly".into());
    }
    Ok(serde_json::from_str(&line)?)
}

pub(super) async fn run(job: Job) -> Result<()> {
    // The coordinator waits for the phase, then sends cleanup commands.
    let signal = tokio::spawn(async {
        let _ = tokio::signal::ctrl_c().await;
    });
    let mut input = BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    let services = Services::build(&job).await;
    let ready = services.as_ref().map(|_| ()).map_err(ToString::to_string);
    send(&mut output, &ready).await?;
    let services = services?;
    let outcome = serve(
        &job,
        &services.client,
        &services.data,
        &mut input,
        &mut output,
    )
    .await;
    let shutdown = services.client.shutdown().await;
    signal.abort();
    let _ = signal.await;
    outcome.and(shutdown.map_err(Into::into))
}

struct Services {
    client: StorageClient,
    data: Bytes,
}

impl Services {
    async fn build(job: &Job) -> Result<Self> {
        let data = Bytes::from(tokio::fs::read(&job.payload).await?);
        if data.len() as u64 != job.size_bytes {
            return Err("benchmark payload size changed".into());
        }
        let client = build_client(job).await?;
        Ok(Self { client, data })
    }
}

async fn build_client(job: &Job) -> Result<StorageClient> {
    let mut loader = ConfigLoader::new();
    if let Some(path) = &job.storage_config {
        if !path.is_file() {
            return Err(
                format!("MSC configuration file does not exist: {}", path.display()).into(),
            );
        }
        loader = loader.with_explicit_paths([path]);
    }
    let config = loader.load()?;
    let client = StorageClient::builder(config, &job.profile)
        .with_max_in_memory_bytes(job.size_bytes)
        .build()
        .await?;
    Ok(client)
}

pub(super) async fn cleanup(job: &Job) -> Result<()> {
    let client = build_client(job).await?;
    let result = async {
        let mut keys = Vec::new();
        for index in job.start..job.end {
            keys.push(multi_storage_client::ObjectPath::new(format!(
                "{}/test-{}-{index}",
                job.prefix, job.size
            ))?);
            if job.files_directory.is_some() {
                keys.push(multi_storage_client::ObjectPath::new(format!(
                    "{}/test-file-{}-{index}",
                    job.prefix, job.size
                ))?);
            }
            if keys.len() >= 1000 {
                delete_owned(&client, &keys).await?;
                keys.clear();
            }
        }
        if !keys.is_empty() {
            delete_owned(&client, &keys).await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let shutdown = client.shutdown().await;
    result.and(shutdown.map_err(Into::into))
}

async fn delete_owned(
    client: &StorageClient,
    keys: &[multi_storage_client::ObjectPath],
) -> Result<()> {
    let batch = client.delete_many(keys, DeleteOptions::default()).await?;
    if !batch.failed.is_empty() {
        return Err(multi_storage_client::BatchError {
            failures: batch.failed,
        }
        .into());
    }
    Ok(())
}

async fn serve(
    job: &Job,
    client: &StorageClient,
    data: &Bytes,
    input: &mut (impl AsyncBufRead + Unpin),
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<()> {
    loop {
        let phase = receive(input).await?;
        if matches!(phase, Phase::Stop) {
            return Ok(());
        }
        let response: std::result::Result<Measurements, String> =
            Ok(measure(job, client, data, phase).await);
        send(output, &response).await?;
    }
}

async fn measure(job: &Job, client: &StorageClient, data: &Bytes, phase: Phase) -> Measurements {
    let shared_job = Arc::new(job.clone());
    let mut tasks = stream::iter(job.start..job.end)
        .map(|index| {
            let job = Arc::clone(&shared_job);
            let client = client.clone();
            let data = data.clone();
            async move {
                tokio::spawn(async move {
                    let start = Instant::now();
                    let result = transfer(&job, &client, &data, phase, index).await;
                    (index, start.elapsed().as_secs_f64(), result)
                })
                .await
                .unwrap_or_else(|error| {
                    (
                        index,
                        0.0,
                        Err(MscError::new(
                            ErrorKind::Cancelled,
                            format!("benchmark task failed: {error}"),
                        )),
                    )
                })
            }
        })
        .buffer_unordered(job.threads);
    let mut measurements = Measurements::default();
    while let Some((index, elapsed, result)) = tasks.next().await {
        match result {
            Ok(bytes) => {
                measurements.succeeded += 1;
                measurements.bytes += bytes;
                measurements.latencies.push(elapsed);
            }
            Err(error) => {
                measurements.failed += 1;
                if measurements.failed <= 10 {
                    eprintln!("{phase} failed for test-{}-{index}: {error}", job.size);
                }
            }
        }
    }
    measurements
}

async fn transfer(
    job: &Job,
    client: &StorageClient,
    data: &Bytes,
    phase: Phase,
    index: usize,
) -> multi_storage_client::Result<u64> {
    let name = format!("test-{}-{index}", job.size);
    let object = format!("{}/{name}", job.prefix);
    let file_object = format!("{}/test-file-{}-{index}", job.prefix, job.size);
    match phase {
        Phase::Upload => {
            client
                .write(&object, data.clone(), WriteOptions::default())
                .await?;
        }
        Phase::Download => {
            let bytes = client.read(&object).await?;
            validate_length(bytes.len() as u64, job.size_bytes)?;
        }
        Phase::UploadFile | Phase::DownloadFile => {
            let directory = job.files_directory.as_ref().ok_or_else(|| {
                MscError::new(
                    ErrorKind::InvalidConfig,
                    "file tests require a scratch directory",
                )
            })?;
            let local = directory.join(name);
            let metadata = if matches!(phase, Phase::UploadFile) {
                client
                    .upload_from(&file_object, local, WriteOptions::default())
                    .await?
            } else {
                client
                    .download_to(&file_object, local, ReadOptions::default())
                    .await?
            };
            validate_length(metadata.content_length, job.size_bytes)?;
        }
        Phase::Delete => {
            let object_result = client
                .delete(&object, false, DeleteOptions::default())
                .await;
            let file_result = if job.files_directory.is_some() {
                client
                    .delete(&file_object, false, DeleteOptions::default())
                    .await
            } else {
                Ok(())
            };
            object_result.and(file_result)?;
            return Ok(0);
        }
        Phase::Stop => unreachable!("control phases do not run transfers"),
    }
    Ok(job.size_bytes)
}

fn validate_length(actual: u64, expected: u64) -> multi_storage_client::Result<()> {
    if actual != expected {
        return Err(MscError::new(
            ErrorKind::Integrity,
            "downloaded object size differs from benchmark configuration",
        ));
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use multi_storage_client::{
        ByteRange, ByteStream, GetConditions, ListOptions, MetadataStream, ObjectMetadata,
        ObjectPath, ProviderCapabilities, PutResult, StorageProvider,
        provider::posix::{PosixOptions, PosixProvider},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ObservedProvider {
        inner: PosixProvider,
        active: AtomicUsize,
        maximum: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl StorageProvider for ObservedProvider {
        fn name(&self) -> &str {
            self.inner.name()
        }
        fn capabilities(&self) -> ProviderCapabilities {
            self.inner.capabilities()
        }
        async fn head(&self, key: &ObjectPath) -> multi_storage_client::Result<ObjectMetadata> {
            self.inner.head(key).await
        }
        async fn get(
            &self,
            key: &ObjectPath,
            conditions: &GetConditions,
        ) -> multi_storage_client::Result<ByteStream> {
            self.inner.get(key, conditions).await
        }
        async fn get_range(
            &self,
            key: &ObjectPath,
            range: ByteRange,
            conditions: &GetConditions,
        ) -> multi_storage_client::Result<Bytes> {
            self.inner.get_range(key, range, conditions).await
        }
        async fn put(
            &self,
            key: &ObjectPath,
            body: ByteStream,
            size: Option<u64>,
            options: &WriteOptions,
        ) -> multi_storage_client::Result<PutResult> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let result = self.inner.put(key, body, size, options).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            result
        }
        async fn delete(
            &self,
            key: &ObjectPath,
            options: &DeleteOptions,
        ) -> multi_storage_client::Result<()> {
            self.inner.delete(key, options).await
        }
        async fn list(
            &self,
            options: &ListOptions,
        ) -> multi_storage_client::Result<MetadataStream> {
            self.inner.list(options).await
        }
    }

    #[tokio::test]
    async fn fallback_cleanup_is_independent_of_worker_channels_and_payload_files() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("objects");
        std::fs::create_dir_all(root.join("owned")).unwrap();
        for name in ["test-1KB-0", "test-file-1KB-0", "sentinel"] {
            std::fs::write(root.join("owned").join(name), b"data").unwrap();
        }
        let config = directory.path().join("storage.json");
        std::fs::write(&config, serde_json::to_vec(&serde_json::json!({"profiles":{"local":{"storage_provider":{"type":"file","options":{"base_path":root}}}}})).unwrap()).unwrap();
        let job = Job {
            profile: "local".into(),
            storage_config: Some(config),
            prefix: "owned".into(),
            size: "1KB".into(),
            size_bytes: 1024,
            start: 0,
            end: 1,
            processes: 1,
            threads: 1,
            payload: directory.path().join("missing-payload"),
            files_directory: Some(directory.path().join("missing-files")),
        };
        cleanup(&job).await.unwrap();
        assert_eq!(std::fs::read_dir(root.join("owned")).unwrap().count(), 1);
        assert_eq!(std::fs::read(root.join("owned/sentinel")).unwrap(), b"data");
    }

    #[tokio::test]
    async fn worker_counts_only_its_partition_and_bounds_active_operations_by_threads() {
        for threads in [1, 3] {
            let directory = tempfile::tempdir().unwrap();
            let provider = Arc::new(ObservedProvider {
                inner: PosixProvider::new(PosixOptions::new(directory.path())).unwrap(),
                active: AtomicUsize::new(0),
                maximum: AtomicUsize::new(0),
            });
            let client = StorageClient::from_provider(provider.clone());
            let job = Job {
                profile: "test".into(),
                storage_config: None,
                prefix: "run".into(),
                size: "1KB".into(),
                size_bytes: 1024,
                start: 2,
                end: 9,
                processes: 4,
                threads,
                payload: PathBuf::new(),
                files_directory: None,
            };
            let result = measure(&job, &client, &Bytes::from(vec![1; 1024]), Phase::Upload).await;
            assert_eq!(result.succeeded, 7);
            assert_eq!(result.failed, 0);
            assert_eq!(result.bytes, 7 * 1024);
            assert_eq!(result.latencies.len(), 7);
            assert_eq!(provider.maximum.load(Ordering::SeqCst), threads);
            assert_eq!(provider.active.load(Ordering::SeqCst), 0);
            assert!(!directory.path().join("run/test-1KB-1").exists());
            assert!(!directory.path().join("run/test-1KB-9").exists());
            client.shutdown().await.unwrap();
        }
    }
}
