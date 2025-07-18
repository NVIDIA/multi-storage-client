// SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, path::Path, PutPayload, WriteMultipart};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use pyo3_async_runtimes::tokio::future_into_py;
use std::path::Path as StdPath;
use std::sync::Arc;
use thiserror::Error;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tokio::sync::{Semaphore, mpsc};
use tempfile::NamedTempFile;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Object store error: {0}")]
    ObjectStoreError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Temp file error: {0}")]
    TempFileError(#[from] tempfile::PersistError),
}

impl From<object_store::Error> for StorageError {
    fn from(err: object_store::Error) -> Self {
        StorageError::ObjectStoreError(err.to_string())
    }
}

impl From<StorageError> for PyErr {
    fn from(err: StorageError) -> PyErr {
        match err {
            StorageError::ConfigError(msg) => {
                pyo3::exceptions::PyValueError::new_err(msg)
            }
            _ => {
                pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
            }
        }
    }
}


const DEFAULT_MULTIPART_CHUNKSIZE: usize = 32 * 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: usize = 32;


fn build_s3_store(configs: Option<&Bound<'_, PyDict>>) -> PyResult<(Arc<dyn ObjectStore>, usize, usize)> {
    // TODO: Add support for other configuration fields of AmazonS3Builder, full list here:
    // https://docs.rs/object_store/latest/src/object_store/aws/builder.rs.html#123
    let mut builder = AmazonS3Builder::new();

    let configs = configs.ok_or_else(|| {
        StorageError::ConfigError("Configuration dictionary is required for S3 provider.".to_string())
    })?;

    if let Some(bucket_val) = configs.get_item("bucket")? {
        builder = builder.with_bucket_name(bucket_val.extract::<String>()?);
    }

    if let Some(region_val) = configs.get_item("region_name")? {
        builder = builder.with_region(region_val.extract::<String>()?);
    }

    if let Some(endpoint_val) = configs.get_item("endpoint_url")? {
        builder = builder.with_endpoint(endpoint_val.extract::<String>()?);
    }

    if let Some(key_id_val) = configs.get_item("aws_access_key_id")? {
        builder = builder.with_access_key_id(key_id_val.extract::<String>()?);
    }

    if let Some(secret_key_val) = configs.get_item("aws_secret_access_key")? {
        builder = builder.with_secret_access_key(secret_key_val.extract::<String>()?);
    }

    if let Some(token_val) = configs.get_item("aws_session_token")? {
        builder = builder.with_token(token_val.extract::<String>()?);
    }

    if let Some(allow_http_val) = configs.get_item("allow_http")? {
        if allow_http_val.extract::<bool>()? {
            builder = builder.with_allow_http(true);
        }
    }

    let store = builder.build().map_err(StorageError::from)?;

    let max_concurrency = if let Some(val) = configs.get_item("max_concurrency")? {
        val.extract::<usize>()?
    } else {
        DEFAULT_MAX_CONCURRENCY
    };
    let multipart_chunksize = if let Some(val) = configs.get_item("multipart_chunksize")? {
        val.extract::<usize>()?
    } else {
        DEFAULT_MULTIPART_CHUNKSIZE
    };

    Ok((Arc::new(store), max_concurrency, multipart_chunksize))
}


fn build_gcs_store(configs: Option<&Bound<'_, PyDict>>) -> PyResult<(Arc<dyn ObjectStore>, usize, usize)> {
    let mut builder = GoogleCloudStorageBuilder::new();

    let configs = configs.ok_or_else(|| {
        StorageError::ConfigError("Configuration dictionary is required for GCS provider.".to_string())
    })?;

    if let Some(bucket_val) = configs.get_item("bucket")? {
        builder = builder.with_bucket_name(bucket_val.extract::<String>()?);
    }

    if let Some(service_account) = configs.get_item("service_account_key")? {
        builder = builder.with_service_account_key(service_account.extract::<String>()?);
    }

    if let Some(service_account_path) = configs.get_item("service_account_path")? {
        builder = builder.with_service_account_path(service_account_path.extract::<String>()?);
    }

    if let Some(application_credentials) = configs.get_item("application_credentials")? {
        builder = builder.with_application_credentials(application_credentials.extract::<String>()?);
    }

    if let Some(skip_signature) = configs.get_item("skip_signature")? {
        if skip_signature.extract::<bool>()? {
            builder = builder.with_skip_signature(true);
        }
    }

    if let Some(proxy_url) = configs.get_item("proxy_url")? {
        builder = builder.with_proxy_url(proxy_url.extract::<String>()?);
    }

    if let Some(proxy_ca_certificate) = configs.get_item("proxy_ca_certificate")? {
        builder = builder.with_proxy_ca_certificate(proxy_ca_certificate.extract::<String>()?);
    }

    if let Some(proxy_excludes) = configs.get_item("proxy_excludes")? {
        builder = builder.with_proxy_excludes(proxy_excludes.extract::<String>()?);
    }

    if let Some(url) = configs.get_item("url")? {
        builder = builder.with_url(url.extract::<String>()?);
    }

    let store = builder.build().map_err(StorageError::from)?;

    let max_concurrency = if let Some(val) = configs.get_item("max_concurrency")? {
        val.extract::<usize>()?
    } else {
        DEFAULT_MAX_CONCURRENCY
    };
    let multipart_chunksize = if let Some(val) = configs.get_item("multipart_chunksize")? {
        val.extract::<usize>()?
    } else {
        DEFAULT_MULTIPART_CHUNKSIZE
    };

    Ok((Arc::new(store), max_concurrency, multipart_chunksize))
}

#[pyclass]
pub struct RustClient {
    store: Arc<dyn ObjectStore>,
    max_concurrency: usize,
    multipart_chunksize: usize,
}

#[pymethods]
impl RustClient {
    #[new]
    #[pyo3(signature = (provider="s3", configs=None))]
    fn new(
        provider: &str,
        configs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let (store, max_concurrency, multipart_chunksize) = match provider.to_lowercase().as_str() {
            "s3" | "s8k" | "gcs_s3" => {
                build_s3_store(configs)?
            }
            "gcs" => {
                build_gcs_store(configs)?
            }
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Unsupported provider type: '{}'. Supported providers are: s3, s8k, gcs_s3, gcs",
                    provider
                )));
            }
        };

        Ok(Self { store, max_concurrency, multipart_chunksize })
    }

    #[pyo3(signature = (path, data))]
    fn put<'p>(&self, py: Python<'p>, path: &str, data: &[u8]) -> PyResult<Bound<'p, PyAny>> {
        let store = Arc::clone(&self.store);
        let path = Path::from(path);
        let payload = PutPayload::from_bytes(Bytes::copy_from_slice(data));

        future_into_py(py, async move {
            store
                .put(&path, payload)
                .await
                .map_err(StorageError::from)?;
            Ok(())
        })
    }

    #[pyo3(signature = (path, start=None, end=None))]
    fn get<'p>(
        &self,
        py: Python<'p>,
        path: &str,
        start: Option<u64>,
        end: Option<u64>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let store = Arc::clone(&self.store);
        let path = Path::from(path);

        if let (Some(start_idx), Some(end_idx)) = (start, end) {
            future_into_py(py, async move {
                let result = store
                    .get_range(&path, start_idx..end_idx)
                    .await
                    .map_err(StorageError::from)?;
                Ok(result.to_vec())
            })
        } else {
            future_into_py(py, async move {
                let result = store.get(&path).await.map_err(StorageError::from)?;
                let data = result.bytes().await.map_err(StorageError::from)?;
                Ok(data.to_vec())
            })
        }
    }

    #[pyo3(signature = (local_path, remote_path))]
    fn upload<'p>(
        &self,
        py: Python<'p>,
        local_path: &str,
        remote_path: &str,
    ) -> PyResult<Bound<'p, PyAny>> {
        let store = Arc::clone(&self.store);
        let local_path = local_path.to_string();
        let remote_path = Path::from(remote_path);

        future_into_py(py, async move {
            let data = fs::read(local_path).await.map_err(StorageError::from)?;
            store
                .put(&remote_path, data.into())
                .await
                .map_err(StorageError::from)?;
            Ok(())
        })
    }

    #[pyo3(signature = (remote_path, local_path))]
    fn download<'p>(
        &self,
        py: Python<'p>,
        remote_path: &str,
        local_path: &str,
    ) -> PyResult<Bound<'p, PyAny>> {
        let store = Arc::clone(&self.store);
        let remote_path = Path::from(remote_path);
        let local_path = local_path.to_string();

        future_into_py(py, async move {
            let result = store.get(&remote_path).await.map_err(StorageError::from)?;
            let data = result.bytes().await.map_err(StorageError::from)?;
            fs::write(&local_path, data)
                .await
                .map_err(StorageError::from)?;
            Ok(())
        })
    }


    #[pyo3(signature = (local_path, remote_path))]
    fn upload_multipart<'p>(
        &self,
        py: Python<'p>,
        local_path: &str,
        remote_path: &str,
    ) -> PyResult<Bound<'p, PyAny>> {
        let store = Arc::clone(&self.store);
        let local_path = local_path.to_string();
        let remote_path = Path::from(remote_path);
        let max_concurrency = self.max_concurrency;
        let multipart_chunksize = self.multipart_chunksize;

        future_into_py(py, async move {
            let mut file = tokio::fs::File::open(local_path).await.map_err(StorageError::from)?;

            let upload = store.put_multipart(&remote_path).await.map_err(StorageError::from)?;
            let mut writer = WriteMultipart::new_with_chunk_size(upload, multipart_chunksize);

            let mut buffer = vec![0u8; multipart_chunksize];
            loop {
                let n = file.read(&mut buffer).await.map_err(StorageError::from)?;
                if n == 0 {
                    break;
                }
                writer.wait_for_capacity(max_concurrency).await.map_err(StorageError::from)?;
                writer.write(&buffer[..n]);
            }

            writer.finish().await.map_err(StorageError::from)?;

            Ok(())
        })
    }


    #[pyo3(signature = (remote_path, local_path))]
    fn download_multipart<'p>(&self, py: Python<'p>, remote_path: &str, local_path: &str) -> PyResult<Bound<'p, PyAny>> {
        let store = Arc::clone(&self.store);
        let remote_path = Path::from(remote_path);
        let local_path = local_path.to_string();
        let max_concurrency = self.max_concurrency;
        let multipart_chunksize = self.multipart_chunksize;

        future_into_py(py, async move {
            let result = store.head(&remote_path).await.map_err(StorageError::from)?;
            let total_size = result.size;
            
            // Create the temp file in the same directory of local_path because tempfile.persist()
            // does not support cross filesystem.
            let target_path = StdPath::new(&local_path);
            let temp_dir = target_path.parent().unwrap_or_else(|| StdPath::new("."));
            let temp_file = NamedTempFile::new_in(temp_dir).map_err(StorageError::from)?;

            let mut output_file = tokio::fs::File::from_std(temp_file.reopen().map_err(StorageError::from)?);
            output_file.set_len(total_size).await.map_err(StorageError::from)?;
            
            let num_chunks = (total_size + multipart_chunksize as u64 - 1) / multipart_chunksize as u64;
            
            let semaphore = Arc::new(Semaphore::new(max_concurrency));
            let (tx , mut rx): (
                mpsc::Sender<Result<(u64, Vec<u8>), StorageError>>,
                mpsc::Receiver<Result<(u64, Vec<u8>), StorageError>>,
            ) = mpsc::channel(max_concurrency);
            
            // Start a task to process downloaded chunks in arrival order and write to file
            let write_handle = tokio::task::spawn(async move {
                while let Some(result) = rx.recv().await {
                    match result {
                        Ok((chunk_index, data)) => {
                            output_file.seek(tokio::io::SeekFrom::Start(chunk_index as u64 * multipart_chunksize as u64)).await.map_err(StorageError::from)?;
                            output_file.write_all(&data).await.map_err(StorageError::from)?;
                        }
                        Err(e) => {
                            return Err(StorageError::from(e));
                        }
                    }
                }
                output_file.flush().await.map_err(StorageError::from)?;
                output_file.sync_all().await.map_err(StorageError::from)?;
                drop(output_file);

                Ok::<(), StorageError>(())
            });

            // Download chunks in parallel
            for chunk_index in 0..num_chunks {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let store = Arc::clone(&store);
                let remote_path = remote_path.clone();
                let tx = tx.clone();
                let start_offset = chunk_index * multipart_chunksize as u64;
                let end_offset = std::cmp::min(start_offset + multipart_chunksize as u64, total_size);
                
                tokio::task::spawn(async move {
                    let range = start_offset..end_offset;
                    match store.get_range(&remote_path, range).await {
                        Ok(result) => {
                            let data = result.to_vec();
                            let _ = tx.send(Ok((chunk_index, data))).await;
                        }
                        Err(e) => {
                            let _ = tx.send(Err(StorageError::from(e))).await;
                        }
                    }
                    drop(permit);
                });
            }

            drop(tx);

            write_handle.await.unwrap()?;

            temp_file.persist(&local_path).map_err(StorageError::from)?;
            
            Ok(())
        })
    }
}

#[pymodule]
fn multistorageclient_rust(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustClient>()?;
    Ok(())
}
