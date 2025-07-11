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
use object_store::{ObjectStore, path::Path, PutPayload, WriteMultipart};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use thiserror::Error;
use tokio::fs;
use tokio::io::AsyncReadExt;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Object store error: {0}")]
    ObjectStoreError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Configuration error: {0}")]
    ConfigError(String),
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
            "s3" => {
                build_s3_store(configs)?
            }
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Unsupported provider type: '{}'. Only 's3' is currently supported.",
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
}

#[pymodule]
fn multistorageclient_rust(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustClient>()?;
    Ok(())
}
