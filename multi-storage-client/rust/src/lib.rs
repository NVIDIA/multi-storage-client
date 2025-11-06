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

use chrono::{DateTime, Duration, Utc};
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::RetryConfig;
use object_store::{path::Path, ObjectMeta, ObjectStore, PutPayload, WriteMultipart};
use object_store::ClientOptions;
use object_store::limit::LimitStore;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use pyo3::{PyAny, PyObject};
use pyo3::exceptions::PyException;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_bytes::PyBytes;
use std::collections::{HashMap, VecDeque};
use std::error::Error as StdError;
use std::path::Path as StdPath;
use std::sync::{Arc, RwLock};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

mod types;
use types::{ListResult, ObjectMetadata};

pyo3::create_exception!(multistorageclient_rust, RustRetryableError, PyException);

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
    #[error("Connection error: {0}")]
    RetryExhaustedError(String),
    #[error("Object not found: {0}")]
    NotFound(String),
    #[error("Permission error: {0}")]
    PermissionError(String),
}

impl From<object_store::Error> for StorageError {
    fn from(err: object_store::Error) -> Self {
        let error_msg = format_error_chain(&err);
        
        if error_msg.contains("not found") ||
           error_msg.contains("404 Not Found") ||
           error_msg.contains("NoSuchKey") {
            StorageError::NotFound(error_msg)
        } else if error_msg.contains("HTTP error: error sending request") ||
           error_msg.contains("HTTP error: request or response body error") {
            StorageError::RetryExhaustedError(error_msg)
        } else if error_msg.contains("The operation lacked the necessary privileges") ||
           error_msg.contains("403 Forbidden") {
            StorageError::PermissionError(error_msg)
        } else {
            StorageError::ObjectStoreError(error_msg)
        }
    }
}

fn format_error_chain(err: &object_store::Error) -> String {
    let mut chain = vec![err.to_string()];
    let mut current = err.source();
    
    while let Some(source) = current {
        chain.push(source.to_string());
        current = source.source();
    }
    
    chain.join(" -> ")
}

impl From<StorageError> for PyErr {
    fn from(err: StorageError) -> PyErr {
        match err {
            StorageError::ConfigError(msg) => {
                pyo3::exceptions::PyValueError::new_err(msg)
            }
            StorageError::RetryExhaustedError(msg) => {
                RustRetryableError::new_err(msg)
            }
            StorageError::PermissionError(msg) => {
                pyo3::exceptions::PyPermissionError::new_err(msg)
            }
            StorageError::NotFound(msg) => {
                pyo3::exceptions::PyFileNotFoundError::new_err(msg)
            }
            _ => {
                pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
            }
        }
    }
}

// Multipart upload and download default settings
const DEFAULT_MULTIPART_CHUNKSIZE: usize = 32 * 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: usize = 8;

// Connection timeout settings
const DEFAULT_CONNECT_TIMEOUT: u64 = 60;
const DEFAULT_READ_TIMEOUT: u64 = 120;
const DEFAULT_POOL_IDLE_TIMEOUT: u64 = 30;
const DEFAULT_POOL_CONNECTIONS: usize = 32;

// Refresh credentials threshold in seconds
const DEFAULT_REFRESH_CREDENTIALS_THRESHOLD: u64 = 900; // 15 minutes

fn get_timeout_secs(configs: &HashMap<String, ConfigValue>, key: &str, default: u64) -> u64 {
    configs.get(key)
        .map(|val| match val {
            ConfigValue::Number(n) => *n as u64,
            ConfigValue::String(s) => s.parse::<u64>().unwrap_or(default),
            _ => default,
        })
        .unwrap_or(default)
}

fn extract_credentials_from_provider(
    credentials_provider: &PyObject,
    configs_map: &mut HashMap<String, ConfigValue>,
) -> PyResult<Option<DateTime<Utc>>> {
    let mut credentials_expire_time = None;
    
    Python::with_gil(|py| {
        let credentials = credentials_provider.call_method0(py, "get_credentials")?;

        if let Ok(access_key) = credentials.getattr(py, "access_key")?.extract::<String>(py) {
            configs_map.insert("access_key".to_string(), ConfigValue::String(access_key));
        }
        if let Ok(secret_key) = credentials.getattr(py, "secret_key")?.extract::<String>(py) {
            configs_map.insert("secret_key".to_string(), ConfigValue::String(secret_key));
        }
        if let Ok(token) = credentials.getattr(py, "token")?.extract::<Option<String>>(py) {
            if let Some(token_val) = token {
                configs_map.insert("token".to_string(), ConfigValue::String(token_val));
            }
        }
        if let Ok(expiration) = credentials.getattr(py, "expiration")?.extract::<Option<String>>(py) {
            if let Some(expiration_val) = expiration {
                configs_map.insert("expiration".to_string(), ConfigValue::String(expiration_val.clone()));
                // Parse expiration time
                if let Ok(dt) = DateTime::parse_from_rfc3339(&expiration_val) {
                    credentials_expire_time = Some(dt.with_timezone(&Utc));
                }
            }
        }
        Ok::<(), PyErr>(())
    })?;
    
    Ok(credentials_expire_time)
}

fn create_store(provider: &str, configs: Option<&HashMap<String, ConfigValue>>, max_pool_connections: usize) -> PyResult<Arc<dyn ObjectStore>> {
    let store = match provider {
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

    // Wrap the store with LimitStore to control concurrency
    let limited_store = LimitStore::new(store, max_pool_connections);
    Ok(Arc::new(limited_store))
}

fn build_s3_store<'a>(configs: Option<&'a HashMap<String, ConfigValue>>) -> PyResult<Arc<dyn ObjectStore>> {
    // TODO: Add support for other configuration fields of AmazonS3Builder, full list here:
    // https://docs.rs/object_store/latest/src/object_store/aws/builder.rs.html#123
    let mut builder = AmazonS3Builder::new();

    let configs = configs.ok_or_else(|| {
        StorageError::ConfigError("Configuration dictionary is required for S3 provider.".to_string())
    })?;

    if let Some(bucket_val) = configs.get("bucket") {
        builder = builder.with_bucket_name(bucket_val.to_string());
    }

    if let Some(region_val) = configs.get("region_name") {
        builder = builder.with_region(region_val.to_string());
    }

    if let Some(endpoint_val) = configs.get("endpoint_url") {
        builder = builder.with_endpoint(endpoint_val.to_string());
    }

    if let Some(access_key_val) = configs.get("access_key") {
        builder = builder.with_access_key_id(access_key_val.to_string());
    }
    if let Some(secret_key_val) = configs.get("secret_key") {
        builder = builder.with_secret_access_key(secret_key_val.to_string());
    }
    if let Some(token_val) = configs.get("token") {
        builder = builder.with_token(token_val.to_string());
    }
    if let Some(skip_signature) = configs.get("skip_signature") {
        match skip_signature {
            ConfigValue::Boolean(b) => {
                if *b {
                    builder = builder.with_skip_signature(true);
                }
            }
            ConfigValue::String(s) => {
                if s.parse::<bool>().unwrap_or(false) {
                    builder = builder.with_skip_signature(true);
                }
            }
            _ => {}
        }
    }

    // Configure retry
    builder = builder.with_retry(RetryConfig::default());

    // Configure client options
    let mut client_options = ClientOptions::new();

    let connect_timeout_secs = get_timeout_secs(&configs, "connect_timeout", DEFAULT_CONNECT_TIMEOUT);
    client_options = client_options.with_connect_timeout(std::time::Duration::from_secs(connect_timeout_secs));

    let read_timeout_secs = get_timeout_secs(&configs, "read_timeout", DEFAULT_READ_TIMEOUT);
    client_options = client_options.with_timeout(std::time::Duration::from_secs(read_timeout_secs));

    if let Some(allow_http_val) = configs.get("allow_http") {
        match allow_http_val {
            ConfigValue::Boolean(b) => {
                if *b {
                    client_options = client_options.with_allow_http(true);
                }
            }
            ConfigValue::String(s) => {
                if s.parse::<bool>().unwrap_or(false) {
                    client_options = client_options.with_allow_http(true);
                }
            }
            _ => {}
        }
    }

    client_options = client_options.with_pool_idle_timeout(std::time::Duration::from_secs(DEFAULT_POOL_IDLE_TIMEOUT));

    builder = builder.with_client_options(client_options);

    let store = builder.build().map_err(StorageError::from)?;

    Ok(Arc::new(store))
}

fn build_gcs_store<'a>(configs: Option<&'a HashMap<String, ConfigValue>>) -> PyResult<Arc<dyn ObjectStore>> {
    let mut builder = GoogleCloudStorageBuilder::new();

    let configs = configs.ok_or_else(|| {
        StorageError::ConfigError("Configuration dictionary is required for GCS provider.".to_string())
    })?;

    if let Some(bucket_val) = configs.get("bucket") {
        builder = builder.with_bucket_name(bucket_val.to_string());
    }

    if let Some(service_account) = configs.get("service_account_key") {
        builder = builder.with_service_account_key(service_account.to_string());
    }

    if let Some(service_account_path) = configs.get("service_account_path") {
        builder = builder.with_service_account_path(service_account_path.to_string());
    }

    if let Some(application_credentials) = configs.get("application_credentials") {
        builder = builder.with_application_credentials(application_credentials.to_string());
    }

    if let Some(skip_signature) = configs.get("skip_signature") {
        match skip_signature {
            ConfigValue::Boolean(b) => {
                if *b {
                    builder = builder.with_skip_signature(true);
                }
            }
            ConfigValue::String(s) => {
                if s.parse::<bool>().unwrap_or(false) {
                    builder = builder.with_skip_signature(true);
                }
            }
            _ => {}
        }
    }

    if let Some(proxy_url) = configs.get("proxy_url") {
        builder = builder.with_proxy_url(proxy_url.to_string());
    }

    if let Some(proxy_ca_certificate) = configs.get("proxy_ca_certificate") {
        builder = builder.with_proxy_ca_certificate(proxy_ca_certificate.to_string());
    }

    if let Some(proxy_excludes) = configs.get("proxy_excludes") {
        builder = builder.with_proxy_excludes(proxy_excludes.to_string());
    }

    if let Some(url) = configs.get("url") {
        builder = builder.with_url(url.to_string());
    }

    // Configure retry
    builder = builder.with_retry(RetryConfig::default());

    // Configure client options
    let mut client_options = ClientOptions::new();

    let connect_timeout_secs = get_timeout_secs(&configs, "connect_timeout", DEFAULT_CONNECT_TIMEOUT);
    client_options = client_options.with_connect_timeout(std::time::Duration::from_secs(connect_timeout_secs));

    let read_timeout_secs = get_timeout_secs(&configs, "read_timeout", DEFAULT_READ_TIMEOUT);
    client_options = client_options.with_timeout(std::time::Duration::from_secs(read_timeout_secs));

    client_options = client_options.with_pool_idle_timeout(std::time::Duration::from_secs(DEFAULT_POOL_IDLE_TIMEOUT));

    builder = builder.with_client_options(client_options);

    let store = builder.build().map_err(StorageError::from)?;

    Ok(Arc::new(store))
}

#[derive(Clone)]
enum ConfigValue {
    String(String),
    Number(i64),
    Boolean(bool),
}

impl ConfigValue {
    fn to_string(&self) -> String {
        match self {
            ConfigValue::String(s) => s.clone(),
            ConfigValue::Number(n) => n.to_string(),
            ConfigValue::Boolean(b) => b.to_string(),
        }
    }
}

#[pyclass]
pub struct RustClient {
    provider: String,
    configs: RwLock<HashMap<String, ConfigValue>>,
    store: RwLock<Arc<dyn ObjectStore>>,
    max_concurrency: usize,
    max_pool_connections: usize,
    multipart_chunksize: usize,
    credentials_provider: Option<PyObject>,
    credentials_expire_time: RwLock<Option<DateTime<Utc>>>,
}

#[pymethods]
impl RustClient {
    #[new]
    #[pyo3(signature = (provider="s3", configs=None, credentials_provider=None))]
    fn new(
        provider: &str,
        configs: Option<&Bound<'_, PyDict>>,
        credentials_provider: Option<PyObject>,
    ) -> PyResult<Self> {
        let provider = provider.to_lowercase();
        
        // Convert Python Dict to Rust HashMap<String, ConfigValue>
        let mut configs_map = HashMap::new();
        let mut credentials_expire_time = None;
        let mut max_concurrency = DEFAULT_MAX_CONCURRENCY;
        let mut max_pool_connections = DEFAULT_POOL_CONNECTIONS;
        let mut multipart_chunksize = DEFAULT_MULTIPART_CHUNKSIZE;

        if let Some(configs_dict) = configs {
            for (key, value) in configs_dict.iter() {
                let key_str = key.extract::<String>()?;
                
                // Convert Python values to ConfigValue
                Python::with_gil(|_py| {
                    if key_str == "max_concurrency" {
                        if let Ok(int_val) = value.extract::<i64>() {
                            max_concurrency = int_val as usize;
                        }
                    } else if key_str == "max_pool_connections" {
                        if let Ok(int_val) = value.extract::<i64>() {
                            max_pool_connections = int_val as usize;
                        }
                    } else if key_str == "multipart_chunksize" {
                        if let Ok(int_val) = value.extract::<i64>() {
                            multipart_chunksize = int_val as usize;
                        }
                    } else {
                        if let Ok(bool_val) = value.extract::<bool>() {
                            configs_map.insert(key_str.clone(), ConfigValue::Boolean(bool_val));
                        } else if let Ok(int_val) = value.extract::<i64>() {
                            configs_map.insert(key_str.clone(), ConfigValue::Number(int_val));
                        } else {
                            // Fallback: try to convert to string
                            if let Ok(str_val) = value.extract::<String>() {
                                configs_map.insert(key_str.clone(), ConfigValue::String(str_val));
                            }
                        }
                    }
                    Ok::<(), PyErr>(())
                })?;
            }
        }
        
        // Handle credentials_provider if provided
        if let Some(creds_provider) = &credentials_provider {
            credentials_expire_time = extract_credentials_from_provider(creds_provider, &mut configs_map)?;
        }
        
        let store = create_store(&provider, Some(&configs_map), max_pool_connections)?;
        
        let client = Self { 
            provider,
            configs: RwLock::new(configs_map),
            store: RwLock::new(store), 
            max_concurrency, 
            max_pool_connections,
            multipart_chunksize, 
            credentials_provider,
            credentials_expire_time: RwLock::new(credentials_expire_time),
        };
        
        Ok(client)
    }

    fn refresh_store_if_needed(&self) -> PyResult<()> {
        let current_expire_time = self.credentials_expire_time.read().unwrap().clone();
        if let (Some(credentials_provider), Some(expire_time)) = (&self.credentials_provider, current_expire_time.as_ref()) {
            let now = Utc::now();
            if now > (*expire_time - Duration::seconds(DEFAULT_REFRESH_CREDENTIALS_THRESHOLD as i64)) {
                let mut expire_time_guard = self.credentials_expire_time.write().unwrap();
                let mut store_guard = self.store.write().unwrap();
                let mut configs_guard = self.configs.write().unwrap();

                let refresh_result = Python::with_gil(|py| {
                    credentials_provider.call_method0(py, "refresh_credentials")?;
                    Ok::<(), PyErr>(())
                });

                match refresh_result {
                    Ok(_) => {
                        let new_credentials_expire_time = extract_credentials_from_provider(credentials_provider, &mut configs_guard)?;
                        let new_store = create_store(&self.provider, Some(&configs_guard), self.max_pool_connections)?;
                        *store_guard = new_store;
                        *expire_time_guard = new_credentials_expire_time;
                    }
                    Err(e) => {
                        return Err(RustRetryableError::new_err(
                            format!("Failed to refresh credentials using credentials provider: {}", e)
                        ));
                    }
                }
            }
        }
        Ok(())
    }


    #[pyo3(signature = (path, data))]
    fn put<'p>(&self, py: Python<'p>, path: &str, data: PyBytes) -> PyResult<Bound<'p, PyAny>> {
        self.refresh_store_if_needed()?;
        let store = Arc::clone(&*self.store.read().unwrap());
        let path = Path::from(path);
        let data_bytes = data.into_inner();
        let bytes_written = data_bytes.len() as u64;
        let payload = PutPayload::from_bytes(data_bytes);

        future_into_py(py, async move {
            store
                .put(&path, payload)
                .await
                .map_err(StorageError::from)?;
            Ok(bytes_written)
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
        self.refresh_store_if_needed()?;
        let store = Arc::clone(&*self.store.read().unwrap());
        let path = Path::from(path);

        if let (Some(start_idx), Some(end_idx)) = (start, end) {
            future_into_py(py, async move {
                let result = store
                    .get_range(&path, start_idx..end_idx+1)
                    .await
                    .map_err(StorageError::from)?;
                Ok(PyBytes::new(result))
            })
        } else {
            future_into_py(py, async move {
                let result = store.get(&path).await.map_err(StorageError::from)?;
                let data = result.bytes().await.map_err(StorageError::from)?;
                Ok(PyBytes::new(data))
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
        self.refresh_store_if_needed()?;
        let store = Arc::clone(&*self.store.read().unwrap());
        let local_path = local_path.to_string();
        let remote_path = Path::from(remote_path);

        future_into_py(py, async move {
            let data = fs::read(local_path).await.map_err(StorageError::from)?;
            let bytes_uploaded = data.len() as u64;
            store
                .put(&remote_path, data.into())
                .await
                .map_err(StorageError::from)?;
            Ok(bytes_uploaded)
        })
    }

    #[pyo3(signature = (remote_path, local_path))]
    fn download<'p>(
        &self,
        py: Python<'p>,
        remote_path: &str,
        local_path: &str,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.refresh_store_if_needed()?;
        let store = Arc::clone(&*self.store.read().unwrap());
        let remote_path = Path::from(remote_path);
        let local_path = local_path.to_string();

        future_into_py(py, async move {
            let result = store.get(&remote_path).await.map_err(StorageError::from)?;
            let data = result.bytes().await.map_err(StorageError::from)?;
            let bytes_downloaded = data.len() as u64;
            fs::write(&local_path, data)
                .await
                .map_err(StorageError::from)?;
            Ok(bytes_downloaded)
        })
    }

    #[pyo3(signature = (local_path, remote_path, multipart_chunksize=None, max_concurrency=None))]
    fn upload_multipart_from_file<'p>(
        &self,
        py: Python<'p>,
        local_path: &str,
        remote_path: &str,
        multipart_chunksize: Option<usize>,
        max_concurrency: Option<usize>,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.refresh_store_if_needed()?;
        let store = Arc::clone(&*self.store.read().unwrap());
        let local_path = local_path.to_string();
        let remote_path = Path::from(remote_path);
        let chunksize = multipart_chunksize.unwrap_or(self.multipart_chunksize);
        let concurrency = max_concurrency.unwrap_or(self.max_concurrency);

        future_into_py(py, async move {
            let mut file = tokio::fs::File::open(local_path).await.map_err(StorageError::from)?;
            let file_size = file.metadata().await.map_err(StorageError::from)?.len();

            let upload = store.put_multipart(&remote_path).await.map_err(StorageError::from)?;
            let mut writer = WriteMultipart::new_with_chunk_size(upload, chunksize);

            let mut buffer = vec![0u8; chunksize];
            loop {
                let n = file.read(&mut buffer).await.map_err(StorageError::from)?;
                if n == 0 {
                    break;
                }
                writer.wait_for_capacity(concurrency).await.map_err(StorageError::from)?;
                writer.write(&buffer[..n]);
            }

            writer.finish().await.map_err(StorageError::from)?;

            Ok(file_size)
        })
    }

    #[pyo3(signature = (remote_path, data, multipart_chunksize=None, max_concurrency=None))]
    fn upload_multipart_from_bytes<'p>(
        &self,
        py: Python<'p>,
        remote_path: &str,
        data: PyBytes,
        multipart_chunksize: Option<usize>,
        max_concurrency: Option<usize>,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.refresh_store_if_needed()?;
        let store = Arc::clone(&*self.store.read().unwrap());
        let remote_path: Path = Path::from(remote_path);
        let data_bytes = data.into_inner();
        let bytes_uploaded = data_bytes.len() as u64;
        let chunksize = multipart_chunksize.unwrap_or(self.multipart_chunksize);
        let concurrency = max_concurrency.unwrap_or(self.max_concurrency);

        future_into_py(py, async move {
            if data_bytes.len() <= chunksize {
                let payload = PutPayload::from_bytes(data_bytes);
                store
                    .put(&remote_path, payload)
                    .await
                    .map_err(StorageError::from)?;
                return Ok(bytes_uploaded);
            }

            let upload = store.put_multipart(&remote_path).await.map_err(StorageError::from)?;
            let mut writer = WriteMultipart::new_with_chunk_size(upload, chunksize);

            let mut offset = 0;
            while offset < data_bytes.len() {
                let end = std::cmp::min(offset + chunksize, data_bytes.len());
                let chunk = &data_bytes[offset..end];

                writer.wait_for_capacity(concurrency).await.map_err(StorageError::from)?;
                writer.write(chunk);

                offset = end;
            }

            writer.finish().await.map_err(StorageError::from)?;

            Ok(bytes_uploaded)
        })
    }

    #[pyo3(signature = (remote_path, local_path, multipart_chunksize=None, max_concurrency=None))]
    fn download_multipart_to_file<'p>(
        &self,
        py: Python<'p>,
        remote_path: &str,
        local_path: &str,
        multipart_chunksize: Option<usize>,
        max_concurrency: Option<usize>,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.refresh_store_if_needed()?;
        let store = Arc::clone(&*self.store.read().unwrap());
        let remote_path = Path::from(remote_path);
        let local_path = local_path.to_string();
        let chunksize = multipart_chunksize.unwrap_or(self.multipart_chunksize);
        let concurrency = max_concurrency.unwrap_or(self.max_concurrency);

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
            
            let num_chunks = (total_size + chunksize as u64 - 1) / chunksize as u64;
            
            let semaphore = Arc::new(Semaphore::new(concurrency));
            let (tx , mut rx): (
                mpsc::Sender<Result<(u64, Vec<u8>), StorageError>>,
                mpsc::Receiver<Result<(u64, Vec<u8>), StorageError>>,
            ) = mpsc::channel(concurrency);
            
            // Start a task to process downloaded chunks in arrival order and write to file
            let write_handle = tokio::task::spawn(async move {
                while let Some(result) = rx.recv().await {
                    match result {
                        Ok((chunk_index, data)) => {
                            output_file.seek(tokio::io::SeekFrom::Start(chunk_index as u64 * chunksize as u64)).await.map_err(StorageError::from)?;
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
                let start_offset = chunk_index * chunksize as u64;
                let end_offset = std::cmp::min(start_offset + chunksize as u64, total_size);
                
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
            
            Ok(total_size)
        })
    }

    #[pyo3(signature = (remote_path, start=None, end=None, multipart_chunksize=None, max_concurrency=None))]
    fn download_multipart_to_bytes<'p>(
        &self,
        py: Python<'p>,
        remote_path: &str,
        start: Option<u64>,
        end: Option<u64>,
        multipart_chunksize: Option<usize>,
        max_concurrency: Option<usize>,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.refresh_store_if_needed()?;
        let store = Arc::clone(&*self.store.read().unwrap());
        let remote_path = Path::from(remote_path);
        let chunksize = multipart_chunksize.unwrap_or(self.multipart_chunksize);
        let concurrency = max_concurrency.unwrap_or(self.max_concurrency);

        future_into_py(py, async move {
            let (start_offset, end_offset, total_size) = if let (Some(start_val), Some(end_val)) = (start, end) {
                // Range read - no HEAD request needed, we know the exact range
                (start_val, end_val, end_val - start_val + 1)
            } else {
                // Full file download - need HEAD request to get total size for chunking
                let result = store.head(&remote_path).await.map_err(StorageError::from)?;
                let file_size = result.size;
                (0, file_size - 1, file_size)
            };

            if total_size <= chunksize as u64 {
                let range = start_offset..end_offset + 1;
                let result = store.get_range(&remote_path, range).await.map_err(StorageError::from)?;
                return Ok(PyBytes::new(result));
            }

            let num_chunks = (total_size + chunksize as u64 - 1) / chunksize as u64;
            let mut chunks = Vec::with_capacity(num_chunks as usize);
            
            for i in 0..num_chunks {
                let chunk_start = start_offset + i * chunksize as u64;
                let chunk_end = std::cmp::min(chunk_start + chunksize as u64 - 1, end_offset);
                chunks.push((chunk_start, chunk_end));
            }

            let semaphore = Arc::new(Semaphore::new(concurrency));
            let mut tasks = Vec::with_capacity(chunks.len());

            for (chunk_start, chunk_end) in chunks {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let store = Arc::clone(&store);
                let remote_path = remote_path.clone();
                
                tasks.push(tokio::task::spawn(async move {
                    let range = chunk_start..chunk_end + 1;
                    let result = store.get_range(&remote_path, range).await.map_err(StorageError::from)?;
                    drop(permit);
                    Ok::<bytes::Bytes, StorageError>(result)
                }));
            }

            let mut segments = Vec::with_capacity(tasks.len());
            for task in tasks {
                let data = task.await.map_err(|e| StorageError::ObjectStoreError(format!("Failed to join multipart download task: {}", e)))??;
                segments.push(data);
            }

            let final_data = segments.concat();

            Ok(PyBytes::new(final_data.into()))
        })
    }

    #[pyo3(signature = (prefixes, limit=None, suffix=None, max_depth=None, max_concurrency=DEFAULT_POOL_CONNECTIONS))]
    fn list_recursive<'p>(
        &self,
        py: Python<'p>,
        prefixes: Vec<String>,
        limit: Option<usize>,
        suffix: Option<String>,
        max_depth: Option<usize>,
        max_concurrency: usize,
    ) -> PyResult<Py<ListResult>> {
        self.refresh_store_if_needed()?;
        let store = Arc::clone(&*self.store.read().unwrap());

        async fn list_single_directory(
            store: Arc<dyn ObjectStore>,
            prefix: Path,
            limit: Option<usize>,
            suffix: Option<&str>,
            depth: usize,
        ) -> Result<(Vec<ObjectMeta>, Vec<Path>, usize), StorageError> {
            let mut objects = Vec::new();
            let mut directories = Vec::new();

            let list_result = store
                .list_with_delimiter(Some(&prefix))
                .await
                .map_err(StorageError::from)?;

            for entry in list_result.objects {
                if limit.is_some_and(|x| objects.len() >= x) {
                    break;
                }

                if let Some(suffix_filter) = suffix {
                    if !entry.location.to_string().ends_with(suffix_filter) {
                        continue;
                    }
                }

                objects.push(entry);
            }

            for common_prefix in list_result.common_prefixes {
                directories.push(common_prefix);
            }

            Ok((objects, directories, depth))
        }

        async fn list_recursive_async(
            store: Arc<dyn ObjectStore>,
            prefixes: Vec<String>,
            limit: Option<usize>,
            suffix: Option<String>,
            max_depth: Option<usize>,
            max_concurrency: usize,
        ) -> Result<(Vec<ObjectMetadata>, Vec<ObjectMetadata>), StorageError> {
            let mut dirs_to_visit = VecDeque::new();
            for prefix in prefixes {
                dirs_to_visit.push_back((Path::from(prefix), 0));
            }

            let mut total_found: usize = 0;
            let mut all_objects: Vec<ObjectMetadata> = Vec::new();
            let mut all_directories: Vec<ObjectMetadata> = Vec::new();
            let mut join_set = JoinSet::new();

            while !dirs_to_visit.is_empty() || !join_set.is_empty() {
                if !join_set.is_empty() {
                    let result: Result<(Vec<ObjectMeta>, Vec<Path>, usize), StorageError> =
                        join_set.join_next().await.unwrap().unwrap();
                    let (objects, directories, depth) = result?;

                    for directory in &directories {
                        if max_depth.map_or(true, |max_d| depth < max_d) {
                            dirs_to_visit.push_back((directory.clone(), depth + 1));
                        }
                    }

                    for obj in objects {
                        let metadata = ObjectMetadata::new(
                            obj.location.to_string(),
                            obj.size,
                            obj.last_modified.to_rfc3339(),
                            "file".to_string(),
                            obj.e_tag.clone(),
                        );
                        all_objects.push(metadata);
                    }

                    for path in directories {
                        let metadata = ObjectMetadata::new(
                            path.to_string(),
                            0,
                            DateTime::<Utc>::from_timestamp(0, 0).unwrap().to_rfc3339(),
                            "directory".to_string(),
                            None,
                        );
                        all_directories.push(metadata);
                    }

                    total_found = all_objects.len();

                    if limit.is_some_and(|x| total_found >= x) {
                        break;
                    }
                }

                while !dirs_to_visit.is_empty() && join_set.len() < max_concurrency {
                    let (prefix, depth) = dirs_to_visit.pop_front().unwrap();

                    if max_depth.is_some_and(|x| depth >= x) {
                        continue;
                    }

                    let store_clone = Arc::clone(&store);
                    let suffix_clone = suffix.clone();
                    let remaining_limit = limit.map(|x| x - total_found);

                    join_set.spawn(async move {
                        list_single_directory(
                            store_clone,
                            prefix,
                            remaining_limit,
                            suffix_clone.as_deref(),
                            depth,
                        )
                        .await
                    });
                }
            }

            all_objects.sort_by(|a, b| a.key.cmp(&b.key));
            all_directories.sort_by(|a, b| a.key.cmp(&b.key));

            if let Some(limit_val) = limit {
                all_objects.truncate(limit_val);
            }

            Ok((all_objects, all_directories))
        }

        let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.block_on(list_recursive_async(
                Arc::clone(&store),
                prefixes,
                limit,
                suffix,
                max_depth,
                max_concurrency,
            ))
        } else {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(list_recursive_async(
                Arc::clone(&store),
                prefixes,
                limit,
                suffix,
                max_depth,
                max_concurrency,
            ))
        }?;

        let (all_objects, all_directories) = result;

        let list_result = ListResult::new(all_objects, all_directories);

        Ok(Py::new(py, list_result).unwrap())
    }
}

#[pymodule]
fn multistorageclient_rust(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustClient>()?;
    m.add_class::<ObjectMetadata>()?;
    m.add_class::<ListResult>()?;
    m.add("RustRetryableError", _py.get_type::<RustRetryableError>())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_error_chain_with_connection_reset() {
        // Test the exact error pattern from production
        let connection_error = io::Error::new(io::ErrorKind::ConnectionReset, "Connection reset by peer (os error 104)");
        let generic_error = object_store::Error::Generic {
            store: "S3",
            source: Box::new(connection_error),
        };
        
        let chain = format_error_chain(&generic_error);
        
        // Should contain the root cause
        assert!(chain.contains("Connection reset by peer (os error 104)"));
        // Should be formatted as a chain
        assert!(chain.contains(" -> "));
        
        // Test that it gets classified as retryable when it contains the HTTP error pattern
        let http_error = object_store::Error::Generic {
            store: "S3",
            source: Box::new(io::Error::new(io::ErrorKind::ConnectionReset, "HTTP error: error sending request")),
        };
        
        let storage_error = StorageError::from(http_error);
        match storage_error {
            StorageError::RetryExhaustedError(msg) => {
                assert!(msg.contains("HTTP error: error sending request"));
                assert!(msg.contains(" -> "));
            }
            _ => panic!("Expected RetryExhaustedError for HTTP error pattern"),
        }
    }

    #[test]
    fn test_permission_error() {
        let access_error = object_store::Error::Generic {
            store: "S3",
            source: Box::new(io::Error::new(io::ErrorKind::PermissionDenied, "The operation lacked the necessary privileges")),
        };

        let storage_error = StorageError::from(access_error);
        match storage_error {
            StorageError::PermissionError(msg) => {
                assert!(msg.contains("The operation lacked the necessary privileges"));
                assert!(msg.contains(" -> "));
            }
            _ => panic!("Expected PermissionError for access error pattern"),
        }
    }

    #[test]
    fn test_get_timeout_secs() {
        let mut configs = HashMap::new();

        // Test with Number values
        configs.insert("read_timeout".to_string(), ConfigValue::Number(600));
        configs.insert("connect_timeout".to_string(), ConfigValue::Number(120));
        assert_eq!(get_timeout_secs(&configs, "read_timeout", DEFAULT_READ_TIMEOUT), 600);
        assert_eq!(get_timeout_secs(&configs, "connect_timeout", DEFAULT_CONNECT_TIMEOUT), 120);

        // Test with String values
        configs.insert("read_timeout".to_string(), ConfigValue::String("300".to_string()));
        configs.insert("connect_timeout".to_string(), ConfigValue::String("90".to_string()));
        assert_eq!(get_timeout_secs(&configs, "read_timeout", DEFAULT_READ_TIMEOUT), 300);
        assert_eq!(get_timeout_secs(&configs, "connect_timeout", DEFAULT_CONNECT_TIMEOUT), 90);

        // Test with invalid String values (should use defaults)
        configs.insert("read_timeout".to_string(), ConfigValue::String("invalid".to_string()));
        configs.insert("connect_timeout".to_string(), ConfigValue::String("bad".to_string()));
        assert_eq!(get_timeout_secs(&configs, "read_timeout", DEFAULT_READ_TIMEOUT), DEFAULT_READ_TIMEOUT);
        assert_eq!(get_timeout_secs(&configs, "connect_timeout", DEFAULT_CONNECT_TIMEOUT), DEFAULT_CONNECT_TIMEOUT);

        // Test without values (should use defaults)
        configs.clear();
        assert_eq!(get_timeout_secs(&configs, "read_timeout", DEFAULT_READ_TIMEOUT), DEFAULT_READ_TIMEOUT);
        assert_eq!(get_timeout_secs(&configs, "connect_timeout", DEFAULT_CONNECT_TIMEOUT), DEFAULT_CONNECT_TIMEOUT);
    }
}
