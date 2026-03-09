// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use object_store::aws::AwsCredential;
use object_store::gcp::GcpCredential;
use pyo3::prelude::*;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};

const DEFAULT_REFRESH_CREDENTIALS_THRESHOLD: i64 = 600; // 10 minutes

/// Generic cached credential representation
#[derive(Debug)]
struct CredentialCache<C> {
    credential: Arc<C>,
    expire_time: DateTime<Utc>,
}

// Core credential provider that handles shared logic for all cloud providers.
// This struct contains all the common functionality for credential caching,
// refreshing, and Python integration, avoiding code duplication.
struct CoreCredentialsProvider {
    // Python credentials provider object
    py_provider: Py<PyAny>,
    // Async mutex to coordinate credential refresh operations (prevents thundering herd)
    refresh_lock: Arc<Mutex<()>>,
    // Time in seconds before expiration to trigger credential refresh
    refresh_threshold: i64,
}

impl CoreCredentialsProvider {
    fn new(py_provider: Py<PyAny>, refresh_threshold: Option<i64>) -> Self {
        Self {
            py_provider,
            refresh_lock: Arc::new(Mutex::new(())),
            refresh_threshold: refresh_threshold.unwrap_or(DEFAULT_REFRESH_CREDENTIALS_THRESHOLD),
        }
    }

    fn should_refresh(&self, expire_time: DateTime<Utc>) -> bool {
        let now = Utc::now();
        let threshold = Duration::seconds(self.refresh_threshold);
        now > (expire_time - threshold)
    }

    fn refresh_credentials(&self, py: Python) -> PyResult<()> {
        self.py_provider
            .call_method0(py, "refresh_credentials")
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to refresh credentials: {}", e)
                )
            })?;
        Ok(())
    }

    async fn acquire_refresh_lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.refresh_lock.lock().await
    }
}

impl Clone for CoreCredentialsProvider {
    fn clone(&self) -> Self {
        Self {
            py_provider: Python::attach(|py| self.py_provider.clone_ref(py)),
            refresh_lock: Arc::clone(&self.refresh_lock),
            refresh_threshold: self.refresh_threshold,
        }
    }
}

// Helper function to parse expiration time from RFC3339 string
fn parse_expiration(expiration: Option<String>) -> DateTime<Utc> {
    if let Some(exp_str) = expiration {
        DateTime::parse_from_rfc3339(&exp_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now() + Duration::hours(1))
    } else {
        Utc::now() + Duration::days(365)
    }
}

// Helper to convert tokio JoinError to object_store::Error
fn join_error_to_object_store_error(e: tokio::task::JoinError) -> object_store::Error {
    object_store::Error::Generic {
        store: "credentials_provider",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Join task failed when refreshing credentials: {}", e),
        )),
    }
}

// Helper to convert PyErr to object_store::Error
fn py_err_to_object_store_error(e: PyErr) -> object_store::Error {
    object_store::Error::Generic {
        store: "credentials_provider",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to refresh credentials: {}", e),
        )),
    }
}

// A credential provider that bridges Python credentials provider to Rust's object_store for AWS.
//
// This provider wraps a Python credentials object and handles credential caching,
// refreshing, and thread-safe access for AWS/S3-compatible storage services.
pub struct AwsCredentialsProvider {
    // Core logic shared across all providers
    core: Arc<CoreCredentialsProvider>,
    // Thread-safe cache for the current AWS credentials
    cached_credentials: Arc<RwLock<Option<CredentialCache<AwsCredential>>>>,
}

impl Clone for AwsCredentialsProvider {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            cached_credentials: Arc::clone(&self.cached_credentials),
        }
    }
}

impl std::fmt::Debug for AwsCredentialsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("AwsCredentialsProvider");
        debug_struct.field("refresh_threshold", &self.core.refresh_threshold);
        debug_struct.finish()
    }
}

impl AwsCredentialsProvider {
    pub fn new(py_provider: Py<PyAny>, refresh_threshold: Option<i64>) -> Self {
        Self {
            core: Arc::new(CoreCredentialsProvider::new(py_provider, refresh_threshold)),
            cached_credentials: Arc::new(RwLock::new(None)),
        }
    }

    fn get_credentials(&self, py: Python) -> PyResult<CredentialCache<AwsCredential>> {
        let credentials = self.core.py_provider.call_method0(py, "get_credentials")?;

        let access_key = credentials.getattr(py, "access_key")?.extract::<String>(py)?;
        let secret_key = credentials.getattr(py, "secret_key")?.extract::<String>(py)?;
        let token = credentials.getattr(py, "token")?.extract::<Option<String>>(py)?;
        let expiration = credentials.getattr(py, "expiration")?.extract::<Option<String>>(py)?;

        let expire_time = parse_expiration(expiration);

        Ok(CredentialCache {
            credential: Arc::new(AwsCredential {
                key_id: access_key,
                secret_key,
                token,
            }),
            expire_time,
        })
    }
}

// Implements object_store's credential provider by delegating to MSC's Python credentials provider.
//
// Uses a two-tier caching strategy with double-checked locking to minimize Python GIL
// contention while ensuring credentials are refreshed before expiration.
#[async_trait]
impl object_store::CredentialProvider for AwsCredentialsProvider {
    type Credential = AwsCredential;

    // Retrieves credentials from Python credentials provider, refreshing them if necessary.
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        // Fast path: Check the cache without blocking
        {
            let cached_guard = self.cached_credentials.read().unwrap();
            if let Some(cached_cred) = cached_guard.as_ref() {
                if !self.core.should_refresh(cached_cred.expire_time) {
                    return Ok(Arc::clone(&cached_cred.credential));
                }
            }
        }

        // Acquire refresh lock to coordinate refresh (prevents thundering herd)
        let _refresh_guard = self.core.acquire_refresh_lock().await;

        // Double-check: another thread might have refreshed while we waited
        {
            let cached_guard = self.cached_credentials.read().unwrap();
            if let Some(cached_cred) = cached_guard.as_ref() {
                if !self.core.should_refresh(cached_cred.expire_time) {
                    return Ok(Arc::clone(&cached_cred.credential));
                }
            }
        }

        // Spawn blocking task to refresh credentials
        let cached_arc = Arc::clone(&self.cached_credentials);
        let core = Arc::clone(&self.core);
        let this = self.clone();

        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                // Get the credentials from the Python credentials provider
                let mut refreshed_credential = this.get_credentials(py)?;

                // Check if the credentials need to be refreshed and refresh them if necessary
                if core.should_refresh(refreshed_credential.expire_time) {
                    core.refresh_credentials(py)?;
                    refreshed_credential = this.get_credentials(py)?;
                }

                // Create credential to return
                let credential = AwsCredential {
                    key_id: refreshed_credential.credential.key_id.clone(),
                    secret_key: refreshed_credential.credential.secret_key.clone(),
                    token: refreshed_credential.credential.token.clone(),
                };

                // Update cache with write lock
                {
                    let mut cached_guard = cached_arc.write().unwrap();
                    *cached_guard = Some(refreshed_credential);
                }

                Ok(credential)
            })
        })
        .await
        .map_err(join_error_to_object_store_error)?
        .map_err(py_err_to_object_store_error)
        .map(Arc::new)
        .map_err(Into::into)
    }
}

// Wrapper for AWS SDK credentials provider that implements object_store's CredentialProvider.
// This allows using AWS SDK's default credential chain (environment variables, instance metadata, etc.)
pub struct AwsSdkCredentialsProvider {
    sdk_provider: SharedCredentialsProvider,
}

impl std::fmt::Debug for AwsSdkCredentialsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsSdkCredentialsProvider").finish()
    }
}

impl AwsSdkCredentialsProvider {
    pub fn new(sdk_provider: SharedCredentialsProvider) -> Self {
        Self { sdk_provider }
    }
}

#[async_trait]
impl object_store::CredentialProvider for AwsSdkCredentialsProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let creds = self.sdk_provider
            .provide_credentials()
            .await
            .map_err(|e| {
                object_store::Error::Generic {
                    store: "AwsSdkCredentialsProvider",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to get AWS credentials: {}", e),
                    )),
                }
            })?;

        Ok(Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(|s| s.to_string()),
        }))
    }
}

// A GCP credential provider that bridges Python credentials provider to Rust's object_store.
pub struct GcpCredentialsProvider {
    // Core logic shared across all providers
    core: Arc<CoreCredentialsProvider>,
    // Thread-safe cache for the current GCP credentials
    cached_credentials: Arc<RwLock<Option<CredentialCache<GcpCredential>>>>,
}

impl Clone for GcpCredentialsProvider {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            cached_credentials: Arc::clone(&self.cached_credentials),
        }
    }
}

impl std::fmt::Debug for GcpCredentialsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("GcpCredentialsProvider");
        debug_struct.field("refresh_threshold", &self.core.refresh_threshold);
        debug_struct.finish()
    }
}

impl GcpCredentialsProvider {
    pub fn new(py_provider: Py<PyAny>, refresh_threshold: Option<i64>) -> Self {
        Self {
            core: Arc::new(CoreCredentialsProvider::new(py_provider, refresh_threshold)),
            cached_credentials: Arc::new(RwLock::new(None)),
        }
    }

    fn get_credentials(&self, py: Python) -> PyResult<CredentialCache<GcpCredential>> {
        let credentials = self.core.py_provider.call_method0(py, "get_credentials")?;

        // GCP Rust credentials provider requires a non-None bearer token
        let token = credentials
            .getattr(py, "token")?
            .extract::<Option<String>>(py)?
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "GCP Rust credentials provider requires a non-None `token` string."
                )
            })?;

        let expiration = credentials.getattr(py, "expiration")?.extract::<Option<String>>(py)?;

        let expire_time = parse_expiration(expiration);

        Ok(CredentialCache {
            credential: Arc::new(GcpCredential {
                bearer: token,
            }),
            expire_time,
        })
    }
}

// Implements object_store's credential provider for GCP by delegating to MSC's Python credentials provider.
//
// Uses a two-tier caching strategy with double-checked locking to minimize Python GIL
// contention while ensuring credentials are refreshed before expiration.
#[async_trait]
impl object_store::CredentialProvider for GcpCredentialsProvider {
    type Credential = GcpCredential;

    // Retrieves GCP credentials from Python credentials provider, refreshing them if necessary.
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        // Fast path: Check the cache without blocking
        {
            let cached_guard = self.cached_credentials.read().unwrap();
            if let Some(cached_cred) = cached_guard.as_ref() {
                if !self.core.should_refresh(cached_cred.expire_time) {
                    return Ok(Arc::clone(&cached_cred.credential));
                }
            }
        }

        // Acquire refresh lock to coordinate refresh (prevents thundering herd)
        let _refresh_guard = self.core.acquire_refresh_lock().await;

        // Double-check: another thread might have refreshed while we waited
        {
            let cached_guard = self.cached_credentials.read().unwrap();
            if let Some(cached_cred) = cached_guard.as_ref() {
                if !self.core.should_refresh(cached_cred.expire_time) {
                    return Ok(Arc::clone(&cached_cred.credential));
                }
            }
        }

        // Spawn blocking task to refresh credentials
        let cached_arc = Arc::clone(&self.cached_credentials);
        let core = Arc::clone(&self.core);
        let this = self.clone();

        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                // Get the credentials from the Python credentials provider
                let mut refreshed_credential = this.get_credentials(py)?;

                // Check if the credentials need to be refreshed and refresh them if necessary
                if core.should_refresh(refreshed_credential.expire_time) {
                    core.refresh_credentials(py)?;
                    refreshed_credential = this.get_credentials(py)?;
                }

                // Return the refreshed credentials and cache them
                let credential = GcpCredential {
                    bearer: refreshed_credential.credential.bearer.clone(),
                };

                // Update cache with write lock
                {
                    let mut cached_guard = cached_arc.write().unwrap();
                    *cached_guard = Some(refreshed_credential);
                }

                Ok(credential)
            })
        })
        .await
        .map_err(join_error_to_object_store_error)?
        .map_err(py_err_to_object_store_error)
        .map(Arc::new)
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Once;

    static INIT: Once = Once::new();

    // Initialize Python interpreter once for all tests
    fn initialize_python() {
        INIT.call_once(|| {
            Python::initialize();
        });
    }

    // Mock Python credentials object with attributes
    #[pyclass]
    struct MockCredentials {
        #[pyo3(get)]
        access_key: String,
        #[pyo3(get)]
        secret_key: String,
        #[pyo3(get)]
        token: Option<String>,
        #[pyo3(get)]
        expiration: Option<String>,
    }

    // Helper function to create a mock Python credentials object
    fn create_mock_credentials(
        py: Python,
        access_key: &str,
        secret_key: &str,
        token: Option<&str>,
        expiration: Option<&str>,
    ) -> Py<PyAny> {
        Py::new(
            py,
            MockCredentials {
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                token: token.map(|s| s.to_string()),
                expiration: expiration.map(|s| s.to_string()),
            },
        )
        .unwrap()
        .into()
    }

    // Mock Python credentials provider for testing
    #[pyclass]
    struct MockCredentialsProvider {
        access_key: String,
        secret_key: String,
        token: Option<String>,
        expiration: Arc<RwLock<Option<String>>>,
        call_count: Arc<AtomicUsize>,
        refresh_count: Arc<AtomicUsize>,
    }

    #[pymethods]
    impl MockCredentialsProvider {
        #[new]
        fn new(
            access_key: String,
            secret_key: String,
            token: Option<String>,
            expiration: Option<String>,
        ) -> Self {
            Self {
                access_key,
                secret_key,
                token,
                expiration: Arc::new(RwLock::new(expiration)),
                call_count: Arc::new(AtomicUsize::new(0)),
                refresh_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn get_credentials(&mut self, py: Python) -> PyResult<Py<PyAny>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let expiration = self.expiration.read().unwrap().clone();
            Ok(create_mock_credentials(
                py,
                &self.access_key,
                &self.secret_key,
                self.token.as_deref(),
                expiration.as_deref(),
            ))
        }

        fn refresh_credentials(&mut self) {
            self.refresh_count.fetch_add(1, Ordering::SeqCst);
            let new_expiration = (Utc::now() + Duration::seconds(5)).to_rfc3339();
            let mut expiration = self.expiration.write().unwrap();
            *expiration = Some(new_expiration);
        }

        fn get_call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }

        fn get_refresh_count(&self) -> usize {
            self.refresh_count.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn test_cached_credential_creation() {
        let credential = Arc::new(AwsCredential {
            key_id: "test_key".to_string(),
            secret_key: "test_secret".to_string(),
            token: Some("test_token".to_string()),
        });

        let cached = CredentialCache {
            credential: credential.clone(),
            expire_time: Utc::now() + Duration::hours(1),
        };

        assert_eq!(cached.credential.key_id, "test_key");
        assert_eq!(cached.credential.secret_key, "test_secret");
        assert_eq!(cached.credential.token, Some("test_token".to_string()));
    }

    #[test]
    fn test_should_refresh_expired() {
        initialize_python();
        Python::attach(|py| {
            let mock_provider = Py::new(
                py,
                MockCredentialsProvider::new(
                    "access".to_string(),
                    "secret".to_string(),
                    None,
                    None,
                ),
            )
            .unwrap();

            let provider = AwsCredentialsProvider::new(mock_provider.into(), Some(900));

            let expire_time = Utc::now() - Duration::hours(1);

            // Already expired
            assert!(provider.core.should_refresh(expire_time));
        });
    }

    #[test]
    fn test_get_credentials_from_python() {
        initialize_python();
        Python::attach(|py| {
            let mock_provider = Py::new(
                py,
                MockCredentialsProvider::new(
                    "test_access".to_string(),
                    "test_secret".to_string(),
                    Some("test_token".to_string()),
                    Some("2025-12-31T23:59:59Z".to_string()),
                ),
            )
            .unwrap();

            let provider = AwsCredentialsProvider::new(mock_provider.into(), None);
            let result = provider.get_credentials(py);

            assert!(result.is_ok());
            let cached = result.unwrap();
            assert_eq!(cached.credential.key_id, "test_access");
            assert_eq!(cached.credential.secret_key, "test_secret");
            assert_eq!(cached.credential.token, Some("test_token".to_string()));
        });
    }

    #[test]
    fn test_refresh_credentials_succeeds() {
        initialize_python();
        Python::attach(|py| {
            let mock_provider = Py::new(
                py,
                MockCredentialsProvider::new(
                    "refreshed_access".to_string(),
                    "refreshed_secret".to_string(),
                    Some("refreshed_token".to_string()),
                    Some("2026-01-01T00:00:00Z".to_string()),
                ),
            )
            .unwrap();

            let provider = AwsCredentialsProvider::new(mock_provider.into(), None);

            // Call refresh_credentials through core which should succeed
            let result = provider.core.refresh_credentials(py);
            assert!(result.is_ok());

            // Then get credentials should return fresh credentials
            let creds = provider.get_credentials(py);
            assert!(creds.is_ok());
            let cached = creds.unwrap();
            assert_eq!(cached.credential.key_id, "refreshed_access");
        });
    }

    #[tokio::test]
    async fn test_concurrent_access_async() {
        use object_store::CredentialProvider;
        use tokio::sync::Barrier;

        initialize_python();

        let (mock_provider_obj, provider) = Python::attach(|py| {
            let mock_provider_obj = Py::new(
                py,
                MockCredentialsProvider::new(
                    "concurrent_access".to_string(),
                    "concurrent_secret".to_string(),
                    Some("concurrent_token".to_string()),
                    Some((Utc::now() - Duration::seconds(1)).to_rfc3339()),
                ),
            )
            .unwrap();

            let provider = Arc::new(AwsCredentialsProvider::new(mock_provider_obj.clone_ref(py).into(), Some(0)));

            (mock_provider_obj, provider)
        });

        let barrier = Arc::new(Barrier::new(4));
        let mut handles = vec![];

        for thread_id in 0..4 {
            let provider_clone = Arc::clone(&provider);
            let barrier_clone = Arc::clone(&barrier);

            let handle = tokio::spawn(async move {
                barrier_clone.wait().await;

                let result = provider_clone.get_credential().await;
                assert!(result.is_ok(), "Thread {} failed to get credentials", thread_id);

                let cred = result.unwrap();
                assert_eq!(cred.key_id, "concurrent_access");
                assert_eq!(cred.secret_key, "concurrent_secret");
                assert_eq!(cred.token, Some("concurrent_token".to_string()));
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("Task panicked");
        }

        Python::attach(|py| {
            let call_count = mock_provider_obj.borrow(py).get_call_count();
            let refresh_count = mock_provider_obj.borrow(py).get_refresh_count();
            println!("Total credential calls: {}, refresh calls: {}", call_count, refresh_count);
            assert!(refresh_count == 1, "Credentials should have been refreshed exactly once");
        });
    }

    // GCP-specific tests: focus on token field extraction and None token error handling
    #[pyclass]
    struct MockGcpCredentials {
        #[pyo3(get)]
        token: Option<String>,
        #[pyo3(get)]
        expiration: Option<String>,
    }

    /// Mock GCP credentials provider
    #[pyclass]
    struct MockGcpCredentialsProvider {
        token: Option<String>,
        expiration: Option<String>,
    }

    #[pymethods]
    impl MockGcpCredentialsProvider {
        #[new]
        fn new(token: Option<String>, expiration: Option<String>) -> Self {
            Self { token, expiration }
        }

        fn get_credentials(&self, py: Python) -> PyResult<Py<PyAny>> {
            Py::new(
                py,
                MockGcpCredentials {
                    token: self.token.clone(),
                    expiration: self.expiration.clone(),
                },
            )
            .map(|obj| obj.into())
        }

        fn refresh_credentials(&self) {}
    }

    #[test]
    fn test_gcp_get_credentials_with_valid_token() {
        initialize_python();
        Python::attach(|py| {
            let mock_provider = Py::new(
                py,
                MockGcpCredentialsProvider::new(
                    Some("ya29.test_access_token".to_string()),
                    Some("2025-12-31T23:59:59Z".to_string()),
                ),
            )
            .unwrap();

            let provider = GcpCredentialsProvider::new(mock_provider.into(), None);
            let result = provider.get_credentials(py);

            assert!(result.is_ok());
            let cached = result.unwrap();
            assert_eq!(cached.credential.bearer, "ya29.test_access_token");
        });
    }

    #[test]
    fn test_gcp_none_token_error() {
        initialize_python();
        Python::attach(|py| {
            let mock_provider = Py::new(
                py,
                MockGcpCredentialsProvider::new(None, None),
            )
            .unwrap();

            let provider = GcpCredentialsProvider::new(mock_provider.into(), None);
            let result = provider.get_credentials(py);

            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("non-None `token` string"),
                "Error should explain token requirement, got: {}",
                err_msg
            );
        });
    }

}

