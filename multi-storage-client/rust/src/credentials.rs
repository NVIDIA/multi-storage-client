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
use pyo3::prelude::*;
use std::sync::{Arc, RwLock};

const DEFAULT_REFRESH_CREDENTIALS_THRESHOLD: i64 = 900; // 15 minutes

/// Internal cached credential representation storing AWS-compatible credentials.
struct CachedAwsCredential {
    /// The AWS credential containing access key, secret key, and optional session token
    credential: Arc<AwsCredential>,
    /// Expiration time of these credentials in UTC
    expire_time: DateTime<Utc>,
}

/// A credential provider that bridges Python credentials provider to Rust's object_store.
pub struct PyCredentialsProvider {
    /// Python credentials provider object that implements get_credentials() and refresh_credentials()
    py_provider: PyObject,
    /// Thread-safe cache for the current credentials
    cached_credentials: Arc<RwLock<Option<CachedAwsCredential>>>,
    /// Time in seconds before expiration to trigger credential refresh
    refresh_threshold: i64,
}

impl Clone for PyCredentialsProvider {
    fn clone(&self) -> Self {
        Self {
            py_provider: Python::with_gil(|py| self.py_provider.clone_ref(py)),
            cached_credentials: Arc::clone(&self.cached_credentials),
            refresh_threshold: self.refresh_threshold,
        }
    }
}

impl std::fmt::Debug for PyCredentialsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("PyCredentialsProvider");
        debug_struct.field("refresh_threshold", &self.refresh_threshold);
        debug_struct.finish()
    }
}

impl PyCredentialsProvider {
    pub fn new(py_provider: PyObject, refresh_threshold: Option<i64>) -> Self {
        Self {
            py_provider,
            cached_credentials: Arc::new(RwLock::new(None)),
            refresh_threshold: refresh_threshold.unwrap_or(DEFAULT_REFRESH_CREDENTIALS_THRESHOLD),
        }
    }

    fn should_refresh(&self, cached: &CachedAwsCredential) -> bool {
        let now = Utc::now();
        let threshold = Duration::seconds(self.refresh_threshold);
        now > (cached.expire_time - threshold)
    }

    fn get_credentials(&self, py: Python) -> PyResult<CachedAwsCredential> {
        let credentials = self.py_provider.call_method0(py, "get_credentials")?;
        
        let access_key = credentials.getattr(py, "access_key")?.extract::<String>(py)?;
        let secret_key = credentials.getattr(py, "secret_key")?.extract::<String>(py)?;
        let token = credentials.getattr(py, "token")?.extract::<Option<String>>(py)?;
        let expiration = credentials.getattr(py, "expiration")?.extract::<Option<String>>(py)?;
        
        let expire_time = if let Some(exp_str) = expiration {
            DateTime::parse_from_rfc3339(&exp_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now() + Duration::hours(1))
        } else {
            Utc::now() + Duration::days(365)
        };

        Ok(CachedAwsCredential {
            credential: Arc::new(AwsCredential {
                key_id: access_key,
                secret_key,
                token,
            }),
            expire_time,
        })
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
}

/// Implements object_store's credential provider by delegating to MSC's Python credentials provider.
/// 
/// Uses a two-tier caching strategy with double-checked locking to minimize Python GIL
/// contention while ensuring credentials are refreshed before expiration.
#[async_trait]
impl object_store::CredentialProvider for PyCredentialsProvider {
    type Credential = AwsCredential;
    
    /// Retrieves credentials from Python credentials provider, refreshing them if necessary.
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        // Check the cache without blocking
        {
            let cached_guard = self.cached_credentials.read().unwrap();
            if let Some(cached_cred) = cached_guard.as_ref() {
                if !self.should_refresh(cached_cred) {
                    // Clone the Arc for cheap reference counting
                    return Ok(Arc::clone(&cached_cred.credential));
                }
            }
        }
        
        // If credentials are not in the cache or are expired, spawn a blocking task to refresh them
        let cached_arc = Arc::clone(&self.cached_credentials);
        let this = self.clone();

        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                let mut cached_guard = cached_arc.write().unwrap();
                
                // Check the cached credentials again (double-checked locking)
                if let Some(cached_cred) = cached_guard.as_ref() {
                    if !this.should_refresh(cached_cred) {
                        return Ok(AwsCredential {
                            key_id: cached_cred.credential.key_id.clone(),
                            secret_key: cached_cred.credential.secret_key.clone(),
                            token: cached_cred.credential.token.clone(),
                        });
                    }
                }
                
                // Get the credentials from the Python credentials provider
                let mut refreshed_credential = this.get_credentials(py)?;

                // Check if the credentials need to be refreshed and refresh them if necessary
                if this.should_refresh(&refreshed_credential) {
                    this.refresh_credentials(py)?;
                    refreshed_credential = this.get_credentials(py)?;
                }
                
                // Return the refreshed credentials and cache them
                let credential = AwsCredential {
                    key_id: refreshed_credential.credential.key_id.clone(),
                    secret_key: refreshed_credential.credential.secret_key.clone(),
                    token: refreshed_credential.credential.token.clone(),
                };
                
                *cached_guard = Some(refreshed_credential);
                
                Ok(credential)
            })
        })
        .await
        .map_err(|e| {
            object_store::Error::Generic {
                store: "PyCredentialsProvider",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Join task failed when refreshing credentials: {}", e),
                )),
            }
        })?
        .map_err(|e: PyErr| {
            object_store::Error::Generic {
                store: "PyCredentialsProvider",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to refresh credentials: {}", e),
                )),
            }
        })
        .map(Arc::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Once;

    static INIT: Once = Once::new();

    /// Initialize Python interpreter once for all tests
    fn initialize_python() {
        INIT.call_once(|| {
            pyo3::prepare_freethreaded_python();
        });
    }

    /// Mock Python credentials object with attributes
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

    /// Helper function to create a mock Python credentials object
    fn create_mock_credentials(
        py: Python,
        access_key: &str,
        secret_key: &str,
        token: Option<&str>,
        expiration: Option<&str>,
    ) -> PyObject {
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

    /// Mock Python credentials provider for testing
    #[pyclass]
    struct MockCredentialsProvider {
        access_key: String,
        secret_key: String,
        token: Option<String>,
        expiration: Option<String>,
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
                expiration,
                call_count: Arc::new(AtomicUsize::new(0)),
                refresh_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn get_credentials(&mut self, py: Python) -> PyResult<PyObject> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(create_mock_credentials(
                py,
                &self.access_key,
                &self.secret_key,
                self.token.as_deref(),
                self.expiration.as_deref(),
            ))
        }

        fn refresh_credentials(&mut self) {
            self.refresh_count.fetch_add(1, Ordering::SeqCst);
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

        let cached = CachedAwsCredential {
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
        Python::with_gil(|py| {
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

            let provider = PyCredentialsProvider::new(mock_provider.into(), Some(900));

            let credential = Arc::new(AwsCredential {
                key_id: "test".to_string(),
                secret_key: "test".to_string(),
                token: None,
            });

            // Already expired
            let cached = CachedAwsCredential {
                credential,
                expire_time: Utc::now() - Duration::hours(1),
            };

            assert!(provider.should_refresh(&cached));
        });
    }

    #[test]
    fn test_get_credentials_from_python() {
        initialize_python();
        Python::with_gil(|py| {
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

            let provider = PyCredentialsProvider::new(mock_provider.into(), None);
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
        Python::with_gil(|py| {
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

            let provider = PyCredentialsProvider::new(mock_provider.into(), None);
            
            // Call refresh_credentials which should succeed
            let result = provider.refresh_credentials(py);
            assert!(result.is_ok());
            
            // Then get credentials should return fresh credentials
            let creds = provider.get_credentials(py);
            assert!(creds.is_ok());
            let cached = creds.unwrap();
            assert_eq!(cached.credential.key_id, "refreshed_access");
        });
    }

}
