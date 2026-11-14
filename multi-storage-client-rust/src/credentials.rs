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

//! Secret-safe credentials and serialized asynchronous refresh.
#[cfg(feature = "s3")]
use crate::config::ProfileConfig;
use crate::error::{ErrorKind, MscError, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

const REFRESH_SKEW: chrono::Duration = chrono::Duration::seconds(60);
const REFRESH_FAILURE_COOLDOWN: Duration = Duration::from_secs(1);

/// Retained secret value whose formatting and serialization always redact.
#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(transparent)]
pub struct SecretString(String);
impl SecretString {
    /// Retain a secret while keeping `Debug`, `Display`, and serialization redacted.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    /// Borrow the unredacted value for signing or an external credential system.
    /// Avoid placing this value in diagnostics or configuration displays.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}
impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}
impl Serialize for SecretString {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str("[REDACTED]")
    }
}
/// AWS-compatible signing credentials.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsCredentials {
    /// Access key identifier; accepts the Python configuration alias `access_key`.
    #[serde(alias = "access_key")]
    pub access_key_id: SecretString,
    /// Signing secret; accepts the Python configuration alias `secret_key`.
    #[serde(alias = "secret_key")]
    pub secret_access_key: SecretString,
    /// Temporary-session token, if the credentials require one.
    #[serde(default)]
    pub session_token: Option<SecretString>,
    /// UTC expiration; `None` denotes non-expiring credentials.
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}
/// Credential material remains tagged by backend.
#[derive(Clone, Debug)]
pub enum Credentials {
    /// AWS-compatible credentials for S3 signing.
    Aws(AwsCredentials),
}
/// Asynchronous source of signing credentials. Implementations must return
/// secret-safe errors and must not install a process-global runtime.
#[async_trait]
pub trait CredentialsProvider: Send + Sync {
    /// Return currently usable credentials or an authentication failure.
    async fn credentials(&self) -> Result<Credentials>;
}

/// Standard AWS shared-profile, process, SSO, role, web-identity and metadata chain.
#[cfg(feature = "s3")]
pub struct AwsDefaultCredentialsProvider(aws_credential_types::provider::SharedCredentialsProvider);
#[cfg(feature = "s3")]
impl std::fmt::Debug for AwsDefaultCredentialsProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AwsDefaultCredentialsProvider([REDACTED])")
    }
}
#[cfg(feature = "s3")]
impl AwsDefaultCredentialsProvider {
    /// Load the standard AWS discovery chain without fetching signing credentials.
    ///
    /// # Errors
    /// Returns an authentication error if no credentials chain can be constructed.
    pub async fn new(profile: Option<&str>, region: &str) -> Result<Self> {
        use aws_smithy_http_client::{
            Builder,
            tls::{Provider, TlsContext, TrustStore, rustls_provider::CryptoMode},
        };
        use base64::Engine as _;
        // Use the same bundled public roots as the storage adapters. SDK native
        // root discovery can panic when macOS trust-store access is restricted.
        let mut trust = TrustStore::empty();
        for certificate in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
            let encoded = base64::engine::general_purpose::STANDARD.encode(certificate.as_ref());
            trust = trust.with_pem_certificate(
                format!("-----BEGIN CERTIFICATE-----\n{encoded}\n-----END CERTIFICATE-----\n")
                    .into_bytes(),
            );
        }
        let tls = TlsContext::builder()
            .with_trust_store(trust)
            .build()
            .map_err(|_| {
                MscError::new(
                    ErrorKind::Unauthenticated,
                    "AWS credential transport could not be configured",
                )
            })?;
        let http = Builder::new()
            .tls_provider(Provider::Rustls(CryptoMode::Ring))
            .tls_context(tls)
            .build_https();
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .http_client(http)
            .region(aws_types::region::Region::new(region.to_owned()))
            .retry_config(aws_config::retry::RetryConfig::disabled());
        if let Some(profile) = profile {
            loader = loader.profile_name(profile);
        }
        let config = loader.load().await;
        config.credentials_provider().map(Self).ok_or_else(|| {
            MscError::new(
                ErrorKind::Unauthenticated,
                "AWS credential discovery is unavailable",
            )
        })
    }
}
#[cfg(feature = "s3")]
#[async_trait]
impl CredentialsProvider for AwsDefaultCredentialsProvider {
    async fn credentials(&self) -> Result<Credentials> {
        use aws_credential_types::provider::ProvideCredentials;
        let credentials = self.0.provide_credentials().await.map_err(|_| {
            MscError::new(
                ErrorKind::Unauthenticated,
                "AWS credentials could not be resolved",
            )
        })?;
        let credentials = AwsCredentials {
            access_key_id: SecretString::new(credentials.access_key_id()),
            secret_access_key: SecretString::new(credentials.secret_access_key()),
            session_token: credentials.session_token().map(SecretString::new),
            expires_at: credentials.expiry().map(DateTime::<Utc>::from),
        };
        validate_credentials(&credentials)?;
        Ok(Credentials::Aws(credentials))
    }
}
/// Static credentials supplied by an application.
#[derive(Clone, Debug)]
pub struct StaticCredentialsProvider(
    /// Credential material validated before each use.
    pub AwsCredentials,
);
#[async_trait]
impl CredentialsProvider for StaticCredentialsProvider {
    async fn credentials(&self) -> Result<Credentials> {
        validate_credentials(&self.0)?;
        Ok(Credentials::Aws(self.0.clone()))
    }
}
/// Atomic file replacement is detected on every request; refresh is single-flight.
pub struct FileCredentialsProvider {
    path: PathBuf,
    cache: Mutex<Option<CachedFileCredentials>>,
}
struct CachedFileCredentials {
    contents: Vec<u8>,
    credentials: AwsCredentials,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct ExternalProcessCredentials {
    version: u8,
    access_key_id: SecretString,
    secret_access_key: SecretString,
    #[serde(default)]
    session_token: Option<SecretString>,
    #[serde(default)]
    expiration: Option<DateTime<Utc>>,
    #[serde(default, rename = "AccountId")]
    _account_id: Option<SecretString>,
}

fn parse_credential_file(data: &[u8]) -> Result<AwsCredentials> {
    let invalid = || {
        MscError::new(
            ErrorKind::Unauthenticated,
            "invalid AWS credentials JSON document",
        )
    };
    let value: serde_json::Value = serde_json::from_slice(data).map_err(|_| invalid())?;
    if value.get("Version").is_some() || value.get("AccessKeyId").is_some() {
        // Decode the original document so duplicate fields remain detectable.
        let external: ExternalProcessCredentials =
            serde_json::from_slice(data).map_err(|_| invalid())?;
        if external.version != 1 {
            return Err(invalid());
        }
        Ok(AwsCredentials {
            access_key_id: external.access_key_id,
            secret_access_key: external.secret_access_key,
            session_token: external.session_token,
            expires_at: external.expiration,
        })
    } else {
        serde_json::from_slice(data).map_err(|_| invalid())
    }
}
impl fmt::Debug for FileCredentialsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileCredentialsProvider")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}
impl FileCredentialsProvider {
    /// Read AWS credential JSON from this file when credentials are requested.
    /// The file may be replaced atomically to rotate secrets; construction does
    /// not read it. This format differs from an AWS shared-profile INI file.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            cache: Mutex::new(None),
        }
    }
}
#[async_trait]
impl CredentialsProvider for FileCredentialsProvider {
    async fn credentials(&self) -> Result<Credentials> {
        let mut cache = self.cache.lock().await;
        let data = tokio::fs::read(&self.path).await.map_err(|_| {
            MscError::new(ErrorKind::Unauthenticated, "cannot read credentials file")
        })?;
        if let Some(previous) = &*cache
            && previous.contents == data
            && credentials_are_fresh(&previous.credentials)
        {
            return Ok(Credentials::Aws(previous.credentials.clone()));
        }
        let credentials = parse_credential_file(&data)?;
        validate_credentials(&credentials)?;
        *cache = Some(CachedFileCredentials {
            contents: data,
            credentials: credentials.clone(),
        });
        Ok(Credentials::Aws(credentials))
    }
}
/// Wrap an external refresh implementation with one shared refresh flight.
pub struct RefreshingCredentialsProvider {
    inner: Arc<dyn CredentialsProvider>,
    cached: Mutex<RefreshState>,
}
#[derive(Default)]
struct RefreshState {
    ready: Option<Credentials>,
    refreshed_at: Option<Instant>,
    failed: Option<(Instant, MscError)>,
}
impl RefreshingCredentialsProvider {
    /// Share one refresh flight across callers. Credentials are refreshed within
    /// 60 seconds of expiry, and refresh failures share a one-second cooldown.
    #[must_use]
    pub fn new(inner: Arc<dyn CredentialsProvider>) -> Self {
        Self {
            inner,
            cached: Mutex::new(RefreshState::default()),
        }
    }
}
#[async_trait]
impl CredentialsProvider for RefreshingCredentialsProvider {
    async fn credentials(&self) -> Result<Credentials> {
        let mut cached = self.cached.lock().await;
        if let Some(Credentials::Aws(credentials)) = &cached.ready
            && (credentials_are_fresh(credentials)
                || (validate_credentials(credentials).is_ok()
                    && cached
                        .refreshed_at
                        .is_some_and(|at| at.elapsed() < REFRESH_FAILURE_COOLDOWN)))
        {
            return Ok(Credentials::Aws(credentials.clone()));
        }
        // Concurrent waiters share failures too; prevent an authentication
        // outage from causing one refresh request per waiting storage request.
        if let Some((at, error)) = &cached.failed
            && at.elapsed() < REFRESH_FAILURE_COOLDOWN
        {
            return usable_cached(&cached).ok_or_else(|| error.clone());
        }
        let result = match self.inner.credentials().await {
            Ok(next) => {
                let Credentials::Aws(credentials) = &next;
                validate_credentials(credentials).map(|()| next)
            }
            Err(_) => Err(MscError::new(
                ErrorKind::Unauthenticated,
                "credential refresh failed",
            )),
        };
        match result {
            Ok(credentials) => {
                cached.ready = Some(credentials.clone());
                cached.refreshed_at = Some(Instant::now());
                cached.failed = None;
                Ok(credentials)
            }
            Err(error) => {
                cached.failed = Some((Instant::now(), error.clone()));
                usable_cached(&cached).ok_or(error)
            }
        }
    }
}
fn usable_cached(state: &RefreshState) -> Option<Credentials> {
    state
        .ready
        .as_ref()
        .filter(|Credentials::Aws(credentials)| validate_credentials(credentials).is_ok())
        .cloned()
}

fn credentials_are_fresh(credentials: &AwsCredentials) -> bool {
    credentials
        .expires_at
        .is_none_or(|expires| expires > Utc::now() + REFRESH_SKEW)
}

pub(crate) fn validate_credentials(credentials: &AwsCredentials) -> Result<()> {
    if credentials
        .expires_at
        .is_some_and(|expires| expires <= Utc::now())
    {
        return Err(MscError::new(
            ErrorKind::Unauthenticated,
            "credentials have expired",
        ));
    }
    if credentials.access_key_id.expose_secret().is_empty()
        || credentials.secret_access_key.expose_secret().is_empty()
    {
        return Err(MscError::new(
            ErrorKind::Unauthenticated,
            "credentials must contain nonempty access and secret keys",
        ));
    }
    if credentials
        .access_key_id
        .expose_secret()
        .bytes()
        .any(|byte| byte.is_ascii_control())
        || credentials.session_token.as_ref().is_some_and(|token| {
            token
                .expose_secret()
                .bytes()
                .any(|byte| byte.is_ascii_control())
        })
    {
        return Err(MscError::new(
            ErrorKind::Unauthenticated,
            "AWS access key and session token must not contain control characters",
        ));
    }
    Ok(())
}

#[cfg(feature = "s3")]
pub(crate) async fn credentials_from_profile(
    profile: &ProfileConfig,
) -> Result<Option<Arc<dyn CredentialsProvider>>> {
    let Some(config) = &profile.credentials_provider else {
        return Ok(None);
    };
    match config.provider_type.as_str() {
        "static" | "StaticS3CredentialsProvider" | "StaticS3Credentials" | "S3Credentials" => {
            let credentials = serde_json::from_value(config.options_value()).map_err(|_| {
                MscError::new(
                    ErrorKind::InvalidConfig,
                    "static credentials require access_key_id and secret_access_key",
                )
            })?;
            validate_credentials(&credentials)?;
            Ok(Some(Arc::new(StaticCredentialsProvider(credentials))))
        }
        "file" | "FileBasedCredentials" | "FileBasedCredentialsProvider" => {
            if config.options.contains_key("path")
                && config.options.contains_key("credential_file_path")
            {
                return Err(MscError::new(
                    ErrorKind::InvalidConfig,
                    "specify one credentials file path",
                ));
            }
            let path = config
                .options
                .get("path")
                .or_else(|| config.options.get("credential_file_path"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    MscError::new(
                        ErrorKind::InvalidConfig,
                        "file credentials require options.path or credential_file_path",
                    )
                })?;
            if config.options.len() != 1 {
                return Err(MscError::new(
                    ErrorKind::InvalidConfig,
                    "file credentials accept only options.path",
                ));
            }
            let provider = Arc::new(FileCredentialsProvider::new(path));
            provider.credentials().await?;
            Ok(Some(provider))
        }
        _ => Err(MscError::new(
            ErrorKind::Unsupported,
            "configured credentials provider is unavailable in this iteration",
        )),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[test]
    fn formatting_and_serialization_never_disclose() {
        let secret = SecretString::new("do-not-print");
        assert!(!format!("{secret:?} {secret}").contains("do-not-print"));
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"[REDACTED]\"");
    }

    #[tokio::test]
    async fn invalid_signing_headers_return_redacted_errors() {
        for (key, token) in [
            ("private\r\naccess", None),
            ("valid-access", Some("private\0token")),
            ("valid-access", Some("private\nsession")),
        ] {
            let provider = StaticCredentialsProvider(AwsCredentials {
                access_key_id: SecretString::new(key),
                secret_access_key: SecretString::new("valid-secret"),
                session_token: token.map(SecretString::new),
                expires_at: None,
            });
            let error = provider.credentials().await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::Unauthenticated);
            assert!(!format!("{error:?} {error}").contains("private"));
        }
    }
    #[tokio::test]
    async fn detects_atomic_rotation_and_sanitizes_bad_documents() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credentials.json");
        tokio::fs::write(
            &path,
            r#"{"access_key_id":"first","secret_access_key":"secret"}"#,
        )
        .await
        .unwrap();
        let provider = FileCredentialsProvider::new(&path);
        let Credentials::Aws(first) = provider.credentials().await.unwrap();
        assert_eq!(first.access_key_id.expose_secret(), "first");
        let next = directory.path().join("next");
        tokio::fs::write(
            &next,
            r#"{"access_key_id":"second","secret_access_key":"rotated"}"#,
        )
        .await
        .unwrap();
        tokio::fs::rename(&next, &path).await.unwrap();
        let Credentials::Aws(second) = provider.credentials().await.unwrap();
        assert_eq!(second.access_key_id.expose_secret(), "second");
        tokio::fs::write(&path, "bad-secret-json").await.unwrap();
        assert!(
            !provider
                .credentials()
                .await
                .unwrap_err()
                .to_string()
                .contains("bad-secret")
        );
        tokio::fs::write(
            &path,
            r#"{"access_key_id":"","secret_access_key":"secret"}"#,
        )
        .await
        .unwrap();
        assert_eq!(
            provider.credentials().await.unwrap_err().kind,
            ErrorKind::Unauthenticated
        );
    }

    #[tokio::test]
    async fn concurrent_refresh_is_one_flight() {
        struct CountingProvider(AtomicUsize);
        #[async_trait]
        impl CredentialsProvider for CountingProvider {
            async fn credentials(&self) -> Result<Credentials> {
                self.0.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                Ok(Credentials::Aws(AwsCredentials {
                    access_key_id: SecretString::new("key"),
                    secret_access_key: SecretString::new("secret"),
                    session_token: None,
                    expires_at: Some(Utc::now() + chrono::Duration::seconds(30)),
                }))
            }
        }
        let inner = Arc::new(CountingProvider(AtomicUsize::new(0)));
        let provider = Arc::new(RefreshingCredentialsProvider::new(inner.clone()));
        let outcomes = futures::future::join_all((0..32).map(|_| provider.credentials())).await;
        assert!(outcomes.iter().all(std::result::Result::is_ok));
        assert_eq!(inner.0.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_early_refresh_retains_usable_credentials_and_shares_cooldown() {
        struct FailsAfterFirst(AtomicUsize);
        #[async_trait]
        impl CredentialsProvider for FailsAfterFirst {
            async fn credentials(&self) -> Result<Credentials> {
                if self.0.fetch_add(1, Ordering::SeqCst) > 0 {
                    return Err(MscError::new(
                        ErrorKind::Unauthenticated,
                        "private upstream failure",
                    ));
                }
                Ok(Credentials::Aws(AwsCredentials {
                    access_key_id: SecretString::new("key"),
                    secret_access_key: SecretString::new("secret"),
                    session_token: None,
                    expires_at: Some(Utc::now() + chrono::Duration::seconds(30)),
                }))
            }
        }
        let inner = Arc::new(FailsAfterFirst(AtomicUsize::new(0)));
        let provider = RefreshingCredentialsProvider::new(inner.clone());
        provider.credentials().await.unwrap();
        provider.cached.lock().await.refreshed_at =
            Instant::now().checked_sub(Duration::from_secs(2));
        let outcomes = futures::future::join_all((0..32).map(|_| provider.credentials())).await;
        assert!(outcomes.iter().all(std::result::Result::is_ok));
        assert_eq!(inner.0.load(Ordering::SeqCst), 2);
        if let Some(Credentials::Aws(credentials)) = &mut provider.cached.lock().await.ready {
            credentials.expires_at = Some(Utc::now() - chrono::Duration::seconds(1));
        }
        assert_eq!(
            provider.credentials().await.unwrap_err().kind,
            ErrorKind::Unauthenticated
        );
        assert_eq!(inner.0.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn concurrent_failed_refresh_shares_a_redacted_error() {
        struct FailingProvider(AtomicUsize);
        #[async_trait]
        impl CredentialsProvider for FailingProvider {
            async fn credentials(&self) -> Result<Credentials> {
                self.0.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                Err(MscError::new(ErrorKind::Provider, "credential-secret"))
            }
        }
        let inner = Arc::new(FailingProvider(AtomicUsize::new(0)));
        let provider = RefreshingCredentialsProvider::new(inner.clone());
        let outcomes = futures::future::join_all((0..32).map(|_| provider.credentials())).await;
        assert!(outcomes.iter().all(std::result::Result::is_err));
        assert_eq!(inner.0.load(Ordering::SeqCst), 1);
        assert!(!format!("{outcomes:?}").contains("credential-secret"));
    }

    #[tokio::test]
    async fn external_process_file_format_rotates_atomically_between_supported_formats() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credentials.json");
        let expiration = Utc::now() + chrono::Duration::hours(1);
        let external = serde_json::json!({"Version": 1, "AccountId": "123456789012", "AccessKeyId": "external-access", "SecretAccessKey": "external-secret", "SessionToken": "external-token", "Expiration": expiration});
        tokio::fs::write(&path, external.to_string()).await.unwrap();
        let provider = Arc::new(FileCredentialsProvider::new(&path));
        let calls = futures::future::join_all((0..16).map(|_| provider.credentials())).await;
        for result in calls {
            let Credentials::Aws(credentials) = result.unwrap();
            assert_eq!(credentials.access_key_id.expose_secret(), "external-access");
            assert_eq!(
                credentials.secret_access_key.expose_secret(),
                "external-secret"
            );
            assert_eq!(
                credentials.session_token.as_ref().unwrap().expose_secret(),
                "external-token"
            );
            assert_eq!(credentials.expires_at, Some(expiration));
            assert!(!format!("{credentials:?}").contains("external-secret"));
        }
        let replacement = directory.path().join("replacement");
        tokio::fs::write(
            &replacement,
            r#"{"access_key":"legacy-access","secret_key":"legacy-secret"}"#,
        )
        .await
        .unwrap();
        tokio::fs::rename(&replacement, &path).await.unwrap();
        let Credentials::Aws(credentials) = provider.credentials().await.unwrap();
        assert_eq!(credentials.access_key_id.expose_secret(), "legacy-access");
        assert_eq!(
            credentials.secret_access_key.expose_secret(),
            "legacy-secret"
        );
        assert!(credentials.session_token.is_none());
        assert!(credentials.expires_at.is_none());
        tokio::fs::write(
            &replacement,
            r#"{"Version":1,"AccessKeyId":"rotated-access","SecretAccessKey":"rotated-secret"}"#,
        )
        .await
        .unwrap();
        tokio::fs::rename(&replacement, &path).await.unwrap();
        let Credentials::Aws(credentials) = provider.credentials().await.unwrap();
        assert_eq!(credentials.access_key_id.expose_secret(), "rotated-access");
    }

    #[tokio::test]
    async fn invalid_versions_types_mixed_formats_and_expiration_do_not_reuse_cached_secrets() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credentials.json");
        let valid =
            r#"{"Version":1,"AccessKeyId":"cached-access","SecretAccessKey":"cached-secret"}"#;
        tokio::fs::write(&path, valid).await.unwrap();
        let provider = FileCredentialsProvider::new(&path);
        provider.credentials().await.unwrap();
        let malformed = [
            "[]",
            "null",
            r#""private-secret""#,
            r#"{"Version":1"#,
            r#"{"Version":0,"AccessKeyId":"private-access","SecretAccessKey":"private-secret"}"#,
            r#"{"Version":2,"AccessKeyId":"private-access","SecretAccessKey":"private-secret"}"#,
            r#"{"Version":"1","AccessKeyId":"private-access","SecretAccessKey":"private-secret"}"#,
            r#"{"Version":true,"AccessKeyId":"private-access","SecretAccessKey":"private-secret"}"#,
            r#"{"Version":1.0,"AccessKeyId":"private-access","SecretAccessKey":"private-secret"}"#,
            r#"{"Version":256,"AccessKeyId":"private-access","SecretAccessKey":"private-secret"}"#,
            r#"{"AccessKeyId":"private-access","SecretAccessKey":"private-secret"}"#,
            r#"{"Version":1,"SecretAccessKey":"private-secret"}"#,
            r#"{"Version":1,"AccessKeyId":"private-access"}"#,
            r#"{"Version":1,"AccessKeyId":null,"SecretAccessKey":"private-secret"}"#,
            r#"{"Version":1,"AccessKeyId":1,"SecretAccessKey":"private-secret"}"#,
            r#"{"Version":1,"AccessKeyId":"private-access","SecretAccessKey":false}"#,
            r#"{"Version":1,"AccessKeyId":"","SecretAccessKey":"private-secret"}"#,
            r#"{"Version":1,"AccessKeyId":"private-access","SecretAccessKey":""}"#,
            r#"{"Version":1,"AccessKeyId":"private-access","SecretAccessKey":"private-secret","SessionToken":1}"#,
            r#"{"Version":1,"AccessKeyId":"private\naccess","SecretAccessKey":"private-secret"}"#,
            r#"{"Version":1,"AccessKeyId":"private-access","SecretAccessKey":"private-secret","SessionToken":"private\rtoken"}"#,
            r#"{"Version":1,"AccessKeyId":"private-access","SecretAccessKey":"private-secret","Expiration":"2000-01-01T00:00:00Z"}"#,
            r#"{"Version":1,"AccessKeyId":"private-access","SecretAccessKey":"private-secret","Expiration":"2030-01-01T00:00:00"}"#,
            r#"{"Version":1,"AccessKeyId":"private-access","SecretAccessKey":"private-secret","Expiration":1}"#,
            r#"{"Version":1,"AccessKeyId":"private-access","SecretAccessKey":"private-secret","expires_at":"2030-01-01T00:00:00Z"}"#,
            r#"{"Version":1,"AccessKeyId":"private-access","SecretAccessKey":"private-secret","secret_access_key":"other-secret"}"#,
            r#"{"Version":1,"AccessKeyId":"private-access","SecretAccessKey":"private-secret","extra":"private-secret"}"#,
            r#"{"Version":2,"Version":1,"AccessKeyId":"private-access","SecretAccessKey":"private-secret"}"#,
            r#"{"Version":1,"AccessKeyId":"private-access","AccessKeyId":"other-access","SecretAccessKey":"private-secret"}"#,
            r#"{"access_key_id":"private-access","access_key_id":"other-access","secret_access_key":"private-secret"}"#,
            r#"{"access_key":"private-access","access_key_id":"other-access","secret_key":"private-secret"}"#,
            r#"{"access_key_id":"private-access","secret_access_key":"private-secret","Expiration":"2030-01-01T00:00:00Z"}"#,
        ];
        for (case, document) in malformed.iter().enumerate() {
            tokio::fs::write(&path, document).await.unwrap();
            let error = provider
                .credentials()
                .await
                .expect_err(&format!("credential case {case} must fail"));
            assert_eq!(
                error.kind,
                ErrorKind::Unauthenticated,
                "credential case {case}"
            );
            assert!(!format!("{error:?} {error}").contains("private"));
            assert!(!format!("{error:?} {error}").contains("cached-secret"));
        }
        tokio::fs::remove_file(&path).await.unwrap();
        assert_eq!(
            provider.credentials().await.unwrap_err().kind,
            ErrorKind::Unauthenticated
        );
        tokio::fs::write(&path, valid).await.unwrap();
        let Credentials::Aws(restored) = provider.credentials().await.unwrap();
        assert_eq!(restored.access_key_id.expose_secret(), "cached-access");
    }

    #[tokio::test]
    async fn a_cached_file_document_is_rechecked_when_its_credentials_expire() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credentials.json");
        tokio::fs::write(&path, serde_json::json!({"Version": 1, "AccessKeyId": "access", "SecretAccessKey": "secret", "Expiration": Utc::now() + chrono::Duration::seconds(1)}).to_string()).await.unwrap();
        let provider = FileCredentialsProvider::new(path);
        provider.credentials().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(
            provider.credentials().await.unwrap_err().kind,
            ErrorKind::Unauthenticated
        );
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn python_file_credential_path_alias_is_validated_before_storage_requests() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credentials.json");
        tokio::fs::write(
            &path,
            r#"{"Version":1,"AccessKeyId":"alias-access","SecretAccessKey":"alias-secret"}"#,
        )
        .await
        .unwrap();
        for tag in [
            "file",
            "FileBasedCredentials",
            "FileBasedCredentialsProvider",
        ] {
            for option in ["path", "credential_file_path"] {
                let profile: ProfileConfig = serde_json::from_value(serde_json::json!({"credentials_provider": {"type": tag, "options": {option: path}}})).unwrap();
                let provider = credentials_from_profile(&profile).await.unwrap().unwrap();
                let Credentials::Aws(credentials) = provider.credentials().await.unwrap();
                assert_eq!(credentials.access_key_id.expose_secret(), "alias-access");
            }
        }
        let both: ProfileConfig = serde_json::from_value(serde_json::json!({"credentials_provider": {"type": "FileBasedCredentialsProvider", "options": {"path": path, "credential_file_path": path}}})).unwrap();
        let Err(error) = credentials_from_profile(&both).await else {
            panic!("conflicting file aliases must be rejected");
        };
        assert_eq!(error.kind, ErrorKind::InvalidConfig);
        let missing: ProfileConfig = serde_json::from_value(serde_json::json!({"credentials_provider": {"type": "FileBasedCredentialsProvider", "options": {"credential_file_path": directory.path().join("absent")}}})).unwrap();
        let Err(error) = credentials_from_profile(&missing).await else {
            panic!("missing credential files must fail eagerly");
        };
        assert_eq!(error.kind, ErrorKind::Unauthenticated);
    }

    #[cfg(feature = "s3")]
    #[test]
    fn aws_shared_profiles_environment_precedence_and_external_process_chain_are_isolated() {
        let directory = tempfile::Builder::new()
            .prefix("aws process ' quoted ")
            .tempdir()
            .unwrap();
        let shared = directory.path().join("shared-credentials");
        let config = directory.path().join("config");
        let process = directory.path().join("process.json");
        std::fs::write(&shared, "[default]\naws_access_key_id=default-access\naws_secret_access_key=default-secret\n[selected]\naws_access_key_id=profile-access\naws_secret_access_key=profile-secret\naws_session_token=profile-token\n").unwrap();
        std::fs::write(
            &config,
            format!(
                "[profile process]\ncredential_process = /bin/cat '{}'\n",
                process.to_str().unwrap().replace('\'', "'\"'\"'")
            ),
        )
        .unwrap();
        std::fs::write(&process, serde_json::json!({"Version":1,"AccessKeyId":"process-access","SecretAccessKey":"process-secret","SessionToken":"process-token","Expiration":Utc::now()+chrono::Duration::hours(1)}).to_string()).unwrap();
        for case in [
            "default",
            "selected",
            "environment_selected",
            "environment",
            "process",
            "missing",
        ] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "credentials::tests::aws_discovery_child_probe",
                    "--ignored",
                    "--nocapture",
                ])
                .env("MSC_AWS_DISCOVERY_CASE", case)
                .env("AWS_SHARED_CREDENTIALS_FILE", &shared)
                .env("AWS_CONFIG_FILE", &config)
                .env("AWS_EC2_METADATA_DISABLED", "true");
            for key in [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_SECURITY_TOKEN",
                "AWS_PROFILE",
                "AWS_DEFAULT_PROFILE",
                "AWS_WEB_IDENTITY_TOKEN_FILE",
                "AWS_ROLE_ARN",
                "AWS_ROLE_SESSION_NAME",
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                "AWS_CONTAINER_AUTHORIZATION_TOKEN",
                "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
                "AWS_ENDPOINT_URL",
                "AWS_ENDPOINT_URL_STS",
                "AWS_REGION",
                "AWS_DEFAULT_REGION",
            ] {
                command.env_remove(key);
            }
            if case == "environment_selected" {
                command.env("AWS_PROFILE", "selected");
            }
            if case == "environment" {
                command
                    .env("AWS_ACCESS_KEY_ID", "environment-access")
                    .env("AWS_SECRET_ACCESS_KEY", "environment-secret")
                    .env("AWS_SESSION_TOKEN", "environment-token");
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "AWS discovery case {case} failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(!String::from_utf8_lossy(&output.stdout).contains("process-secret"));
        }
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    #[ignore = "child probe invoked by the isolated AWS discovery test"]
    async fn aws_discovery_child_probe() {
        let case = std::env::var("MSC_AWS_DISCOVERY_CASE").unwrap();
        let profile = match case.as_str() {
            "selected" | "environment" => Some("selected"),
            "process" => Some("process"),
            "missing" => Some("nonexistent"),
            _ => None,
        };
        let provider = AwsDefaultCredentialsProvider::new(profile, "us-east-1")
            .await
            .unwrap();
        assert!(!format!("{provider:?}").contains("profile-secret"));
        if case == "missing" {
            let error = provider.credentials().await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::Unauthenticated);
            assert!(!format!("{error:?}").contains("secret"));
            return;
        }
        let Credentials::Aws(credentials) = provider.credentials().await.unwrap();
        let prefix = match case.as_str() {
            "default" => "default",
            "process" => "process",
            "environment" => "environment",
            _ => "profile",
        };
        assert_eq!(
            credentials.access_key_id.expose_secret(),
            format!("{prefix}-access")
        );
        assert_eq!(
            credentials.secret_access_key.expose_secret(),
            format!("{prefix}-secret")
        );
        if prefix == "default" {
            assert!(credentials.session_token.is_none());
        } else {
            assert_eq!(
                credentials.session_token.as_ref().unwrap().expose_secret(),
                format!("{prefix}-token")
            );
        }
        if prefix == "process" {
            assert!(
                credentials
                    .expires_at
                    .is_some_and(|expiration| expiration > Utc::now())
            );
        }
    }
}
