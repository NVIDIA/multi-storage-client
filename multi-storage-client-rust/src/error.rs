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

//! Provider-neutral failures. Messages supplied by adapters must be secret safe.
use std::time::Duration;

/// Stable classification independent of a provider SDK's error text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// The requested logical object or filesystem path is absent.
    NotFound,
    /// A required read-only manifest snapshot could not be discovered.
    ManifestNotFound,
    /// The caller lacks permission for the operation.
    PermissionDenied,
    /// Credentials are missing, invalid, or expired.
    Unauthenticated,
    /// A create-only destination already exists.
    AlreadyExists,
    /// The object does not satisfy the requested generation or entity tag.
    PreconditionFailed,
    /// A mutation may have succeeded before a response was lost.
    AmbiguousOutcome,
    /// A conditional read found an unchanged entity tag.
    NotModified,
    /// The backend throttled the request; retryable by default.
    RateLimited,
    /// A request exceeded its timeout; retryable by default.
    Timeout,
    /// A transient backend or transport failure; retryable by default.
    Unavailable,
    /// Configuration violates a schema or runtime contract.
    InvalidConfig,
    /// A key, URL, range, or path boundary is invalid.
    InvalidPath,
    /// An optional operation or deferred feature is unavailable.
    Unsupported,
    /// The operation was cancelled; cancellation is never retried.
    Cancelled,
    /// Bytes, metadata, or routing violate a consistency or safety contract.
    Integrity,
    /// A backend-specific failure without a more precise classification.
    Provider,
    /// A local I/O failure without a more precise classification.
    Io,
}

/// A contextual error with an explicit retry classification.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct MscError {
    /// Stable category suitable for application control flow.
    pub kind: ErrorKind,
    /// Human-readable explanation; adapters must omit secrets and response bodies.
    pub message: String,
    /// Operation name, when known.
    pub operation: Option<Box<str>>,
    /// Backend identifier, when known.
    pub provider: Option<Box<str>>,
    /// Normalized logical object key, when known.
    pub key: Option<Box<str>>,
    /// Backend HTTP status, when applicable.
    pub http_status: Option<u16>,
    /// Backend request identifier useful for diagnostics.
    pub request_id: Option<Box<str>>,
    /// Backend-supplied retry delay, when available.
    pub retry_after: Option<Duration>,
    /// Whether the operation can spend another client retry attempt.
    /// Multipart adapters clear this after consuming their own part budget.
    pub retryable: bool,
}

impl MscError {
    /// Construct an error. Only transient categories are retryable by default.
    #[must_use]
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            operation: None,
            provider: None,
            key: None,
            http_status: None,
            request_id: None,
            retry_after: None,
            retryable: matches!(
                kind,
                ErrorKind::RateLimited | ErrorKind::Timeout | ErrorKind::Unavailable
            ),
        }
    }
    pub(crate) fn ambiguous_outcome(mut self) -> Self {
        self.kind = ErrorKind::AmbiguousOutcome;
        self.retryable = false;
        self.message =
            "an earlier mutation attempt may have succeeded before its response was lost".into();
        self
    }

    /// Attach the operation, backend tag, and normalized object key.
    #[must_use]
    pub fn context(mut self, operation: &str, provider: &str, key: &str) -> Self {
        self.operation = Some(operation.into());
        self.provider = Some(provider.into());
        self.key = Some(key.into());
        self
    }
    /// Construct the standard failure for an optional operation.
    #[must_use]
    pub fn unsupported(operation: &str) -> Self {
        Self::new(
            ErrorKind::Unsupported,
            format!("{operation} is not supported by this provider"),
        )
    }
}

impl From<std::io::Error> for MscError {
    fn from(error: std::io::Error) -> Self {
        let kind = match error.kind() {
            std::io::ErrorKind::NotFound => ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            std::io::ErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
            std::io::ErrorKind::TimedOut => ErrorKind::Timeout,
            _ => ErrorKind::Io,
        };
        Self::new(kind, error.to_string())
    }
}

/// The crate's standard result type.
pub type Result<T> = std::result::Result<T, MscError>;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_classification_ignores_message_text() {
        assert!(!MscError::new(ErrorKind::NotFound, "timeout unavailable 503").retryable);
        assert!(MscError::new(ErrorKind::Unavailable, "not found").retryable);
        for kind in [
            ErrorKind::Cancelled,
            ErrorKind::PermissionDenied,
            ErrorKind::Unauthenticated,
            ErrorKind::PreconditionFailed,
            ErrorKind::InvalidConfig,
            ErrorKind::InvalidPath,
        ] {
            assert!(!MscError::new(kind, "retry me").retryable);
        }
        assert_eq!(
            MscError::from(std::io::Error::from(std::io::ErrorKind::NotFound)).kind,
            ErrorKind::NotFound
        );
    }
}
