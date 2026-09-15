// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authentication boundary over raw request components.

mod secrets;
mod sigv4;
mod snapshot;

pub use secrets::{
    CredentialCipher, DurableCredentialRecord, EncryptedCredentialRecord, IssuedUserToken, MasterKey,
    SecretError,
};
pub use sigv4::{Credential, CredentialProvider, SigV4Verifier};
pub use snapshot::{CredentialCache, CredentialCacheError};

use hyper::{HeaderMap, Method, Uri};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadMode<'a> {
    Empty,
    ContentLength(u64),
    Chunked,
    Signed(&'a str),
}

#[derive(Clone, Copy)]
pub struct RawAuthRequest<'a> {
    pub method: &'a Method,
    pub uri: &'a Uri,
    pub headers: &'a HeaderMap,
    pub payload_mode: PayloadMode<'a>,
}

impl<'a> RawAuthRequest<'a> {
    #[must_use]
    pub fn from_parts(method: &'a Method, uri: &'a Uri, headers: &'a HeaderMap) -> Self {
        let payload_mode = headers
            .get("x-amz-content-sha256")
            .and_then(|value| value.to_str().ok())
            .map(PayloadMode::Signed)
            .or_else(|| {
                headers
                    .get(hyper::header::CONTENT_LENGTH)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse().ok())
                    .map(|length| {
                        if length == 0 {
                            PayloadMode::Empty
                        } else {
                            PayloadMode::ContentLength(length)
                        }
                    })
            })
            .unwrap_or_else(|| {
                if headers.contains_key(hyper::header::TRANSFER_ENCODING) {
                    PayloadMode::Chunked
                } else {
                    PayloadMode::Empty
                }
            });
        Self {
            method,
            uri,
            headers,
            payload_mode,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AuthError {
    #[error("request authentication failed")]
    Rejected,
    #[error("authentication provider is unavailable")]
    Unavailable,
}

#[async_trait::async_trait]
pub trait RequestAuthenticator: Send + Sync {
    async fn authenticate(&self, request: RawAuthRequest<'_>) -> Result<(), AuthError>;
}

/// Explicit trusted-network bypass. Construction emits the required warning.
pub struct TrustedNetworkAuthenticator;

impl TrustedNetworkAuthenticator {
    #[must_use]
    pub fn new() -> Self {
        tracing::warn!("S3 trusted-network mode bypasses request authentication");
        Self
    }
}

impl Default for TrustedNetworkAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl RequestAuthenticator for TrustedNetworkAuthenticator {
    async fn authenticate(&self, _request: RawAuthRequest<'_>) -> Result<(), AuthError> {
        Ok(())
    }
}
