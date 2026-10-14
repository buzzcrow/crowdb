// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ServiceError {
    pub code: String,
    pub message: String,
    pub request_id: String,
    pub retryable: bool,
}

/// Transport details deliberately omit request URLs and credentials.
#[derive(Debug, thiserror::Error)]
#[error("HTTP transport failed")]
pub struct TransportError {
    #[source]
    inner: reqwest::Error,
}

impl TransportError {
    pub(crate) fn retryable(&self) -> bool {
        self.inner.is_timeout() || (self.inner.is_request() && !self.inner.is_connect())
    }
}

impl From<reqwest::Error> for TransportError {
    fn from(error: reqwest::Error) -> Self {
        Self {
            inner: error.without_url(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid SDK configuration or request: {0}")]
    Invalid(String),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("service returned {status}: {error:?}")]
    Service { status: u16, error: ServiceError },
    #[error("dataset stream failed: {error:?}")]
    Stream { error: ServiceError },
    #[error("request or response exceeded the configured byte limit")]
    Limit,
    #[error("shared SDK request or byte capacity is exhausted")]
    Capacity,
    #[error("invalid response encoding")]
    Codec(#[from] serde_json::Error),
    #[error("local I/O failed")]
    Io(#[from] std::io::Error),
    #[error("mutation outcome is unknown; recover using identity {identity}: {source}")]
    Ambiguous {
        identity: String,
        #[source]
        source: Box<Error>,
    },
    #[error("operation deadline expired")]
    Deadline,
    #[error("payload integrity check failed")]
    Integrity,
}

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Self::Transport(error.into())
    }
}

impl Error {
    #[must_use]
    pub fn with_identity(self, identity: &str) -> Self {
        if matches!(self, Self::Transport(_) | Self::Deadline) {
            Self::Ambiguous {
                identity: identity.into(),
                source: Box::new(self),
            }
        } else {
            self
        }
    }
}
