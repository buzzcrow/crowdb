// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use thiserror::Error;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ClientError {
    #[error("chunk KV catalog is unavailable: {0}")]
    CatalogUnavailable(String),
    #[error("chunk KV catalog is invalid: {0}")]
    InvalidCatalog(String),
    #[error("chunk KV transport failed: {0}")]
    Transport(String),
    #[error("chunk KV operation deadline elapsed")]
    Deadline,
    #[error("chunk KV request identity sequence is exhausted")]
    SequenceExhausted,
    #[error("chunk KV client input exceeds configured bounds")]
    TooLarge,
    #[error("chunk KV request is invalid: {0}")]
    InvalidRequest(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;
