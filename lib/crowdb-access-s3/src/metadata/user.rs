// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded opaque user metadata shared by HTTP and durable publication.

use bincode::Options as _;
use hyper::header::{HeaderName, HeaderValue};
use hyper::HeaderMap;
use std::collections::BTreeMap;

use crate::error::S3ErrorCode;

const PREFIX: &str = "x-amz-meta-";
const MAX_BYTES: usize = 2048;
const MAX_ENCODED_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UserMetadata(BTreeMap<String, String>);

impl UserMetadata {
    /// Captures normalized names and opaque printable ASCII values.
    ///
    /// # Errors
    /// Rejects duplicate names, empty names, non-ASCII values and over 2 KiB.
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, S3ErrorCode> {
        let mut values = BTreeMap::new();
        for name in headers.keys() {
            let Some(key) = name.as_str().strip_prefix(PREFIX) else {
                continue;
            };
            let mut entries = headers.get_all(name).iter();
            let value = entries.next().ok_or(S3ErrorCode::InvalidRequest)?;
            if entries.next().is_some() {
                return Err(S3ErrorCode::InvalidRequest);
            }
            values.insert(
                key.to_owned(),
                value
                    .to_str()
                    .map_err(|_| S3ErrorCode::InvalidRequest)?
                    .to_owned(),
            );
        }
        let metadata = Self(values);
        metadata.validate()?;
        Ok(metadata)
    }

    /// Encodes the complete map for atomic object/session publication.
    ///
    /// # Errors
    /// Rejects serialization failures.
    pub fn encode(&self) -> Result<Vec<u8>, S3ErrorCode> {
        if self.0.is_empty() {
            return Ok(Vec::new());
        }
        codec().serialize(&self.0).map_err(|_| S3ErrorCode::InternalError)
    }

    /// Decodes bounded stored attributes and revalidates their HTTP contract.
    ///
    /// # Errors
    /// Rejects malformed, oversized or noncanonical attributes.
    pub fn decode(bytes: &[u8]) -> Result<Self, S3ErrorCode> {
        if bytes.is_empty() {
            return Ok(Self::default());
        }
        if bytes.len() as u64 > MAX_ENCODED_BYTES {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let metadata = Self(
            codec()
                .deserialize(bytes)
                .map_err(|_| S3ErrorCode::InvalidRequest)?,
        );
        metadata.validate()?;
        if metadata.encode()? != bytes {
            return Err(S3ErrorCode::InvalidRequest);
        }
        Ok(metadata)
    }

    /// Restores validated metadata headers on HEAD and GET responses.
    ///
    /// # Errors
    /// Rejects invalid header representations.
    pub fn append_headers(&self, headers: &mut HeaderMap) -> Result<(), S3ErrorCode> {
        for (key, value) in &self.0 {
            let name = HeaderName::from_bytes(format!("{PREFIX}{key}").as_bytes())
                .map_err(|_| S3ErrorCode::InternalError)?;
            let value = HeaderValue::from_str(value).map_err(|_| S3ErrorCode::InternalError)?;
            headers.insert(name, value);
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), S3ErrorCode> {
        let mut bytes = 0;
        for (key, value) in &self.0 {
            if key.is_empty()
                || key.bytes().any(|byte| byte.is_ascii_uppercase())
                || HeaderName::from_bytes(key.as_bytes()).is_err()
                || !value.bytes().all(|byte| (b' '..=b'~').contains(&byte))
            {
                return Err(S3ErrorCode::InvalidRequest);
            }
            bytes += key.len() + value.len();
            if bytes > MAX_BYTES {
                return Err(S3ErrorCode::InvalidRequest);
            }
        }
        Ok(())
    }
}

fn codec() -> impl bincode::Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_ENCODED_BYTES)
        .reject_trailing_bytes()
}
