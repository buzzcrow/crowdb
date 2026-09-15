// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Stateless, bounded continuation tokens for ordered object listing.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::metadata::BucketId;

const VERSION: u8 = 1;
const TAG_BYTES: usize = 32;
const MAX_FIELD_BYTES: usize = 1024;
const MAX_TOKEN_BYTES: usize = 4608;

type HmacSha256 = Hmac<Sha256>;

/// The durable scan position and normalized request identity carried by a token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinuationPosition {
    pub bucket_id: BucketId,
    pub prefix: Vec<u8>,
    pub delimiter: Option<Vec<u8>>,
    pub last_key: Vec<u8>,
    pub expires_at_unix_seconds: u64,
}

#[derive(Clone)]
pub struct ContinuationTokenSigner {
    key: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ContinuationTokenError {
    #[error("malformed or unauthenticated token")]
    Invalid,
    #[error("expired token")]
    Expired,
    #[error("token does not match the listing request")]
    RequestMismatch,
}

impl ContinuationTokenSigner {
    /// Creates a signer from configured secret bytes.
    ///
    /// # Errors
    ///
    /// Rejects empty keys so deployments cannot accidentally emit public tokens.
    pub fn new(key: Vec<u8>) -> Result<Self, ContinuationTokenError> {
        if key.is_empty() {
            return Err(ContinuationTokenError::Invalid);
        }
        Ok(Self { key })
    }

    /// Signs a bounded opaque token.
    ///
    /// # Errors
    ///
    /// Rejects fields outside the metadata key bounds.
    pub fn encode(&self, position: &ContinuationPosition) -> Result<String, ContinuationTokenError> {
        validate(position)?;
        let mut payload = Vec::with_capacity(64 + position.prefix.len() + position.last_key.len());
        payload.push(VERSION);
        payload.extend_from_slice(position.bucket_id.as_bytes());
        payload.extend_from_slice(&position.expires_at_unix_seconds.to_be_bytes());
        push_field(&mut payload, &position.prefix)?;
        match &position.delimiter {
            Some(delimiter) => {
                payload.push(1);
                push_field(&mut payload, delimiter)?;
            }
            None => payload.push(0),
        }
        push_field(&mut payload, &position.last_key)?;
        let mut mac = HmacSha256::new_from_slice(&self.key).map_err(|_| ContinuationTokenError::Invalid)?;
        mac.update(&payload);
        payload.extend_from_slice(&mac.finalize().into_bytes());
        Ok(URL_SAFE_NO_PAD.encode(payload))
    }

    /// Verifies and decodes a token before any metadata scan.
    ///
    /// # Errors
    ///
    /// Rejects malformed, expired, wrong-bucket, or parameter-mismatched tokens.
    pub fn decode_for_request(
        &self,
        token: &str,
        bucket_id: BucketId,
        prefix: &[u8],
        delimiter: Option<&[u8]>,
        now_unix_seconds: u64,
    ) -> Result<ContinuationPosition, ContinuationTokenError> {
        if token.len() > MAX_TOKEN_BYTES * 2 {
            return Err(ContinuationTokenError::Invalid);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| ContinuationTokenError::Invalid)?;
        if bytes.len() > MAX_TOKEN_BYTES || bytes.len() <= TAG_BYTES {
            return Err(ContinuationTokenError::Invalid);
        }
        let (payload, tag) = bytes.split_at(bytes.len() - TAG_BYTES);
        let mut mac = HmacSha256::new_from_slice(&self.key).map_err(|_| ContinuationTokenError::Invalid)?;
        mac.update(payload);
        mac.verify_slice(tag)
            .map_err(|_| ContinuationTokenError::Invalid)?;
        let position = decode_payload(payload)?;
        if position.expires_at_unix_seconds < now_unix_seconds {
            return Err(ContinuationTokenError::Expired);
        }
        if position.bucket_id != bucket_id
            || position.prefix != prefix
            || position.delimiter.as_deref() != delimiter
        {
            return Err(ContinuationTokenError::RequestMismatch);
        }
        Ok(position)
    }
}

fn decode_payload(payload: &[u8]) -> Result<ContinuationPosition, ContinuationTokenError> {
    let mut cursor = Cursor::new(payload);
    if cursor.byte()? != VERSION {
        return Err(ContinuationTokenError::Invalid);
    }
    let bucket_id = BucketId::new(cursor.array()?);
    let expires_at_unix_seconds = u64::from_be_bytes(cursor.array()?);
    let prefix = cursor.field()?;
    let delimiter = match cursor.byte()? {
        0 => None,
        1 => Some(cursor.field()?),
        _ => return Err(ContinuationTokenError::Invalid),
    };
    let last_key = cursor.field()?;
    if !cursor.remaining().is_empty() {
        return Err(ContinuationTokenError::Invalid);
    }
    let position = ContinuationPosition {
        bucket_id,
        prefix,
        delimiter,
        last_key,
        expires_at_unix_seconds,
    };
    validate(&position)?;
    Ok(position)
}

fn validate(position: &ContinuationPosition) -> Result<(), ContinuationTokenError> {
    if position.prefix.len() > MAX_FIELD_BYTES
        || position.last_key.is_empty()
        || position.last_key.len() > MAX_FIELD_BYTES
        || position
            .delimiter
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > MAX_FIELD_BYTES)
    {
        return Err(ContinuationTokenError::Invalid);
    }
    Ok(())
}

fn push_field(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ContinuationTokenError> {
    let length = u16::try_from(value.len()).map_err(|_| ContinuationTokenError::Invalid)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    fn byte(&mut self) -> Result<u8, ContinuationTokenError> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ContinuationTokenError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ContinuationTokenError::Invalid)
    }

    fn field(&mut self) -> Result<Vec<u8>, ContinuationTokenError> {
        let length = u16::from_be_bytes(self.array()?) as usize;
        if length > MAX_FIELD_BYTES {
            return Err(ContinuationTokenError::Invalid);
        }
        Ok(self.take(length)?.to_vec())
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ContinuationTokenError> {
        if self.remaining.len() < length {
            return Err(ContinuationTokenError::Invalid);
        }
        let (head, tail) = self.remaining.split_at(length);
        self.remaining = tail;
        Ok(head)
    }

    const fn remaining(&self) -> &'a [u8] {
        self.remaining
    }
}
