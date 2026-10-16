// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::InspectionError;
use crate::{AuthorityError, DatasetError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u16,
    query: [u8; 32],
    key: Vec<u8>,
}
pub(crate) struct CursorScope([u8; 32]);
impl CursorScope {
    pub(crate) fn new(prefix: &[u8], query: &[u8]) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"crowdb-inspection-cursor-v1\0");
        digest.update((prefix.len() as u64).to_be_bytes());
        digest.update(prefix);
        digest.update(query);
        Self(digest.finalize().into())
    }
    pub(crate) fn decode(&self, after: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, InspectionError> {
        let Some(bytes) = after else {
            return Ok(None);
        };
        if bytes.len() > 256 * 1024 {
            return Err(mismatch());
        }
        let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| mismatch())?;
        if cursor.version != 1 || cursor.query != self.0 || cursor.key.len() >= 64 * 1024 {
            return Err(mismatch());
        }
        Ok(Some(cursor.key))
    }
    pub(crate) fn encode(&self, key: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, InspectionError> {
        key.map(|key| {
            serde_json::to_vec(&Cursor {
                version: 1,
                query: self.0,
                key,
            })
            .map_err(|_| InspectionError::Corrupt)
        })
        .transpose()
    }
}
fn mismatch() -> InspectionError {
    AuthorityError::Invalid(DatasetError::CursorMismatch).into()
}
