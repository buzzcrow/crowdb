// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resource formats distinguish independent parts from strict historical quotas.

use super::{MultipartRecordError, MultipartSessionRecord};

const STRICT_SESSION_MAGIC: [u8; 5] = *b"S3MS\x04";

impl MultipartSessionRecord {
    pub(crate) fn uses_independent_parts(&self) -> bool {
        self.legacy_observation.is_none()
            && u64::from(self.max_parts)
                .checked_mul(self.max_part_bytes)
                .is_some_and(|bound| bound <= self.max_staged_bytes)
    }

    pub(super) fn encode_session(&self) -> Result<Vec<u8>, MultipartRecordError> {
        let magic = if self.uses_independent_parts() {
            if self.pending.is_some() {
                return Err(MultipartRecordError::Invalid);
            }
            super::SESSION_MAGIC
        } else {
            STRICT_SESSION_MAGIC
        };
        super::encode(magic, self)
    }
}

pub(super) fn decode_session(bytes: &[u8]) -> Result<MultipartSessionRecord, MultipartRecordError> {
    if bytes.starts_with(b"S3MS\x03") {
        return super::legacy::decode_session(bytes);
    }
    if bytes.starts_with(&STRICT_SESSION_MAGIC) {
        let mut record: MultipartSessionRecord = super::decode(STRICT_SESSION_MAGIC, bytes)?;
        record.legacy_observation = Some((record.revision, bytes.to_vec()));
        return Ok(record);
    }
    let record: MultipartSessionRecord = super::decode(super::SESSION_MAGIC, bytes)?;
    if !record.uses_independent_parts() || record.pending.is_some() {
        return Err(MultipartRecordError::Invalid);
    }
    Ok(record)
}
