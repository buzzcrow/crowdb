// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode, AuthorityError, VersionedRecord};
use crate::{HeadRecord, OperationId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct HeadEnvelope {
    head: HeadRecord,
    fence: OperationId,
}
pub(super) fn decode_head(bytes: &[u8]) -> Result<HeadRecord, AuthorityError> {
    let version = bytes.get(..2).ok_or(AuthorityError::Corrupt)?;
    if version == 1u16.to_le_bytes() {
        return decode(bytes);
    }
    let record: VersionedRecord<HeadEnvelope> =
        bincode::deserialize(bytes).map_err(|_| AuthorityError::Corrupt)?;
    if record.version != 2 {
        return Err(AuthorityError::Corrupt);
    }
    Ok(record.payload.head)
}
pub(super) fn encode_head(head: HeadRecord) -> Result<Vec<u8>, AuthorityError> {
    bincode::serialize(&VersionedRecord {
        version: 2,
        payload: HeadEnvelope {
            head,
            fence: OperationId::random(),
        },
    })
    .map_err(|_| AuthorityError::Corrupt)
}
