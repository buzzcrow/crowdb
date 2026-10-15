// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode, AuthorityError, VersionedRecord};
use crate::{key, CommitGeneration, DatasetAuthority, DatasetIdentity, HeadRecord, OperationId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct HeadEnvelope {
    head: HeadRecord,
    fence: OperationId,
}

/// One consistent observation of mutable names and the exact head generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetHead {
    pub latest: Option<crate::SnapshotId>,
    pub stable: Option<crate::SnapshotId>,
    pub generation: Option<CommitGeneration>,
}

impl DatasetAuthority {
    /// # Errors
    /// Rejects missing Datasets and malformed or unavailable head records.
    pub async fn inspect_head(&self, identity: &DatasetIdentity) -> Result<DatasetHead, AuthorityError> {
        self.get_dataset(identity).await?;
        let Some(stored) = self.store.get(&key::head(identity)?).await? else {
            return Ok(DatasetHead {
                latest: None,
                stable: None,
                generation: None,
            });
        };
        let head = decode_head(&stored.bytes)?;
        Ok(DatasetHead {
            latest: head.latest,
            stable: head.stable,
            generation: Some(CommitGeneration::from_head(&stored.bytes)),
        })
    }
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
