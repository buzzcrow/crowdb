// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode, AuthorityError, VersionedRecord};
use crate::{
    key, CommitAttempt, CommitGeneration, DatasetAuthority, DatasetIdentity, HeadRecord, OperationId,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct HeadEnvelope {
    head: HeadRecord,
    fence: OperationId,
}

#[derive(Serialize, Deserialize)]
struct CommitHeadEnvelope {
    head: HeadRecord,
    fence: OperationId,
    attempt: CommitAttempt,
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
    if version == 3u16.to_le_bytes() {
        let record: VersionedRecord<CommitHeadEnvelope> =
            bincode::deserialize(bytes).map_err(|_| AuthorityError::Corrupt)?;
        validate_commit_head(&record.payload)?;
        return Ok(record.payload.head);
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

pub(super) fn head_attempt(bytes: &[u8]) -> Result<Option<CommitAttempt>, AuthorityError> {
    if bytes.get(..2) != Some(3u16.to_le_bytes().as_slice()) {
        decode_head(bytes)?;
        return Ok(None);
    }
    let record: VersionedRecord<CommitHeadEnvelope> =
        bincode::deserialize(bytes).map_err(|_| AuthorityError::Corrupt)?;
    validate_commit_head(&record.payload)?;
    Ok(Some(record.payload.attempt))
}
pub(super) fn encode_commit_head(
    head: HeadRecord,
    attempt: CommitAttempt,
) -> Result<Vec<u8>, AuthorityError> {
    attempt.validate()?;
    bincode::serialize(&VersionedRecord {
        version: 3,
        payload: CommitHeadEnvelope {
            head,
            fence: OperationId::random(),
            attempt,
        },
    })
    .map_err(|_| AuthorityError::Corrupt)
}

fn validate_commit_head(head: &CommitHeadEnvelope) -> Result<(), AuthorityError> {
    head.attempt.validate()?;
    match head.attempt.target {
        crate::CommitTarget::Snapshot { snapshot } if head.head.latest == Some(snapshot) => Ok(()),
        _ => Err(AuthorityError::Corrupt),
    }
}
