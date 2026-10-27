// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{AuthorityError, DatasetId, DatasetRecord};
use bincode::Options;
use serde::{Deserialize, Serialize};

pub(crate) const RETIRED_RESOURCE: &[u8] = b"retired-candidate-operation-v1";
pub(super) const MAX_RESERVATIONS: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetDeletion {
    pub id: DatasetId,
    pub reclaim_after: u64,
    pub complete: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Reservation {
    pub key: Vec<u8>,
    pub expires_at: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct DatasetEnvelope {
    pub record: DatasetRecord,
    pub(super) pending: Vec<Reservation>,
    pub deletion: Option<DatasetDeletion>,
}

impl DatasetEnvelope {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, AuthorityError> {
        let value = match bytes.get(..2) {
            Some([1, 0]) => Self {
                record: super::super::decode(bytes)?,
                pending: Vec::new(),
                deletion: None,
            },
            Some([2, 0]) => {
                let stored: super::super::VersionedRecord<Self> = bincode::DefaultOptions::new()
                    .with_fixint_encoding()
                    .with_limit(64 * 1024)
                    .reject_trailing_bytes()
                    .deserialize(bytes)
                    .map_err(|_| AuthorityError::Corrupt)?;
                stored.payload
            }
            _ => return Err(AuthorityError::Corrupt),
        };
        value.record.validate()?;
        if value.pending.len() > MAX_RESERVATIONS
            || value
                .deletion
                .as_ref()
                .is_some_and(|d| d.id != value.record.id || !value.pending.is_empty())
        {
            return Err(AuthorityError::Corrupt);
        }
        for pending in &value.pending {
            let prefix = if pending.key.starts_with(crate::key::SNAPSHOT_PREFIX) {
                crate::key::SNAPSHOT_PREFIX
            } else {
                b"DS\0U\0"
            };
            let (identity, _) = crate::key::decode_scoped_operation(&pending.key, prefix)?;
            if identity != value.record.identity {
                return Err(AuthorityError::Corrupt);
            }
        }
        Ok(value)
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, AuthorityError> {
        bincode::serialize(&super::super::VersionedRecord {
            version: 2,
            payload: self,
        })
        .map_err(|_| AuthorityError::Corrupt)
    }

    pub(crate) fn active(self) -> Result<DatasetRecord, AuthorityError> {
        if self.deletion.is_some() {
            return Err(AuthorityError::NotFound);
        }
        Ok(self.record)
    }
}
