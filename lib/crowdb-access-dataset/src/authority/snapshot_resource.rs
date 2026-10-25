// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{AuthorityError, VersionedRecord};
use crate::{SnapshotPublication, SnapshotRecord};
use bincode::Options;
use serde::{Deserialize, Serialize};

pub(super) const CANDIDATE_LIFETIME_SECONDS: u64 = 86400;

/// Internal resource metadata; the public Snapshot representation stays unchanged.
pub(crate) struct SnapshotResource {
    pub record: SnapshotRecord,
    pub gc_after: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct TimedSnapshot {
    publication: SnapshotPublication,
    gc_after: u64,
}

#[derive(Serialize)]
struct BorrowedSnapshot<'a> {
    publication: &'a SnapshotPublication,
    gc_after: u64,
}

#[derive(Serialize)]
struct BorrowedLegacySnapshot<'a> {
    publication: &'a SnapshotPublication,
}

impl SnapshotResource {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, AuthorityError> {
        let version = bytes.get(..2).ok_or(AuthorityError::Corrupt)?;
        match u16::from_le_bytes([version[0], version[1]]) {
            1 => {
                let stored: VersionedRecord<SnapshotRecord> = decode(bytes)?;
                Ok(Self {
                    record: stored.payload,
                    gc_after: None,
                })
            }
            2 => {
                let stored: VersionedRecord<TimedSnapshot> = decode(bytes)?;
                if stored.payload.gc_after == 0 {
                    return Err(AuthorityError::Corrupt);
                }
                Ok(Self {
                    record: SnapshotRecord {
                        publication: stored.payload.publication,
                    },
                    gc_after: Some(stored.payload.gc_after),
                })
            }
            _ => Err(AuthorityError::Corrupt),
        }
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, AuthorityError> {
        encode_snapshot(&self.record.publication, self.gc_after)
    }
}

pub(crate) fn decode_snapshot(bytes: &[u8]) -> Result<SnapshotRecord, AuthorityError> {
    Ok(SnapshotResource::decode(bytes)?.record)
}

pub(super) fn encode_snapshot(
    publication: &SnapshotPublication,
    gc_after: Option<u64>,
) -> Result<Vec<u8>, AuthorityError> {
    match gc_after {
        Some(gc_after) if gc_after != 0 => bincode::serialize(&VersionedRecord {
            version: 2,
            payload: BorrowedSnapshot {
                publication,
                gc_after,
            },
        })
        .map_err(|_| AuthorityError::Corrupt),
        Some(_) => Err(AuthorityError::Corrupt),
        None => super::encode(&BorrowedLegacySnapshot { publication }),
    }
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, AuthorityError> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(bytes.len() as u64)
        .reject_trailing_bytes()
        .deserialize(bytes)
        .map_err(|_| AuthorityError::Corrupt)
}
