// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Stateless immutable reads. Continuations are caller-owned, never metadata records.

mod binding;
mod page;
use crate::{AuthorityError, DatasetAuthority, DatasetError, DatasetIdentity, Selection, SnapshotId};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadPartition {
    pub worker: u16,
    pub workers: u16,
    pub rank: u16,
    pub world_size: u16,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadShuffle {
    pub seed: u64,
    pub group_size: usize,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadSplit {
    pub revision: Option<crate::SplitRevisionId>,
    pub name: crate::split::SplitName,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadSpec {
    pub snapshot: String,
    pub selection: Selection,
    pub fields: Vec<String>,
    pub batch_size: usize,
    pub partition: ReadPartition,
    pub shuffle: Option<ReadShuffle>,
    pub split: Option<ReadSplit>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBinding {
    pub snapshot: SnapshotId,
    pub split: Option<crate::split::SplitSource>,
    pub cursor: String,
}
pub struct ReadPage {
    pub snapshot: SnapshotId,
    pub ids: Vec<Vec<u8>>,
    pub fields: Vec<String>,
    pub next: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Continuation {
    version: u8,
    identity: Vec<u8>,
    spec: ReadSpec,
    snapshot: SnapshotId,
    split: Option<crate::split::SplitSource>,
    after: Option<String>,
    ordinal: u64,
    offset: usize,
}
const MAX_TOKEN_BYTES: usize = 256 * 1024;
impl Continuation {
    fn encode(&self) -> Result<String, AuthorityError> {
        let bytes = serde_json::to_vec(self).map_err(|_| AuthorityError::Corrupt)?;
        if bytes.len() > MAX_TOKEN_BYTES {
            return Err(DatasetError::ReadLimitExceeded.into());
        }
        Ok(URL_SAFE_NO_PAD.encode(bytes))
    }
    fn decode(identity: &DatasetIdentity, token: &str) -> Result<Self, AuthorityError> {
        if token.len() > MAX_TOKEN_BYTES * 4 / 3 + 4 {
            return Err(DatasetError::ReadLimitExceeded.into());
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| DatasetError::CursorMismatch)?;
        let value: Self = serde_json::from_slice(&bytes).map_err(|_| DatasetError::CursorMismatch)?;
        if value.version != 1 || value.identity != identity.key()? {
            return Err(DatasetError::CursorMismatch.into());
        }
        validate_delivery(&value.spec)?;
        if value.spec.snapshot != value.snapshot.to_string()
            || value.offset > 100_000
            || match (&value.spec.split, &value.split) {
                (None, None) => false,
                (Some(request), Some(source)) => {
                    source.snapshot != value.snapshot
                        || request.revision != Some(source.revision)
                        || request.name != source.name
                }
                _ => true,
            }
        {
            return Err(DatasetError::CursorMismatch.into());
        }
        Ok(value)
    }
}
pub struct DatasetReads {
    authority: Arc<DatasetAuthority>,
}
impl DatasetReads {
    #[must_use]
    pub fn new(authority: Arc<DatasetAuthority>) -> Self {
        Self { authority }
    }
}
pub(super) fn validate_delivery(spec: &ReadSpec) -> Result<(), AuthorityError> {
    let p = &spec.partition;
    if p.workers == 0
        || p.world_size == 0
        || p.worker >= p.workers
        || p.rank >= p.world_size
        || spec.fields.len() > 256
    {
        return Err(DatasetError::InvalidManifest.into());
    }
    let mut names = std::collections::BTreeSet::new();
    if spec.batch_size == 0
        || spec.batch_size > crate::MAX_BATCH_SIZE
        || spec
            .fields
            .iter()
            .any(|name| name.is_empty() || !names.insert(name))
        || spec
            .shuffle
            .as_ref()
            .is_some_and(|shuffle| shuffle.group_size == 0 || shuffle.group_size > 100_000)
    {
        return Err(DatasetError::InvalidManifest.into());
    }
    Ok(())
}
