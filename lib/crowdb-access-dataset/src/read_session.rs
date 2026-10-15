// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable scoped reads. Fetch records an issued cursor; only acknowledgement advances it.

mod pages;
mod planning;
mod progress;
mod split;
pub use split::{ReadPreparation, SessionSpecV2, SessionSplit};

use crate::{
    AuthorityError, CasOutcome, DatasetAuthority, DatasetError, DatasetIdentity, OperationId, Ordering,
    ReadPlan, Selection, SnapshotId,
};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const TTL: u64 = 60;
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionPartition {
    pub worker: u16,
    pub workers: u16,
    pub rank: u16,
    pub world_size: u16,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionShuffle {
    pub seed: u64,
    pub group_size: usize,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionSpec {
    pub id: OperationId,
    pub snapshot: String,
    pub selection: Selection,
    pub fields: Vec<String>,
    pub batch_size: usize,
    pub partition: SessionPartition,
    pub shuffle: Option<SessionShuffle>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadSession {
    pub id: String,
    pub snapshot: String,
    pub plan: String,
    pub acknowledged: String,
    pub expires_at: u64,
    pub closed: bool,
}
#[derive(Clone, Serialize, Deserialize)]
struct SessionRecord {
    version: u16,
    id: OperationId,
    fields: Vec<String>,
    batch_size: usize,
    count: usize,
    snapshot: SnapshotId,
    plan: [u8; 16],
    cursor: String,
    position: usize,
    issued: Option<(String, usize)>,
    deadline: u64,
    closed: bool,
}
impl SessionRecord {
    fn public(&self) -> ReadSession {
        ReadSession {
            id: self.id.to_string(),
            snapshot: self.snapshot.to_string(),
            plan: hex_digest(self.plan),
            acknowledged: self.cursor.clone(),
            expires_at: self.deadline,
            closed: self.closed,
        }
    }
}
fn hex_digest(value: [u8; 16]) -> String {
    use std::fmt::Write;
    value.iter().fold(String::with_capacity(32), |mut out, b| {
        write!(&mut out, "{b:02x}").expect("string writing cannot fail");
        out
    })
}
fn session_key(identity: &DatasetIdentity, id: OperationId) -> Result<Vec<u8>, AuthorityError> {
    let mut key = b"DS\0RS\0".to_vec();
    key.extend(identity.key()?);
    key.extend(id.as_bytes());
    Ok(key)
}
fn encode(record: &SessionRecord) -> Result<Vec<u8>, AuthorityError> {
    let bytes = bincode::serialize(record).map_err(|_| AuthorityError::Corrupt)?;
    if bytes.len() > 8 * 1024 * 1024 {
        return Err(DatasetError::ReadLimitExceeded.into());
    }
    Ok(bytes)
}
fn decode(bytes: &[u8]) -> Result<SessionRecord, AuthorityError> {
    let record: SessionRecord = bincode::deserialize(bytes).map_err(|_| AuthorityError::Corrupt)?;
    if record.version != 2 {
        return Err(AuthorityError::Corrupt);
    }
    Ok(record)
}

pub struct SessionBatch {
    pub snapshot: SnapshotId,
    pub ids: Vec<Vec<u8>>,
    pub fields: Vec<String>,
    pub next: String,
    pub end: bool,
}
pub struct DatasetSessions {
    authority: Arc<DatasetAuthority>,
}
impl DatasetSessions {
    #[must_use]
    pub fn new(authority: Arc<DatasetAuthority>) -> Self {
        Self { authority }
    }
    async fn record(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
    ) -> Result<(Vec<u8>, SessionRecord), AuthorityError> {
        let stored = self
            .authority
            .store
            .get(&session_key(identity, id)?)
            .await?
            .ok_or(AuthorityError::NotFound)?;
        let record = decode(&stored.bytes)?;
        Ok((stored.bytes, record))
    }
}
