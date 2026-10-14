// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    session_key, AuthorityError, CasOutcome, DatasetIdentity, DatasetSessions, OperationId, SessionSpec,
    SnapshotId,
};
use serde::{Deserialize, Serialize};
const PAGE_SIZE: usize = 1024;
#[derive(Serialize, Deserialize)]
pub(super) struct BoundSpec {
    version: u16,
    pub spec: SessionSpec,
    pub snapshot: SnapshotId,
}
fn key(identity: &DatasetIdentity, id: OperationId, suffix: &[u8]) -> Result<Vec<u8>, AuthorityError> {
    let mut key = session_key(identity, id)?;
    key.extend(suffix);
    Ok(key)
}
fn page_key(identity: &DatasetIdentity, id: OperationId, index: usize) -> Result<Vec<u8>, AuthorityError> {
    let mut suffix = Vec::from(b"\0P".as_slice());
    suffix.extend(
        u64::try_from(index)
            .map_err(|_| AuthorityError::Corrupt)?
            .to_be_bytes(),
    );
    key(identity, id, &suffix)
}
fn decode(bytes: &[u8]) -> Result<BoundSpec, AuthorityError> {
    let record: BoundSpec = bincode::deserialize(bytes).map_err(|_| AuthorityError::Corrupt)?;
    if record.version != 1 {
        return Err(AuthorityError::Corrupt);
    }
    Ok(record)
}
impl DatasetSessions {
    pub(super) async fn read_spec(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
    ) -> Result<BoundSpec, AuthorityError> {
        let record = self
            .authority
            .store
            .get(&key(identity, id, b"\0SPEC")?)
            .await?
            .ok_or(AuthorityError::NotFound)?;
        decode(&record.bytes)
    }
    pub(super) async fn bind_spec(
        &self,
        identity: &DatasetIdentity,
        spec: &SessionSpec,
        snapshot: SnapshotId,
    ) -> Result<BoundSpec, AuthorityError> {
        let bound = BoundSpec {
            version: 1,
            spec: spec.clone(),
            snapshot,
        };
        let bytes = bincode::serialize(&bound).map_err(|_| AuthorityError::Corrupt)?;
        if bytes.len() > 8 * 1024 * 1024 {
            return Err(crate::DatasetError::ReadLimitExceeded.into());
        }
        match self
            .authority
            .store
            .compare_exchange(&key(identity, spec.id, b"\0SPEC")?, None, &bytes)
            .await?
        {
            CasOutcome::Applied(_) => Ok(bound),
            CasOutcome::Conflict(Some(old)) => {
                let old = decode(&old.bytes)?;
                if old.spec != *spec {
                    return Err(AuthorityError::CursorConflict);
                }
                Ok(old)
            }
            CasOutcome::Conflict(None) => Err(AuthorityError::Corrupt),
        }
    }
    pub(super) async fn save_ids(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
        ids: &[Vec<u8>],
    ) -> Result<(), AuthorityError> {
        for (index, page) in ids.chunks(PAGE_SIZE).enumerate() {
            let bytes = bincode::serialize(page).map_err(|_| AuthorityError::Corrupt)?;
            match self
                .authority
                .store
                .compare_exchange(&page_key(identity, id, index)?, None, &bytes)
                .await?
            {
                CasOutcome::Applied(_) => {}
                CasOutcome::Conflict(Some(old)) if old.bytes == bytes => {}
                CasOutcome::Conflict(_) => return Err(AuthorityError::CursorConflict),
            }
        }
        Ok(())
    }
    pub(super) async fn load_ids(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
        start: usize,
        end: usize,
    ) -> Result<Vec<Vec<u8>>, AuthorityError> {
        let mut result = Vec::with_capacity(end.saturating_sub(start));
        for index in start / PAGE_SIZE..end.div_ceil(PAGE_SIZE) {
            let value = self
                .authority
                .store
                .get(&page_key(identity, id, index)?)
                .await?
                .ok_or(AuthorityError::Corrupt)?;
            let page: Vec<Vec<u8>> =
                bincode::deserialize(&value.bytes).map_err(|_| AuthorityError::Corrupt)?;
            let low = start.saturating_sub(index * PAGE_SIZE);
            let high = (end - index * PAGE_SIZE).min(page.len());
            result.extend_from_slice(page.get(low..high).ok_or(AuthorityError::Corrupt)?);
        }
        if result.len() != end - start {
            return Err(AuthorityError::Corrupt);
        }
        Ok(result)
    }
}
