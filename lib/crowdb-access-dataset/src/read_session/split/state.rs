// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::SessionSpecV2;
use crate::{
    split::SplitSource, AuthorityError, CasOutcome, DatasetIdentity, DatasetSessions, MetadataScanBudget,
    OperationId, ReadCancellation, SnapshotId, StoredValue,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BoundSplitSpec {
    pub version: u16,
    pub spec: SessionSpecV2,
    pub snapshot: SnapshotId,
    pub split: Option<SplitSource>,
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparationProgress {
    pub next_page: u64,
    pub after: Option<Vec<u8>>,
    pub admitted: u64,
    pub bytes: u64,
    pub binding: [u8; 32],
    pub complete: bool,
    pub cancelled: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IdPage {
    pub ids: Vec<Vec<u8>>,
    pub after: Option<Vec<u8>>,
}

pub(super) fn key(
    identity: &DatasetIdentity,
    id: OperationId,
    suffix: &[u8],
) -> Result<Vec<u8>, AuthorityError> {
    let mut key = super::super::session_key(identity, id)?;
    key.extend_from_slice(suffix);
    Ok(key)
}
pub(super) fn page_key(
    identity: &DatasetIdentity,
    id: OperationId,
    page: u64,
) -> Result<Vec<u8>, AuthorityError> {
    let mut key = key(identity, id, b"\0SELECT2")?;
    key.extend_from_slice(&page.to_be_bytes());
    Ok(key)
}
pub(super) fn budget() -> MetadataScanBudget {
    MetadataScanBudget {
        items: 20,
        bytes: 1024 * 1024,
        timeout: Duration::from_secs(2),
    }
}
pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, AuthorityError> {
    crate::split::codec::encode(value).map_err(|_| crate::DatasetError::ReadLimitExceeded.into())
}
pub(super) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, AuthorityError> {
    crate::split::codec::decode(bytes).map_err(|_| AuthorityError::Corrupt)
}
impl DatasetSessions {
    pub(in crate::read_session) async fn reserve_session_version(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
        version: u8,
    ) -> Result<(), AuthorityError> {
        let key = key(identity, id, b"\0VERSION")?;
        if version == 2
            && self
                .authority
                .store
                .get_bounded(&key, budget(), &ReadCancellation::new())
                .await?
                .is_none()
        {
            let old_key = self::key(identity, id, b"\0SPEC")?;
            if self
                .authority
                .store
                .get_bounded(&old_key, budget(), &ReadCancellation::new())
                .await?
                .is_some()
            {
                return Err(AuthorityError::CursorConflict);
            }
        }
        self.immutable_session_metadata(&key, &[version]).await
    }
    pub(super) async fn immutable_session_metadata(
        &self,
        key: &[u8],
        bytes: &[u8],
    ) -> Result<(), AuthorityError> {
        match self.authority.store.compare_exchange(key, None, bytes).await? {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::CursorConflict),
        }
    }
    pub(super) async fn split_preparation_progress(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
        cancel: &ReadCancellation,
    ) -> Result<(Option<StoredValue>, PreparationProgress), AuthorityError> {
        let stored = self
            .authority
            .store
            .get_bounded(&key(identity, id, b"\0PLAN2")?, budget(), cancel)
            .await?;
        let progress: PreparationProgress = stored
            .as_ref()
            .map(|s| decode(&s.bytes))
            .transpose()?
            .unwrap_or_default();
        if progress.admitted > 100_000
            || progress.bytes > 8 * 1024 * 1024
            || (progress.complete && progress.after.is_some())
        {
            return Err(AuthorityError::Corrupt);
        }
        Ok((stored, progress))
    }
    pub(super) async fn replace_split_preparation(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
        old: Option<&StoredValue>,
        next: &PreparationProgress,
    ) -> Result<(), AuthorityError> {
        let bytes = encode(next)?;
        match self
            .authority
            .store
            .compare_exchange(
                &key(identity, id, b"\0PLAN2")?,
                old.map(|s| s.bytes.as_slice()),
                &bytes,
            )
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::CursorConflict),
        }
    }
}
