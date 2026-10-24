// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{head::decode_head, VersionedRecord};
use crate::inspection::{work::InspectionWork, InspectionError};
use crate::{
    key, AuthorityError, CasOutcome, DatasetAuthority, DatasetError, DatasetIdentity, HeadRecord,
    MetadataScanBudget, PublicationState, ReadCancellation, SnapshotId, SnapshotRecord, StoredValue,
};
use bincode::Options;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
pub(super) struct LegacyAnchor {
    version: u16,
    pub head: HeadRecord,
}
#[derive(Serialize, Deserialize)]
struct LegacyVisibility {
    version: u16,
    snapshot: SnapshotId,
}

impl DatasetAuthority {
    pub(in crate::authority) async fn capture_legacy_head(
        &self,
        identity: &DatasetIdentity,
        stored: &StoredValue,
    ) -> Result<(), AuthorityError> {
        let head = decode_head(&stored.bytes)?;
        let mut legacy = false;
        for snapshot in [head.latest, head.stable].into_iter().flatten() {
            legacy |= self.snapshot_candidate(identity, snapshot).await?.is_none();
        }
        if !legacy {
            return Ok(());
        }
        let key = anchor_key(identity)?;
        let bytes =
            serde_json::to_vec(&LegacyAnchor { version: 1, head }).map_err(|_| AuthorityError::Corrupt)?;
        match self.store.compare_exchange(&key, None, &bytes).await? {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) => {
                decode_anchor(&value.bytes)?;
                Ok(())
            }
            CasOutcome::Conflict(None) => Err(AuthorityError::Corrupt),
        }
    }

    pub(in crate::authority) async fn legacy_commit_exists(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<bool, AuthorityError> {
        let Some(stored) = self.store.get(&visibility_key(identity, snapshot)?).await? else {
            return Ok(false);
        };
        validate_visibility(&stored.bytes, snapshot)?;
        Ok(true)
    }

    /// Checks a live complete result, resolving bounded legacy publication evidence when needed.
    /// # Errors
    /// Returns unavailable or retired snapshots, corrupt proof or storage limits.
    pub async fn require_committed_snapshot(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        self.ensure_snapshot_live(identity, snapshot).await?;
        let candidate = self.snapshot_candidate(identity, snapshot).await?;
        if candidate.as_ref().is_some_and(|c| c.version == 2) {
            return Ok(());
        }
        if candidate.as_ref().is_some_and(|c| c.version == 3) {
            return Err(AuthorityError::SnapshotNotFound);
        }
        if self.snapshot_commit(identity, snapshot).await?.is_some()
            || self.legacy_commit_exists(identity, snapshot).await?
        {
            return Ok(());
        }
        if let Some(candidate) = candidate {
            if self.legacy_candidate_ready(identity, &candidate).await? {
                return Ok(());
            }
            return Err(AuthorityError::SnapshotNotFound);
        }
        let cancel = ReadCancellation::new();
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            &cancel,
            MetadataScanBudget {
                items: 64,
                bytes: 16 * 1024 * 1024,
                timeout: Duration::from_secs(2),
            },
        )
        .map_err(legacy_read_error)?;
        let head = work
            .get(&key::head(identity)?, 8192)
            .await
            .map_err(legacy_read_error)?;
        if self
            .legacy_snapshot_visible(identity, snapshot, head.as_ref(), &mut work)
            .await?
        {
            return Ok(());
        }
        Err(AuthorityError::SnapshotNotFound)
    }

    pub(super) async fn legacy_snapshot_visible(
        &self,
        identity: &DatasetIdentity,
        target: SnapshotId,
        head: Option<&StoredValue>,
        work: &mut InspectionWork<'_>,
    ) -> Result<bool, AuthorityError> {
        let mut roots = Vec::new();
        if let Some(head) = head {
            let head = decode_head(&head.bytes)?;
            roots.extend([head.latest, head.stable].into_iter().flatten());
        }
        if let Some(anchor) = work
            .get(&anchor_key(identity)?, 2048)
            .await
            .map_err(legacy_read_error)?
        {
            let anchor = decode_anchor(&anchor.bytes)?;
            roots.extend([anchor.head.latest, anchor.head.stable].into_iter().flatten());
        }
        let mut seen = std::collections::HashSet::new();
        for root in roots {
            let mut current = Some(root);
            let mut chain = std::collections::HashSet::new();
            while let Some(snapshot) = current {
                if !chain.insert(snapshot) {
                    return Err(AuthorityError::AncestryCycle);
                }
                if !seen.insert(snapshot) {
                    break;
                }
                if seen.len() > 64 {
                    return Err(DatasetError::ReadLimitExceeded.into());
                }
                let stored = work
                    .get(&key::snapshot(identity, snapshot)?, 16 * 1024 * 1024)
                    .await
                    .map_err(legacy_read_error)?
                    .ok_or(AuthorityError::Corrupt)?;
                let record: VersionedRecord<SnapshotRecord> = bincode::DefaultOptions::new()
                    .with_fixint_encoding()
                    .with_limit(stored.bytes.len() as u64)
                    .reject_trailing_bytes()
                    .deserialize(&stored.bytes)
                    .map_err(|_| AuthorityError::Corrupt)?;
                if record.version != 1 {
                    return Err(AuthorityError::Corrupt);
                }
                let record = record.payload;
                if record.publication.snapshot != snapshot
                    || record.publication.state == PublicationState::Prepared
                {
                    return Err(AuthorityError::Corrupt);
                }
                if snapshot == target {
                    return Ok(record.publication.state == PublicationState::Published);
                }
                current = record.publication.parent;
            }
        }
        Ok(false)
    }
}
pub(super) fn decode_anchor(bytes: &[u8]) -> Result<LegacyAnchor, AuthorityError> {
    if bytes.len() > 1024 {
        return Err(AuthorityError::Corrupt);
    }
    let anchor: LegacyAnchor = serde_json::from_slice(bytes).map_err(|_| AuthorityError::Corrupt)?;
    if anchor.version != 1 {
        return Err(AuthorityError::Corrupt);
    }
    Ok(anchor)
}
pub(super) fn anchor_key(identity: &DatasetIdentity) -> Result<Vec<u8>, DatasetError> {
    let mut key = b"DS\0LA\0".to_vec();
    key.extend_from_slice(&identity.key()?);
    Ok(key)
}
pub(crate) fn visibility_key(
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
) -> Result<Vec<u8>, DatasetError> {
    let mut key = b"DS\0LC\0".to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(snapshot.as_bytes());
    Ok(key)
}

pub(super) fn validate_visibility(bytes: &[u8], snapshot: SnapshotId) -> Result<(), AuthorityError> {
    if bytes.len() > 1024 {
        return Err(AuthorityError::Corrupt);
    }
    let record: LegacyVisibility = serde_json::from_slice(bytes).map_err(|_| AuthorityError::Corrupt)?;
    if record.version != 1 || record.snapshot != snapshot {
        return Err(AuthorityError::Corrupt);
    }
    Ok(())
}

pub(super) fn legacy_read_error(error: InspectionError) -> AuthorityError {
    match error {
        InspectionError::Authority(error) => error,
        InspectionError::Store(error) => error.into(),
        InspectionError::Limit => DatasetError::ReadLimitExceeded.into(),
        InspectionError::Cancelled => DatasetError::ReadCancelled.into(),
        _ => AuthorityError::Corrupt,
    }
}
