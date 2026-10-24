// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    AuthorityError, CasOutcome, DatasetAuthority, DatasetError, DatasetIdentity, MetadataScan,
    MetadataScanBudget, ReadCancellation, ReclaimState, SnapshotId,
};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataCleanupProgress {
    pub version: u16,
    pub snapshot: SnapshotId,
    pub phase: usize,
    pub after: Option<Vec<u8>>,
    pub complete: bool,
    identity: Option<DatasetIdentity>,
}
impl MetadataCleanupProgress {
    #[must_use]
    pub fn new(snapshot: SnapshotId) -> Self {
        Self {
            version: 1,
            snapshot,
            phase: 0,
            after: None,
            complete: false,
            identity: None,
        }
    }
}
impl DatasetAuthority {
    #[cfg(feature = "test-util")]
    /// Advances an isolated metadata fixture without the background owner pipeline.
    /// # Errors
    /// Rejects live owners, malformed progress, cancellation and storage failures.
    pub async fn reclaim_snapshot_metadata_step_for_tests(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        progress: &mut MetadataCleanupProgress,
        cancel: &ReadCancellation,
    ) -> Result<MetadataCleanupProgress, AuthorityError> {
        self.reclaim_snapshot_metadata_step(identity, snapshot, progress, cancel)
            .await
    }

    /// Deletes one bounded window after terminal retirement using caller-owned progress.
    /// Published receipts and tombstones remain so old retries cannot resurrect the owner.
    /// # Errors
    /// Rejects protected owners, invalid progress, cancellation and storage limits.
    pub(crate) async fn reclaim_snapshot_metadata_step(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        progress: &mut MetadataCleanupProgress,
        cancel: &ReadCancellation,
    ) -> Result<MetadataCleanupProgress, AuthorityError> {
        if cancel.is_cancelled() {
            return Err(DatasetError::ReadCancelled.into());
        }
        let record = self.begin_reclaim(identity, snapshot).await?;
        self.require_reclaim_ready(identity, snapshot).await?;
        let scopes = cleanup_scopes(identity, snapshot, record.publication.parent)?;
        let budget = MetadataScanBudget {
            items: 20,
            bytes: 16 * 1024 * 1024,
            timeout: Duration::from_secs(2),
        };
        if progress.identity.as_ref().is_some_and(|bound| bound != identity) {
            return Err(AuthorityError::Corrupt);
        }
        progress.identity = Some(identity.clone());
        if progress.version != 1
            || progress.snapshot != snapshot
            || progress.phase > scopes.len()
            || progress.complete != (progress.phase == scopes.len())
        {
            return Err(AuthorityError::Corrupt);
        }
        if progress.complete {
            self.finish_snapshot_metadata_cleanup(identity, snapshot).await?;
            return Ok(progress.clone());
        }
        let page = self
            .store
            .scan_page(
                &MetadataScan {
                    prefix: scopes[progress.phase].clone(),
                    after: progress.after.clone(),
                    budget,
                },
                cancel,
            )
            .await?;
        let mut inspection_summary = scoped(b"DS\0I\0", identity, snapshot)?;
        inspection_summary.push(b'H');
        for (key, value) in &page.entries {
            if cancel.is_cancelled() {
                return Err(DatasetError::ReadCancelled.into());
            }
            if key.starts_with(b"DS\0SR\0") || key.starts_with(b"DS\0SM\0") {
                if value.bytes == crate::split::RETIRED_RECORD {
                    continue;
                }
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    self.store
                        .compare_exchange(key, Some(&value.bytes), crate::split::RETIRED_RECORD),
                )
                .await
                .map_err(|_| AuthorityError::Invalid(DatasetError::ReadLimitExceeded))??
                {
                    CasOutcome::Applied(_) => {}
                    CasOutcome::Conflict(Some(value)) if value.bytes == crate::split::RETIRED_RECORD => {}
                    CasOutcome::Conflict(_) => return Err(AuthorityError::HeadConflict),
                }
                continue;
            }
            if key == &inspection_summary
                || value.bytes == crate::inspection::RETIRED_RECORD
                || key.starts_with(b"DS\0SRI\0")
                || key.starts_with(b"DS\0II\0")
                || key.starts_with(b"DS\0IC\0")
            {
                continue;
            }
            tokio::time::timeout(Duration::from_secs(2), self.store.delete(key))
                .await
                .map_err(|_| AuthorityError::Invalid(DatasetError::ReadLimitExceeded))??;
        }
        progress.after = page.after;
        if progress.after.is_none() {
            progress.phase += 1;
        }
        progress.complete = progress.phase == scopes.len();
        if progress.complete {
            self.finish_snapshot_metadata_cleanup(identity, snapshot).await?;
        }
        Ok(progress.clone())
    }
    async fn finish_snapshot_metadata_cleanup(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        let key = crate::key::reclaim_progress(identity, snapshot)?;
        let bytes = crate::authority::encode(&crate::ReclaimProgress {
            snapshot,
            state: ReclaimState::Completed,
            chunks_reclaimed: 0,
        })?;
        let old = self.store.get(&key).await?;
        match self
            .store
            .compare_exchange(&key, old.as_ref().map(|v| v.bytes.as_slice()), &bytes)
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::HeadConflict),
        }
    }
}
fn cleanup_scopes(
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    parent: Option<SnapshotId>,
) -> Result<Vec<Vec<u8>>, AuthorityError> {
    let mut scopes = Vec::new();
    for prefix in [
        b"DS\0M\0".as_slice(),
        b"DS\0MH\0",
        b"DS\0I\0",
        b"DS\0SR\0",
        b"DS\0SM\0",
        b"DS\0SRI\0",
        b"DS\0II\0",
    ] {
        scopes.push(scoped(prefix, identity, snapshot)?);
    }
    let mut child = b"DS\0IC\0".to_vec();
    child.extend_from_slice(&identity.key()?);
    child.push(u8::from(parent.is_some()));
    if let Some(parent) = parent {
        child.extend_from_slice(parent.as_bytes());
    }
    child.extend_from_slice(snapshot.as_bytes());
    scopes.push(child);
    Ok(scopes)
}
fn scoped(
    prefix: &[u8],
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
) -> Result<Vec<u8>, AuthorityError> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(snapshot.as_bytes());
    Ok(key)
}
