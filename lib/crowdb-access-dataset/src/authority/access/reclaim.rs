// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::DatasetAuthority;
use crate::authority::{decode, decode_head, encode_head};
use crate::{key, AuthorityError, CasOutcome, DatasetIdentity, HeadRecord, OperationId, SnapshotId};

pub(crate) enum CandidateRetirement {
    Retired,
    Complete,
    LegacyCommitted(crate::SuccessfulCommit),
}
impl CandidateRetirement {
    pub(crate) fn retained(&self) -> bool {
        !matches!(self, Self::Retired)
    }
}

impl DatasetAuthority {
    pub(super) async fn fence_reclaim(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<Option<OperationId>, AuthorityError> {
        let owner = OperationId::random();
        let mut retiring = false;
        self.update_access(identity, snapshot, |a| {
            retiring = a.retiring;
            if retiring {
                return Ok(());
            }
            a.reclaiming = true;
            a.reclaim_owner = Some(owner);
            Ok(())
        })
        .await?;
        if retiring {
            return Ok(None);
        }
        self.fence_split_publication(identity, snapshot).await?;
        // Invalidate publishers that read the old head before checking the admission fence.
        let key = key::head(identity)?;
        let mut old = self.store.get(&key).await?;
        for _ in 0..64 {
            if let Some(old) = &old {
                self.help_head_commit(identity, old).await?;
            }
            let head = old
                .as_ref()
                .map(|v| decode_head(&v.bytes))
                .transpose()?
                .unwrap_or(HeadRecord {
                    latest: None,
                    stable: None,
                });
            match self
                .store
                .compare_exchange(
                    &key,
                    old.as_ref().map(|v| v.bytes.as_slice()),
                    &encode_head(head)?,
                )
                .await?
            {
                CasOutcome::Applied(_) => return Ok(Some(owner)),
                CasOutcome::Conflict(observed) => old = observed,
            }
        }
        Err(AuthorityError::HeadConflict)
    }
    async fn unfence_reclaim(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        owner: OperationId,
    ) -> Result<(), AuthorityError> {
        self.update_access(identity, snapshot, |a| {
            if a.retiring || a.reclaim_owner != Some(owner) {
                return Err(AuthorityError::HeadConflict);
            }
            a.reclaiming = false;
            a.reclaim_owner = None;
            Ok(())
        })
        .await
    }
    /// Fences publication/read admission and retains an aborted operation tombstone.
    /// The original manifest remains in that record for crash-safe payload cleanup.
    /// # Errors
    /// Rejects head/ancestry/retention protection or storage failures.
    pub async fn begin_reclaim(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<crate::SnapshotRecord, AuthorityError> {
        // Avoid fencing an obviously protected head. The checks after fencing
        // remain authoritative against concurrent publication and retention.
        if let Some(value) = self.store.get(&key::head(identity)?).await? {
            let head = decode_head(&value.bytes)?;
            if head.latest == Some(snapshot) || head.stable == Some(snapshot) {
                return Err(AuthorityError::SnapshotProtected);
            }
        }
        if let Some(owner) = self.fence_reclaim(identity, snapshot).await? {
            let protected = async {
                let head = self.store.get(&key::head(identity)?).await?;
                let head = head.as_ref().map(|v| decode_head(&v.bytes)).transpose()?;
                Ok::<_, AuthorityError>(
                    head.is_some_and(|h| h.latest == Some(snapshot) || h.stable == Some(snapshot))
                        || self
                            .store
                            .get(&key::retention(identity, snapshot)?)
                            .await?
                            .is_some()
                        || self.snapshot_reachable_from_head(identity, snapshot).await?
                        || self.has_live_snapshot_children(identity, snapshot).await?
                        || self.legacy_live_descendant(identity, snapshot).await?,
                )
            }
            .await;
            match protected {
                Ok(false) => {}
                Ok(true) => {
                    self.unfence_reclaim(identity, snapshot, owner).await?;
                    return Err(AuthorityError::SnapshotProtected);
                }
                Err(error) => {
                    self.unfence_reclaim(identity, snapshot, owner).await?;
                    return Err(error);
                }
            }
            self.begin_terminal_retirement(
                identity,
                snapshot,
                owner,
                self.reclaim_time().saturating_add(super::RECLAIM_GRACE_SECONDS),
            )
            .await?;
        }
        self.retire_snapshot_record(identity, snapshot).await
    }
    async fn begin_terminal_retirement(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        owner: OperationId,
        reclaim_after: u64,
    ) -> Result<(), AuthorityError> {
        self.update_access(identity, snapshot, |a| {
            if !a.reclaiming || a.retiring || a.reclaim_owner != Some(owner) {
                return Err(AuthorityError::HeadConflict);
            }
            a.retiring = true;
            a.reclaim_after = reclaim_after;
            Ok(())
        })
        .await
    }
    async fn retire_snapshot_record(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<crate::SnapshotRecord, AuthorityError> {
        let key = key::snapshot(identity, snapshot)?;
        let mut old = self
            .store
            .get(&key)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        for _ in 0..64 {
            let mut record: crate::SnapshotRecord = decode(&old.bytes)?;
            if record.publication.state == crate::PublicationState::Aborted {
                self.mark_snapshot_retired(identity, snapshot).await?;
                return Ok(record);
            }
            record.publication.state = crate::PublicationState::Aborted;
            match self
                .store
                .compare_exchange(&key, Some(&old.bytes), &crate::authority::encode(&record)?)
                .await?
            {
                CasOutcome::Applied(_) => {
                    self.mark_snapshot_retired(identity, snapshot).await?;
                    return Ok(record);
                }
                CasOutcome::Conflict(Some(observed)) => old = observed,
                CasOutcome::Conflict(None) => return Err(AuthorityError::SnapshotNotFound),
            }
        }
        Err(AuthorityError::HeadConflict)
    }
    /// Fences a cancelled modern candidate without retiring a successful historical publication.
    pub(crate) async fn abort_uncommitted_candidate(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<CandidateRetirement, AuthorityError> {
        if let Some(receipt) = self.snapshot_commit(identity, snapshot).await? {
            return Ok(CandidateRetirement::LegacyCommitted(receipt));
        }
        if self.seal_incomplete_snapshot(identity, snapshot).await? {
            return Ok(CandidateRetirement::Complete);
        }
        if let Some(owner) = self.fence_reclaim(identity, snapshot).await? {
            if let Some(receipt) = self.snapshot_commit(identity, snapshot).await? {
                self.unfence_reclaim(identity, snapshot, owner).await?;
                return Ok(CandidateRetirement::LegacyCommitted(receipt));
            }
            self.begin_terminal_retirement(identity, snapshot, owner, 0)
                .await?;
        } else if let Some(receipt) = self.snapshot_commit(identity, snapshot).await? {
            return Ok(CandidateRetirement::LegacyCommitted(receipt));
        }
        self.retire_snapshot_record(identity, snapshot).await?;
        Ok(CandidateRetirement::Retired)
    }
}
