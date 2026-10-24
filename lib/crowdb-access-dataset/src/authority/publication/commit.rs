// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{
    decode, encode,
    head::{decode_head, encode_commit_head},
};
use crate::{
    key, AuthorityError, CasOutcome, CommitAttempt, CommitGeneration, CommitTarget, DatasetAuthority,
    DatasetError, DatasetIdentity, HeadRecord, PublicationState, SnapshotId, SnapshotRecord,
    SuccessfulCommit,
};
use sha2::{Digest, Sha256};

impl DatasetAuthority {
    /// Commits one immutable candidate using an explicit, independently retryable attempt.
    /// # Errors
    /// Rejects divergent attempt IDs, stale heads, unavailable parents and incomplete candidates.
    pub async fn publish_snapshot_candidate(
        &self,
        identity: &DatasetIdentity,
        attempt: &CommitAttempt,
    ) -> Result<SuccessfulCommit, AuthorityError> {
        self.commit_snapshot_attempt(identity, attempt, None).await
    }

    pub(super) async fn commit_snapshot_attempt(
        &self,
        identity: &DatasetIdentity,
        attempt: &CommitAttempt,
        stable: Option<SnapshotId>,
    ) -> Result<SuccessfulCommit, AuthorityError> {
        attempt.validate()?;
        let CommitTarget::Snapshot { snapshot } = attempt.target else {
            return Err(DatasetError::InvalidPublicationState.into());
        };
        if let Some(receipt) = self.commit_status(identity, attempt.id).await? {
            if receipt.attempt != *attempt {
                return Err(AuthorityError::AlreadyExists);
            }
            return Ok(receipt);
        }
        let candidate = self
            .snapshot_candidate(identity, snapshot)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        if candidate.version != 2 {
            return Err(AuthorityError::SnapshotNotFound);
        }
        self.persist_attempt(identity, attempt).await?;
        if let Some(receipt) = self.snapshot_commit(identity, snapshot).await? {
            return Ok(receipt);
        }
        let current = self.store.get(&key::head(identity)?).await?;
        if let Some(current) = &current {
            self.help_head_commit(identity, current).await?;
        }
        if current
            .as_ref()
            .map(|value| CommitGeneration::from_head(&value.bytes))
            != attempt.expected_head
        {
            return Err(AuthorityError::HeadConflict);
        }
        if let Some(parent) = candidate.parent {
            self.require_committed_snapshot(identity, parent).await?;
        }
        self.ensure_snapshot_live(identity, snapshot).await?;
        self.validate_initial_split(identity, snapshot, candidate.initial_split)
            .await?;
        self.mark_candidate_published(identity, &candidate).await?;
        let old_head = current
            .as_ref()
            .map(|value| decode_head(&value.bytes))
            .transpose()?
            .unwrap_or(HeadRecord {
                latest: None,
                stable: None,
            });
        let bytes = encode_commit_head(
            HeadRecord {
                latest: Some(snapshot),
                stable: stable.or(old_head.stable),
            },
            attempt.clone(),
        )?;
        match self
            .store
            .compare_exchange(
                &key::head(identity)?,
                current.as_ref().map(|value| value.bytes.as_slice()),
                &bytes,
            )
            .await?
        {
            CasOutcome::Applied(_) => {
                let receipt = SuccessfulCommit {
                    version: 1,
                    attempt: attempt.clone(),
                    committed_head: CommitGeneration::from_head(&bytes),
                };
                self.persist_commit_receipt(identity, &receipt).await?;
                Ok(receipt)
            }
            CasOutcome::Conflict(_) => self
                .snapshot_commit(identity, snapshot)
                .await?
                .ok_or(AuthorityError::HeadConflict),
        }
    }

    pub(super) async fn mark_candidate_published(
        &self,
        identity: &DatasetIdentity,
        candidate: &super::candidate::CandidateBinding,
    ) -> Result<(), AuthorityError> {
        let key = key::snapshot(identity, candidate.snapshot)?;
        let stored = self
            .store
            .get(&key)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let mut record: SnapshotRecord = decode(&stored.bytes)?;
        let digest: [u8; 32] = Sha256::digest(&record.publication.manifest).into();
        if record.publication.snapshot != candidate.snapshot
            || record.publication.parent != candidate.parent
            || record.publication.operation != candidate.operation
            || digest != candidate.manifest_sha256
        {
            return Err(AuthorityError::Corrupt);
        }
        if record.publication.state == PublicationState::Aborted {
            return Err(AuthorityError::SnapshotProtected);
        }
        if record.publication.state == PublicationState::Published {
            return Ok(());
        }
        record.publication.state = PublicationState::Published;
        let published = encode(&record)?;
        match self
            .store
            .compare_exchange(&key, Some(&stored.bytes), &published)
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == published => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::HeadConflict),
        }
    }
}
