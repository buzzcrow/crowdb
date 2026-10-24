// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{
    decode, encode,
    head::{encode_head, reference},
};
use crate::{
    key, AuthorityError, CasOutcome, CommitAttempt, CommitGeneration, CommitTarget, DatasetAuthority,
    DatasetError, DatasetHead, DatasetIdentity, HeadRecord, PublicationState, SnapshotHeadUpdate, SnapshotId,
    SnapshotRecord, StoredValue, SuccessfulCommit,
};
use sha2::{Digest, Sha256};

impl DatasetAuthority {
    /// Compatibility adapter for caller-owned expected-base publication inputs.
    /// # Errors
    /// Rejects divergent attempt IDs, stale heads, unavailable parents and incomplete candidates.
    pub async fn publish_snapshot_candidate(
        &self,
        identity: &DatasetIdentity,
        attempt: &CommitAttempt,
    ) -> Result<SuccessfulCommit, AuthorityError> {
        attempt.validate()?;
        let CommitTarget::Snapshot { snapshot } = attempt.target else {
            return Err(DatasetError::InvalidPublicationState.into());
        };
        let head = self
            .update_snapshot_head(
                identity,
                &SnapshotHeadUpdate {
                    snapshot,
                    expected_head: attempt.expected_head,
                },
            )
            .await?;
        Ok(SuccessfulCommit {
            version: 1,
            attempt: attempt.clone(),
            committed_head: head.generation.ok_or(AuthorityError::Corrupt)?,
        })
    }

    /// Conditionally selects one complete Snapshot without request or response persistence.
    /// # Errors
    /// Returns incomplete/deleted resources, the observed conflicting head or unknown storage outcomes.
    pub async fn update_snapshot_head(
        &self,
        identity: &DatasetIdentity,
        update: &SnapshotHeadUpdate,
    ) -> Result<DatasetHead, AuthorityError> {
        SnapshotId::from_bytes(update.snapshot.as_bytes())?;
        let current = self.store.get(&key::head(identity)?).await?;
        self.commit_snapshot_head(identity, update, None, current).await
    }

    pub(super) async fn commit_snapshot_head(
        &self,
        identity: &DatasetIdentity,
        update: &SnapshotHeadUpdate,
        stable: Option<SnapshotId>,
        current: Option<StoredValue>,
    ) -> Result<DatasetHead, AuthorityError> {
        let snapshot = update.snapshot;
        SnapshotId::from_bytes(snapshot.as_bytes())?;
        let candidate = self
            .snapshot_candidate(identity, snapshot)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        if candidate.version != 2 && !self.legacy_candidate_ready(identity, &candidate).await? {
            return Err(AuthorityError::SnapshotNotFound);
        }
        self.ensure_snapshot_live(identity, snapshot).await?;
        let observed = reference(current.as_ref())?;
        if observed.latest == Some(snapshot) && stable.map_or(true, |stable| observed.stable == Some(stable))
        {
            return Ok(observed);
        }
        if observed.generation != update.expected_head {
            return Err(AuthorityError::SnapshotHeadConflict {
                observed: Box::new(observed),
            });
        }
        if let Some(current) = &current {
            self.preserve_legacy_head(identity, current).await?;
        }
        let bytes = encode_head(HeadRecord {
            latest: Some(snapshot),
            stable: stable.or(observed.stable),
        })?;
        match self
            .store
            .compare_exchange(
                &key::head(identity)?,
                current.as_ref().map(|value| value.bytes.as_slice()),
                &bytes,
            )
            .await?
        {
            CasOutcome::Applied(_) => Ok(DatasetHead {
                latest: Some(snapshot),
                stable: stable.or(observed.stable),
                generation: Some(CommitGeneration::from_head(&bytes)),
            }),
            CasOutcome::Conflict(value) => {
                let observed = reference(value.as_ref())?;
                if observed.latest == Some(snapshot)
                    && stable.map_or(true, |stable| observed.stable == Some(stable))
                {
                    return Ok(observed);
                }
                Err(AuthorityError::SnapshotHeadConflict {
                    observed: Box::new(observed),
                })
            }
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
