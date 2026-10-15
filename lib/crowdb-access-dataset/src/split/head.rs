// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{codec, key, SplitHead, SplitLifecycleError};
use crate::{
    CommitAttempt, CommitGeneration, CommitTarget, DatasetAuthority, DatasetIdentity, SnapshotId,
    SplitRevisionId, StoredValue, SuccessfulCommit,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SplitReference {
    pub revision: Option<SplitRevisionId>,
    pub generation: Option<CommitGeneration>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CommittedSplitHead {
    version: u16,
    pub head: SplitHead,
    pub attempt: CommitAttempt,
}
impl CommittedSplitHead {
    pub(super) fn encode(head: SplitHead, attempt: CommitAttempt) -> Result<Vec<u8>, SplitLifecycleError> {
        head.encode()?;
        attempt.validate()?;
        let value = Self {
            version: 2,
            head,
            attempt,
        };
        value.validate()?;
        Ok(codec::encode(&value)?)
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, SplitLifecycleError> {
        let value: Self = codec::decode(bytes)?;
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<(), SplitLifecycleError> {
        self.head.encode()?;
        self.attempt.validate()?;
        if self.version != 2
            || self.attempt.target
                != (CommitTarget::SplitRevision {
                    snapshot: self.head.snapshot,
                    revision: self.head.revision.ok_or(SplitLifecycleError::Corrupt)?,
                })
        {
            return Err(SplitLifecycleError::Corrupt);
        }
        Ok(())
    }
}
impl DatasetAuthority {
    /// Returns this Snapshot's current whole-set revision and exact CAS generation.
    /// # Errors
    /// Rejects unavailable Snapshot owners and malformed head metadata.
    pub async fn inspect_split_head(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<SplitReference, SplitLifecycleError> {
        self.require_committed_snapshot(identity, snapshot).await?;
        let stored = self
            .split_state_get(&key::head(identity, snapshot).map_err(crate::AuthorityError::from)?)
            .await?;
        reference(stored.as_ref(), snapshot)
    }
    pub(super) async fn help_split_head(
        &self,
        identity: &DatasetIdentity,
        stored: &StoredValue,
    ) -> Result<(), SplitLifecycleError> {
        if let Ok(head) = SplitHead::decode(&stored.bytes) {
            if let Some(revision) = head.revision {
                let stored = self
                    .split_state_get(
                        &key::committed_revision(identity, head.snapshot, revision)
                            .map_err(crate::AuthorityError::from)?,
                    )
                    .await?
                    .ok_or(SplitLifecycleError::Corrupt)?;
                let receipt = SuccessfulCommit::decode(&stored.bytes)?;
                if receipt.attempt.target
                    != (CommitTarget::SplitRevision {
                        snapshot: head.snapshot,
                        revision,
                    })
                {
                    return Err(SplitLifecycleError::Corrupt);
                }
            }
            return Ok(());
        }
        let value = CommittedSplitHead::decode(&stored.bytes)?;
        self.persist_commit_receipt(
            identity,
            &SuccessfulCommit {
                version: 1,
                attempt: value.attempt,
                committed_head: CommitGeneration::from_head(&stored.bytes),
            },
        )
        .await?;
        Ok(())
    }
    pub(super) async fn split_revision_receipt(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
    ) -> Result<Option<SuccessfulCommit>, SplitLifecycleError> {
        let target = CommitTarget::SplitRevision { snapshot, revision };
        if let Some(stored) = self
            .split_state_get(
                &key::committed_revision(identity, snapshot, revision)
                    .map_err(crate::AuthorityError::from)?,
            )
            .await?
        {
            let receipt = SuccessfulCommit::decode(&stored.bytes)?;
            if receipt.attempt.target != target {
                return Err(SplitLifecycleError::Corrupt);
            }
            return Ok(Some(receipt));
        }
        let Some(stored) = self
            .split_state_get(&key::head(identity, snapshot).map_err(crate::AuthorityError::from)?)
            .await?
        else {
            return Ok(None);
        };
        if SplitHead::decode(&stored.bytes).is_ok() {
            return Ok(None);
        }
        let head = CommittedSplitHead::decode(&stored.bytes)?;
        if head.attempt.target != target {
            return Ok(None);
        }
        self.help_split_head(identity, &stored).await?;
        Ok(Some(SuccessfulCommit {
            version: 1,
            attempt: head.attempt,
            committed_head: CommitGeneration::from_head(&stored.bytes),
        }))
    }
}
pub(super) fn reference(
    stored: Option<&StoredValue>,
    snapshot: SnapshotId,
) -> Result<SplitReference, SplitLifecycleError> {
    let Some(stored) = stored else {
        return Ok(SplitReference {
            revision: None,
            generation: None,
        });
    };
    let head = if let Ok(head) = SplitHead::decode(&stored.bytes) {
        head
    } else {
        CommittedSplitHead::decode(&stored.bytes)?.head
    };
    if head.snapshot != snapshot {
        return Err(SplitLifecycleError::Corrupt);
    }
    Ok(SplitReference {
        revision: head.revision,
        generation: Some(CommitGeneration::from_head(&stored.bytes)),
    })
}
