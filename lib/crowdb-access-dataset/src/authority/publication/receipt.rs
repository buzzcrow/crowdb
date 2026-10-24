// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::head::head_attempt;
use crate::{
    AuthorityError, CommitAttempt, CommitGeneration, CommitTarget, DatasetAuthority, DatasetError,
    DatasetIdentity, OperationId, SnapshotId, StoredValue, SuccessfulCommit,
};

impl DatasetAuthority {
    pub(in crate::authority) async fn preserve_legacy_head(
        &self,
        identity: &DatasetIdentity,
        head: &StoredValue,
    ) -> Result<(), AuthorityError> {
        if head.bytes.get(..2) == Some(4u16.to_le_bytes().as_slice()) {
            return Ok(());
        }
        self.capture_legacy_head(identity, head).await
    }

    /// Recovers success from durable evidence or the current head's commit marker.
    /// # Errors
    /// Rejects malformed evidence or unavailable storage.
    pub async fn commit_status(
        &self,
        identity: &DatasetIdentity,
        attempt: OperationId,
    ) -> Result<Option<SuccessfulCommit>, AuthorityError> {
        if let Some(value) = self.store.get(&receipt_key(identity, attempt)?).await? {
            let receipt = SuccessfulCommit::decode(&value.bytes)?;
            if receipt.attempt.id != attempt {
                return Err(AuthorityError::Corrupt);
            }
            return Ok(Some(receipt));
        }
        if let Some(head) = self.store.get(&crate::key::head(identity)?).await? {
            if head_attempt(&head.bytes)?.is_some_and(|pending| pending.id == attempt) {
                let receipt = SuccessfulCommit {
                    version: 1,
                    attempt: head_attempt(&head.bytes)?.ok_or(AuthorityError::Corrupt)?,
                    committed_head: CommitGeneration::from_head(&head.bytes),
                };
                return Ok(Some(receipt));
            }
        }
        if let Some(value) = self.store.get(&attempt_key(identity, attempt)?).await? {
            let bound = CommitAttempt::decode(&value.bytes)?;
            if bound.id != attempt {
                return Err(AuthorityError::Corrupt);
            }
            if matches!(bound.target, CommitTarget::SplitRevision { .. }) {
                return self.recover_split_commit(identity, &bound).await;
            }
        }
        Ok(None)
    }

    /// Returns durable evidence that this candidate was committed at least once.
    /// # Errors
    /// Rejects malformed evidence or unavailable storage.
    pub async fn snapshot_commit(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<Option<SuccessfulCommit>, AuthorityError> {
        if let Some(value) = self.store.get(&snapshot_commit_key(identity, snapshot)?).await? {
            let receipt = SuccessfulCommit::decode(&value.bytes)?;
            if receipt.attempt.target != (CommitTarget::Snapshot { snapshot }) {
                return Err(AuthorityError::Corrupt);
            }
            return Ok(Some(receipt));
        }
        if let Some(head) = self.store.get(&crate::key::head(identity)?).await? {
            if let Some(attempt) = head_attempt(&head.bytes)? {
                if attempt.target == (CommitTarget::Snapshot { snapshot }) {
                    return Ok(Some(SuccessfulCommit {
                        version: 1,
                        attempt,
                        committed_head: CommitGeneration::from_head(&head.bytes),
                    }));
                }
            }
        }
        Ok(None)
    }

    pub(crate) async fn stored_commit_attempt(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
    ) -> Result<Option<CommitAttempt>, AuthorityError> {
        self.store
            .get(&attempt_key(identity, id)?)
            .await?
            .map(|value| {
                let attempt = CommitAttempt::decode(&value.bytes)?;
                if attempt.id != id {
                    return Err(AuthorityError::Corrupt);
                }
                Ok(attempt)
            })
            .transpose()
    }
}
fn scoped(prefix: &[u8], identity: &DatasetIdentity, id: &[u8]) -> Result<Vec<u8>, DatasetError> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(id);
    Ok(key)
}
pub(crate) fn snapshot_commit_key(
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
) -> Result<Vec<u8>, DatasetError> {
    scoped(b"DS\0SC\0", identity, snapshot.as_bytes())
}
pub(crate) fn receipt_key(identity: &DatasetIdentity, attempt: OperationId) -> Result<Vec<u8>, DatasetError> {
    scoped(b"DS\0CR\0", identity, attempt.as_bytes())
}
fn attempt_key(identity: &DatasetIdentity, attempt: OperationId) -> Result<Vec<u8>, DatasetError> {
    scoped(b"DS\0CA\0", identity, attempt.as_bytes())
}
