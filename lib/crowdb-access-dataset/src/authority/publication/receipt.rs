// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::head::head_attempt;
use crate::{
    AuthorityError, CasOutcome, CommitAttempt, CommitGeneration, CommitTarget, DatasetAuthority,
    DatasetError, DatasetIdentity, OperationId, SnapshotId, StoredValue, SuccessfulCommit,
};

impl DatasetAuthority {
    pub(in crate::authority) async fn help_head_commit(
        &self,
        identity: &DatasetIdentity,
        head: &StoredValue,
    ) -> Result<(), AuthorityError> {
        let Some(attempt) = head_attempt(&head.bytes)? else {
            return self.capture_legacy_head(identity, head).await;
        };
        let receipt = SuccessfulCommit {
            version: 1,
            attempt,
            committed_head: CommitGeneration::from_head(&head.bytes),
        };
        self.persist_commit_receipt(identity, &receipt).await
    }

    pub(in crate::authority) async fn persist_commit_receipt(
        &self,
        identity: &DatasetIdentity,
        receipt: &SuccessfulCommit,
    ) -> Result<(), AuthorityError> {
        let bytes = receipt.encode()?;
        self.put_commit_record(&receipt_key(identity, receipt.attempt.id)?, &bytes)
            .await?;
        let CommitTarget::Snapshot { snapshot } = receipt.attempt.target else {
            return Err(AuthorityError::Corrupt);
        };
        self.put_commit_record(&snapshot_commit_key(identity, snapshot)?, &bytes)
            .await
    }

    pub(in crate::authority) async fn put_commit_record(
        &self,
        key: &[u8],
        bytes: &[u8],
    ) -> Result<(), AuthorityError> {
        match self.store.compare_exchange(key, None, bytes).await? {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::AlreadyExists),
        }
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
                self.help_head_commit(identity, &head).await?;
                let receipt = SuccessfulCommit {
                    version: 1,
                    attempt: head_attempt(&head.bytes)?.ok_or(AuthorityError::Corrupt)?,
                    committed_head: CommitGeneration::from_head(&head.bytes),
                };
                return Ok(Some(receipt));
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
                    self.help_head_commit(identity, &head).await?;
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

    pub(in crate::authority) async fn persist_attempt(
        &self,
        identity: &DatasetIdentity,
        attempt: &CommitAttempt,
    ) -> Result<(), AuthorityError> {
        self.put_commit_record(&attempt_key(identity, attempt.id)?, &attempt.encode()?)
            .await
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
