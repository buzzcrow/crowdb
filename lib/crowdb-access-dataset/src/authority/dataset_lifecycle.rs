// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod admission;
mod emptiness;
mod record;
mod sweep;
pub use record::DatasetDeletion;
pub(crate) use record::{DatasetEnvelope, RETIRED_RESOURCE};
pub use sweep::DatasetDeletionSweep;

use crate::{
    key, AuthorityError, CasOutcome, DatasetAuthority, DatasetId, DatasetIdentity, ReadCancellation,
};
use std::time::Duration;

impl DatasetAuthority {
    /// Marks an empty Dataset unavailable. The immutable ID fences stale UI requests.
    /// Repetition preserves the original grace deadline, including across restart.
    /// # Errors
    /// Rejects nonempty, missing or replaced Datasets and unavailable metadata.
    pub async fn delete_dataset(
        &self,
        identity: &DatasetIdentity,
        expected_id: DatasetId,
        cancel: &ReadCancellation,
    ) -> Result<DatasetDeletion, AuthorityError> {
        tokio::time::timeout(
            Duration::from_secs(30),
            self.delete_empty_dataset(identity, expected_id, cancel),
        )
        .await
        .map_err(|_| crate::DatasetError::ReadLimitExceeded)?
    }

    async fn delete_empty_dataset(
        &self,
        identity: &DatasetIdentity,
        expected_id: DatasetId,
        cancel: &ReadCancellation,
    ) -> Result<DatasetDeletion, AuthorityError> {
        let key = key::dataset(identity)?;
        for _ in 0..16 {
            let stored = self.store.get(&key).await?.ok_or(AuthorityError::NotFound)?;
            let mut envelope = DatasetEnvelope::decode(&stored.bytes)?;
            if envelope.record.id != expected_id {
                return Err(AuthorityError::HeadConflict);
            }
            if let Some(deletion) = envelope.deletion {
                return Ok(deletion);
            }
            self.settle_dataset_reservations(&mut envelope).await?;
            if !envelope.pending.is_empty() {
                return Err(AuthorityError::DatasetNotEmpty);
            }
            self.require_empty_dataset(identity, cancel).await?;
            let deletion = DatasetDeletion {
                id: expected_id,
                reclaim_after: self.reclaim_time().saturating_add(self.reclaim_grace_seconds),
                complete: false,
            };
            envelope.deletion = Some(deletion.clone());
            match self
                .store
                .compare_exchange(&key, Some(&stored.bytes), &envelope.encode()?)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(deletion),
                CasOutcome::Conflict(_) => {}
            }
        }
        Err(AuthorityError::HeadConflict)
    }

    /// Reads the durable deletion receipt without admitting normal Dataset access.
    /// # Errors
    /// Returns missing/replaced identities, storage failures or corrupt metadata.
    pub async fn dataset_deletion(
        &self,
        identity: &DatasetIdentity,
        expected_id: DatasetId,
    ) -> Result<Option<DatasetDeletion>, AuthorityError> {
        let stored = self
            .store
            .get(&key::dataset(identity)?)
            .await?
            .ok_or(AuthorityError::NotFound)?;
        let envelope = DatasetEnvelope::decode(&stored.bytes)?;
        if envelope.record.id != expected_id {
            return Err(AuthorityError::HeadConflict);
        }
        Ok(envelope.deletion)
    }
}
