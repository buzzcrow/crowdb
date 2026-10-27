// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    record::{Reservation, MAX_RESERVATIONS},
    DatasetEnvelope, RETIRED_RESOURCE,
};
use crate::{
    key, AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity, DatasetUploads, OperationId,
};

impl DatasetAuthority {
    /// Reserves creation before the upload's first durable session CAS.
    /// Low-level upload storage must use this boundary when attached to a Dataset.
    /// # Errors
    /// Rejects unavailable Datasets, fenced resources or exhausted admission.
    pub async fn reserve_upload_creation(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
    ) -> Result<(), AuthorityError> {
        let key = DatasetUploads::location(identity, id).map_err(|_| AuthorityError::Corrupt)?;
        self.reserve_dataset_resource(identity, key).await
    }

    pub(in crate::authority) async fn reserve_dataset_resource(
        &self,
        identity: &DatasetIdentity,
        resource: Vec<u8>,
    ) -> Result<(), AuthorityError> {
        let key = key::dataset(identity)?;
        let mut current = self.store.get(&key).await?.ok_or(AuthorityError::NotFound)?;
        let id = DatasetEnvelope::decode(&current.bytes)?.active()?.id;
        for _ in 0..64 {
            let mut envelope = DatasetEnvelope::decode(&current.bytes)?;
            if envelope.record.id != id || envelope.deletion.is_some() {
                return Err(AuthorityError::NotFound);
            }
            if let Some(value) = self.store.get(&resource).await? {
                return if value.bytes == RETIRED_RESOURCE {
                    Err(AuthorityError::HeadConflict)
                } else {
                    Ok(())
                };
            }
            if envelope.pending.iter().any(|p| p.key == resource) {
                return Ok(());
            }
            if envelope.pending.len() == MAX_RESERVATIONS {
                self.settle_dataset_reservations(&mut envelope).await?;
                if envelope.pending.len() == MAX_RESERVATIONS {
                    return Err(AuthorityError::HeadConflict);
                }
            }
            envelope.pending.push(Reservation {
                key: resource.clone(),
                expires_at: self
                    .reclaim_time()
                    .saturating_add(super::super::access::RECLAIM_GRACE_SECONDS),
            });
            match self
                .store
                .compare_exchange(&key, Some(&current.bytes), &envelope.encode()?)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(()),
                CasOutcome::Conflict(Some(value)) => current = value,
                CasOutcome::Conflict(None) => return Err(AuthorityError::NotFound),
            }
        }
        Err(AuthorityError::HeadConflict)
    }

    pub(super) async fn settle_dataset_reservations(
        &self,
        envelope: &mut DatasetEnvelope,
    ) -> Result<(), AuthorityError> {
        let mut remaining = Vec::new();
        for pending in &envelope.pending {
            if self.store.get(&pending.key).await?.is_some() {
                continue;
            }
            if pending.expires_at > self.reclaim_time() {
                remaining.push(pending.clone());
                continue;
            }
            // A permanent child fence makes a delayed creator fail even after name reuse.
            self.store
                .compare_exchange(&pending.key, None, RETIRED_RESOURCE)
                .await?;
        }
        envelope.pending = remaining;
        Ok(())
    }
}
