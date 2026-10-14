// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    decode_part, decode_session, encode, part_key, DatasetUploads, PartState, UploadError, UploadRecord,
    UploadState,
};
use crate::{CasOutcome, OperationId};

impl DatasetUploads {
    /// Pins a complete upload for one publication operation.
    /// # Errors
    /// Rejects incomplete, expired, aborted or over-subscribed ownership.
    pub async fn claim(
        &self,
        location: &[u8],
        owner: OperationId,
        now: u64,
    ) -> Result<UploadRecord, UploadError> {
        for _ in 0..16 {
            let stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
            let mut record = decode_session(&stored.bytes)?;
            if record.owners.contains(&owner) {
                return Ok(record);
            }
            if !matches!(record.state, UploadState::Complete | UploadState::Claimed)
                || (record.state == UploadState::Complete && now >= record.expires_at)
            {
                return Err(UploadError::Conflict);
            }
            if record.owners.len() >= 256 {
                return Err(UploadError::Capacity);
            }
            record.state = UploadState::Claimed;
            record.owners.push(owner);
            record.owners.sort();
            if matches!(
                self.store
                    .compare_exchange(location, Some(&stored.bytes), &encode(&record)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(record);
            }
        }
        Err(UploadError::Pending)
    }

    /// Releases only one publication's reference, preserving other owners.
    /// # Errors
    /// Returns unavailable storage, malformed records or exhausted retries.
    pub async fn release(&self, location: &[u8], owner: OperationId) -> Result<(), UploadError> {
        self.register(location).await?;
        for _ in 0..16 {
            let stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
            let mut record = decode_session(&stored.bytes)?;
            if !record.owners.contains(&owner) {
                return Ok(());
            }
            record.owners.retain(|v| *v != owner);
            if record.owners.is_empty() {
                record.state = UploadState::Complete;
            }
            if matches!(
                self.store
                    .compare_exchange(location, Some(&stored.bytes), &encode(&record)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(());
            }
        }
        Err(UploadError::Pending)
    }

    /// # Errors
    /// Claimed data conflicts. Deferred physical reclamation remains observable.
    pub async fn abort(&self, location: &[u8]) -> Result<UploadRecord, UploadError> {
        for _ in 0..16 {
            let stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
            let mut record = decode_session(&stored.bytes)?;
            if !record.owners.is_empty() {
                return Err(UploadError::Conflict);
            }
            if record.state != UploadState::Aborted {
                record.state = UploadState::Aborted;
                if !matches!(
                    self.store
                        .compare_exchange(location, Some(&stored.bytes), &encode(&record)?)
                        .await?,
                    CasOutcome::Applied(_)
                ) {
                    continue;
                }
            }
            return self.cleanup(location, record).await;
        }
        Err(UploadError::Pending)
    }

    async fn cleanup(&self, location: &[u8], record: UploadRecord) -> Result<UploadRecord, UploadError> {
        let entries = self.entries(location, &record).await?;
        let mut complete = true;
        for entry in entries {
            complete &= self.cleanup_part(&part_key(location, entry.part.index)).await?;
        }
        if complete {
            for _ in 0..16 {
                let stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
                let mut current = decode_session(&stored.bytes)?;
                if current.state != UploadState::Aborted {
                    return Err(UploadError::Conflict);
                }
                current.cleanup_complete = true;
                if matches!(
                    self.store
                        .compare_exchange(location, Some(&stored.bytes), &encode(&current)?)
                        .await?,
                    CasOutcome::Applied(_)
                ) {
                    self.untrack(location).await?;
                    break;
                }
            }
        }
        self.get(location).await
    }

    async fn cleanup_part(&self, key: &[u8]) -> Result<bool, UploadError> {
        for _ in 0..16 {
            let Some(stored) = self.store.get(key).await? else {
                return Ok(true);
            };
            let mut record = decode_part(&stored.bytes)?;
            if record.state != PartState::Reclaimed {
                record.state = PartState::Reclaimed;
                if !matches!(
                    self.store
                        .compare_exchange(key, Some(&stored.bytes), &encode(&record)?)
                        .await?,
                    CasOutcome::Applied(_)
                ) {
                    continue;
                }
            }
            if record.part.locations.is_empty() {
                return Ok(true);
            }
            if !self.payloads.discard(&record.part.locations).await? {
                return Ok(false);
            }
            let expected = encode(&record)?;
            record.part.locations.clear();
            if matches!(
                self.store
                    .compare_exchange(key, Some(&expected), &encode(&record)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(true);
            }
        }
        Err(UploadError::Pending)
    }
}
