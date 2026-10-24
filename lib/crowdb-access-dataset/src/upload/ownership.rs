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
        let mut stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
        for _ in 0..16 {
            let mut record = decode_session(&stored.bytes)?;
            if record.retired_owners.binary_search(&owner).is_ok() {
                return Err(UploadError::Conflict);
            }
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
            match self
                .store
                .compare_exchange(location, Some(&stored.bytes), &encode(&record)?)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(record),
                CasOutcome::Conflict(Some(value)) => stored = value,
                CasOutcome::Conflict(None) => return Err(UploadError::NotFound),
            }
        }
        Err(UploadError::Pending)
    }

    /// Releases only one publication's reference, preserving other owners.
    /// # Errors
    /// Returns unavailable storage, malformed records or exhausted retries.
    pub async fn release(&self, location: &[u8], owner: OperationId) -> Result<(), UploadError> {
        let mut stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
        for _ in 0..16 {
            let mut record = decode_session(&stored.bytes)?;
            if !record.owners.contains(&owner) {
                return Ok(());
            }
            record.owners.retain(|v| *v != owner);
            if record.owners.is_empty() {
                record.state = UploadState::Complete;
            }
            match self
                .store
                .compare_exchange(location, Some(&stored.bytes), &encode(&record)?)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(()),
                CasOutcome::Conflict(Some(value)) => stored = value,
                CasOutcome::Conflict(None) => return Err(UploadError::NotFound),
            }
        }
        Err(UploadError::Pending)
    }

    /// # Errors
    /// Claimed data conflicts. Deferred cleanup notifications remain observable.
    pub async fn abort(&self, location: &[u8]) -> Result<UploadRecord, UploadError> {
        let mut stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
        for _ in 0..16 {
            let mut record = decode_session(&stored.bytes)?;
            if !record.owners.is_empty() {
                return Err(UploadError::Conflict);
            }
            if record.state != UploadState::Aborted {
                record.state = UploadState::Aborted;
                match self
                    .store
                    .compare_exchange(location, Some(&stored.bytes), &encode(&record)?)
                    .await?
                {
                    CasOutcome::Applied(_) => {}
                    CasOutcome::Conflict(Some(observed)) => {
                        stored = observed;
                        continue;
                    }
                    CasOutcome::Conflict(None) => return Err(UploadError::NotFound),
                }
            }
            return self.cleanup(location, record).await;
        }
        Err(UploadError::Pending)
    }

    async fn cleanup(&self, location: &[u8], mut record: UploadRecord) -> Result<UploadRecord, UploadError> {
        if record.cleanup_complete {
            record.parts = self.accepted_parts(location, &record).await?;
            return Ok(record);
        }
        let entries = self.entries(location, &record).await?;
        let mut complete = true;
        for entry in entries {
            complete &= self
                .cleanup_part(&part_key(location, entry.part.index), entry)
                .await?;
        }
        if complete {
            let mut expected = encode(&record)?;
            for _ in 0..16 {
                if record.state != UploadState::Aborted {
                    return Err(UploadError::Conflict);
                }
                if record.cleanup_complete {
                    break;
                }
                record.cleanup_complete = true;
                match self
                    .store
                    .compare_exchange(location, Some(&expected), &encode(&record)?)
                    .await?
                {
                    CasOutcome::Applied(_) => break,
                    CasOutcome::Conflict(Some(observed)) => {
                        record = decode_session(&observed.bytes)?;
                        expected = observed.bytes;
                    }
                    CasOutcome::Conflict(None) => return Err(UploadError::NotFound),
                }
            }
            if !record.cleanup_complete {
                return Err(UploadError::Pending);
            }
        }
        record.parts = self.accepted_parts(location, &record).await?;
        Ok(record)
    }

    async fn cleanup_part(&self, key: &[u8], mut record: super::PartEntry) -> Result<bool, UploadError> {
        let mut expected = encode(&record)?;
        for _ in 0..16 {
            if record.state != PartState::Reclaimed {
                record.state = PartState::Reclaimed;
                let value = encode(&record)?;
                match self.store.compare_exchange(key, Some(&expected), &value).await? {
                    CasOutcome::Applied(_) => expected = value,
                    CasOutcome::Conflict(Some(observed)) => {
                        record = decode_part(&observed.bytes)?;
                        expected = observed.bytes;
                        continue;
                    }
                    CasOutcome::Conflict(None) => return Ok(true),
                }
            }
            if record.part.locations.is_empty() {
                return Ok(true);
            }
            if !self.payloads.discard(&record.part.locations).await? {
                return Ok(false);
            }
            record.part.locations.clear();
            match self
                .store
                .compare_exchange(key, Some(&expected), &encode(&record)?)
                .await?
            {
                CasOutcome::Applied(_) | CasOutcome::Conflict(None) => return Ok(true),
                CasOutcome::Conflict(Some(observed)) => {
                    record = decode_part(&observed.bytes)?;
                    expected = observed.bytes;
                }
            }
        }
        Err(UploadError::Pending)
    }
}
