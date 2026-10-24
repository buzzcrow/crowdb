// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode_session, encode, DatasetUploads, UploadError, UploadState, MAX_RETIRED_OWNERS};
use crate::{CasOutcome, OperationId};

impl DatasetUploads {
    /// Permanently revokes one terminal publication's ownership without affecting other owners.
    /// A delayed or retried claim for that operation cannot reacquire bytes after cleanup.
    /// # Errors
    /// Returns unknown uploads, corrupt metadata, exhausted retries or storage failures.
    pub async fn retire_owner(&self, location: &[u8], owner: OperationId) -> Result<(), UploadError> {
        OperationId::from_bytes(owner.as_bytes()).map_err(|_| UploadError::Invalid)?;
        let mut stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
        for _ in 0..16 {
            let mut record = decode_session(&stored.bytes)?;
            if record.retired_owners.binary_search(&owner).is_ok() {
                return Ok(());
            }
            if record.retired_owners.len() == MAX_RETIRED_OWNERS {
                if record.version == 5 && !record.owners.contains(&owner) {
                    return Ok(());
                }
                // Older writers must reject retirement without an individual negative owner ID.
                record.version = 5;
            } else {
                record.retired_owners.push(owner);
                record.retired_owners.sort();
            }
            record.owners.retain(|current| *current != owner);
            if record.owners.is_empty() && record.state == UploadState::Claimed {
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
}
