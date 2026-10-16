// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode_session, encode, DatasetUploads, UploadError, UploadState};
use crate::{CasOutcome, OperationId};

impl DatasetUploads {
    /// Permanently revokes one terminal publication's ownership without affecting other owners.
    /// A delayed or retried claim for that operation cannot reacquire bytes after cleanup.
    /// # Errors
    /// Returns unknown uploads, corrupt metadata, bounded retirement capacity or storage failures.
    pub async fn retire_owner(&self, location: &[u8], owner: OperationId) -> Result<(), UploadError> {
        OperationId::from_bytes(owner.as_bytes()).map_err(|_| UploadError::Invalid)?;
        self.register(location).await?;
        for _ in 0..16 {
            let stored = self.store.get(location).await?.ok_or(UploadError::NotFound)?;
            let mut record = decode_session(&stored.bytes)?;
            if record.retired_owners.binary_search(&owner).is_ok() {
                if !record.owners.is_empty() || record.cleanup_complete {
                    self.untrack(location).await?;
                }
                return Ok(());
            }
            if record.retired_owners.len() >= 4096 {
                return Err(UploadError::Capacity);
            }
            record.version = 3;
            record.retired_owners.push(owner);
            record.retired_owners.sort();
            record.owners.retain(|current| *current != owner);
            if record.owners.is_empty() && record.state == UploadState::Claimed {
                record.state = UploadState::Complete;
            }
            if matches!(
                self.store
                    .compare_exchange(location, Some(&stored.bytes), &encode(&record)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                if !record.owners.is_empty() || record.cleanup_complete {
                    self.untrack(location).await?;
                }
                return Ok(());
            }
        }
        Err(UploadError::Pending)
    }
}
