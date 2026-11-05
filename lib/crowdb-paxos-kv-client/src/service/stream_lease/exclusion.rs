// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Irreversible process exclusion competes with renewal on the lease revision.

use super::{invalid, Result, StreamProcessLeaseClient, StreamProcessLeaseError};
use crowdb_protocol::{
    chunk_stream::StreamProcessIdentity,
    timing::{MAX_WRITE_CLOCK_SKEW_MS, MAX_WRITE_REQUEST_AGE_MS},
};

impl StreamProcessLeaseClient {
    /// Confirm that this fixed process can no longer renew or submit a valid
    /// late request. The returned boundary still requires physical IO drain.
    /// An active or not-yet-excluded identity returns None without mutation.
    /// # Errors
    /// Rejects missing/malformed identities and unresolved authority updates.
    pub async fn confirm_exclusion(
        &self,
        identity: StreamProcessIdentity,
        now_ms: u64,
    ) -> Result<Option<u64>> {
        for attempt in 0..8 {
            let Some((mut record, revision)) = self.read(identity.purpose, identity.instance_id).await?
            else {
                return Err(StreamProcessLeaseError::Replaced);
            };
            let expires = if record.identity == identity {
                record.expires_at_ms.max(record.grant_expires_at_ms)
            } else if record.identity.process_epoch > identity.process_epoch {
                record.prior_exclusion_deadline_ms
            } else {
                return Err(StreamProcessLeaseError::Replaced);
            };
            let safe = expires
                .checked_add(MAX_WRITE_CLOCK_SKEW_MS)
                .and_then(|value| value.checked_add(MAX_WRITE_REQUEST_AGE_MS))
                .ok_or_else(|| invalid("process exclusion deadline overflow"))?;
            if now_ms < safe {
                return Ok(None);
            }
            if record.identity != identity || record.revoked {
                return Ok(Some(safe));
            }
            record.revoked = true;
            if self.write(&record, revision, attempt).await? {
                return Ok(Some(safe));
            }
        }
        Err(StreamProcessLeaseError::OutcomeUnknown)
    }
}
