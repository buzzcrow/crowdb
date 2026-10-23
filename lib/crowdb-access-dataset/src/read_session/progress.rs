// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    decode, encode, session_key, AuthorityError, CasOutcome, DatasetError, DatasetIdentity, DatasetSessions,
    OperationId, ReadSession, SessionBatch, TTL,
};

impl DatasetSessions {
    /// Resumes acknowledged progress and refreshes its admission deadline.
    /// # Errors
    /// Rejects closed/reclaimed sessions or unavailable storage.
    pub async fn resume(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
        now: u64,
    ) -> Result<ReadSession, AuthorityError> {
        let mut current = self.record(identity, id).await?;
        for _ in 0..64 {
            let (old, mut record) = current;
            if record.closed {
                return Err(DatasetError::ReadCancelled.into());
            }
            self.authority
                .require_committed_snapshot(identity, record.snapshot)
                .await?;
            self.authority
                .check_snapshot_readable(identity, record.snapshot)
                .await?;
            record.deadline = record.deadline.max(now.saturating_add(TTL));
            record.prepare_next();
            let bytes = encode(&record)?;
            if bytes == old {
                return Ok(record.public());
            }
            match self
                .authority
                .store
                .compare_exchange(&session_key(identity, id)?, Some(&old), &bytes)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(record.public()),
                CasOutcome::Conflict(Some(value)) => current = (value.bytes.clone(), decode(&value.bytes)?),
                CasOutcome::Conflict(None) => return Err(AuthorityError::NotFound),
            }
        }
        Err(AuthorityError::CursorConflict)
    }
    /// Fetch is read-only for prepared sessions and replays the same next token.
    /// Only creation, explicit resume and acknowledgement refresh admission.
    /// # Errors
    /// Rejects disconnected/closed sessions or unavailable storage.
    pub async fn next(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
        now: u64,
    ) -> Result<SessionBatch, AuthorityError> {
        let mut current = self.record(identity, id).await?;
        for _ in 0..64 {
            let (old, mut r) = current;
            if r.closed || now >= r.deadline {
                return Err(DatasetError::ReadCancelled.into());
            }
            self.authority
                .check_snapshot_readable(identity, r.snapshot)
                .await?;
            if let Some((next, end)) = r.issued.clone() {
                return Ok(SessionBatch {
                    snapshot: r.snapshot,
                    ids: self.load_ids(identity, id, r.position, end).await?,
                    fields: r.fields,
                    next,
                    end: end == r.count,
                });
            }
            // Old records may not have a prepared token yet. Upgrade once.
            r.prepare_next();
            match self
                .authority
                .store
                .compare_exchange(&session_key(identity, id)?, Some(&old), &encode(&r)?)
                .await?
            {
                CasOutcome::Applied(_) => current = (encode(&r)?, r),
                CasOutcome::Conflict(Some(value)) => current = (value.bytes.clone(), decode(&value.bytes)?),
                CasOutcome::Conflict(None) => return Err(AuthorityError::NotFound),
            }
        }
        Err(AuthorityError::CursorConflict)
    }
    /// # Errors
    /// Rejects forged, stale or divergent cursors. Retrying accepted progress is idempotent.
    pub async fn acknowledge(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
        expected: &str,
        next: &str,
        now: u64,
    ) -> Result<ReadSession, AuthorityError> {
        let mut current = self.record(identity, id).await?;
        for _ in 0..64 {
            let (old, mut r) = current;
            if r.closed || now >= r.deadline {
                return Err(DatasetError::ReadCancelled.into());
            }
            if r.cursor == next {
                return Ok(r.public());
            }
            if r.cursor != expected || !r.issued.as_ref().is_some_and(|(token, _)| token == next) {
                return Err(AuthorityError::CursorConflict);
            }
            let (_, position) = r.issued.take().ok_or(AuthorityError::CursorConflict)?;
            r.position = position;
            r.cursor = next.into();
            r.deadline = r.deadline.max(now.saturating_add(TTL));
            r.prepare_next();
            self.authority
                .check_snapshot_readable(identity, r.snapshot)
                .await?;
            let bytes = encode(&r)?;
            if bytes == old {
                return Ok(r.public());
            }
            match self
                .authority
                .store
                .compare_exchange(&session_key(identity, id)?, Some(&old), &bytes)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(r.public()),
                CasOutcome::Conflict(Some(value)) => current = (value.bytes.clone(), decode(&value.bytes)?),
                CasOutcome::Conflict(None) => return Err(AuthorityError::NotFound),
            }
        }
        Err(AuthorityError::CursorConflict)
    }
    /// Checks cancellation without advancing activity or acknowledged progress.
    /// # Errors
    /// Returns a closed/missing session or storage failure.
    pub async fn check_open(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
    ) -> Result<(), AuthorityError> {
        let (_, r) = self.record(identity, id).await?;
        if r.closed {
            return Err(DatasetError::ReadCancelled.into());
        }
        Ok(())
    }
    /// # Errors
    /// Returns missing sessions or storage failures. Other readers remain admitted.
    pub async fn close(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
    ) -> Result<ReadSession, AuthorityError> {
        let mut current = self.record(identity, id).await?;
        for _ in 0..64 {
            let (old, mut r) = current;
            r.closed = true;
            let bytes = encode(&r)?;
            if bytes == old {
                return Ok(r.public());
            }
            match self
                .authority
                .store
                .compare_exchange(&session_key(identity, id)?, Some(&old), &bytes)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(r.public()),
                CasOutcome::Conflict(Some(value)) => current = (value.bytes.clone(), decode(&value.bytes)?),
                CasOutcome::Conflict(None) => return Err(AuthorityError::NotFound),
            }
        }
        Err(AuthorityError::CursorConflict)
    }
}
