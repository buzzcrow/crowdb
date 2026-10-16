// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    encode, session_key, AuthorityError, CasOutcome, DatasetError, DatasetIdentity, DatasetSessions,
    OperationId, ReadSession, SessionBatch, TTL,
};

impl DatasetSessions {
    /// Resumes acknowledged progress, including after a disconnected lease expired.
    /// # Errors
    /// Rejects closed/reclaimed sessions or unavailable storage.
    pub async fn resume(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
        now: u64,
    ) -> Result<ReadSession, AuthorityError> {
        for _ in 0..64 {
            let (old, mut record) = self.record(identity, id).await?;
            if record.closed {
                return Err(DatasetError::ReadCancelled.into());
            }
            self.authority
                .require_committed_snapshot(identity, record.snapshot)
                .await?;
            self.authority
                .touch_read_handle(identity, record.snapshot, id, now, TTL)
                .await?;
            record.deadline = now.saturating_add(TTL);
            if matches!(
                self.authority
                    .store
                    .compare_exchange(&session_key(identity, id)?, Some(&old), &encode(&record)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(record.public());
            }
        }
        Err(AuthorityError::CursorConflict)
    }
    /// Fetch never advances acknowledged progress. Identical fetches replay the issued batch.
    /// # Errors
    /// Rejects disconnected/closed sessions or unavailable storage.
    pub async fn next(
        &self,
        identity: &DatasetIdentity,
        id: OperationId,
        now: u64,
    ) -> Result<SessionBatch, AuthorityError> {
        for _ in 0..64 {
            let (old, mut r) = self.record(identity, id).await?;
            if r.closed || now >= r.deadline {
                return Err(DatasetError::ReadCancelled.into());
            }
            let end = r.position.saturating_add(r.batch_size).min(r.count);
            let next = r
                .issued
                .get_or_insert_with(|| (OperationId::random().to_string(), end))
                .0
                .clone();
            r.deadline = now.saturating_add(TTL);
            self.authority
                .touch_read_handle(identity, r.snapshot, id, now, TTL)
                .await?;
            if matches!(
                self.authority
                    .store
                    .compare_exchange(&session_key(identity, id)?, Some(&old), &encode(&r)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(SessionBatch {
                    snapshot: r.snapshot,
                    ids: self.load_ids(identity, id, r.position, end).await?,
                    fields: r.fields,
                    next,
                    end: end == r.count,
                });
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
        for _ in 0..64 {
            let (old, mut r) = self.record(identity, id).await?;
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
            r.deadline = now.saturating_add(TTL);
            self.authority
                .touch_read_handle(identity, r.snapshot, id, now, TTL)
                .await?;
            if matches!(
                self.authority
                    .store
                    .compare_exchange(&session_key(identity, id)?, Some(&old), &encode(&r)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                return Ok(r.public());
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
        for _ in 0..64 {
            let (old, mut r) = self.record(identity, id).await?;
            r.closed = true;
            if matches!(
                self.authority
                    .store
                    .compare_exchange(&session_key(identity, id)?, Some(&old), &encode(&r)?)
                    .await?,
                CasOutcome::Applied(_)
            ) {
                self.authority
                    .release_read_handle(identity, r.snapshot, id)
                    .await?;
                return Ok(r.public());
            }
        }
        Err(AuthorityError::CursorConflict)
    }
}
