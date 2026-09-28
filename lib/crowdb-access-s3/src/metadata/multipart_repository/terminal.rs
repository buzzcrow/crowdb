// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Terminal multipart session transitions.

use super::{MultipartPhase, MultipartRepository, MultipartRepositoryError, MultipartSessionRecord};

impl MultipartRepository {
    /// Logically aborts an open upload, retaining part evidence for cleanup.
    ///
    /// # Errors
    /// Rejects an upload whose completion or publication already won.
    pub async fn abort(
        &self,
        session: &MultipartSessionRecord,
    ) -> Result<MultipartSessionRecord, MultipartRepositoryError> {
        let current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        if current.phase == MultipartPhase::Aborted {
            return Ok(current);
        }
        if current.phase != MultipartPhase::Open {
            return Err(MultipartRepositoryError::Conflict);
        }
        let mut next = current.clone();
        next.revision = current
            .revision
            .checked_add(1)
            .ok_or(MultipartRepositoryError::Conflict)?;
        next.phase = MultipartPhase::Aborted;
        if self.exchange(&current, &next).await? {
            Ok(next)
        } else {
            self.load(session)
                .await?
                .filter(|record| record.phase == MultipartPhase::Aborted)
                .ok_or(MultipartRepositoryError::Conflict)
        }
    }
}
