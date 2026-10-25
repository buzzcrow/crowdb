// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_multipart::next_revision;

use super::{MultipartRepository, MultipartRepositoryError, MultipartSessionRecord};

pub(super) enum MultipartExchange {
    Applied,
    Conflict(Option<Box<MultipartSessionRecord>>),
}

impl MultipartRepository {
    /// Moves one upload under an exact-value CAS fence.
    /// # Errors
    /// Rejects invalid revisions, changed identity and unconfirmed storage writes.
    pub async fn exchange(
        &self,
        previous: &MultipartSessionRecord,
        next: &MultipartSessionRecord,
    ) -> Result<bool, MultipartRepositoryError> {
        Ok(match self.exchange_observed(previous, next).await? {
            MultipartExchange::Applied => true,
            MultipartExchange::Conflict(observed) => observed.as_deref() == Some(next),
        })
    }

    pub(super) async fn exchange_observed(
        &self,
        previous: &MultipartSessionRecord,
        next: &MultipartSessionRecord,
    ) -> Result<MultipartExchange, MultipartRepositoryError> {
        if previous.bucket_id != next.bucket_id
            || previous.object_key != next.object_key
            || previous.upload_id != next.upload_id
            || previous.uses_independent_parts() != next.uses_independent_parts()
            || next_revision(previous.revision) != Some(next.revision)
        {
            return Err(MultipartRepositoryError::Conflict);
        }
        let expected = previous.expected_value()?;
        let value = next.encode()?;
        match self
            .store
            .compare_exchange_observed(self.session_key(previous), expected, value)
            .await?
        {
            (true, _) => Ok(MultipartExchange::Applied),
            (false, observed) => Ok(MultipartExchange::Conflict(
                observed
                    .map(|observed| {
                        let current = MultipartSessionRecord::decode(
                            &observed.value,
                            previous.bucket_id,
                            &previous.object_key,
                            &previous.upload_id,
                        )?;
                        if current.uses_independent_parts() != previous.uses_independent_parts() {
                            return Err(MultipartRepositoryError::Conflict);
                        }
                        Ok(current)
                    })
                    .transpose()?
                    .map(Box::new),
            )),
        }
    }
}
