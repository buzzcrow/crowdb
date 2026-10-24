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
            || next_revision(previous.revision) != Some(next.revision)
        {
            return Err(MultipartRepositoryError::Conflict);
        }
        let expected = previous.encode()?;
        let value = next.encode()?;
        match self
            .store
            .compare_exchange_observed(self.session_key(previous), expected, value)
            .await
        {
            Ok((true, _)) => Ok(MultipartExchange::Applied),
            Ok((false, observed)) => Ok(MultipartExchange::Conflict(
                observed
                    .map(|observed| {
                        MultipartSessionRecord::decode(
                            &observed.value,
                            previous.bucket_id,
                            &previous.object_key,
                            &previous.upload_id,
                        )
                    })
                    .transpose()?
                    .map(Box::new),
            )),
            Err(error) => {
                if self.load(next).await?.as_ref() == Some(next) {
                    Ok(MultipartExchange::Applied)
                } else {
                    Err(error.into())
                }
            }
        }
    }
}
