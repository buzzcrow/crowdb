// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Per-part conditional publication after one upload admission.

use crowdb_access_multipart::{live_at, next_part_revision};

use super::{
    MetadataKey, MultipartPartRecord, MultipartPhase, MultipartRepository, MultipartRepositoryError,
    MultipartSessionRecord, PutIfAbsentOutcome,
};

enum PartExchange {
    Applied,
    Conflict(Option<MultipartPartRecord>),
}

impl MultipartRepository {
    pub(super) async fn put_independent_part(
        &self,
        session: &MultipartSessionRecord,
        part: &MultipartPartRecord,
        now_ms: u64,
    ) -> Result<Option<MultipartPartRecord>, MultipartRepositoryError> {
        validate_part(session, part)?;
        if session.phase != MultipartPhase::Open || !live_at(session.created_ms, session.expires_ms, now_ms) {
            return Err(MultipartRepositoryError::Conflict);
        }
        let mut before = self.part(session, part.number).await?;
        if let Some(existing) = &before {
            if existing.length == part.length && existing.raw_md5 == part.raw_md5 {
                return Ok(Some(existing.clone()));
            }
        }
        loop {
            if let Some(existing) = &before {
                if existing.length == part.length && existing.raw_md5 == part.raw_md5 {
                    self.acknowledge_independent_part(session, existing, now_ms)
                        .await?;
                    return Ok(Some(existing.clone()));
                }
            }
            let mut after = part.clone();
            after.revision = next_part_revision(before.as_ref().map(|part| part.revision))
                .ok_or(MultipartRepositoryError::Conflict)?;
            after.modified_ms = now_ms;
            let after = self.persist_generation(session, after).await?;
            match self
                .exchange_independent_part(session, before.as_ref(), &after)
                .await?
            {
                PartExchange::Applied => {
                    self.acknowledge_independent_part(session, &after, now_ms).await?;
                    return Ok(Some(after));
                }
                PartExchange::Conflict(observed) => before = observed,
            }
        }
    }

    async fn exchange_independent_part(
        &self,
        session: &MultipartSessionRecord,
        before: Option<&MultipartPartRecord>,
        after: &MultipartPartRecord,
    ) -> Result<PartExchange, MultipartRepositoryError> {
        let key =
            MetadataKey::multipart_part(&self.tenant, session.bucket_id, &session.upload_id, after.number)?;
        let value = after.encode()?;
        let (applied, observed) = if let Some(before) = before {
            self.store
                .compare_exchange_observed(key, before.encode()?, value)
                .await?
        } else {
            match self.store.put_if_absent(key, value).await? {
                PutIfAbsentOutcome::Inserted { .. } => (true, None),
                PutIfAbsentOutcome::Existing(observed) => (false, Some(observed)),
            }
        };
        if applied {
            return Ok(PartExchange::Applied);
        }
        Ok(PartExchange::Conflict(
            observed
                .map(|observed| {
                    MultipartPartRecord::decode(
                        &observed.value,
                        session.bucket_id,
                        &session.upload_id,
                        after.number,
                    )
                })
                .transpose()?,
        ))
    }

    async fn acknowledge_independent_part(
        &self,
        session: &MultipartSessionRecord,
        part: &MultipartPartRecord,
        now_ms: u64,
    ) -> Result<(), MultipartRepositoryError> {
        let current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        validate_part(&current, part)?;
        if !current.uses_independent_parts() {
            return Err(MultipartRepositoryError::Conflict);
        }
        match current.phase {
            MultipartPhase::Open if live_at(current.created_ms, current.expires_ms, now_ms) => Ok(()),
            MultipartPhase::Publishing => Ok(()),
            _ => Err(MultipartRepositoryError::Conflict),
        }
    }
}

fn validate_part(
    session: &MultipartSessionRecord,
    part: &MultipartPartRecord,
) -> Result<(), MultipartRepositoryError> {
    if part.bucket_id != session.bucket_id
        || part.upload_id != session.upload_id
        || part.number == 0
        || part.number > session.max_parts
        || part.length > session.max_part_bytes
    {
        return Err(MultipartRepositoryError::Conflict);
    }
    Ok(())
}
