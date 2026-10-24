// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable part-pointer reservations shared with completion's session fence.

use crate::metadata::PendingPartMutation;
use crowdb_access_multipart::{
    live_at, next_part_revision, next_revision, reserve_part_accounting, PartAccounting,
};
use sha2::{Digest as _, Sha256};

use super::{
    MetadataKey, MultipartExchange, MultipartPartRecord, MultipartPhase, MultipartRepository,
    MultipartRepositoryError, MultipartSessionRecord, PutIfAbsentOutcome,
};

pub(super) struct PartProgress {
    pub(super) accepted: Option<MultipartPartRecord>,
    pub(super) current: MultipartSessionRecord,
}

impl MultipartRepository {
    /// Publishes a streamed part only while the upload remains open.
    ///
    /// The immutable generation is written first. A session CAS then reserves
    /// the current-part pointer mutation, so completion cannot freeze a stale
    /// pointer while an earlier part writer publishes. An abandoned generation
    /// remains available to R95's chunk-centered reclamation.
    ///
    /// # Errors
    /// Rejects closed, expired or foreign sessions, invalid parts and
    /// unconfirmed storage errors. Returns `None` if a reservation cannot settle
    /// and the durable session has not advanced. Concurrent session progress is
    /// re-evaluated without uploading the payload again.
    pub async fn put_stream_part(
        &self,
        session: &MultipartSessionRecord,
        part: &MultipartPartRecord,
        now_ms: u64,
    ) -> Result<Option<MultipartPartRecord>, MultipartRepositoryError> {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.put_part_until_settled(session, part, now_ms),
        )
        .await
        .map_err(|_| MultipartRepositoryError::Busy)?
    }

    async fn put_part_until_settled(
        &self,
        session: &MultipartSessionRecord,
        part: &MultipartPartRecord,
        now_ms: u64,
    ) -> Result<Option<MultipartPartRecord>, MultipartRepositoryError> {
        let mut current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        loop {
            let helping = current.pending.is_some();
            let progress = if helping {
                self.settle_current_part(&current).await?
            } else {
                self.reserve_stream_part(&current, part, now_ms).await?
            };
            if let Some(saved) = progress.accepted {
                if !helping
                    || (saved.number == part.number
                        && saved.length == part.length
                        && saved.raw_md5 == part.raw_md5)
                {
                    return Ok(Some(saved));
                }
            }
            current = progress.current;
        }
    }

    async fn reserve_stream_part(
        &self,
        current: &MultipartSessionRecord,
        part: &MultipartPartRecord,
        now_ms: u64,
    ) -> Result<PartProgress, MultipartRepositoryError> {
        if current.phase != MultipartPhase::Open
            || !live_at(current.created_ms, current.expires_ms, now_ms)
            || part.bucket_id != current.bucket_id
            || part.upload_id != current.upload_id
            || part.number == 0
            || part.number > current.max_parts
            || part.length > current.max_part_bytes
        {
            return Err(MultipartRepositoryError::Conflict);
        }
        let before = self.part(current, part.number).await?;
        if let Some(existing) = &before {
            if existing.length == part.length && existing.raw_md5 == part.raw_md5 {
                return Ok(PartProgress {
                    accepted: Some(existing.clone()),
                    current: current.clone(),
                });
            }
        }
        let mut after = part.clone();
        after.revision = next_part_revision(before.as_ref().map(|record| record.revision))
            .ok_or(MultipartRepositoryError::Conflict)?;
        after.modified_ms = now_ms;
        let accounting = reserve_part_accounting(
            PartAccounting {
                count: current.part_count,
                staged_bytes: current.staged_bytes,
            },
            before.as_ref().map(|record| record.length),
            after.length,
            current.max_parts,
            current.max_staged_bytes,
        )
        .map_err(|_| MultipartRepositoryError::InvalidPart)?;
        let after = self.persist_generation(current, after).await?;
        let mut reserved = current.clone();
        reserved.revision = next_revision(current.revision).ok_or(MultipartRepositoryError::Conflict)?;
        reserved.part_count = accounting.count;
        reserved.staged_bytes = accounting.staged_bytes;
        let before_digest = before
            .as_ref()
            .map(|record| record.encode().map(|value| Sha256::digest(value).into()))
            .transpose()?;
        reserved.pending = Some(PendingPartMutation {
            number: after.number,
            before_revision: before.as_ref().map(|record| record.revision),
            before_digest,
            after_revision: after.revision,
            after_digest: Sha256::digest(after.encode()?).into(),
            after_length: after.length,
        });
        match self.exchange_observed(current, &reserved).await? {
            MultipartExchange::Applied => self.finish_reserved_part(&reserved, after, before).await,
            MultipartExchange::Conflict(Some(observed)) => Ok(PartProgress {
                accepted: None,
                current: *observed,
            }),
            MultipartExchange::Conflict(None) => Err(MultipartRepositoryError::Conflict),
        }
    }

    async fn persist_generation(
        &self,
        session: &MultipartSessionRecord,
        mut after: MultipartPartRecord,
    ) -> Result<MultipartPartRecord, MultipartRepositoryError> {
        loop {
            let key = MetadataKey::multipart_part_generation(
                &self.tenant,
                session.bucket_id,
                &session.upload_id,
                after.number,
                after.revision,
            )?;
            let value = after.encode()?;
            match self.store.put_if_absent(key.clone(), value.clone()).await {
                Ok(PutIfAbsentOutcome::Inserted { .. }) => return Ok(after),
                Ok(PutIfAbsentOutcome::Existing(existing)) => {
                    let retained = MultipartPartRecord::decode(
                        &existing.value,
                        after.bucket_id,
                        &after.upload_id,
                        after.number,
                    )?;
                    if retained.revision != after.revision {
                        return Err(MultipartRepositoryError::InvalidPart);
                    }
                    if retained.length == after.length && retained.raw_md5 == after.raw_md5 {
                        return Ok(retained);
                    }
                    // A failed reservation may leave a different immutable candidate.
                    // Preserve it and select a fresh monotonically increasing revision.
                    after.revision =
                        next_part_revision(Some(after.revision)).ok_or(MultipartRepositoryError::Conflict)?;
                }
                Err(error) => {
                    if self
                        .store
                        .get(key)
                        .await?
                        .is_some_and(|entry| entry.value == value)
                    {
                        return Ok(after);
                    }
                    return Err(error.into());
                }
            }
        }
    }

    /// Helps a reserved pointer mutation after response loss or restart.
    ///
    /// # Errors
    /// Rejects changed immutable evidence or an unconfirmed metadata write.
    pub async fn settle_pending_part(
        &self,
        session: &MultipartSessionRecord,
    ) -> Result<Option<MultipartPartRecord>, MultipartRepositoryError> {
        Ok(self.settle_current_part(session).await?.accepted)
    }

    pub(super) async fn settle_current_part(
        &self,
        session: &MultipartSessionRecord,
    ) -> Result<PartProgress, MultipartRepositoryError> {
        let current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        if current != *session {
            return Ok(PartProgress {
                accepted: None,
                current,
            });
        }
        let pending = current
            .pending
            .as_ref()
            .ok_or(MultipartRepositoryError::Conflict)?;
        let after = self
            .part_generation(&current, pending.number, pending.after_revision)
            .await?
            .ok_or(MultipartRepositoryError::InvalidPart)?;
        let before = self.part(&current, pending.number).await?;
        self.finish_reserved_part(&current, after, before).await
    }

    async fn finish_reserved_part(
        &self,
        current: &MultipartSessionRecord,
        after: MultipartPartRecord,
        before: Option<MultipartPartRecord>,
    ) -> Result<PartProgress, MultipartRepositoryError> {
        let pending = current
            .pending
            .as_ref()
            .ok_or(MultipartRepositoryError::Conflict)?;
        let digest: [u8; 32] = Sha256::digest(after.encode()?).into();
        if after.length != pending.after_length || digest != pending.after_digest {
            return Err(MultipartRepositoryError::InvalidPart);
        }
        self.publish_reserved_pointer(current, pending, &after, before)
            .await?;
        let mut settled = current.clone();
        settled.revision = next_revision(current.revision).ok_or(MultipartRepositoryError::Conflict)?;
        settled.pending = None;
        match self.exchange_observed(current, &settled).await? {
            MultipartExchange::Applied => Ok(PartProgress {
                accepted: Some(after),
                current: settled,
            }),
            MultipartExchange::Conflict(Some(observed)) if observed.revision > current.revision => {
                Ok(PartProgress {
                    accepted: Some(after),
                    current: *observed,
                })
            }
            MultipartExchange::Conflict(Some(observed)) => Ok(PartProgress {
                accepted: None,
                current: *observed,
            }),
            MultipartExchange::Conflict(None) => Err(MultipartRepositoryError::Conflict),
        }
    }

    async fn publish_reserved_pointer(
        &self,
        session: &MultipartSessionRecord,
        pending: &PendingPartMutation,
        after: &MultipartPartRecord,
        current: Option<MultipartPartRecord>,
    ) -> Result<(), MultipartRepositoryError> {
        if current.as_ref() == Some(after) {
            return Ok(());
        }
        let before_digest = current
            .as_ref()
            .map(|record| record.encode().map(|value| Sha256::digest(value).into()))
            .transpose()?;
        if current.as_ref().map(|record| record.revision) != pending.before_revision
            || before_digest != pending.before_digest
        {
            return Err(MultipartRepositoryError::InvalidPart);
        }
        let key = MetadataKey::multipart_part(
            &self.tenant,
            session.bucket_id,
            &session.upload_id,
            pending.number,
        )?;
        let value = after.encode()?;
        let mutation = match current {
            Some(before) => {
                self.store
                    .compare_exchange_observed(key.clone(), before.encode()?, value.clone())
                    .await
            }
            None => self
                .store
                .put_if_absent(key.clone(), value.clone())
                .await
                .map(|outcome| match outcome {
                    PutIfAbsentOutcome::Inserted { .. } => (true, None),
                    PutIfAbsentOutcome::Existing(value) => (false, Some(value)),
                }),
        };
        match mutation {
            Ok((true, _)) => Ok(()),
            Ok((false, observed)) => {
                if observed.is_some_and(|observed| observed.value == value) {
                    Ok(())
                } else {
                    Err(MultipartRepositoryError::InvalidPart)
                }
            }
            Err(error) => {
                if self
                    .store
                    .get(key)
                    .await?
                    .is_some_and(|entry| entry.value == value)
                {
                    Ok(())
                } else {
                    Err(error.into())
                }
            }
        }
    }
}
