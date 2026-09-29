// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable part-pointer reservations shared with completion's session fence.

use crate::metadata::PendingPartMutation;
use crowdb_access_multipart::{
    live_at, next_part_revision, next_revision, reserve_part_accounting, PartAccounting,
};
use sha2::{Digest as _, Sha256};

use super::{
    MetadataKey, MultipartPartRecord, MultipartPhase, MultipartRepository, MultipartRepositoryError,
    MultipartSessionRecord, PutIfAbsentOutcome,
};

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
    /// unconfirmed storage errors. A competing writer returns `None`.
    pub async fn put_stream_part(
        &self,
        session: &MultipartSessionRecord,
        part: &MultipartPartRecord,
        now_ms: u64,
    ) -> Result<Option<MultipartPartRecord>, MultipartRepositoryError> {
        for _ in 0..2 {
            let current = self
                .load(session)
                .await?
                .ok_or(MultipartRepositoryError::Conflict)?;
            if current.pending.is_some() {
                self.settle_pending_part(&current).await?;
                continue;
            }
            return self.reserve_stream_part(&current, part, now_ms).await;
        }
        Ok(None)
    }

    async fn reserve_stream_part(
        &self,
        current: &MultipartSessionRecord,
        part: &MultipartPartRecord,
        now_ms: u64,
    ) -> Result<Option<MultipartPartRecord>, MultipartRepositoryError> {
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
                return Ok(Some(existing.clone()));
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
        if !self.persist_generation(current, &after).await? {
            return Ok(None);
        }
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
        if !self.exchange(current, &reserved).await? {
            return Ok(None);
        }
        self.settle_pending_part(&reserved).await
    }

    async fn persist_generation(
        &self,
        session: &MultipartSessionRecord,
        after: &MultipartPartRecord,
    ) -> Result<bool, MultipartRepositoryError> {
        let key = MetadataKey::multipart_part_generation(
            &self.tenant,
            session.bucket_id,
            &session.upload_id,
            after.number,
            after.revision,
        )?;
        let value = after.encode()?;
        match self.store.put_if_absent(key.clone(), value.clone()).await {
            Ok(PutIfAbsentOutcome::Inserted { .. }) => Ok(true),
            Ok(PutIfAbsentOutcome::Existing(existing)) if existing.value == value => Ok(true),
            Ok(PutIfAbsentOutcome::Existing(_)) => Ok(false),
            Err(error) => {
                if self
                    .store
                    .get(key)
                    .await?
                    .is_some_and(|entry| entry.value == value)
                {
                    Ok(true)
                } else {
                    Err(error.into())
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
        let current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        if current != *session {
            return Ok(None);
        }
        let pending = current
            .pending
            .as_ref()
            .ok_or(MultipartRepositoryError::Conflict)?;
        let after = self
            .part_generation(&current, pending.number, pending.after_revision)
            .await?
            .ok_or(MultipartRepositoryError::InvalidPart)?;
        let digest: [u8; 32] = Sha256::digest(after.encode()?).into();
        if after.length != pending.after_length || digest != pending.after_digest {
            return Err(MultipartRepositoryError::InvalidPart);
        }
        self.publish_reserved_pointer(&current, pending, &after).await?;
        let mut settled = current.clone();
        settled.revision = next_revision(current.revision).ok_or(MultipartRepositoryError::Conflict)?;
        settled.pending = None;
        if self.exchange(&current, &settled).await? {
            return Ok(Some(after));
        }
        let latest = self
            .load(&current)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        if latest.pending.is_none() && self.part(&latest, after.number).await?.as_ref() == Some(&after) {
            Ok(Some(after))
        } else {
            Ok(None)
        }
    }

    async fn publish_reserved_pointer(
        &self,
        session: &MultipartSessionRecord,
        pending: &PendingPartMutation,
        after: &MultipartPartRecord,
    ) -> Result<(), MultipartRepositoryError> {
        let current = self.part(session, pending.number).await?;
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
                    .compare_exchange(key.clone(), before.encode()?, value.clone())
                    .await
            }
            None => self
                .store
                .put_if_absent(key.clone(), value.clone())
                .await
                .map(|outcome| matches!(outcome, PutIfAbsentOutcome::Inserted { .. })),
        };
        match mutation {
            Ok(true) => Ok(()),
            Ok(false) => {
                if self
                    .store
                    .get(key)
                    .await?
                    .is_some_and(|entry| entry.value == value)
                {
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
