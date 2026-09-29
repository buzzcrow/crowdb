// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! CAS-backed multipart authority in the S3 Chunk-KV namespace.

use std::sync::Arc;

use super::{
    ChunkKvMetadataStore, MetadataKey, MetadataKeyError, MetadataStoreError, MultipartPartRecord,
    MultipartPhase, MultipartRecordError, MultipartSessionRecord, PutIfAbsentOutcome, TenantId,
};

mod completion;
mod listing;
mod publication;
mod terminal;

pub use completion::CompletionPart;
pub use listing::{MultipartPartPage, MultipartUploadPage};
pub use terminal::MultipartExpiryPage;

#[derive(Debug, thiserror::Error)]
pub enum MultipartRepositoryError {
    #[error(transparent)]
    Key(#[from] MetadataKeyError),
    #[error(transparent)]
    Record(#[from] MultipartRecordError),
    #[error(transparent)]
    Store(#[from] MetadataStoreError),
    #[error("multipart operation conflicts with durable state")]
    Conflict,
    #[error("multipart completion references a missing or changed part")]
    InvalidPart,
    #[error("a nonfinal multipart part is smaller than 5 MiB")]
    EntityTooSmall,
    #[error("multipart listing exhausted its bounded scan budget")]
    ScanBudgetExhausted,
}

pub struct MultipartRepository {
    store: Arc<ChunkKvMetadataStore>,
    tenant: TenantId,
}

impl MultipartRepository {
    #[must_use]
    pub fn new(store: Arc<ChunkKvMetadataStore>, tenant: TenantId) -> Self {
        Self { store, tenant }
    }

    /// Creates one upload or confirms the exact record after a lost reply.
    ///
    /// # Errors
    /// Rejects a different record at the same upload key or unavailable store.
    pub async fn begin(
        &self,
        session: &MultipartSessionRecord,
    ) -> Result<MultipartSessionRecord, MultipartRepositoryError> {
        if session.phase != MultipartPhase::Open || session.revision != 1 {
            return Err(MultipartRepositoryError::Conflict);
        }
        let value = session.encode()?;
        let key = self.session_key(session);
        let index = MetadataKey::multipart_upload_index(
            &self.tenant,
            session.bucket_id,
            &session.object_key,
            &session.upload_id,
        )?;
        let indexed = self.store.put_if_absent(index.clone(), key.clone()).await;
        match indexed {
            Ok(PutIfAbsentOutcome::Inserted { .. }) => {}
            Ok(PutIfAbsentOutcome::Existing(existing)) if existing.value == key => {}
            Ok(PutIfAbsentOutcome::Existing(_)) => return Err(MultipartRepositoryError::Conflict),
            Err(error) => {
                if !self
                    .store
                    .get(index)
                    .await?
                    .is_some_and(|entry| entry.value == key)
                {
                    return Err(error.into());
                }
            }
        }
        let result = self.store.put_if_absent(key, value.clone()).await;
        match result {
            Ok(PutIfAbsentOutcome::Inserted { .. }) => Ok(session.clone()),
            Ok(PutIfAbsentOutcome::Existing(existing)) if existing.value == value => Ok(session.clone()),
            Ok(PutIfAbsentOutcome::Existing(_)) => Err(MultipartRepositoryError::Conflict),
            Err(error) => {
                if self.load(session).await?.as_ref() == Some(session) {
                    Ok(session.clone())
                } else {
                    Err(error.into())
                }
            }
        }
    }

    /// Reads one upload through its exact bucket/object/upload identity.
    ///
    /// # Errors
    /// Rejects corrupt or foreign values and storage failures.
    pub async fn load(
        &self,
        identity: &MultipartSessionRecord,
    ) -> Result<Option<MultipartSessionRecord>, MultipartRepositoryError> {
        self.load_identity(identity.bucket_id, &identity.object_key, &identity.upload_id)
            .await
    }

    /// Reads one session through its bucket, object and upload identity.
    ///
    /// # Errors
    /// Rejects corrupt or foreign values and storage failures.
    pub async fn load_identity(
        &self,
        bucket: super::BucketId,
        object: &[u8],
        upload_id: &[u8; 16],
    ) -> Result<Option<MultipartSessionRecord>, MultipartRepositoryError> {
        let key = MetadataKey::multipart_upload_index(&self.tenant, bucket, object, upload_id)?;
        let Some(index) = self.store.get(key).await? else {
            return Ok(None);
        };
        let expected = MetadataKey::multipart_session(&self.tenant, bucket, upload_id);
        if index.value != expected {
            return Err(MultipartRepositoryError::Conflict);
        }
        self.store
            .get(expected)
            .await?
            .map(|value| {
                MultipartSessionRecord::decode(&value.value, bucket, object, upload_id).map_err(Into::into)
            })
            .transpose()
    }

    /// Moves one session phase under an exact-value CAS fence.
    ///
    /// # Errors
    /// Rejects invalid revisions, changed identity and uncertain storage
    /// writes that cannot be confirmed by a matching read.
    pub async fn exchange(
        &self,
        previous: &MultipartSessionRecord,
        next: &MultipartSessionRecord,
    ) -> Result<bool, MultipartRepositoryError> {
        if previous.bucket_id != next.bucket_id
            || previous.object_key != next.object_key
            || previous.upload_id != next.upload_id
            || previous.revision.checked_add(1) != Some(next.revision)
        {
            return Err(MultipartRepositoryError::Conflict);
        }
        let key = self.session_key(previous);
        let expected = previous.encode()?;
        let value = next.encode()?;
        match self.store.compare_exchange(key, expected, value).await {
            Ok(true) => Ok(true),
            Ok(false) => Ok(self.load(next).await?.as_ref() == Some(next)),
            Err(error) => {
                if self.load(next).await?.as_ref() == Some(next) {
                    Ok(true)
                } else {
                    Err(error.into())
                }
            }
        }
    }

    /// Reads one current part generation.
    ///
    /// # Errors
    /// Rejects corrupt or foreign records and storage failures.
    pub async fn part(
        &self,
        session: &MultipartSessionRecord,
        number: u16,
    ) -> Result<Option<MultipartPartRecord>, MultipartRepositoryError> {
        let key = MetadataKey::multipart_part(&self.tenant, session.bucket_id, &session.upload_id, number)?;
        self.store
            .get(key)
            .await?
            .map(|value| {
                MultipartPartRecord::decode(&value.value, session.bucket_id, &session.upload_id, number)
                    .map_err(Into::into)
            })
            .transpose()
    }

    /// Reads one immutable part generation, including replaced generations.
    ///
    /// # Errors
    /// Rejects corrupt or foreign generation bytes and unavailable storage.
    pub async fn part_generation(
        &self,
        session: &MultipartSessionRecord,
        number: u16,
        revision: u64,
    ) -> Result<Option<MultipartPartRecord>, MultipartRepositoryError> {
        let key = MetadataKey::multipart_part_generation(
            &self.tenant,
            session.bucket_id,
            &session.upload_id,
            number,
            revision,
        )?;
        self.store
            .get(key)
            .await?
            .map(|value| {
                let part =
                    MultipartPartRecord::decode(&value.value, session.bucket_id, &session.upload_id, number)?;
                if part.revision != revision {
                    return Err(MultipartRepositoryError::InvalidPart);
                }
                Ok(part)
            })
            .transpose()
    }

    /// Conditionally publishes an independently streamed part generation.
    ///
    /// Distinct part numbers write independent keys. The conservative
    /// `max_parts * max_part_bytes` bound prevents aggregate staged bytes from
    /// exceeding the session budget even when all parts arrive concurrently.
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
        let current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        if current.phase != MultipartPhase::Open
            || now_ms < current.created_ms
            || now_ms >= current.expires_ms
            || u64::from(current.max_parts)
                .checked_mul(current.max_part_bytes)
                .map_or(true, |bytes| bytes > current.max_staged_bytes)
            || part.bucket_id != current.bucket_id
            || part.upload_id != current.upload_id
            || part.length > current.max_part_bytes
            || part.number > current.max_parts
        {
            return Err(MultipartRepositoryError::Conflict);
        }
        let before = self.part(&current, part.number).await?;
        if let Some(existing) = &before {
            if existing.length == part.length && existing.raw_md5 == part.raw_md5 {
                return Ok(Some(existing.clone()));
            }
        }
        let mut after = part.clone();
        after.revision = before
            .as_ref()
            .map_or(Some(1), |before| before.revision.checked_add(1))
            .ok_or(MultipartRepositoryError::Conflict)?;
        after.modified_ms = now_ms;
        let value = after.encode()?;
        let generation_key = MetadataKey::multipart_part_generation(
            &self.tenant,
            current.bucket_id,
            &current.upload_id,
            part.number,
            after.revision,
        )?;
        let generation_write = self.store.put_if_absent(generation_key, value.clone()).await;
        match generation_write {
            Ok(PutIfAbsentOutcome::Inserted { .. }) => {}
            Ok(PutIfAbsentOutcome::Existing(existing)) if existing.value == value => {}
            Ok(PutIfAbsentOutcome::Existing(_)) => return Ok(None),
            Err(error) => {
                if self
                    .part_generation(&current, part.number, after.revision)
                    .await?
                    .as_ref()
                    != Some(&after)
                {
                    return Err(error.into());
                }
            }
        }
        let key =
            MetadataKey::multipart_part(&self.tenant, current.bucket_id, &current.upload_id, part.number)?;
        let outcome = if let Some(before) = &before {
            self.store
                .compare_exchange(key, before.encode()?, value.clone())
                .await
                .map(|applied| applied.then_some(after.clone()))
        } else {
            self.store
                .put_if_absent(key, value.clone())
                .await
                .map(|outcome| match outcome {
                    PutIfAbsentOutcome::Inserted { .. } => Some(after.clone()),
                    PutIfAbsentOutcome::Existing(existing) if existing.value == value => Some(after.clone()),
                    PutIfAbsentOutcome::Existing(_) => None,
                })
        };
        match outcome {
            Ok(Some(result)) => Ok(Some(result)),
            Ok(None) => Ok(self
                .part(&current, part.number)
                .await?
                .filter(|existing| existing == &after)),
            Err(error) => {
                if self.part(&current, part.number).await?.as_ref() == Some(&after) {
                    Ok(Some(after))
                } else {
                    Err(error.into())
                }
            }
        }
    }

    fn session_key(&self, session: &MultipartSessionRecord) -> Vec<u8> {
        MetadataKey::multipart_session(&self.tenant, session.bucket_id, &session.upload_id)
    }
}
