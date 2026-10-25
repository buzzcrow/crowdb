// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! CAS-backed multipart authority in the S3 Chunk-KV namespace.

use std::sync::Arc;

use super::{
    ChunkKvMetadataStore, MetadataKey, MetadataKeyError, MetadataStoreError, MultipartPartRecord,
    MultipartPhase, MultipartRecordError, MultipartSessionRecord, PutIfAbsentOutcome, TenantId,
};

mod completion;
mod exchange;
mod listing;
mod parts;
mod publication;
mod terminal;

pub use completion::CompletionPart;
use exchange::MultipartExchange;
pub use listing::{MultipartPartPage, MultipartUploadPage};
pub use publication::MultipartCompletion;
pub use terminal::{MultipartExpiryError, MultipartExpiryPage, MultipartExpiryStep, MultipartExpirySweep};

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
    #[error("multipart mutation is still settling")]
    Busy,
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

    /// Creates one upload or reuses the exact record on an explicit retry.
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
        match self.store.put_if_absent(index, key.clone()).await? {
            PutIfAbsentOutcome::Inserted { .. } => {}
            PutIfAbsentOutcome::Existing(existing) if existing.value == key => {}
            PutIfAbsentOutcome::Existing(_) => return Err(MultipartRepositoryError::Conflict),
        }
        match self.store.put_if_absent(key, value.clone()).await? {
            PutIfAbsentOutcome::Inserted { .. } => Ok(session.clone()),
            PutIfAbsentOutcome::Existing(existing) if existing.value == value => Ok(session.clone()),
            PutIfAbsentOutcome::Existing(_) => Err(MultipartRepositoryError::Conflict),
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
        self.store
            .get(self.session_key(identity))
            .await?
            .map(|value| {
                MultipartSessionRecord::decode(
                    &value.value,
                    identity.bucket_id,
                    &identity.object_key,
                    &identity.upload_id,
                )
                .map_err(Into::into)
            })
            .transpose()
    }

    /// Reads the authority bound to the bucket, object and upload identity.
    ///
    /// # Errors
    /// Rejects corrupt or foreign values and storage failures.
    pub async fn load_identity(
        &self,
        bucket: super::BucketId,
        object: &[u8],
        upload_id: &[u8; 16],
    ) -> Result<Option<MultipartSessionRecord>, MultipartRepositoryError> {
        MetadataKey::object(&self.tenant, bucket, object)?;
        let key = MetadataKey::multipart_session(&self.tenant, bucket, upload_id);
        self.store.get(key).await?.map_or(Ok(None), |value| {
            match MultipartSessionRecord::decode(&value.value, bucket, object, upload_id) {
                Ok(session) => Ok(Some(session)),
                Err(MultipartRecordError::Identity) => Ok(None),
                Err(error) => Err(error.into()),
            }
        })
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

    fn session_key(&self, session: &MultipartSessionRecord) -> Vec<u8> {
        MetadataKey::multipart_session(&self.tenant, session.bucket_id, &session.upload_id)
    }
}
