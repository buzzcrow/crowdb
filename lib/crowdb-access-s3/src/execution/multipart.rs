// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared multipart identity, admission and completion over the durable repository.

mod part;
pub use part::PreparedPart;

use super::{CopyMetadata, S3Executor};
use crate::metadata::{
    new_upload_id, CompletionPart, MetadataStoreError, MultipartCompletion, MultipartPhase,
    MultipartRepository, MultipartRepositoryError, MultipartSessionRecord,
};
use crate::S3ErrorCode;
use crowdb_chunk_kv_client::ClientError;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug)]
pub enum MultipartFailure {
    S3(S3ErrorCode),
    Uncertain,
}

impl MultipartFailure {
    #[must_use]
    pub fn code(&self) -> S3ErrorCode {
        match self {
            Self::S3(code) => *code,
            Self::Uncertain => S3ErrorCode::ServiceUnavailable,
        }
    }
}

impl S3Executor {
    #[must_use]
    pub fn multipart_repository(&self) -> MultipartRepository {
        MultipartRepository::new(self.storage.metadata.clone(), self.tenant.clone())
    }

    /// # Errors
    /// Returns namespace/admission failures or an unconfirmed session mutation.
    pub async fn begin_multipart(
        &self,
        bucket: &[u8],
        key: &[u8],
        metadata: CopyMetadata,
    ) -> Result<MultipartSessionRecord, MultipartFailure> {
        let bucket_id = self.resolve_bucket(bucket).await.map_err(MultipartFailure::S3)?;
        if metadata.content_type.is_empty() {
            return Err(MultipartFailure::S3(S3ErrorCode::InvalidRequest));
        }
        let now = unix_millis();
        let session = MultipartSessionRecord {
            bucket_id,
            object_key: key.to_vec(),
            upload_id: new_upload_id(now),
            revision: 1,
            phase: MultipartPhase::Open,
            created_ms: now,
            expires_ms: now.saturating_add(7 * 24 * 60 * 60 * 1000),
            content_type: metadata.content_type,
            attributes: metadata.user_metadata.encode().map_err(MultipartFailure::S3)?,
            max_parts: 10_000,
            max_part_bytes: 5 << 30,
            max_object_bytes: 5 << 40,
            max_staged_bytes: 10_000 * (5 << 30),
            part_count: 0,
            staged_bytes: 0,
            pending: None,
            selection: None,
            legacy_observation: None,
        };
        self.multipart_repository()
            .begin(&session)
            .await
            .map_err(mutation_error)
    }

    /// # Errors
    /// Returns a missing/expired identity or unavailable metadata. Expiry follows
    /// the same durable abort path for HTTP and native callers.
    pub async fn multipart_session(
        &self,
        bucket: &[u8],
        key: &[u8],
        upload_id: &[u8; 16],
        abort: bool,
    ) -> Result<MultipartSessionRecord, MultipartFailure> {
        let bucket_id = self.resolve_bucket(bucket).await.map_err(MultipartFailure::S3)?;
        let repository = self.multipart_repository();
        let session = repository
            .load_identity(bucket_id, key, upload_id)
            .await
            .map_err(|error| MultipartFailure::S3(repository_error(&error)))?
            .ok_or(MultipartFailure::S3(S3ErrorCode::NoSuchUpload))?;
        self.check_multipart_expiry(session, abort).await
    }

    /// Refreshes retained identity before admitting another part or completion.
    /// # Errors
    /// Returns missing/expired identity or metadata/expiry-abort failure.
    pub async fn refresh_multipart(
        &self,
        session: &MultipartSessionRecord,
        abort: bool,
    ) -> Result<MultipartSessionRecord, MultipartFailure> {
        let current = self
            .multipart_repository()
            .load(session)
            .await
            .map_err(|error| MultipartFailure::S3(repository_error(&error)))?
            .ok_or(MultipartFailure::S3(S3ErrorCode::NoSuchUpload))?;
        self.check_multipart_expiry(current, abort).await
    }

    async fn check_multipart_expiry(
        &self,
        session: MultipartSessionRecord,
        abort: bool,
    ) -> Result<MultipartSessionRecord, MultipartFailure> {
        let repository = self.multipart_repository();
        if session.phase == MultipartPhase::Open && session.expires_ms <= unix_millis() {
            let expired = repository.abort_observed(session).await.map_err(mutation_error)?;
            if !abort {
                return Err(MultipartFailure::S3(S3ErrorCode::NoSuchUpload));
            }
            return Ok(expired);
        }
        Ok(session)
    }

    /// # Errors
    /// Returns a frozen-selection conflict or an unconfirmed object publication.
    pub async fn complete_multipart(
        &self,
        session: &MultipartSessionRecord,
        requested: &[CompletionPart],
    ) -> Result<MultipartCompletion, MultipartFailure> {
        if requested.is_empty() || requested.len() > 10_000 {
            return Err(MultipartFailure::S3(S3ErrorCode::InvalidRequest));
        }
        if requested.windows(2).any(|pair| pair[0].number >= pair[1].number) {
            return Err(MultipartFailure::S3(S3ErrorCode::InvalidPartOrder));
        }
        let repository = self.multipart_repository();
        let current = self.refresh_multipart(session, false).await?;
        let frozen = repository
            .freeze_current(current, requested, unix_millis())
            .await
            .map_err(mutation_error)?
            .ok_or(MultipartFailure::S3(S3ErrorCode::ServiceUnavailable))?;
        repository
            .publish_frozen(frozen, Some(requested))
            .await
            .map_err(mutation_error)
    }

    /// # Errors
    /// Returns a conflicting identity or unconfirmed abort mutation.
    pub async fn abort_multipart(&self, session: &MultipartSessionRecord) -> Result<(), MultipartFailure> {
        self.multipart_repository()
            .abort(session)
            .await
            .map_err(mutation_error)?;
        Ok(())
    }
}

pub(super) fn mutation_error(error: MultipartRepositoryError) -> MultipartFailure {
    match error {
        MultipartRepositoryError::Store(MetadataStoreError::Client(
            ClientError::Deadline | ClientError::Transport(_),
        )) => MultipartFailure::Uncertain,
        other => MultipartFailure::S3(repository_error(&other)),
    }
}

#[must_use]
pub fn repository_error(error: &MultipartRepositoryError) -> S3ErrorCode {
    match error {
        MultipartRepositoryError::Key(_) | MultipartRepositoryError::Record(_) => S3ErrorCode::InvalidRequest,
        MultipartRepositoryError::Store(_) => S3ErrorCode::ServiceUnavailable,
        MultipartRepositoryError::Conflict => S3ErrorCode::NoSuchUpload,
        MultipartRepositoryError::Busy | MultipartRepositoryError::ScanBudgetExhausted => {
            S3ErrorCode::SlowDown
        }
        MultipartRepositoryError::InvalidPart => S3ErrorCode::InvalidPart,
        MultipartRepositoryError::EntityTooSmall => S3ErrorCode::EntityTooSmall,
    }
}

pub(super) fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
