// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Publish the frozen multipart object through one object-key mutation.

use crowdb_access_multipart::MultipartComposer;
use std::time::{SystemTime, UNIX_EPOCH};

use super::{
    CompletionPart, MetadataKey, MultipartPhase, MultipartRepository, MultipartRepositoryError,
    MultipartSessionRecord,
};
use crate::integrity::multipart_checksum_marker;
use crate::metadata::ObjectRecord;

/// Request-local result of an acknowledged object publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartCompletion {
    pub upload_id: [u8; 16],
    pub object_key: Vec<u8>,
    pub logical_length: u64,
    pub etag: String,
}

impl MultipartRepository {
    /// Publishes selected durable part locations without rereading part bytes.
    ///
    /// # Errors
    /// Rejects changed part generations and unconfirmed storage failures.
    /// Every explicit retry performs the same unconditional object overwrite.
    pub async fn publish_completion(
        &self,
        session: &MultipartSessionRecord,
    ) -> Result<MultipartCompletion, MultipartRepositoryError> {
        let current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        self.publish_frozen(current, None).await
    }

    // The caller just selected this bound authority. Independent uploads use
    // frozen immutable generations; strict uploads also recheck current pointers.
    pub(crate) async fn publish_frozen(
        &self,
        current: MultipartSessionRecord,
        requested: Option<&[CompletionPart]>,
    ) -> Result<MultipartCompletion, MultipartRepositoryError> {
        if current.phase != MultipartPhase::Publishing {
            return Err(MultipartRepositoryError::Conflict);
        }
        let selection = current
            .selection
            .as_ref()
            .ok_or(MultipartRepositoryError::Conflict)?;
        let mut composer = MultipartComposer::new(current.max_object_bytes);
        for (index, selected) in selection.iter().enumerate() {
            if !current.uses_independent_parts()
                && self
                    .part(&current, selected.number)
                    .await?
                    .as_ref()
                    .map(|part| part.revision)
                    != Some(selected.revision)
            {
                return Err(MultipartRepositoryError::InvalidPart);
            }
            let part = self
                .part_generation(&current, selected.number, selected.revision)
                .await?
                .ok_or(MultipartRepositoryError::InvalidPart)?;
            if part.revision != selected.revision || part.selection_digest()? != selected.digest {
                return Err(MultipartRepositoryError::InvalidPart);
            }
            if requested.is_some_and(|parts| {
                parts.get(index).map_or(true, |request| {
                    request.number != selected.number
                        || !super::completion::etag_matches(&request.etag, &part.raw_md5)
                })
            }) {
                return Err(MultipartRepositoryError::Conflict);
            }
            composer
                .push(part.length, part.raw_md5, &part.locations)
                .map_err(|_| MultipartRepositoryError::InvalidPart)?;
        }
        let assembled = composer
            .finish()
            .map_err(|_| MultipartRepositoryError::InvalidPart)?;
        if assembled.length != current.staged_bytes {
            return Err(MultipartRepositoryError::InvalidPart);
        }
        let published_at = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let marker = multipart_checksum_marker(&assembled.etag).ok_or(MultipartRepositoryError::Conflict)?;
        let object = ObjectRecord {
            bucket_id: current.bucket_id,
            key: current.object_key.clone(),
            logical_length: assembled.length,
            checksum: marker.to_vec(),
            etag: assembled.etag.clone(),
            created_at_ms: published_at,
            modified_at_ms: published_at,
            content_type: current.content_type.clone(),
            attributes: current.attributes.clone(),
            data_reference: bincode::serialize(&assembled.locations)
                .map_err(|_| MultipartRepositoryError::Conflict)?,
            data_length: assembled.length,
        };
        let value = object.encode().map_err(|_| MultipartRepositoryError::Conflict)?;
        let key = MetadataKey::object(&self.tenant, current.bucket_id, &current.object_key)?;
        self.store.put(key, value).await?;
        Ok(MultipartCompletion {
            upload_id: current.upload_id,
            object_key: current.object_key,
            logical_length: assembled.length,
            etag: assembled.etag,
        })
    }
}
