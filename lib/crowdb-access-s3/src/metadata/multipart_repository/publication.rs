// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Publish the frozen multipart object through one object-key mutation.

use crowdb_access_multipart::{next_revision, MultipartComposer};
use sha2::{Digest as _, Sha256};

use super::{
    MetadataKey, MultipartPhase, MultipartRepository, MultipartRepositoryError, MultipartSessionRecord,
    PutIfAbsentOutcome,
};
use crate::integrity::multipart_checksum_marker;
use crate::metadata::ObjectRecord;

impl MultipartRepository {
    /// Publishes selected durable part locations without rereading part bytes.
    ///
    /// # Errors
    /// Rejects changed part generations or object predecessors and unconfirmed
    /// storage failures. A retry confirms only an exact published generation.
    pub async fn publish_completion(
        &self,
        session: &MultipartSessionRecord,
    ) -> Result<MultipartSessionRecord, MultipartRepositoryError> {
        let current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        if current.phase == MultipartPhase::Published {
            return Ok(current);
        }
        if current.phase != MultipartPhase::Publishing {
            return Err(MultipartRepositoryError::Conflict);
        }
        let selection = current
            .selection
            .as_ref()
            .ok_or(MultipartRepositoryError::Conflict)?;
        let mut composer = MultipartComposer::new(current.max_object_bytes);
        for selected in selection {
            if self
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
            composer
                .push(part.length, part.raw_md5, &part.locations)
                .map_err(|_| MultipartRepositoryError::InvalidPart)?;
        }
        let assembled = composer
            .finish()
            .map_err(|_| MultipartRepositoryError::InvalidPart)?;
        if assembled.length != current.staged_bytes || Some(&assembled.etag) != current.etag.as_ref() {
            return Err(MultipartRepositoryError::InvalidPart);
        }
        let published_at = current.publication_ms.ok_or(MultipartRepositoryError::Conflict)?;
        let marker = multipart_checksum_marker(&assembled.etag).ok_or(MultipartRepositoryError::Conflict)?;
        let object = ObjectRecord {
            bucket_id: current.bucket_id,
            key: current.object_key.clone(),
            logical_length: assembled.length,
            checksum: marker.to_vec(),
            etag: assembled.etag,
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
        let observed = self.store.get(key.clone()).await?;
        if observed.as_ref().is_some_and(|entry| entry.value == value) {
            return self.finish_publication(&current).await;
        }
        let expected_digest = current
            .object_predecessor
            .ok_or(MultipartRepositoryError::Conflict)?;
        if observed.as_ref().map(|entry| Sha256::digest(&entry.value).into()) != expected_digest {
            return Err(MultipartRepositoryError::Conflict);
        }
        let mutation = if let Some(before) = observed {
            self.store
                .compare_exchange(key.clone(), before.value, value.clone())
                .await
        } else {
            self.store
                .put_if_absent(key.clone(), value.clone())
                .await
                .map(|outcome| matches!(outcome, PutIfAbsentOutcome::Inserted { .. }))
        };
        match mutation {
            Ok(true) => {}
            Ok(false) => {
                if self.store.get(key).await?.as_ref().map(|entry| &entry.value) != Some(&value) {
                    return Err(MultipartRepositoryError::Conflict);
                }
            }
            Err(error) => {
                if self.store.get(key).await?.as_ref().map(|entry| &entry.value) != Some(&value) {
                    return Err(error.into());
                }
            }
        }
        self.finish_publication(&current).await
    }

    async fn finish_publication(
        &self,
        current: &MultipartSessionRecord,
    ) -> Result<MultipartSessionRecord, MultipartRepositoryError> {
        let mut next = current.clone();
        next.revision = next_revision(current.revision).ok_or(MultipartRepositoryError::Conflict)?;
        next.phase = MultipartPhase::Published;
        if self.exchange(current, &next).await? {
            Ok(next)
        } else {
            self.load(current)
                .await?
                .filter(|record| record.phase == MultipartPhase::Published)
                .ok_or(MultipartRepositoryError::Conflict)
        }
    }
}
