// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable object completion followed by exactly one metadata publication.

use super::{ObjectWriter, S3Executor, S3WritePolicy};
use crate::metadata::{BucketId, ChunkKvMetadataStore, MetadataKey, ObjectRecord, UserMetadata};
use crate::object::SelectedObject;
use crate::publication::{publish_versioned, PublicationRequest};
use crate::streaming::{attach_completed_locations, classify_publication_error, PutErrorCode, PutOutcome};
use crate::S3ErrorCode;
use crowdb_chunk_client::ChunkIoWriter;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct PutObjectInfo {
    pub content_length: Option<u64>,
    pub content_type: String,
    pub user_metadata: UserMetadata,
}

impl Default for PutObjectInfo {
    fn default() -> Self {
        Self {
            content_length: None,
            content_type: "application/octet-stream".into(),
            user_metadata: UserMetadata::default(),
        }
    }
}

pub struct PreparedPut {
    writer: ObjectWriter,
    store: Arc<ChunkKvMetadataStore>,
    publication: PublicationRequest,
    declared_length: Option<u64>,
}

impl S3Executor {
    /// Resolves the destination and reserves its writer before accepting input.
    /// # Errors
    /// Rejects invalid metadata, missing bucket or exhausted writer capacity.
    pub async fn prepare_put(
        &self,
        bucket: &[u8],
        key: &[u8],
        info: PutObjectInfo,
        policy: &S3WritePolicy,
    ) -> Result<PreparedPut, S3ErrorCode> {
        let bucket_id = self.resolve_bucket(bucket).await?;
        self.prepare_put_in_bucket(bucket_id, key, info, policy).await
    }

    pub(super) async fn prepare_put_in_bucket(
        &self,
        bucket_id: BucketId,
        key: &[u8],
        info: PutObjectInfo,
        policy: &S3WritePolicy,
    ) -> Result<PreparedPut, S3ErrorCode> {
        if info.content_type.is_empty() || info.content_length.is_some_and(|length| length > 5 << 40) {
            return Err(S3ErrorCode::InvalidRequest);
        }
        MetadataKey::object(&self.tenant, bucket_id, key).map_err(|_| S3ErrorCode::InvalidRequest)?;
        let mut route_key = Vec::with_capacity(self.tenant.as_bytes().len() + 16 + key.len());
        route_key.extend_from_slice(self.tenant.as_bytes());
        route_key.extend_from_slice(bucket_id.as_bytes());
        route_key.extend_from_slice(key);
        let attributes = info.user_metadata.encode()?;
        let writer = self
            .prepare_writer(info.content_length, &route_key, policy)
            .await?;
        Ok(PreparedPut {
            writer,
            store: Arc::clone(&self.storage.metadata),
            declared_length: info.content_length,
            publication: PublicationRequest {
                tenant: self.tenant.clone(),
                object: ObjectRecord {
                    bucket_id,
                    key: key.to_vec(),
                    logical_length: 0,
                    checksum: Vec::new(),
                    etag: String::new(),
                    created_at_ms: 0,
                    modified_at_ms: 0,
                    content_type: info.content_type,
                    attributes,
                    data_reference: Vec::new(),
                    data_length: 0,
                },
            },
        })
    }
}

impl PreparedPut {
    pub fn writer_mut(&mut self) -> &mut ObjectWriter {
        &mut self.writer
    }

    #[must_use]
    pub fn is_large(&self) -> bool {
        self.writer.is_large()
    }

    pub async fn abort(&mut self) {
        let _ = self.writer.on_error().await;
    }

    /// Seals data before publication. Uncertain publication retains its locations
    /// for subsequent reconciliation; physical reclamation remains deferred.
    /// # Errors
    /// Returns definite write/length/encoding failures or an uncertain mutation.
    pub async fn finish(mut self, etag: String, checksum: Vec<u8>) -> Result<SelectedObject, PutOutcome> {
        let locations = match self.writer.on_finish().await {
            Ok(locations) => locations,
            Err(error) => {
                self.abort().await;
                return Err(failure(PutErrorCode::ChunkWrite, error));
            }
        };
        let length = locations.iter().try_fold(0u64, |length, location| {
            length.checked_add(location.logical_length)
        });
        let length = length.ok_or_else(|| failure(PutErrorCode::ChunkWrite, "object length overflow"))?;
        if self.declared_length.is_some_and(|expected| expected != length) {
            return Err(failure(
                PutErrorCode::BodyRead,
                "body differs from declared length",
            ));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
            });
        let object = &mut self.publication.object;
        object.logical_length = length;
        object.data_length = length;
        object.etag = etag;
        object.checksum = checksum;
        object.created_at_ms = now;
        object.modified_at_ms = now;
        attach_completed_locations(object, &locations)
            .map_err(|error| failure(PutErrorCode::LocationEncoding, error))?;
        let revision = publish_versioned(&self.store, &self.publication)
            .await
            .map_err(classify_publication_error)?;
        Ok(SelectedObject {
            record: self.publication.object,
            revision,
        })
    }
}

fn failure(code: PutErrorCode, error: impl std::fmt::Display) -> PutOutcome {
    PutOutcome::Error {
        code,
        message: error.to_string(),
    }
}
