// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::metadata::{
    ChunkKvMetadataStore, MetadataKey, MetadataKeyError, MetadataRecordError, MetadataStoreError,
    ObjectRecord, TenantId,
};

#[derive(Debug, thiserror::Error)]
pub enum PublicationError {
    #[error("S3 metadata encoding failed: {0}")]
    Metadata(#[from] MetadataRecordError),
    #[error("S3 metadata key is invalid: {0}")]
    Key(#[from] MetadataKeyError),
    #[error("S3 metadata storage failed: {0}")]
    Store(#[from] MetadataStoreError),
}

/// Sealed metadata ready for one final object publication.
pub struct PublicationRequest {
    pub tenant: TenantId,
    pub object: ObjectRecord,
}

/// Publishes a completed object's complete metadata in one KV overwrite.
///
/// # Errors
///
/// Returns encoding or routed storage failure.
pub async fn publish(
    store: &ChunkKvMetadataStore,
    request: &mut PublicationRequest,
) -> Result<(), PublicationError> {
    let object_key = MetadataKey::object(&request.tenant, request.object.bucket_id, &request.object.key)?;
    store.put(object_key, request.object.encode()?).await?;
    Ok(())
}
