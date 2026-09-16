// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Metadata-only object lookup, ordered listing, and logical deletion.

use crate::continuation::{ContinuationPosition, ContinuationTokenError, ContinuationTokenSigner};
use crate::metadata::{
    BucketId, ChunkKvMetadataStore, MetadataKey, MetadataKeyError, MetadataRecordError, MetadataStoreError,
    ObjectRecord, TenantId,
};

/// A bounded ordered page from one bucket's direct object-key interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectListPage {
    pub objects: Vec<ObjectRecord>,
}

/// Bounded, normalized `ListObjectsV2` inputs.
pub struct ListObjectsV2Request<'a> {
    pub tenant: &'a TenantId,
    pub bucket: BucketId,
    pub prefix: &'a [u8],
    pub delimiter: Option<&'a [u8]>,
    pub start_after: Option<&'a [u8]>,
    pub continuation_token: Option<&'a str>,
    pub max_keys: usize,
    pub max_scan_items: usize,
    pub max_scan_bytes: usize,
    pub now_unix_seconds: u64,
    pub token_ttl_seconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListObjectsV2Page {
    pub objects: Vec<ObjectRecord>,
    pub common_prefixes: Vec<Vec<u8>>,
    pub next_continuation_token: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectMetadataError {
    #[error("S3 metadata key is invalid: {0}")]
    Key(#[from] MetadataKeyError),
    #[error("S3 object metadata is invalid: {0}")]
    Record(#[from] MetadataRecordError),
    #[error("S3 metadata storage failed: {0}")]
    Store(#[from] MetadataStoreError),
    #[error("invalid S3 continuation token: {0:?}")]
    Continuation(#[from] ContinuationTokenError),
    #[error("invalid S3 listing limit")]
    InvalidListLimit,
}

/// Reads exactly one published object record. This is the HEAD metadata path;
/// it never contacts chunk storage.
///
/// # Errors
///
/// Returns invalid metadata-key, record, or storage errors.
pub async fn head(
    store: &ChunkKvMetadataStore,
    tenant: &TenantId,
    bucket: BucketId,
    key: &[u8],
) -> Result<Option<ObjectRecord>, ObjectMetadataError> {
    let key = MetadataKey::object(tenant, bucket, key)?;
    store
        .get(key)
        .await?
        .map(|value| ObjectRecord::decode(&value.value))
        .transpose()
        .map_err(Into::into)
}

/// Lists directly from the tenant/bucket prefix range. The value is the one
/// published object record, so no visibility side record or per-object lookup
/// is needed.
///
/// # Errors
///
/// Returns invalid record or routed scan errors.
pub async fn list(
    store: &ChunkKvMetadataStore,
    tenant: &TenantId,
    bucket: BucketId,
    max_items: usize,
    max_bytes: usize,
) -> Result<ObjectListPage, ObjectMetadataError> {
    let values = store
        .scan(
            MetadataKey::object_prefix(tenant, bucket),
            MetadataKey::object_end(tenant, bucket),
            max_items,
            max_bytes,
        )
        .await?;
    let objects = values
        .into_iter()
        .map(|value| ObjectRecord::decode(&value.value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ObjectListPage { objects })
}

/// Performs one stateless, non-snapshot `ListObjectsV2` page.
///
/// The scan starts directly in the encoded object-prefix interval. A token
/// resumes strictly after its last scanned object, so delimiter collapsing
/// cannot make an unchanged key loop or reappear across access-server nodes.
///
/// # Errors
///
/// Returns before scanning for invalid limits or continuation tokens.
pub async fn list_v2(
    store: &ChunkKvMetadataStore,
    signer: &ContinuationTokenSigner,
    request: &ListObjectsV2Request<'_>,
) -> Result<ListObjectsV2Page, ObjectMetadataError> {
    if request.max_scan_items == 0 || request.max_scan_bytes == 0 {
        return Err(ObjectMetadataError::InvalidListLimit);
    }
    if request.max_keys == 0 {
        return Ok(ListObjectsV2Page {
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            next_continuation_token: None,
        });
    }
    let resume_key = if let Some(token) = request.continuation_token {
        Some(
            signer
                .decode_for_request(
                    token,
                    request.bucket,
                    request.prefix,
                    request.delimiter,
                    request.now_unix_seconds,
                )?
                .last_key,
        )
    } else {
        request.start_after.map(<[u8]>::to_vec)
    };
    let prefix_start = MetadataKey::object_key_prefix(request.tenant, request.bucket, request.prefix)?;
    let start = match resume_key.as_deref() {
        Some(key) => MetadataKey::object_after(request.tenant, request.bucket, key)?.max(prefix_start),
        None => prefix_start,
    };
    let end = MetadataKey::object_key_prefix_end(request.tenant, request.bucket, request.prefix)?;
    let scan_limit = request.max_scan_items.saturating_add(1);
    let values = store.scan(start, end, scan_limit, request.max_scan_bytes).await?;
    let has_scan_lookahead = values.len() > request.max_scan_items;
    let mut records = values
        .into_iter()
        .take(request.max_scan_items)
        .map(|value| ObjectRecord::decode(&value.value))
        .collect::<Result<Vec<_>, _>>()?;
    let mut objects = Vec::with_capacity(request.max_keys.min(records.len()));
    let mut common_prefixes = Vec::new();
    let mut last_common_prefix: Option<Vec<u8>> = None;
    let mut last_scanned = None;
    let mut stopped_for_output_limit = false;

    for record in records.drain(..) {
        last_scanned = Some(record.key.clone());
        if let Some(common_prefix) = common_prefix(&record.key, request.prefix, request.delimiter) {
            if last_common_prefix.as_deref() != Some(&common_prefix) {
                last_common_prefix = Some(common_prefix.clone());
                common_prefixes.push(common_prefix);
            }
        } else {
            objects.push(record);
        }
        if objects.len() + common_prefixes.len() == request.max_keys {
            stopped_for_output_limit = true;
            break;
        }
    }

    let truncated = has_scan_lookahead || stopped_for_output_limit;
    let next_continuation_token = if truncated {
        last_scanned
            .map(|last_key| {
                signer.encode(&ContinuationPosition {
                    bucket_id: request.bucket,
                    prefix: request.prefix.to_vec(),
                    delimiter: request.delimiter.map(<[u8]>::to_vec),
                    last_key,
                    expires_at_unix_seconds: request
                        .now_unix_seconds
                        .saturating_add(request.token_ttl_seconds),
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(ListObjectsV2Page {
        objects,
        common_prefixes,
        next_continuation_token,
    })
}

fn common_prefix(key: &[u8], prefix: &[u8], delimiter: Option<&[u8]>) -> Option<Vec<u8>> {
    let delimiter = delimiter.filter(|value| !value.is_empty())?;
    let suffix = key.strip_prefix(prefix)?;
    find_subslice(suffix, delimiter).map(|position| {
        let end = prefix.len() + position + delimiter.len();
        key[..end].to_vec()
    })
}

fn find_subslice(value: &[u8], needle: &[u8]) -> Option<usize> {
    value
        .windows(needle.len())
        .position(|candidate| candidate == needle)
}

/// Deletes the direct object-key value. Physical chunk reclamation is deferred
/// and must never delay this logical disappearance.
///
/// # Errors
///
/// Returns invalid metadata-key or routed deletion errors.
pub async fn delete(
    store: &ChunkKvMetadataStore,
    tenant: &TenantId,
    bucket: BucketId,
    key: &[u8],
) -> Result<(), ObjectMetadataError> {
    store
        .delete_idempotent(MetadataKey::object(tenant, bucket, key)?)
        .await?;
    Ok(())
}
