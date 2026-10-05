// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded metadata-only storage extent inspection. This module has no chunk reader.

use bincode::Options;
use crowdb_protocol::chunkdb::rpc::Location;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::continuation::{ContinuationPosition, ContinuationTokenError, ContinuationTokenSigner};
use crate::metadata::{BucketId, ObjectRecord};

pub const MAX_REFERENCE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum InspectionError {
    #[error("inspection limit must be between 1 and 100")]
    Limit,
    #[error("object reference exceeds the 4 MiB inspection limit")]
    ReferenceLimit,
    #[error("object reference is corrupt or has inconsistent extents")]
    Reference,
    #[error("object generation changed; refresh storage locations from the first page")]
    Stale,
    #[error("invalid or expired inspection cursor")]
    Cursor,
    #[error("storage locations exceed the 1 MiB response limit")]
    ResponseLimit,
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageLocation {
    pub index: String,
    pub chunk_id: Option<String>,
    pub offset: String,
    pub length: String,
    pub logical_offset: String,
    pub logical_length: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageLocationPage {
    pub bucket: String,
    pub key: String,
    pub generation: String,
    pub etag: String,
    pub logical_length: String,
    pub locations: Vec<StorageLocation>,
    pub next_cursor: Option<String>,
}

pub struct LocationInspector {
    signer: ContinuationTokenSigner,
}

impl LocationInspector {
    /// # Errors
    /// Rejects an empty cursor signing key.
    pub fn new(key: Vec<u8>) -> Result<Self, InspectionError> {
        Ok(Self {
            signer: ContinuationTokenSigner::new(key).map_err(|_| InspectionError::Cursor)?,
        })
    }

    /// Inspects exactly one already-resolved record, without reading any payload.
    /// Revision participates in generation identity, including identical overwrites.
    /// # Errors
    /// Rejects corrupt/oversized references, stale continuations and invalid limits.
    pub fn page(
        &self,
        bucket: &str,
        record: &ObjectRecord,
        revision: u64,
        limit: usize,
        cursor: Option<&str>,
        now: u64,
    ) -> Result<StorageLocationPage, InspectionError> {
        if !(1..=100).contains(&limit) {
            return Err(InspectionError::Limit);
        }
        if record.data_reference.len() > MAX_REFERENCE_BYTES {
            return Err(InspectionError::ReferenceLimit);
        }
        let generation = generation(record, revision)?;
        let start = self.position(record.bucket_id, &record.key, &generation, cursor, now)?;
        let locations: Vec<Location> = if record.data_reference.is_empty() && record.logical_length == 0 {
            Vec::new()
        } else {
            bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_limit(MAX_REFERENCE_BYTES as u64)
                .reject_trailing_bytes()
                .deserialize(&record.data_reference)
                .map_err(|_| InspectionError::Reference)?
        };
        validate(&locations, record.logical_length)?;
        if start > locations.len() {
            return Err(InspectionError::Cursor);
        }
        let end = start.saturating_add(limit).min(locations.len());
        let next_cursor = if end < locations.len() {
            Some(
                self.signer
                    .encode(&ContinuationPosition {
                        bucket_id: record.bucket_id,
                        prefix: record.key.clone(),
                        delimiter: Some(generation.as_bytes().to_vec()),
                        last_key: u64::try_from(end)
                            .map_err(|_| InspectionError::Cursor)?
                            .to_be_bytes()
                            .to_vec(),
                        expires_at_unix_seconds: now.saturating_add(900),
                    })
                    .map_err(|_| InspectionError::Cursor)?,
            )
        } else {
            None
        };
        let page = StorageLocationPage {
            bucket: bucket.to_owned(),
            key: String::from_utf8(record.key.clone()).map_err(|_| InspectionError::Reference)?,
            generation,
            etag: record.etag.clone(),
            logical_length: record.logical_length.to_string(),
            next_cursor,
            locations: locations[start..end]
                .iter()
                .enumerate()
                .map(|(index, location)| StorageLocation {
                    index: (start + index).to_string(),
                    chunk_id: location
                        .chunk_id
                        .as_ref()
                        .map(|id| format!("{:016x}{:016x}", id.high, id.low)),
                    offset: location.offset.to_string(),
                    length: location.length.to_string(),
                    logical_offset: location.logical_offset.to_string(),
                    logical_length: location.logical_length.to_string(),
                })
                .collect(),
        };
        if serde_json::to_vec(&page)
            .map_err(|_| InspectionError::ResponseLimit)?
            .len()
            > MAX_RESPONSE_BYTES
        {
            return Err(InspectionError::ResponseLimit);
        }
        Ok(page)
    }

    fn position(
        &self,
        bucket: BucketId,
        key: &[u8],
        generation: &str,
        cursor: Option<&str>,
        now: u64,
    ) -> Result<usize, InspectionError> {
        let Some(cursor) = cursor else {
            return Ok(0);
        };
        let position = self
            .signer
            .decode_for_request(cursor, bucket, key, Some(generation.as_bytes()), now)
            .map_err(|error| {
                if error == ContinuationTokenError::RequestMismatch {
                    InspectionError::Stale
                } else {
                    InspectionError::Cursor
                }
            })?;
        let index = u64::from_be_bytes(
            position
                .last_key
                .try_into()
                .map_err(|_| InspectionError::Cursor)?,
        );
        usize::try_from(index).map_err(|_| InspectionError::Cursor)
    }
}

fn generation(record: &ObjectRecord, revision: u64) -> Result<String, InspectionError> {
    let encoded = record.encode().map_err(|_| InspectionError::Reference)?;
    let mut digest = Sha256::new();
    digest.update(b"crowdb-s3-location-generation-v1");
    digest.update(revision.to_be_bytes());
    digest.update(encoded);
    Ok(format!("{:x}", digest.finalize()))
}

fn validate(locations: &[Location], logical_length: u64) -> Result<(), InspectionError> {
    let mut position = 0;
    for location in locations {
        if location.logical_offset != position
            || location.logical_length == 0
            || location.length == 0
            || location.offset.checked_add(location.length).is_none()
        {
            return Err(InspectionError::Reference);
        }
        position = position
            .checked_add(location.logical_length)
            .ok_or(InspectionError::Reference)?;
    }
    if position != logical_length {
        return Err(InspectionError::Reference);
    }
    Ok(())
}
