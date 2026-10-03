// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Versioned durable S3 multipart session and part values.

use bincode::Options as _;
use crowdb_access_multipart::{
    next_part_revision, validate_selected_parts, MultipartBounds, MultipartComposer, SelectedPart,
};
use crowdb_protocol::chunkdb::rpc::Location;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::BucketId;

const SESSION_MAGIC: [u8; 5] = *b"S3MS\x03";
const PART_MAGIC: [u8; 5] = *b"S3MP\x01";
const MAX_RECORD_BYTES: u64 = 1024 * 1024;
const MAX_OBJECT_KEY_BYTES: usize = 1024;
const MAX_CONTENT_TYPE_BYTES: usize = 1024;

pub use crowdb_access_multipart::MultipartPhase;

/// Creates a random upload identity whose byte order follows initiation time.
/// Uploads created in the same millisecond have an unspecified relative order.
#[must_use]
pub fn new_upload_id(now_ms: u64) -> [u8; 16] {
    let mut id = *uuid::Uuid::new_v4().as_bytes();
    id[..8].copy_from_slice(&now_ms.to_be_bytes());
    id
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultipartSessionRecord {
    pub bucket_id: BucketId,
    pub object_key: Vec<u8>,
    pub upload_id: [u8; 16],
    pub revision: u64,
    pub phase: MultipartPhase,
    pub created_ms: u64,
    pub expires_ms: u64,
    pub content_type: String,
    pub attributes: Vec<u8>,
    pub max_parts: u16,
    pub max_part_bytes: u64,
    pub max_object_bytes: u64,
    pub max_staged_bytes: u64,
    pub part_count: u16,
    pub staged_bytes: u64,
    pub pending: Option<PendingPartMutation>,
    pub selection: Option<Vec<SelectedPart>>,
    pub completion_request_digest: Option<[u8; 32]>,
    pub publication_ms: Option<u64>,
    pub object_predecessor: Option<Option<[u8; 32]>>,
    pub etag: Option<String>,
}

/// A durable session fence for publishing one current-part pointer.
/// The immutable after-generation is stored before reserving this mutation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PendingPartMutation {
    pub number: u16,
    pub before_revision: Option<u64>,
    pub before_digest: Option<[u8; 32]>,
    pub after_revision: u64,
    pub after_digest: [u8; 32],
    pub after_length: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MultipartPartRecord {
    pub bucket_id: BucketId,
    pub upload_id: [u8; 16],
    pub number: u16,
    pub revision: u64,
    pub modified_ms: u64,
    pub length: u64,
    pub raw_md5: [u8; 16],
    pub locations: Vec<Location>,
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum MultipartRecordError {
    #[error("invalid multipart record")]
    Invalid,
    #[error("multipart record exceeds its byte limit")]
    TooLarge,
    #[error("multipart record belongs to another key")]
    Identity,
}

impl MultipartSessionRecord {
    /// Encodes one validated session with a distinct schema version.
    ///
    /// # Errors
    /// Rejects incoherent phases, bounds and oversized records.
    pub fn encode(&self) -> Result<Vec<u8>, MultipartRecordError> {
        self.validate()?;
        encode(SESSION_MAGIC, self)
    }

    /// Decodes and binds one session to the requested bucket, key and upload.
    ///
    /// # Errors
    /// Rejects corrupt, oversized, foreign or incoherent values.
    pub fn decode(
        bytes: &[u8],
        bucket: BucketId,
        object: &[u8],
        upload_id: &[u8; 16],
    ) -> Result<Self, MultipartRecordError> {
        let record: Self = decode(SESSION_MAGIC, bytes)?;
        record.validate()?;
        if record.bucket_id != bucket || record.object_key != object || &record.upload_id != upload_id {
            return Err(MultipartRecordError::Identity);
        }
        Ok(record)
    }

    pub(crate) fn decode_unbound(bytes: &[u8]) -> Result<Self, MultipartRecordError> {
        let record: Self = decode(SESSION_MAGIC, bytes)?;
        record.validate()?;
        Ok(record)
    }

    fn validate(&self) -> Result<(), MultipartRecordError> {
        if self.object_key.is_empty()
            || self.object_key.len() > MAX_OBJECT_KEY_BYTES
            || self.upload_id == [0; 16]
            || self.revision == 0
            || self.created_ms >= self.expires_ms
            || self.content_type.len() > MAX_CONTENT_TYPE_BYTES
            || super::UserMetadata::decode(&self.attributes).is_err()
            || !(MultipartBounds {
                max_parts: self.max_parts,
                max_part_bytes: self.max_part_bytes,
                max_object_bytes: self.max_object_bytes,
                max_staged_bytes: self.max_staged_bytes,
            })
            .valid()
            || self.part_count > self.max_parts
            || self.staged_bytes > self.max_staged_bytes
        {
            return Err(MultipartRecordError::Invalid);
        }
        if let Some(selection) = &self.selection {
            validate_selected_parts(selection, self.max_parts).map_err(|_| MultipartRecordError::Invalid)?;
            if selection.len() > usize::from(self.part_count) {
                return Err(MultipartRecordError::Invalid);
            }
        }
        if let Some(pending) = &self.pending {
            if self.phase != MultipartPhase::Open
                || self.part_count == 0
                || pending.number == 0
                || pending.number > self.max_parts
                || pending.after_length > self.max_part_bytes
                || self.staged_bytes < pending.after_length
                || pending.before_revision.is_some() != pending.before_digest.is_some()
                || next_part_revision(pending.before_revision)
                    .map_or(true, |minimum| pending.after_revision < minimum)
            {
                return Err(MultipartRecordError::Invalid);
            }
        }
        let selected_count = self.selection.as_ref().map_or(0, Vec::len);
        match self.phase {
            MultipartPhase::Open
                if self.selection.is_none()
                    && self.completion_request_digest.is_none()
                    && self.publication_ms.is_none()
                    && self.object_predecessor.is_none()
                    && self.etag.is_none() =>
            {
                Ok(())
            }
            MultipartPhase::Completing
                if self.selection.is_some()
                    && self.completion_request_digest.is_some()
                    && self.publication_ms.is_none()
                    && self.object_predecessor.is_none()
                    && self.etag.is_none() =>
            {
                Ok(())
            }
            MultipartPhase::Publishing | MultipartPhase::Published
                if self.selection.is_some()
                    && self.completion_request_digest.is_some()
                    && self
                        .publication_ms
                        .is_some_and(|time| time >= self.created_ms && time < self.expires_ms)
                    && self.object_predecessor.is_some()
                    && self
                        .etag
                        .as_ref()
                        .is_some_and(|etag| valid_etag(etag, selected_count)) =>
            {
                Ok(())
            }
            MultipartPhase::Aborted if self.etag.is_none() => Ok(()),
            _ => Err(MultipartRecordError::Invalid),
        }
    }
}

impl MultipartPartRecord {
    /// Binds a completion selection to this exact persisted part generation.
    ///
    /// # Errors
    /// Rejects an invalid part record before calculating its identity.
    pub fn selection_digest(&self) -> Result<[u8; 32], MultipartRecordError> {
        Ok(Sha256::digest(self.encode()?).into())
    }

    /// Encodes one immutable selected part generation.
    ///
    /// # Errors
    /// Rejects invalid identity, locations or oversized records.
    pub fn encode(&self) -> Result<Vec<u8>, MultipartRecordError> {
        self.validate()?;
        encode(PART_MAGIC, self)
    }

    /// Decodes and binds a part to the requested bucket, upload and number.
    ///
    /// # Errors
    /// Rejects corrupt, oversized, foreign or incoherent values.
    pub fn decode(
        bytes: &[u8],
        bucket: BucketId,
        upload_id: &[u8; 16],
        number: u16,
    ) -> Result<Self, MultipartRecordError> {
        let record: Self = decode(PART_MAGIC, bytes)?;
        record.validate()?;
        if record.bucket_id != bucket || &record.upload_id != upload_id || record.number != number {
            return Err(MultipartRecordError::Identity);
        }
        Ok(record)
    }

    fn validate(&self) -> Result<(), MultipartRecordError> {
        if self.upload_id == [0; 16] || self.number == 0 || self.number > 10_000 || self.revision == 0 {
            return Err(MultipartRecordError::Invalid);
        }
        let mut composer = MultipartComposer::new(self.length);
        composer
            .push(self.length, self.raw_md5, &self.locations)
            .map_err(|_| MultipartRecordError::Invalid)?;
        composer.finish().map_err(|_| MultipartRecordError::Invalid)?;
        Ok(())
    }
}

fn valid_etag(etag: &str, selected_count: usize) -> bool {
    let Some((digest, count)) = etag.split_once('-') else {
        return false;
    };
    digest.len() == 32
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        && count
            .parse::<u16>()
            .is_ok_and(|count| count > 0 && usize::from(count) == selected_count)
}

fn encode<T: Serialize>(magic: [u8; 5], value: &T) -> Result<Vec<u8>, MultipartRecordError> {
    let mut bytes = magic.to_vec();
    let encoded = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_RECORD_BYTES - 5)
        .serialize(value)
        .map_err(|_| MultipartRecordError::TooLarge)?;
    bytes.extend_from_slice(&encoded);
    Ok(bytes)
}

fn decode<T: for<'de> Deserialize<'de>>(magic: [u8; 5], bytes: &[u8]) -> Result<T, MultipartRecordError> {
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(MultipartRecordError::TooLarge);
    }
    if bytes.get(..5) != Some(&magic[..]) {
        return Err(MultipartRecordError::Invalid);
    }
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_RECORD_BYTES - 5)
        .reject_trailing_bytes()
        .deserialize(&bytes[5..])
        .map_err(|_| MultipartRecordError::Invalid)
}
