// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable Dataset uploads with independent immutable part records.

mod index;
mod ownership;
mod parts;
mod registry;
mod retirement;

use bytes::Bytes;
use md5::{Digest, Md5};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;

use crate::{CasOutcome, DatasetIdentity, DatasetStore, OperationId, PayloadStorage, StoreError};

pub const MAX_UPLOAD_PART_BYTES: usize = 4 * 1024 * 1024;
const MAX_PARTS: u64 = 10_000;
const WRITE_LEASE_SECONDS: u64 = 60;

#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("invalid upload request")]
    Invalid,
    #[error("upload was not found")]
    NotFound,
    #[error("upload content or state conflicts")]
    Conflict,
    #[error("upload integrity check failed")]
    Integrity,
    #[error("upload is awaiting durable storage recovery")]
    Pending,
    #[error("upload registry capacity exhausted")]
    Capacity,
    #[error("upload storage unavailable: {0}")]
    Storage(String),
    #[error(transparent)]
    Store(#[from] StoreError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadState {
    Open,
    Complete,
    Claimed,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UploadPart {
    pub index: u32,
    pub length: u64,
    pub md5: [u8; 16],
    pub locations: Vec<crowdb_protocol::chunkdb::rpc::Location>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UploadRecord {
    pub version: u16,
    pub length: u64,
    pub part_size: u32,
    pub md5: [u8; 16],
    pub expires_at: u64,
    pub state: UploadState,
    #[serde(skip)]
    pub parts: Vec<UploadPart>,
    owners: Vec<OperationId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    retired_owners: Vec<OperationId>,
    registry_page: Option<u64>,
    #[serde(default)]
    registry_generation: Option<OperationId>,
    #[serde(default)]
    registry_fence: Option<OperationId>,
    pub cleanup_complete: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
enum PartState {
    Reserved,
    Intent,
    Accepted,
    Reclaimed,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PartEntry {
    version: u16,
    owner: OperationId,
    deadline: u64,
    state: PartState,
    part: UploadPart,
}

/// All synchronization uses storage CAS on the session or its particular part.
#[derive(Clone)]
pub struct DatasetUploads {
    store: Arc<dyn DatasetStore>,
    payloads: Arc<dyn PayloadStorage>,
}
impl DatasetUploads {
    #[must_use]
    pub fn new(store: Arc<dyn DatasetStore>, payloads: Arc<dyn PayloadStorage>) -> Self {
        Self { store, payloads }
    }

    /// # Errors
    /// Rejects invalid Dataset identity.
    pub fn location(identity: &DatasetIdentity, id: OperationId) -> Result<Vec<u8>, UploadError> {
        let mut key = b"DS\0U\0".to_vec();
        key.extend(identity.key().map_err(|_| UploadError::Invalid)?);
        key.extend(id.as_bytes());
        Ok(key)
    }

    /// # Errors
    /// Rejects invalid declarations, divergent retries and unavailable storage.
    pub async fn create(
        &self,
        location: &[u8],
        length: u64,
        part_size: u32,
        md5: [u8; 16],
        expires_at: u64,
    ) -> Result<UploadRecord, UploadError> {
        if length == 0
            || part_size == 0
            || part_size as usize > MAX_UPLOAD_PART_BYTES
            || length.div_ceil(u64::from(part_size)) > MAX_PARTS
        {
            return Err(UploadError::Invalid);
        }
        let record = UploadRecord {
            version: 2,
            length,
            part_size,
            md5,
            expires_at,
            state: UploadState::Open,
            parts: Vec::new(),
            owners: Vec::new(),
            retired_owners: Vec::new(),
            registry_page: None,
            registry_generation: None,
            registry_fence: None,
            cleanup_complete: false,
        };
        let created = match self
            .store
            .compare_exchange(location, None, &encode(&record)?)
            .await?
        {
            CasOutcome::Applied(_) => true,
            CasOutcome::Conflict(Some(value)) => {
                let existing = decode_session(&value.bytes)?;
                if existing.length != length || existing.part_size != part_size || existing.md5 != md5 {
                    return Err(UploadError::Conflict);
                }
                false
            }
            CasOutcome::Conflict(None) => return Err(UploadError::Conflict),
        };
        self.register(location).await?;
        if created {
            self.session(location).await
        } else {
            self.get(location).await
        }
    }

    /// # Errors
    /// Returns missing or malformed session/part metadata.
    pub async fn get(&self, location: &[u8]) -> Result<UploadRecord, UploadError> {
        let mut session = self.session(location).await?;
        session.parts = self.accepted_parts(location, &session).await?;
        Ok(session)
    }
    async fn session(&self, location: &[u8]) -> Result<UploadRecord, UploadError> {
        decode_session(
            &self
                .store
                .get(location)
                .await?
                .ok_or(UploadError::NotFound)?
                .bytes,
        )
    }

    /// Returns bounded session metadata without enumerating parts.
    /// # Errors
    /// Returns missing, malformed or unavailable metadata.
    pub async fn metadata(&self, location: &[u8]) -> Result<UploadRecord, UploadError> {
        self.session(location).await
    }

    /// Recovers a scoped public identifier from an opaque Dataset-owned locator.
    /// # Errors
    /// Rejects foreign or malformed locators.
    pub fn upload_id(identity: &DatasetIdentity, location: &[u8]) -> Result<OperationId, UploadError> {
        let prefix = Self::location(
            identity,
            OperationId::from_bytes(&[1; 16]).map_err(|_| UploadError::Invalid)?,
        )?;
        let prefix = &prefix[..prefix.len() - 16];
        let id = location.strip_prefix(prefix).ok_or(UploadError::Invalid)?;
        OperationId::from_bytes(id).map_err(|_| UploadError::Invalid)
    }
    async fn entries(&self, location: &[u8], session: &UploadRecord) -> Result<Vec<PartEntry>, UploadError> {
        use futures::{stream, StreamExt, TryStreamExt};
        stream::iter(0..part_count(session))
            .map(|index| async move {
                self.store
                    .get(&part_key(location, index))
                    .await?
                    .map(|v| decode_part(&v.bytes))
                    .transpose()
            })
            .buffered(16)
            .try_filter_map(|entry| async move { Ok(entry) })
            .try_collect()
            .await
    }

    /// # Errors
    /// Rejects missing parts, incomplete uploads and corrupt stored bytes.
    pub async fn read_part(&self, location: &[u8], index: u32) -> Result<Bytes, UploadError> {
        self.read_part_verified(location, index)
            .await
            .map(|(bytes, _)| bytes)
    }

    /// Returns verified bytes and their already verified frame digest.
    /// # Errors
    /// Rejects unavailable, uncommitted or corrupt parts.
    pub async fn read_part_verified(
        &self,
        location: &[u8],
        index: u32,
    ) -> Result<(Bytes, [u8; 16]), UploadError> {
        let session = self.session(location).await?;
        if !matches!(session.state, UploadState::Complete | UploadState::Claimed) {
            return Err(UploadError::Conflict);
        }
        let entry = self
            .store
            .get(&part_key(location, index))
            .await?
            .ok_or(UploadError::NotFound)?;
        let entry = decode_part(&entry.bytes)?;
        if entry.state != PartState::Accepted {
            return Err(UploadError::Conflict);
        }
        Ok((self.verified_part(&entry.part).await?, entry.part.md5))
    }
    async fn verified_part(&self, part: &UploadPart) -> Result<Bytes, UploadError> {
        let bytes = self.payloads.read(&part.locations).await?;
        if bytes.len() as u64 != part.length || digest(&bytes) != part.md5 {
            return Err(UploadError::Integrity);
        }
        Ok(bytes)
    }
}
fn part_count(session: &UploadRecord) -> u32 {
    u32::try_from(session.length.div_ceil(u64::from(session.part_size))).unwrap_or(0)
}
fn part_key(location: &[u8], index: u32) -> Vec<u8> {
    let mut key = location.to_vec();
    key.extend_from_slice(b"\0P");
    key.extend_from_slice(&index.to_be_bytes());
    key
}
fn digest(bytes: &[u8]) -> [u8; 16] {
    Md5::digest(bytes).into()
}
fn encode<T: Serialize>(record: &T) -> Result<Vec<u8>, UploadError> {
    serde_json::to_vec(record).map_err(|_| UploadError::Integrity)
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, UploadError> {
    serde_json::from_slice(bytes).map_err(|_| UploadError::Integrity)
}
fn decode_session(bytes: &[u8]) -> Result<UploadRecord, UploadError> {
    let record: UploadRecord = decode(bytes)?;
    if !matches!(record.version, 2 | 3)
        || (record.version == 2 && !record.retired_owners.is_empty())
        || record.retired_owners.len() > 4096
        || record.retired_owners.windows(2).any(|pair| pair[0] >= pair[1])
        || record
            .owners
            .iter()
            .any(|owner| record.retired_owners.binary_search(owner).is_ok())
        || record.length == 0
        || record.part_size == 0
        || record.part_size as usize > MAX_UPLOAD_PART_BYTES
        || record.length.div_ceil(u64::from(record.part_size)) > MAX_PARTS
        || record.owners.len() > 256
    {
        return Err(UploadError::Integrity);
    }
    Ok(record)
}
fn decode_part(bytes: &[u8]) -> Result<PartEntry, UploadError> {
    let record: PartEntry = decode(bytes)?;
    if record.version != 1
        || record.part.length == 0
        || record.part.length > MAX_UPLOAD_PART_BYTES as u64
        || record.part.locations.len() > 32
    {
        return Err(UploadError::Integrity);
    }
    Ok(record)
}

mod reclaim;
pub use reclaim::{SnapshotUploadCleanup, UploadReclaimError};
