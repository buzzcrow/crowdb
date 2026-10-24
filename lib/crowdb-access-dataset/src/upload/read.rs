// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;

use super::{decode_part, part_key, DatasetUploads, PartState, UploadError, UploadPart, UploadState};

/// Immutable upload description for one admitted read. Own this in the request,
/// not in a cross-request admission cache. It does not register or pin a reader;
/// concurrent reclamation may still make a part read fail.
pub struct UploadReadDescriptor {
    pub(super) uploads: DatasetUploads,
    location: Vec<u8>,
    length: u64,
    part_size: u32,
    parts: u32,
    md5: [u8; 16],
}

impl DatasetUploads {
    /// Loads and validates the upload once for all parts of this read request.
    /// # Errors
    /// Rejects missing, malformed or uncommitted uploads.
    pub async fn open_read(&self, location: &[u8]) -> Result<UploadReadDescriptor, UploadError> {
        let record = self.session(location).await?;
        if !matches!(record.state, UploadState::Complete | UploadState::Claimed) {
            return Err(UploadError::Conflict);
        }
        Ok(UploadReadDescriptor {
            uploads: self.clone(),
            location: location.to_vec(),
            length: record.length,
            part_size: record.part_size,
            parts: super::part_count(&record),
            md5: record.md5,
        })
    }
}

impl UploadReadDescriptor {
    #[must_use]
    pub fn length(&self) -> u64 {
        self.length
    }

    #[must_use]
    pub fn md5(&self) -> [u8; 16] {
        self.md5
    }

    #[must_use]
    pub fn part_count(&self) -> u32 {
        self.parts
    }

    pub(crate) fn part_size(&self) -> u32 {
        self.part_size
    }

    pub(super) async fn part(&self, index: u32) -> Result<UploadPart, UploadError> {
        if index >= self.part_count() {
            return Err(UploadError::Invalid);
        }
        let value = self
            .uploads
            .store
            .get(&part_key(&self.location, index))
            .await?
            .ok_or(UploadError::NotFound)?;
        let entry = decode_part(&value.bytes)?;
        let start = u64::from(index) * u64::from(self.part_size);
        if entry.state != PartState::Accepted
            || entry.part.index != index
            || entry.part.length != (self.length - start).min(u64::from(self.part_size))
        {
            return Err(UploadError::Integrity);
        }
        Ok(entry.part)
    }

    /// Returns a bounded part and its verified digest without rereading the upload.
    /// # Errors
    /// Rejects invalid boundaries, reclaimed parts and corrupt or unavailable data.
    pub async fn read_part_verified(&self, index: u32) -> Result<(Bytes, [u8; 16]), UploadError> {
        let part = self.part(index).await?;
        Ok((self.uploads.verified_part(&part).await?, part.md5))
    }
}
