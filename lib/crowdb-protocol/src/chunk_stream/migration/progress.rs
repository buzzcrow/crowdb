// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use super::super::record::{decode_body, decode_envelope, encode_record, StreamRecordEncoding};
use super::super::{StreamDirectoryRef, MAX_STREAM_CHUNK_PHYSICAL_BYTES, MAX_STREAM_DIRECTORY_RECORD_BYTES};
use super::{LegacyManifestView, StreamDirectoryError, StreamMigrationHead};

const PROGRESS_KIND: u8 = 7;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamMigrationPhase {
    History { fence: u64, extent: u64 },
    Tail { physical: u64, sealed_end: u64 },
    Complete,
}

/// Bounded progress under the migration head fence. It is not a reader root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMigrationProgress {
    pub marker: StreamMigrationHead,
    pub sequence: u64,
    pub root: Option<StreamDirectoryRef>,
    pub legacy_root: Option<StreamDirectoryRef>,
    pub logical_tail: u64,
    pub phase: StreamMigrationPhase,
}

impl StreamMigrationProgress {
    /// # Errors
    /// Rejects malformed root checkpoints, sequence and physical scan bounds.
    pub fn validate(&self) -> Result<(), StreamDirectoryError> {
        self.marker.validate()?;
        if self.sequence == 0 {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        if let Some(root) = self.root {
            root.validate()?;
            if root.end_logical != self.logical_tail {
                return Err(StreamDirectoryError::InvalidBounds);
            }
        }
        if let Some(legacy) = self.legacy_root {
            legacy.validate()?;
            if self.root.map_or(true, |root| {
                root.first_logical != legacy.first_logical || legacy.end_logical > root.end_logical
            }) {
                return Err(StreamDirectoryError::InvalidBounds);
            }
        }
        if let StreamMigrationPhase::Tail { physical, sealed_end } = self.phase {
            if physical > sealed_end || sealed_end > MAX_STREAM_CHUNK_PHYSICAL_BYTES {
                return Err(StreamDirectoryError::InvalidBounds);
            }
        }
        Ok(())
    }

    /// # Errors
    /// Rejects progress outside its exact frozen source and phase coverage.
    pub fn validate_source(&self, source: &LegacyManifestView<'_>) -> Result<(), StreamDirectoryError> {
        self.validate()?;
        if self.marker.stream_name != source.stream_name
            || self.marker.purpose != source.purpose
            || self.marker.metadata_group_id != source.metadata_group_id
            || self.marker.legacy.writer_epoch != source.writer_epoch
            || self.marker.legacy.generation != source.generation
            || source.trim_offset > source.sealed_tail
        {
            return Err(StreamDirectoryError::IdentityMismatch);
        }
        let first = if source.fence_count() == 0 {
            source.sealed_tail
        } else {
            source.fence(0)?.first_logical
        };
        if first > source.trim_offset
            || self.root.is_some_and(|root| root.first_logical != first)
            || (self.root.is_none() && self.logical_tail != first)
        {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        match self.phase {
            StreamMigrationPhase::History { fence, extent } => {
                let count = source.fence_count() as u64;
                if fence > count
                    || self.logical_tail > source.sealed_tail
                    || (fence == count && (extent != 0 || self.logical_tail != source.sealed_tail))
                    || (fence < count && self.legacy_root.is_some())
                {
                    return Err(StreamDirectoryError::InvalidBounds);
                }
            }
            StreamMigrationPhase::Tail { physical, .. } => {
                let active = source
                    .active
                    .as_ref()
                    .ok_or(StreamDirectoryError::InvalidBounds)?;
                if physical < active.acknowledged_cursor || self.logical_tail < source.sealed_tail {
                    return Err(StreamDirectoryError::InvalidBounds);
                }
                self.validate_legacy_root(source)?;
            }
            StreamMigrationPhase::Complete => {
                if self.logical_tail < source.sealed_tail {
                    return Err(StreamDirectoryError::InvalidBounds);
                }
                self.validate_legacy_root(source)?;
            }
        }
        Ok(())
    }

    fn validate_legacy_root(&self, source: &LegacyManifestView<'_>) -> Result<(), StreamDirectoryError> {
        match self.legacy_root {
            Some(root) if root.end_logical == source.sealed_tail => Ok(()),
            None if source.fence_count() == 0 && source.trim_offset == source.sealed_tail => Ok(()),
            _ => Err(StreamDirectoryError::InvalidBounds),
        }
    }

    /// # Errors
    /// Rejects invalid progress or serialization failures.
    pub fn encode(&self) -> Result<Vec<u8>, StreamDirectoryError> {
        self.validate()?;
        encode_record(PROGRESS_KIND, self).map_err(|error| StreamDirectoryError::Codec(error.to_string()))
    }

    /// # Errors
    /// Rejects wrong versions/kinds, malformed bounds or trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, StreamDirectoryError> {
        if bytes.len() > MAX_STREAM_DIRECTORY_RECORD_BYTES {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        let body = decode_envelope(PROGRESS_KIND, bytes)
            .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
        let progress: Self = decode_body(body, StreamRecordEncoding::Version1)
            .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?
            .value;
        progress.validate()?;
        Ok(progress)
    }
}
