// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A bounded migration head fences legacy publishers before indexed conversion.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{LegacyManifestView, StreamDirectoryError, StreamName, StreamPurpose, StreamRootIdentity};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamLegacySourceId(pub [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMigrationHead {
    pub stream_name: StreamName,
    pub purpose: StreamPurpose,
    pub metadata_group_id: u64,
    pub writer_epoch: u64,
    pub owner_session: [u8; 16],
    pub generation: u64,
    pub legacy: StreamRootIdentity,
    pub source: StreamLegacySourceId,
}

impl StreamMigrationHead {
    /// # Errors
    /// Rejects invalid scope, stale adoption, generation overflow and empty identities.
    pub fn validate(&self) -> Result<(), StreamDirectoryError> {
        if self.metadata_group_id == 0
            || self.legacy.writer_epoch == 0
            || self.legacy.generation == 0
            || self.writer_epoch < self.legacy.writer_epoch
            || self.owner_session == [0; 16]
            || self.legacy.generation.checked_add(1) != Some(self.generation)
            || self.source.0 == [0; 32]
        {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        Ok(())
    }

    /// # Errors
    /// Checks the exact retained source bytes and fixed identity without allocating history.
    pub fn validate_source(&self, bytes: &[u8]) -> Result<(), StreamDirectoryError> {
        self.validate()?;
        if legacy_stream_source_id(bytes) != self.source {
            return Err(StreamDirectoryError::IdentityMismatch);
        }
        let source = LegacyManifestView::decode(bytes)?;
        if source.stream_name != self.stream_name
            || source.purpose != self.purpose
            || source.metadata_group_id != self.metadata_group_id
            || source.writer_epoch != self.legacy.writer_epoch
            || source.generation != self.legacy.generation
        {
            return Err(StreamDirectoryError::IdentityMismatch);
        }
        Ok(())
    }
}

#[must_use]
pub fn legacy_stream_source_id(bytes: &[u8]) -> StreamLegacySourceId {
    StreamLegacySourceId(Sha256::digest(bytes).into())
}
