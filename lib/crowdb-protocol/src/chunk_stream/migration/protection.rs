// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::super::record::{decode_body, decode_envelope, encode_record, StreamRecordEncoding};
use super::super::{
    StreamDirectoryPageId, StreamDirectoryRef, StreamName, StreamPublicationId,
    MAX_STREAM_DIRECTORY_RECORD_BYTES, MAX_STREAM_PUBLICATION_PAGES,
};
use super::{StreamDirectoryError, StreamMigrationHead};

const MIGRATION_GUARD_KIND: u8 = 6;

/// Protects the current conversion root and its bounded path mutation, including
/// the completed historical root while a durable legacy tail is reconstructed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMigrationGuard {
    pub marker: StreamMigrationHead,
    pub sequence: u64,
    pub base: Option<StreamDirectoryRef>,
    pub root: Option<StreamDirectoryRef>,
    pub legacy_root: Option<StreamDirectoryRef>,
    #[serde(deserialize_with = "super::super::directory::bounded::publication_pages")]
    pub pages: Vec<StreamDirectoryPageId>,
}

impl StreamMigrationGuard {
    /// # Errors
    /// Rejects oversized page sets and inconsistent conversion roots.
    pub fn validate(&self) -> Result<(), StreamDirectoryError> {
        self.marker.validate()?;
        if self.sequence == 0
            || self.pages.len() > MAX_STREAM_PUBLICATION_PAGES
            || self.pages.iter().any(|id| id.0 == [0; 32])
            || self.pages.iter().collect::<HashSet<_>>().len() != self.pages.len()
        {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        for reference in [self.base, self.root, self.legacy_root].into_iter().flatten() {
            reference.validate()?;
        }
        if let Some(base) = self.base {
            if self.root.map_or(true, |root| {
                root.first_logical != base.first_logical || root.end_logical < base.end_logical
            }) {
                return Err(StreamDirectoryError::InvalidBounds);
            }
        }
        if let Some(root) = self.root {
            if self.base != Some(root) && !self.pages.contains(&root.id) {
                return Err(StreamDirectoryError::InvalidBounds);
            }
        }
        if let Some(legacy) = self.legacy_root {
            if self.root.map_or(true, |root| {
                root.first_logical != legacy.first_logical || root.end_logical < legacy.end_logical
            }) {
                return Err(StreamDirectoryError::InvalidBounds);
            }
        }
        Ok(())
    }
}

/// # Errors
/// Rejects invalid protection or serialization errors. The kind disambiguates its
/// canonical-byte hash from the ordinary indexed-head publication identity.
pub fn encode_stream_migration_guard(
    guard: &StreamMigrationGuard,
) -> Result<(StreamPublicationId, Vec<u8>), StreamDirectoryError> {
    guard.validate()?;
    let bytes = encode_record(MIGRATION_GUARD_KIND, guard)
        .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
    Ok((StreamPublicationId(Sha256::digest(&bytes).into()), bytes))
}

/// # Errors
/// Rejects wrong identity, kind/version, invented lengths, bounds and trailing bytes.
pub fn decode_stream_migration_guard(
    bytes: &[u8],
    expected: StreamPublicationId,
    name: StreamName,
) -> Result<StreamMigrationGuard, StreamDirectoryError> {
    if bytes.len() > MAX_STREAM_DIRECTORY_RECORD_BYTES {
        return Err(StreamDirectoryError::InvalidBounds);
    }
    let actual: [u8; 32] = Sha256::digest(bytes).into();
    if actual != expected.0 {
        return Err(StreamDirectoryError::IdentityMismatch);
    }
    let body = decode_envelope(MIGRATION_GUARD_KIND, bytes)
        .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
    let guard: StreamMigrationGuard = decode_body(body, StreamRecordEncoding::Version1)
        .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?
        .value;
    guard.validate()?;
    if guard.marker.stream_name != name {
        return Err(StreamDirectoryError::IdentityMismatch);
    }
    Ok(guard)
}
