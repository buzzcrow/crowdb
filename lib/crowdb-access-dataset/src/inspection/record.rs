// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{FieldLocator, SnapshotId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum InspectionState {
    Ready,
    Unsupported { reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSummary {
    pub version: u16,
    pub snapshot: SnapshotId,
    pub parent: Option<SnapshotId>,
    pub schema_version: u32,
    pub field_count: u32,
    pub sample_count: u64,
    pub manifest_sha256: [u8; 32],
    pub state: InspectionState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FieldStorageKind {
    Inline,
    Chunk,
    Tombstone,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldMetadata {
    pub name: String,
    pub storage: FieldStorageKind,
    pub length: u64,
    pub md5: Option<[u8; 16]>,
    pub source_snapshot: SnapshotId,
}

impl FieldMetadata {
    pub(super) fn new(name: &str, value: &FieldLocator, snapshot: SnapshotId) -> Self {
        let (storage, length, md5) = match value {
            FieldLocator::Inline { value, md5 } => (FieldStorageKind::Inline, value.len() as u64, Some(*md5)),
            FieldLocator::Chunk { length, md5, .. } => (FieldStorageKind::Chunk, *length, Some(*md5)),
            FieldLocator::Tombstone => (FieldStorageKind::Tombstone, 0, None),
        };
        Self {
            name: name.into(),
            storage,
            length,
            md5,
            source_snapshot: snapshot,
        }
    }
}
