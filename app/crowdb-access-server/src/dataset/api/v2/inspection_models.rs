// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::inspection::{
    FieldMetadata, FieldStorageKind, InspectionPage, InspectionSample, InspectionState, SnapshotSummary,
};
use serde::Serialize;

#[derive(Serialize)]
pub(super) struct Page<T> {
    pub entries: Vec<T>,
    pub after: Option<String>,
}
impl<T> Page<T> {
    pub(super) fn map<U>(page: InspectionPage<U>, convert: impl Fn(U) -> T) -> Self {
        Self {
            entries: page.entries.into_iter().map(convert).collect(),
            after: page.after.map(|v| STANDARD.encode(v)),
        }
    }
}
#[derive(Serialize)]
pub(super) struct Summary {
    id: String,
    parent: Option<String>,
    schema_version: u32,
    field_count: u32,
    sample_count: String,
    manifest_sha256: String,
    inspection: &'static str,
    reason: Option<String>,
}
impl From<SnapshotSummary> for Summary {
    fn from(value: SnapshotSummary) -> Self {
        let (inspection, reason) = match value.state {
            InspectionState::Ready => ("ready", None),
            InspectionState::Unsupported { reason } => ("unsupported", Some(reason)),
        };
        Self {
            id: value.snapshot.to_string(),
            parent: value.parent.map(|v| v.to_string()),
            schema_version: value.schema_version,
            field_count: value.field_count,
            sample_count: value.sample_count.to_string(),
            manifest_sha256: hex::encode(value.manifest_sha256),
            inspection,
            reason,
        }
    }
}
#[derive(Serialize)]
pub(super) struct Field {
    name: String,
    storage: &'static str,
    length: String,
    md5: Option<String>,
    source_snapshot: String,
}
impl From<FieldMetadata> for Field {
    fn from(value: FieldMetadata) -> Self {
        Self {
            name: value.name,
            storage: match value.storage {
                FieldStorageKind::Inline => "inline",
                FieldStorageKind::Chunk => "payload",
                FieldStorageKind::Tombstone => "tombstone",
            },
            length: value.length.to_string(),
            md5: value.md5.map(hex::encode),
            source_snapshot: value.source_snapshot.to_string(),
        }
    }
}
#[derive(Serialize)]
pub(super) struct Sample {
    id: String,
    fields: Vec<Option<Field>>,
}
impl From<InspectionSample> for Sample {
    fn from(value: InspectionSample) -> Self {
        Self {
            id: STANDARD.encode(value.sample_id),
            fields: value
                .fields
                .into_iter()
                .map(|field| field.map(Field::from))
                .collect(),
        }
    }
}
