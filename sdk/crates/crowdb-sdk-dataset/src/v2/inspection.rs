// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{required_option, Decimal, OpaqueId, SampleId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Page<T> {
    pub entries: Vec<T>,
    #[serde(deserialize_with = "required_option")]
    pub after: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionState {
    Ready,
    Unsupported,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotSummary {
    pub id: OpaqueId,
    #[serde(deserialize_with = "required_option")]
    pub parent: Option<OpaqueId>,
    pub schema_version: u32,
    pub field_count: u32,
    pub sample_count: Decimal,
    pub manifest_sha256: String,
    pub inspection: InspectionState,
    #[serde(deserialize_with = "required_option")]
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldStorage {
    Inline,
    Payload,
    Tombstone,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldMetadata {
    pub name: String,
    pub storage: FieldStorage,
    pub length: Decimal,
    #[serde(deserialize_with = "required_option")]
    pub md5: Option<String>,
    pub source_snapshot: OpaqueId,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MatrixRow {
    pub id: SampleId,
    pub fields: Vec<Option<FieldMetadata>>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatrixRequest {
    pub sample_ids: Vec<SampleId>,
    pub fields: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExactMatrix {
    pub entries: Vec<MatrixRow>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldPreview {
    pub state: PreviewState,
    #[serde(deserialize_with = "required_option")]
    pub length: Option<Decimal>,
    #[serde(deserialize_with = "required_option")]
    pub md5: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub data: Option<String>,
    pub truncated: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreviewState {
    Missing,
    Tombstone,
    Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparationState {
    Preparing,
    Ready,
    Unsupported,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InspectionPreparation {
    pub snapshot: OpaqueId,
    pub revision: Decimal,
    pub rows_written: Decimal,
    pub state: PreparationState,
    #[serde(deserialize_with = "required_option")]
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReclaimPhase {
    UploadOwners,
    SplitDrafts,
    IndexFences,
    Metadata,
    Complete,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotReclaim {
    pub snapshot: OpaqueId,
    pub phase: ReclaimPhase,
    pub complete: bool,
    pub physical_cleanup_deferred: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetainedSnapshot {
    pub snapshot: OpaqueId,
    pub retained: bool,
}
