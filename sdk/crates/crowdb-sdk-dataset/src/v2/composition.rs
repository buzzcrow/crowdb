// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{
    required_option, Decimal, DraftProgress, DraftSplit, Generation, OpaqueId, Predicate, SampleId,
    SplitCleanup, SplitPolicy, SplitPreview, SplitSource,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    pub version: u32,
    pub fields: Vec<crate::FieldDefinition>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompositionSource {
    All,
    Selection {
        predicate: Predicate,
    },
    Split {
        source: SplitSource,
        selection: Option<Predicate>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeginComposition {
    pub operation: OpaqueId,
    #[serde(deserialize_with = "required_option")]
    pub parent: Option<OpaqueId>,
    pub source: CompositionSource,
    pub schema: Option<Schema>,
    pub inherit_parent_splits: bool,
    pub parent_split_revision: Option<OpaqueId>,
    pub splits: Vec<DraftSplit>,
    pub policy: Option<SplitPolicy>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FieldValue {
    Inline { data: String },
    Upload { id: OpaqueId },
    Tombstone,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub name: String,
    pub value: FieldValue,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SampleChange {
    Remove { id: SampleId },
    Upsert { id: SampleId, fields: Vec<Field> },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EditComposition {
    pub edit: OpaqueId,
    pub expected_revision: Decimal,
    pub schema: Option<Schema>,
    pub samples: Vec<SampleChange>,
    pub splits: Option<Vec<DraftSplit>>,
    pub policy: Option<SplitPolicy>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompositionPreparation {
    pub snapshot: OpaqueId,
    #[serde(deserialize_with = "required_option")]
    pub split_preview: Option<SplitPreview>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompositionUploadStatus {
    pub publication_ready: bool,
    pub staged_uploads: Decimal,
    pub candidate_uploads: Decimal,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompositionDraft {
    pub operation: OpaqueId,
    pub revision: Decimal,
    #[serde(deserialize_with = "required_option")]
    pub parent: Option<OpaqueId>,
    pub source: CompositionSource,
    #[serde(deserialize_with = "required_option")]
    pub parent_split_revision: Option<OpaqueId>,
    pub schema: Schema,
    pub splits: Vec<DraftSplit>,
    pub policy: SplitPolicy,
    pub progress: DraftProgress,
    pub source_lease_deadline: Decimal,
    #[serde(deserialize_with = "required_option")]
    pub preparation: Option<CompositionPreparation>,
    pub uploads: CompositionUploadStatus,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompositionPreview {
    pub input_revision: Decimal,
    pub next_page: Decimal,
    pub members: Decimal,
    pub complete: bool,
    pub binding: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishComposition {
    pub attempt: OpaqueId,
    pub snapshot: OpaqueId,
    #[serde(deserialize_with = "required_option")]
    pub expected_head: Option<Generation>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateCleanup {
    pub input_revision: Decimal,
    pub complete: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnerCleanup {
    pub retired_owners: Decimal,
    pub complete: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EditCleanup {
    pub retired_pages: Decimal,
    pub complete: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompositionCleanup {
    pub complete: bool,
    pub candidates: CandidateCleanup,
    pub uploads: OwnerCleanup,
    pub scratch: SplitCleanup,
    #[serde(deserialize_with = "required_option")]
    pub edits: Option<EditCleanup>,
    pub physical_cleanup_deferred: bool,
}
