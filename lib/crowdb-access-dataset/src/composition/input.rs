// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{CompositionRecord, CompositionSource};
use crate::{
    split::{DraftSplit, SplitPolicy},
    OperationId, SampleRecord, SchemaRecord, SnapshotId, SplitRevisionId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositionInput {
    pub parent: Option<SnapshotId>,
    pub source: CompositionSource,
    pub schema: Option<SchemaRecord>,
    pub inherit_parent_splits: bool,
    pub parent_split_revision: Option<SplitRevisionId>,
    pub splits: Vec<DraftSplit>,
    pub policy: Option<SplitPolicy>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SampleChange {
    Upsert(SampleRecord),
    Remove(Vec<u8>),
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositionEdit {
    pub id: OperationId,
    pub expected_revision: u64,
    pub schema: Option<SchemaRecord>,
    pub samples: Vec<SampleChange>,
    pub splits: Option<Vec<DraftSplit>>,
    pub policy: Option<SplitPolicy>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositionDraft {
    pub record: CompositionRecord,
    pub schema: SchemaRecord,
    pub splits: Vec<DraftSplit>,
    pub policy: SplitPolicy,
    pub preparation: Option<CompositionPreparation>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositionPreparation {
    pub candidate: SnapshotId,
    pub split_operation: Option<OperationId>,
    pub split_preview: Option<crate::split::SplitPreview>,
}
