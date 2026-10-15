// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::ApiError,
    composition_models::{CompositionSource, Schema},
    split_models::{DraftSplit, Policy},
    split_responses,
};
use crowdb_access_dataset::{
    composition::{CompositionDraft, CompositionError, CompositionPreview, CompositionUploadStatus},
    SnapshotReclaimError, UploadReclaimError,
};
use hyper::StatusCode;
use serde_json::{json, Value};

pub(super) fn draft(d: CompositionDraft, uploads: &CompositionUploadStatus) -> Value {
    json!({"operation":d.record.operation.to_string(), "revision":d.record.revision.to_string(), "parent":d.record.parent.map(|v|v.to_string()), "source":CompositionSource::from(d.record.source),
        "parent_split_revision":d.record.parent_split_revision.map(|v|v.to_string()), "schema":Schema::from(d.schema), "splits":d.splits.into_iter().map(DraftSplit::from).collect::<Vec<_>>(), "policy":Policy::from(d.policy),
        "progress":split_responses::progress(&d.record.progress), "source_lease_deadline":d.record.source_lease_deadline.to_string(),
        "preparation":d.preparation.map(|p| json!({"snapshot":p.candidate.to_string(), "split_preview":p.split_preview.map(split_responses::preview)})),
        "uploads":{"publication_ready":uploads.publication_ready, "staged_uploads":uploads.staged_uploads.to_string(), "candidate_uploads":uploads.candidate_uploads.to_string()}})
}
pub(super) fn preview(p: &CompositionPreview) -> Value {
    json!({"input_revision":p.input_revision.to_string(), "next_page":p.next_page.to_string(), "members":p.members.to_string(), "complete":p.complete, "binding":hex::encode(p.binding)})
}
impl From<CompositionError> for ApiError {
    fn from(error: CompositionError) -> Self {
        match error {
            CompositionError::Authority(error) => error.into(),
            CompositionError::Store(error) => crowdb_access_dataset::AuthorityError::Store(error).into(),
            CompositionError::Inspection(error) => error.into(),
            CompositionError::Split(error) => error.into(),
            CompositionError::Upload(error) => error.into(),
            CompositionError::NotFound => Self::not_found(),
            CompositionError::Conflict => Self::conflict(),
            CompositionError::Corrupt => Self::integrity(),
            CompositionError::Limit
            | CompositionError::Record(crowdb_access_dataset::split::SplitError::Limit) => {
                Self::new(StatusCode::PAYLOAD_TOO_LARGE, "read_limit")
            }
            CompositionError::Frozen => Self::new(StatusCode::CONFLICT, "draft_frozen"),
            CompositionError::Record(_) => Self::invalid(),
        }
    }
}
impl From<UploadReclaimError> for ApiError {
    fn from(error: UploadReclaimError) -> Self {
        match error {
            UploadReclaimError::Authority(error) => error.into(),
            UploadReclaimError::Upload(error) => error.into(),
            UploadReclaimError::Store(error) => crowdb_access_dataset::AuthorityError::Store(error).into(),
            UploadReclaimError::Corrupt => Self::integrity(),
            UploadReclaimError::Limit => Self::new(StatusCode::PAYLOAD_TOO_LARGE, "read_limit"),
        }
    }
}
impl From<SnapshotReclaimError> for ApiError {
    fn from(error: SnapshotReclaimError) -> Self {
        match error {
            SnapshotReclaimError::Authority(error) => error.into(),
            SnapshotReclaimError::Upload(error) => error.into(),
            SnapshotReclaimError::Store(error) => crowdb_access_dataset::AuthorityError::Store(error).into(),
            SnapshotReclaimError::Split(error) => error.into(),
            SnapshotReclaimError::Inspection(error) => error.into(),
            SnapshotReclaimError::Limit => Self::timeout(),
        }
    }
}
