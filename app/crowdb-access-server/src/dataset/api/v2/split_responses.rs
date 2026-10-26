// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::ApiError;
use super::split_models::{DraftSplit, Policy};
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::{
    composition::DraftProgress,
    split::{self, SplitLifecycleError},
    CommitTarget, SuccessfulCommit,
};
use serde_json::{json, Value};

pub(super) fn target(target: CommitTarget) -> Value {
    match target {
        CommitTarget::Snapshot { snapshot } => json!({"kind":"snapshot", "snapshot":snapshot.to_string()}),
        CommitTarget::SplitRevision { snapshot, revision } => {
            json!({"kind":"split_revision", "snapshot":snapshot.to_string(), "revision":revision.to_string()})
        }
    }
}
pub(super) fn progress(progress: &DraftProgress) -> Value {
    match progress {
        DraftProgress::Editing => json!({"state":"editing"}),
        DraftProgress::Evaluating {
            input_revision,
            next_page,
        } => {
            json!({"state":"evaluating", "input_revision":input_revision.to_string(), "next_page":next_page.to_string()})
        }
        DraftProgress::Evaluated {
            input_revision,
            binding,
        } => {
            json!({"state":"evaluated", "input_revision":input_revision.to_string(), "binding":hex::encode(binding)})
        }
        DraftProgress::Prepared { target: value } => json!({"state":"prepared", "target":target(*value)}),
        DraftProgress::Committing {
            target: value,
            attempt,
        } => json!({"state":"committing", "target":target(*value), "attempt":attempt.to_string()}),
        DraftProgress::Published {
            target: value,
            attempt,
        } => json!({"state":"published", "target":target(*value), "attempt":attempt.to_string()}),
        DraftProgress::Abandoned => json!({"state":"abandoned"}),
    }
}
pub(super) fn draft(d: split::SplitDraftRecord) -> Value {
    json!({"operation":d.operation.to_string(), "snapshot":d.snapshot.to_string(), "expected_revision":d.expected_revision.map(|r|r.to_string()), "revision":d.revision.to_string(), "policy":Policy::from(d.policy), "splits":d.splits.into_iter().map(DraftSplit::from).collect::<Vec<_>>(), "progress":progress(&d.progress)})
}
pub(super) fn revision(r: split::SplitRevisionRecord) -> Value {
    let splits: Vec<_> = r.splits.into_iter().map(|s| {
        let definition = DraftSplit::from(split::DraftSplit { name:s.name, definition:s.definition, origin:s.origin });
        json!({"name":definition.name, "definition":definition.definition, "origin":definition.origin, "count":s.count.to_string()})
    }).collect();
    json!({"snapshot":r.snapshot.to_string(), "revision":r.revision.to_string(), "operation":r.operation.to_string(), "evaluator_version":r.evaluator_version, "policy":Policy::from(r.policy), "splits":splits})
}
pub(super) fn deletion(value: &split::SplitRevisionDeletion) -> Value {
    json!({"snapshot":value.snapshot.to_string(), "revision":value.revision.to_string(),
        "reclaim_after":value.reclaim_after.map(|deadline|deadline.to_string()), "complete":value.complete})
}
pub(super) fn preview(p: split::SplitPreview) -> Value {
    let v = p.validation;
    json!({"input_revision":p.input_revision.to_string(), "next_page":p.next_page.to_string(), "complete":p.complete, "member_counts":p.member_counts.iter().map(ToString::to_string).collect::<Vec<_>>(), "validation":{
        "scanned":v.scanned.to_string(), "assigned":v.assigned.to_string(), "unassigned":v.unassigned.to_string(), "overlapping":v.overlapping.to_string(), "unassigned_examples":v.unassigned_examples.into_iter().map(|id|STANDARD.encode(id)).collect::<Vec<_>>(), "overlap_examples":v.overlap_examples.into_iter().map(|id|STANDARD.encode(id)).collect::<Vec<_>>()
    }})
}
pub(super) fn receipt(r: &SuccessfulCommit) -> Value {
    json!({"attempt":r.attempt.id.to_string(), "target":target(r.attempt.target), "expected_head":r.attempt.expected_head.map(|h|hex::encode(h.as_bytes())), "committed_head":hex::encode(r.committed_head.as_bytes())})
}
pub(super) fn head(snapshot: crowdb_access_dataset::SnapshotId, head: &split::SplitReference) -> Value {
    json!({"snapshot":snapshot.to_string(),"revision":head.revision.map(|id|id.to_string()),"generation":head.generation.map(|g|hex::encode(g.as_bytes()))})
}
impl From<SplitLifecycleError> for ApiError {
    fn from(error: SplitLifecycleError) -> Self {
        use hyper::StatusCode;
        match error {
            SplitLifecycleError::Authority(error) => error.into(),
            SplitLifecycleError::Store(error) => crowdb_access_dataset::AuthorityError::Store(error).into(),
            SplitLifecycleError::Inspection(error) => error.into(),
            SplitLifecycleError::NotFound => Self::not_found(),
            SplitLifecycleError::Conflict => Self::conflict(),
            SplitLifecycleError::HeadConflict { snapshot, observed } => {
                Self::conflict().with_observed_head(head(snapshot, &observed))
            }
            SplitLifecycleError::Corrupt => Self::integrity(),
            SplitLifecycleError::Limit | SplitLifecycleError::Split(split::SplitError::Limit) => {
                Self::new(StatusCode::PAYLOAD_TOO_LARGE, "read_limit")
            }
            SplitLifecycleError::NotEvaluated => Self::new(StatusCode::CONFLICT, "not_evaluated"),
            SplitLifecycleError::PolicyViolation => {
                Self::new(StatusCode::UNPROCESSABLE_ENTITY, "split_policy_violation")
            }
            SplitLifecycleError::Frozen => Self::new(StatusCode::CONFLICT, "draft_frozen"),
            SplitLifecycleError::Split(_) => Self::invalid(),
        }
    }
}
