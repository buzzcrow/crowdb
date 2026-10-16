// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    key, state::DraftState, MembershipPage, MembershipPageBinding, SplitLifecycleError, SplitValidation,
};
use crate::{
    composition::DraftProgress, DatasetAuthority, DatasetIdentity, MetadataScanBudget, OperationId,
    ReadCancellation, SplitRevisionId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EvaluationProgress {
    pub input_revision: u64,
    pub candidate: SplitRevisionId,
    pub next_page: u64,
    pub after: Option<Vec<u8>>,
    pub complete: bool,
    pub validation: SplitValidation,
    pub bindings: Vec<Vec<MembershipPageBinding>>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SplitPreview {
    pub input_revision: u64,
    pub next_page: u64,
    pub complete: bool,
    pub validation: SplitValidation,
    pub member_counts: Vec<u64>,
}
impl EvaluationProgress {
    pub(super) fn validate(&self, state: &DraftState) -> Result<(), SplitLifecycleError> {
        if self.input_revision != state.record.revision
            || self.candidate != Self::initial(state)?.candidate
            || self.bindings.len() != state.record.splits.len()
            || self.next_page == 0
            || self.complete != self.after.is_none()
            || self.validation.assigned.checked_add(self.validation.unassigned)
                != Some(self.validation.scanned)
            || self.validation.overlapping > self.validation.assigned
        {
            return Err(SplitLifecycleError::Corrupt);
        }
        for pages in &self.bindings {
            for (index, page) in pages.iter().enumerate() {
                if page.page as usize != index
                    || page.count == 0
                    || page.count as usize > super::MAX_MEMBERSHIP_PAGE_ITEMS
                {
                    return Err(SplitLifecycleError::Corrupt);
                }
            }
        }
        match state.record.progress {
            DraftProgress::Evaluating { next_page, .. } if !self.complete && next_page == self.next_page => {}
            DraftProgress::Evaluated { binding, .. } if self.complete => {
                let digest: [u8; 32] = Sha256::digest(super::codec::encode(self)?).into();
                if binding != digest {
                    return Err(SplitLifecycleError::Corrupt);
                }
            }
            DraftProgress::Prepared { .. }
            | DraftProgress::Published { .. }
            | DraftProgress::Committing { .. }
                if self.complete => {}
            DraftProgress::Abandoned => {}
            _ => return Err(SplitLifecycleError::Corrupt),
        }
        Ok(())
    }
    pub(super) fn preview(&self) -> SplitPreview {
        SplitPreview {
            input_revision: self.input_revision,
            next_page: self.next_page,
            complete: self.complete,
            validation: self.validation.clone(),
            member_counts: self
                .bindings
                .iter()
                .map(|pages| pages.iter().map(|p| u64::from(p.count)).sum())
                .collect(),
        }
    }
    fn initial(state: &DraftState) -> Result<Self, SplitLifecycleError> {
        let candidate = candidate_revision(state.record.operation, state.record.revision)?;
        Ok(Self {
            input_revision: state.record.revision,
            candidate,
            next_page: 0,
            after: None,
            complete: false,
            validation: SplitValidation::default(),
            bindings: vec![Vec::new(); state.record.splits.len()],
        })
    }
}
impl DatasetAuthority {
    /// Advances at most twenty Samples, persisting immutable membership pages before progress.
    /// Retrying an already completed page returns current progress without applying it twice.
    /// # Errors
    /// Rejects stale draft/page revisions, invalid members, frozen drafts and bounded-work limits.
    pub async fn evaluate_split_draft(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        expected_revision: u64,
        expected_page: u64,
        cancel: &ReadCancellation,
    ) -> Result<SplitPreview, SplitLifecycleError> {
        let (old, state) = self.load_split_draft(identity, operation).await?;
        if state.record.revision != expected_revision {
            return Err(SplitLifecycleError::Conflict);
        }
        if !matches!(
            state.record.progress,
            DraftProgress::Editing | DraftProgress::Evaluating { .. } | DraftProgress::Evaluated { .. }
        ) {
            return Err(SplitLifecycleError::Frozen);
        }
        self.renew_split_draft_lease(identity, &state).await?;
        let mut progress = state
            .evaluation
            .clone()
            .map_or_else(|| EvaluationProgress::initial(&state), Ok)?;
        if progress.input_revision != expected_revision {
            return Err(SplitLifecycleError::Corrupt);
        }
        if expected_page > progress.next_page {
            return Err(SplitLifecycleError::Conflict);
        }
        if expected_page < progress.next_page || progress.complete {
            return Ok(progress.preview());
        }
        let window = self
            .evaluate_split_window(
                identity,
                state.record.snapshot,
                &state.record.splits,
                progress.after.clone(),
                MetadataScanBudget {
                    items: 20,
                    bytes: 1024 * 1024,
                    timeout: Duration::from_secs(2),
                },
                cancel,
                state.initial,
            )
            .await?;
        let mut pages = Vec::new();
        for (index, members) in window.members.into_iter().enumerate() {
            if members.is_empty() {
                continue;
            }
            let page = MembershipPage {
                version: 1,
                snapshot: state.record.snapshot,
                revision: progress.candidate,
                name: state.record.splits[index].name.clone(),
                page: u32::try_from(progress.bindings[index].len())
                    .map_err(|_| SplitLifecycleError::Limit)?,
                members,
            };
            progress.bindings[index].push(page.binding()?);
            pages.push(page);
        }
        progress.validation.merge(&window.validation)?;
        progress.after = window.after;
        progress.complete = progress.after.is_none();
        if progress.validation.scanned > window.universe_count
            || (progress.complete && progress.validation.scanned != window.universe_count)
        {
            return Err(SplitLifecycleError::Corrupt);
        }
        progress.next_page = progress
            .next_page
            .checked_add(1)
            .ok_or(SplitLifecycleError::Limit)?;
        let mut next = state.clone();
        next.record.progress = if progress.complete {
            DraftProgress::Evaluated {
                input_revision: expected_revision,
                binding: Sha256::digest(super::codec::encode(&progress)?).into(),
            }
        } else {
            DraftProgress::Evaluating {
                input_revision: expected_revision,
                next_page: progress.next_page,
            }
        };
        next.evaluation = Some(progress.clone());
        next.edit = None;
        next.encode()?;
        self.register_split_window(identity, &state, expected_page, &pages)
            .await?;
        for page in pages {
            if cancel.is_cancelled() {
                return Err(crate::inspection::InspectionError::Cancelled.into());
            }
            self.put_split_state(
                &key::membership_page(identity, page.snapshot, page.revision, &page.name, page.page)
                    .map_err(crate::AuthorityError::from)?,
                &page.encode()?,
            )
            .await?;
        }
        self.replace_split_draft(identity, &old, &state, &next).await?;
        Ok(progress.preview())
    }
}

pub(crate) fn candidate_revision(
    operation: OperationId,
    revision: u64,
) -> Result<SplitRevisionId, crate::AuthorityError> {
    let mut hash = Sha256::new();
    hash.update(b"crowdb-split-candidate-v1\0");
    hash.update(operation.as_bytes());
    hash.update(revision.to_be_bytes());
    Ok(SplitRevisionId::from_bytes(&hash.finalize()[..16])?)
}
