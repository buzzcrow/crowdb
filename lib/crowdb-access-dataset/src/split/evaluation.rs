// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    candidate_revision, state::DraftState, MembershipPageBinding, SplitLifecycleError, SplitValidation,
};
use crate::{composition::DraftProgress, SplitRevisionId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
/// Historical preview data retained for stored Composition draft decoding.
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
            || self.candidate != candidate_revision(state.record.operation, state.record.revision)?
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
}
