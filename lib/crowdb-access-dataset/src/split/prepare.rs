// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, NamedSplitRecord, SplitLifecycleError, SplitRevisionRecord};
use crate::{composition::DraftProgress, CommitTarget, DatasetAuthority, DatasetIdentity, OperationId};
use sha2::{Digest, Sha256};

impl DatasetAuthority {
    /// Freezes the complete evaluated definition set and immutable member-page bindings.
    /// # Errors
    /// Rejects stale/incomplete previews, overlap/coverage violations and unavailable owners.
    pub async fn prepare_split_draft(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        expected_revision: u64,
    ) -> Result<SplitRevisionRecord, SplitLifecycleError> {
        let (old, state) = self.load_split_draft(identity, operation).await?;
        if state.record.revision != expected_revision {
            return Err(SplitLifecycleError::Conflict);
        }
        self.check_split_draft_sources(identity, &state).await?;
        if let DraftProgress::Prepared {
            target: CommitTarget::SplitRevision { snapshot, revision },
        } = state.record.progress
        {
            let value = self
                .split_state_get(
                    &key::revision(identity, snapshot, revision).map_err(crate::AuthorityError::from)?,
                )
                .await?
                .ok_or(SplitLifecycleError::Corrupt)?;
            return Ok(SplitRevisionRecord::decode(&value.bytes)?);
        }
        let DraftProgress::Evaluated {
            input_revision,
            binding,
        } = state.record.progress
        else {
            return Err(SplitLifecycleError::NotEvaluated);
        };
        let progress = state.evaluation.as_ref().ok_or(SplitLifecycleError::Corrupt)?;
        let digest: [u8; 32] = Sha256::digest(super::codec::encode(progress)?).into();
        if input_revision != expected_revision
            || progress.input_revision != expected_revision
            || !progress.complete
            || binding != digest
            || progress.bindings.len() != state.record.splits.len()
        {
            return Err(SplitLifecycleError::Corrupt);
        }
        if !progress.validation.satisfies(state.record.policy) {
            return Err(SplitLifecycleError::PolicyViolation);
        }
        let record = SplitRevisionRecord {
            version: 1,
            snapshot: state.record.snapshot,
            revision: progress.candidate,
            operation,
            evaluator_version: 1,
            policy: state.record.policy,
            splits: state
                .record
                .splits
                .iter()
                .zip(&progress.bindings)
                .map(|(split, pages)| NamedSplitRecord {
                    name: split.name.clone(),
                    definition: split.definition.clone(),
                    origin: split.origin.clone(),
                    count: pages.iter().map(|page| u64::from(page.count)).sum(),
                    pages: pages.clone(),
                })
                .collect(),
        };
        let bytes = record.encode()?;
        self.put_split_state(
            &key::revision(identity, record.snapshot, record.revision)
                .map_err(crate::AuthorityError::from)?,
            &bytes,
        )
        .await?;
        let summary = super::SplitRevisionSummary {
            version: 1,
            snapshot: record.snapshot,
            revision: record.revision,
            operation,
            split_count: u32::try_from(record.splits.len()).map_err(|_| SplitLifecycleError::Limit)?,
        };
        self.put_split_state(
            &key::revision_summary(identity, record.snapshot, record.revision)
                .map_err(crate::AuthorityError::from)?,
            &super::codec::encode(&summary)?,
        )
        .await?;
        let mut next = state.clone();
        next.record.progress = DraftProgress::Prepared {
            target: CommitTarget::SplitRevision {
                snapshot: record.snapshot,
                revision: record.revision,
            },
        };
        next.edit = None;
        self.replace_split_draft(identity, &old, &state, &next).await?;
        Ok(record)
    }
}
