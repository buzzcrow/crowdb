// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    content::validate_page_budget, cursor::CursorScope, key, read::PreparedInspectionRead,
    work::InspectionWork, InspectionError, InspectionPage,
};
use crate::{
    split::{DraftSplit, SplitValidation},
    AuthorityError, DatasetAuthority, DatasetError, DatasetIdentity, MetadataScanBudget, ReadCancellation,
    SnapshotId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SplitEvaluationWindow {
    pub after: Option<Vec<u8>>,
    pub universe_count: u64,
    pub members: Vec<Vec<Vec<u8>>>,
    pub validation: SplitValidation,
}
impl DatasetAuthority {
    /// Previews a Split over a bounded window of a committed Snapshot without creating a revision.
    /// An empty matching page can still have a continuation. Cursors bind the complete rule.
    /// # Errors
    /// Rejects invalid definitions, mismatched cursors, unavailable sources and read limits.
    pub async fn inspection_split_page(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        split: &DraftSplit,
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<Vec<u8>>, InspectionError> {
        validate_page_budget(budget)?;
        split
            .definition
            .validate()
            .map_err(|_| AuthorityError::Invalid(DatasetError::InvalidManifest))?;
        let prefix = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
        let mut query = b"split-preview:".to_vec();
        query.extend(serde_json::to_vec(split).map_err(|_| InspectionError::Corrupt)?);
        let cursor = CursorScope::new(&prefix, &query);
        let mut window = self
            .evaluate_split_window(
                identity,
                snapshot,
                std::slice::from_ref(split),
                cursor.decode(after)?,
                budget,
                cancel,
                false,
            )
            .await?;
        Ok(InspectionPage {
            entries: window.members.pop().ok_or(InspectionError::Corrupt)?,
            after: cursor.encode(window.after)?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn evaluate_split_window(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        splits: &[DraftSplit],
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
        prepared_owner: bool,
    ) -> Result<SplitEvaluationWindow, InspectionError> {
        validate_page_budget(budget)?;
        if splits.len() > 64 {
            return Err(InspectionError::Limit);
        }
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        let summary = if prepared_owner {
            self.ready_inspection_projection(identity, snapshot, &mut work)
                .await?
        } else {
            self.ready_inspection(identity, snapshot, &mut work).await?
        };
        let schema = self
            .inspection_schema_definitions(identity, &summary, &mut work)
            .await?;
        let compiled =
            super::split_rules::compile(self, identity, summary.parent, splits, &schema, &mut work).await?;
        let universe_count = summary.sample_count;
        let mut read = PreparedInspectionRead {
            authority: self,
            identity,
            snapshot,
            schema,
            summaries: BTreeMap::from([(snapshot, summary)]),
        };
        let explicit: Vec<_> = compiled.explicit.iter().cloned().collect();
        read.members(&explicit, &mut work).await?;
        let samples = self
            .inspection_member_window(identity, snapshot, after, budget.items, &mut work)
            .await?;
        let fields: Vec<_> = compiled.fields.into_iter().collect();
        let values = read.inline_matrix(&samples.entries, &fields, &mut work).await?;
        let mut result = SplitEvaluationWindow {
            universe_count,
            after: samples.after,
            members: vec![Vec::new(); splits.len()],
            validation: SplitValidation::default(),
        };
        for (id, values) in samples.entries.into_iter().zip(values) {
            let mut count = 0;
            for index in 0..splits.len() {
                let matches = compiled.rules[index]
                    .matches(self, identity, &id, &values, &mut work)
                    .await?;
                if matches {
                    result.members[index].push(id.clone());
                    count += 1;
                }
            }
            result
                .validation
                .observe(&id, count)
                .map_err(|_| InspectionError::Limit)?;
        }
        Ok(result)
    }
}
