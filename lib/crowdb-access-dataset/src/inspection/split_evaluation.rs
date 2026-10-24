// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    content::validate_page_budget, read::PreparedInspectionRead, work::InspectionWork, InspectionError,
};
use crate::{
    split::{DraftSplit, SplitValidation},
    DatasetAuthority, DatasetIdentity, MetadataScanBudget, ReadCancellation, SnapshotId,
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
