// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{content::validate_page_budget, key, work::InspectionWork, FieldStorageKind, InspectionError};
use crate::{
    selection::EffectiveSelection,
    split::{DraftSplit, MembershipOrigin, SamplePartition, SplitDefinition, SplitValidation},
    AuthorityError, DatasetAuthority, DatasetError, DatasetIdentity, MetadataScanBudget, ReadCancellation,
    SnapshotId,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SplitEvaluationWindow {
    pub after: Option<Vec<u8>>,
    pub universe_count: u64,
    pub members: Vec<Vec<Vec<u8>>>,
    pub validation: SplitValidation,
}
impl DatasetAuthority {
    pub(crate) async fn evaluate_split_window(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        splits: &[DraftSplit],
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<SplitEvaluationWindow, InspectionError> {
        validate_page_budget(budget)?;
        if splits.len() > 64 {
            return Err(InspectionError::Limit);
        }
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        let summary = self.ready_inspection(identity, snapshot, &mut work).await?;
        let schema = self
            .inspection_schema_definitions(identity, &summary, &mut work)
            .await?;
        let mut rules = Vec::new();
        let mut partitions = Vec::new();
        let mut fields = BTreeSet::new();
        let mut explicit = BTreeSet::new();
        for split in splits {
            if !matches!(split.origin, MembershipOrigin::Evaluated) {
                return Err(InspectionError::Unsupported(
                    "frozen membership source is not available".into(),
                ));
            }
            match &split.definition {
                SplitDefinition::Selection(selection) => {
                    let rule = EffectiveSelection::new(selection, &schema).map_err(AuthorityError::from)?;
                    fields.extend(rule.fields.iter().cloned());
                    explicit.extend(rule.explicit_ids.iter().cloned());
                    rules.push(Some(rule));
                    partitions.push(None);
                }
                SplitDefinition::WeightedPartition { seed, weights, index } => {
                    split
                        .definition
                        .validate()
                        .map_err(|_| InspectionError::Corrupt)?;
                    let partition =
                        SamplePartition::new(*seed, weights.clone()).map_err(|_| InspectionError::Corrupt)?;
                    partitions.push(Some((partition, weights[*index as usize].name.clone())));
                    rules.push(None);
                }
            }
        }
        let prefix = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
        for id in explicit {
            let mut key = prefix.clone();
            key::component(&mut key, &id);
            if work.get(&key, 128 * 1024).await?.is_none() {
                return Err(AuthorityError::Invalid(DatasetError::SampleNotFound).into());
            }
        }
        let samples = self
            .inspection_member_window(identity, snapshot, after, budget.items, &mut work)
            .await?;
        let universe_count = summary.sample_count;
        let mut summaries = BTreeMap::from([(snapshot, summary)]);
        let mut result = SplitEvaluationWindow {
            universe_count,
            after: samples.after,
            members: vec![Vec::new(); splits.len()],
            validation: SplitValidation::default(),
        };
        for id in samples.entries {
            let mut values = BTreeMap::new();
            for field in &fields {
                if let Some(metadata) = self
                    .effective_field_metadata(identity, snapshot, &id, field, &mut summaries, &mut work)
                    .await?
                {
                    if metadata.storage == FieldStorageKind::Inline {
                        values.insert(
                            field.clone(),
                            self.inspection_inline_value(identity, &id, &metadata, &mut work)
                                .await?,
                        );
                    }
                }
            }
            let mut count = 0;
            for index in 0..splits.len() {
                let matches = if let Some(rule) = &rules[index] {
                    rule.matches(&id, &values)
                } else if let Some((partition, name)) = &partitions[index] {
                    partition.assign(&id).map_err(|_| InspectionError::Corrupt)? == name
                } else {
                    false
                };
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
