// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{work::InspectionWork, InspectionError};
use crate::{
    selection::EffectiveSelection,
    split::{
        DraftSplit, MembershipOrigin, SamplePartition, SplitDefinition, SplitLifecycleError, SplitName,
        SplitRevisionRecord,
    },
    AuthorityError, DatasetAuthority, DatasetIdentity, FieldDefinition, FieldLocator, SnapshotId,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub(super) enum Rule<'a> {
    Selection(EffectiveSelection<'a>),
    Partition(SamplePartition, SplitName),
    Frozen(Arc<SplitRevisionRecord>, SplitName),
}
pub(super) struct CompiledSplits<'a> {
    pub rules: Vec<Rule<'a>>,
    pub fields: BTreeSet<String>,
    pub explicit: BTreeSet<Vec<u8>>,
}
pub(super) async fn compile<'a>(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    parent: Option<SnapshotId>,
    splits: &'a [DraftSplit],
    schema: &[FieldDefinition],
    work: &mut InspectionWork<'_>,
) -> Result<CompiledSplits<'a>, InspectionError> {
    let mut compiled = CompiledSplits {
        rules: Vec::new(),
        fields: BTreeSet::new(),
        explicit: BTreeSet::new(),
    };
    let mut frozen = BTreeMap::new();
    for split in splits {
        if let MembershipOrigin::Inherited(source) = &split.origin {
            if parent != Some(source.snapshot) {
                return Err(InspectionError::Corrupt);
            }
            let key = (source.snapshot, source.revision);
            if let std::collections::btree_map::Entry::Vacant(entry) = frozen.entry(key) {
                let record = authority
                    .read_split_revision(identity, source.snapshot, source.revision, work)
                    .await
                    .map_err(source_error)?;
                entry.insert(Arc::new(record));
            }
            let record = Arc::clone(&frozen[&key]);
            if !record.splits.iter().any(|s| s.name == source.name) {
                return Err(AuthorityError::SnapshotNotFound.into());
            }
            compiled.rules.push(Rule::Frozen(record, source.name.clone()));
            continue;
        }
        match &split.definition {
            SplitDefinition::Selection(selection) => {
                let rule = EffectiveSelection::new(selection, schema).map_err(AuthorityError::from)?;
                compiled.fields.extend(rule.fields.iter().cloned());
                compiled.explicit.extend(rule.explicit_ids.iter().cloned());
                compiled.rules.push(Rule::Selection(rule));
            }
            SplitDefinition::WeightedPartition { seed, weights, index } => {
                split
                    .definition
                    .validate()
                    .map_err(|_| InspectionError::Corrupt)?;
                let partition =
                    SamplePartition::new(*seed, weights.clone()).map_err(|_| InspectionError::Corrupt)?;
                compiled
                    .rules
                    .push(Rule::Partition(partition, weights[*index as usize].name.clone()));
            }
        }
    }
    Ok(compiled)
}
impl Rule<'_> {
    pub(super) async fn matches(
        &self,
        authority: &DatasetAuthority,
        identity: &DatasetIdentity,
        id: &[u8],
        values: &BTreeMap<String, FieldLocator>,
        work: &mut InspectionWork<'_>,
    ) -> Result<bool, InspectionError> {
        match self {
            Self::Selection(rule) => Ok(rule.matches(id, values)),
            Self::Partition(partition, name) => {
                Ok(partition.assign(id).map_err(|_| InspectionError::Corrupt)? == name)
            }
            Self::Frozen(record, name) => authority
                .frozen_split_contains(identity, record, name, id, work)
                .await
                .map_err(source_error),
        }
    }
}
fn source_error(error: SplitLifecycleError) -> InspectionError {
    match error {
        SplitLifecycleError::Inspection(error) => error,
        SplitLifecycleError::Authority(error) => error.into(),
        SplitLifecycleError::Store(error) => error.into(),
        SplitLifecycleError::NotFound => AuthorityError::SnapshotNotFound.into(),
        SplitLifecycleError::Limit => InspectionError::Limit,
        _ => InspectionError::Corrupt,
    }
}
