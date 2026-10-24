// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    authority::decode, key, record::ManifestBinding, AuthorityError, DatasetAuthority, DatasetError,
    DatasetIdentity, FieldLocator, SampleView, SnapshotId,
};

#[derive(Default)]
struct Projection {
    copies: usize,
    fields: BTreeMap<String, FieldLocator>,
}

impl DatasetAuthority {
    pub(in crate::authority) async fn legacy_batch_locators(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        ids: &[Vec<u8>],
        fields: &[&str],
    ) -> Result<Vec<SampleView>, AuthorityError> {
        if ids.is_empty() {
            self.get_snapshot(identity, snapshot).await?;
            return Ok(Vec::new());
        }
        let chain = if fields.is_empty() {
            vec![self.get_snapshot(identity, snapshot).await?]
        } else {
            self.snapshot_chain(identity, snapshot).await?
        };
        let requested: BTreeSet<_> = fields.iter().copied().collect();
        let mut resolved = BTreeMap::<_, Projection>::new();
        for id in ids {
            resolved.entry(id).or_default().copies += 1;
        }
        for record in chain {
            let current = record.publication.snapshot;
            let binding = self.store.get(&key::manifest_binding(identity, current)?).await?;
            let Some(binding) = binding else {
                if current == snapshot {
                    return Err(AuthorityError::Corrupt);
                }
                continue;
            };
            let binding: ManifestBinding = decode(&binding.bytes)?;
            let mut members = BTreeSet::new();
            for partition in 0..binding.partitions {
                let part = self.get_manifest_partition(identity, current, partition).await?;
                for sample in part.samples {
                    let Some(values) = resolved.get_mut(&sample.sample_id) else {
                        continue;
                    };
                    members.insert(sample.sample_id);
                    for field in sample.fields {
                        if requested.contains(field.name.as_str()) {
                            values.fields.entry(field.name).or_insert(field.value);
                        }
                    }
                }
                if members.len() == resolved.len() && all_resolved(&resolved, requested.len()) {
                    break;
                }
            }
            if current == snapshot && members.len() != resolved.len() {
                return Err(DatasetError::SampleNotFound.into());
            }
            if all_resolved(&resolved, requested.len()) {
                break;
            }
        }
        Ok(ids
            .iter()
            .map(|id| {
                let projection = resolved.get_mut(id).expect("requested sample has a projection");
                projection.copies -= 1;
                let fields = if projection.copies == 0 {
                    std::mem::take(&mut projection.fields)
                } else {
                    projection.fields.clone()
                };
                SampleView {
                    sample_id: id.clone(),
                    fields: fields
                        .into_iter()
                        .filter(|(_, value)| !matches!(value, FieldLocator::Tombstone))
                        .collect(),
                    values: BTreeMap::new(),
                }
            })
            .collect())
    }
}

fn all_resolved(resolved: &BTreeMap<&Vec<u8>, Projection>, fields: usize) -> bool {
    resolved.values().all(|values| values.fields.len() == fields)
}
