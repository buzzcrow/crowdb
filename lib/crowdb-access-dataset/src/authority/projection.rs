// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    AuthorityError, DatasetAuthority, DatasetError, DatasetIdentity, FieldLocator, SampleView, SnapshotId,
};
use std::collections::{BTreeMap, BTreeSet};

impl DatasetAuthority {
    /// Resolves a bounded projection in one metadata traversal per ancestor.
    /// Tombstones remain explicit; no payloads are read here.
    /// # Errors
    /// Rejects unknown samples/fields, invalid bounds and malformed ancestry.
    pub async fn resolve_batch_locators(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        ids: &[Vec<u8>],
        fields: &[String],
    ) -> Result<Vec<SampleView>, AuthorityError> {
        if ids.is_empty() || ids.len() > crate::MAX_BATCH_SIZE || fields.len() > 256 {
            return Err(DatasetError::InvalidManifest.into());
        }
        let requested: BTreeSet<_> = ids.iter().collect();
        let projection: BTreeSet<_> = fields.iter().collect();
        if requested.len() != ids.len()
            || projection.len() != fields.len()
            || fields.iter().any(String::is_empty)
        {
            return Err(DatasetError::InvalidManifest.into());
        }
        let mut output: BTreeMap<_, _> = ids.iter().map(|id| (id.clone(), BTreeMap::new())).collect();
        let mut current = Some(snapshot);
        let mut seen = BTreeSet::new();
        let mut first = true;
        let mut bytes = 0usize;
        while let Some(id) = current {
            if !seen.insert(id) {
                return Err(AuthorityError::AncestryCycle);
            }
            current = self.get_snapshot(identity, id).await?.publication.parent;
            let manifest = self.get_manifest(identity, id).await?;
            if first {
                if fields
                    .iter()
                    .any(|name| !manifest.schema.fields.iter().any(|field| &field.name == name))
                {
                    return Err(DatasetError::InvalidManifest.into());
                }
                let members: BTreeSet<_> = manifest.samples.iter().map(|sample| &sample.sample_id).collect();
                if ids.iter().any(|id| !members.contains(id)) {
                    return Err(DatasetError::SampleNotFound.into());
                }
                first = false;
            }
            for sample in manifest.samples {
                let Some(resolved) = output.get_mut(&sample.sample_id) else {
                    continue;
                };
                for field in sample.fields {
                    if projection.contains(&field.name) && !resolved.contains_key(&field.name) {
                        bytes = bytes
                            .saturating_add(field.name.len())
                            .saturating_add(match &field.value {
                                FieldLocator::Inline { value, .. } => value.len(),
                                FieldLocator::Chunk { location, .. } => location.len(),
                                FieldLocator::Tombstone => 0,
                            });
                        if bytes > 8 * 1024 * 1024 {
                            return Err(DatasetError::ReadLimitExceeded.into());
                        }
                        resolved.insert(field.name, field.value);
                    }
                }
            }
            if output.values().all(|resolved| resolved.len() == projection.len()) {
                break;
            }
        }
        Ok(ids
            .iter()
            .map(|id| SampleView {
                sample_id: id.clone(),
                fields: output.remove(id).unwrap_or_default(),
                values: BTreeMap::new(),
            })
            .collect())
    }
}
