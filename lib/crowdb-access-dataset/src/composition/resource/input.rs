// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{cancelled, CompositionRequest, MAX_MANIFEST_BYTES};
use crate::{
    composition::{CompositionError, CompositionSource, SampleChange},
    AuthorityError, DatasetAuthority, DatasetIdentity, DatasetUploads, FieldLocator, ManifestRecord,
    ReadCancellation, SampleRecord, SchemaRecord, SnapshotId,
};
use std::collections::{BTreeMap, BTreeSet};

impl CompositionRequest {
    pub(super) fn validate(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), CompositionError> {
        SnapshotId::from_bytes(snapshot.as_bytes()).map_err(AuthorityError::from)?;
        if self.parent == Some(snapshot)
            || (self.parent.is_none() && self.source != CompositionSource::All)
            || bincode::serialized_size(self).map_err(|_| CompositionError::Corrupt)? > MAX_MANIFEST_BYTES
        {
            return Err(CompositionError::Limit);
        }
        if let Some(parent) = self.parent {
            SnapshotId::from_bytes(parent.as_bytes()).map_err(AuthorityError::from)?;
        }
        match &self.source {
            CompositionSource::All => {}
            CompositionSource::Selection(selection) => {
                crate::split::definition::validate_selection(selection, 0)?;
            }
            CompositionSource::Split { source, selection } => {
                if self.parent != Some(source.snapshot) {
                    return Err(CompositionError::Conflict);
                }
                crate::SplitRevisionId::from_bytes(source.revision.as_bytes())
                    .map_err(AuthorityError::from)?;
                if let Some(selection) = selection {
                    crate::split::definition::validate_selection(selection, 0)?;
                }
            }
        }
        super::super::state::validate_splits(&self.splits)?;
        if let Some(schema) = &self.schema {
            super::super::state::validate_schema(schema)?;
        }
        let mut ids = BTreeSet::new();
        for change in &self.samples {
            let id = match change {
                SampleChange::Upsert(sample) => {
                    let mut names = BTreeSet::new();
                    if sample.fields.len() > 256 {
                        return Err(CompositionError::Limit);
                    }
                    for field in &sample.fields {
                        if field.name.is_empty() || field.name.len() > 1024 || !names.insert(&field.name) {
                            return Err(CompositionError::Conflict);
                        }
                        field.value.validate().map_err(AuthorityError::from)?;
                        if let FieldLocator::Chunk { location, .. } = &field.value {
                            DatasetUploads::upload_id(identity, location)?;
                        }
                    }
                    &sample.sample_id
                }
                SampleChange::Remove(id) => id,
            };
            if id.is_empty() || id.len() > 16 * 1024 || !ids.insert(id) {
                return Err(CompositionError::Conflict);
            }
        }
        Ok(())
    }
}

impl DatasetAuthority {
    pub(super) async fn composition_resource_manifest(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        request: &CompositionRequest,
        schema: SchemaRecord,
        cancel: &ReadCancellation,
    ) -> Result<ManifestRecord, CompositionError> {
        let mut builder = ManifestBuilder::new(snapshot, schema)?;
        self.composition_resource_source(identity, request, &mut builder, cancel)
            .await?;
        if let Some(parent) = request.parent {
            let missing: Vec<_> = request
                .samples
                .iter()
                .filter_map(|change| match change {
                    SampleChange::Upsert(sample) if !builder.samples.contains_key(&sample.sample_id) => {
                        Some(sample.sample_id.clone())
                    }
                    _ => None,
                })
                .collect();
            for ids in missing.chunks(20) {
                cancelled(cancel)?;
                for sample in self
                    .composition_source_samples(
                        identity,
                        parent,
                        ids,
                        &builder.schema,
                        super::super::state::budget(),
                        cancel,
                    )
                    .await?
                {
                    builder.insert(sample)?;
                }
            }
        }
        for change in &request.samples {
            cancelled(cancel)?;
            builder.apply(change)?;
        }
        builder.finish(snapshot)
    }
}

pub(super) struct ManifestBuilder {
    pub schema: SchemaRecord,
    samples: BTreeMap<Vec<u8>, SampleRecord>,
    bytes: u64,
}

impl ManifestBuilder {
    fn new(snapshot: SnapshotId, schema: SchemaRecord) -> Result<Self, CompositionError> {
        let empty = ManifestRecord {
            version: 1,
            snapshot,
            schema: schema.clone(),
            samples: Vec::new(),
        };
        Ok(Self {
            schema,
            samples: BTreeMap::new(),
            bytes: bincode::serialized_size(&empty).map_err(|_| CompositionError::Corrupt)?,
        })
    }

    pub(super) fn insert(&mut self, sample: SampleRecord) -> Result<(), CompositionError> {
        let size = bincode::serialized_size(&sample).map_err(|_| CompositionError::Corrupt)?;
        let old = self
            .samples
            .get(&sample.sample_id)
            .map_or(Ok(0), bincode::serialized_size)
            .map_err(|_| CompositionError::Corrupt)?;
        let bytes = self
            .bytes
            .checked_sub(old)
            .and_then(|n| n.checked_add(size))
            .ok_or(CompositionError::Limit)?;
        if bytes > MAX_MANIFEST_BYTES {
            return Err(CompositionError::Limit);
        }
        self.bytes = bytes;
        self.samples.insert(sample.sample_id.clone(), sample);
        Ok(())
    }

    fn apply(&mut self, change: &SampleChange) -> Result<(), CompositionError> {
        match change {
            SampleChange::Remove(id) => {
                if let Some(sample) = self.samples.remove(id) {
                    self.bytes -= bincode::serialized_size(&sample).map_err(|_| CompositionError::Corrupt)?;
                }
            }
            SampleChange::Upsert(patch) => {
                if patch.fields.iter().any(|field| {
                    !self
                        .schema
                        .fields
                        .iter()
                        .any(|defined| defined.name == field.name)
                }) {
                    return Err(CompositionError::Conflict);
                }
                let mut sample = if let Some(existing) = self.samples.remove(&patch.sample_id) {
                    self.bytes -=
                        bincode::serialized_size(&existing).map_err(|_| CompositionError::Corrupt)?;
                    existing
                } else {
                    SampleRecord {
                        sample_id: patch.sample_id.clone(),
                        fields: Vec::new(),
                    }
                };
                for field in &patch.fields {
                    sample.fields.retain(|existing| existing.name != field.name);
                    sample.fields.push(field.clone());
                }
                self.insert(sample)?;
            }
        }
        Ok(())
    }

    fn finish(self, snapshot: SnapshotId) -> Result<ManifestRecord, CompositionError> {
        let manifest = ManifestRecord {
            version: 1,
            snapshot,
            schema: self.schema,
            samples: self.samples.into_values().collect(),
        };
        if manifest.samples.is_empty() {
            return Err(CompositionError::Conflict);
        }
        manifest.validate().map_err(AuthorityError::from)?;
        Ok(manifest)
    }
}
