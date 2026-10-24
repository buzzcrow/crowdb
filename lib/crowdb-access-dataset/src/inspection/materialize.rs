// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, work::InspectionWork, FieldMetadata, FieldStorageKind, InspectionError};
use crate::{
    AuthorityError, DatasetAuthority, DatasetIdentity, FieldLocator, FieldRecord, MetadataScanBudget,
    ReadCancellation, SampleRecord, SchemaRecord, SnapshotId,
};
use std::collections::{BTreeMap, BTreeSet};

impl DatasetAuthority {
    /// Copies effective locator bindings for server-side composition; chunk payloads stay untouched.
    pub(crate) async fn composition_source_samples(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        ids: &[Vec<u8>],
        target_schema: &SchemaRecord,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Vec<SampleRecord>, InspectionError> {
        if ids.len() > 20 || target_schema.fields.len() > 256 {
            return Err(InspectionError::Limit);
        }
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        let summary = self.ready_inspection(identity, snapshot, &mut work).await?;
        let schema = self
            .inspection_schema_definitions(identity, &summary, &mut work)
            .await?;
        let names: BTreeSet<_> = schema.iter().map(|field| field.name.as_str()).collect();
        let mut summaries = BTreeMap::from([(snapshot, summary)]);
        let mut samples = Vec::new();
        for id in ids {
            if id.is_empty() || id.len() > 16 * 1024 {
                return Err(InspectionError::Limit);
            }
            let mut member = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
            key::component(&mut member, id);
            let Some(value) = work.get(&member, 128 * 1024).await? else {
                continue;
            };
            let stored: Vec<u8> =
                serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
            if stored != *id {
                return Err(InspectionError::Corrupt);
            }
            let mut sample = SampleRecord {
                sample_id: id.clone(),
                fields: Vec::new(),
            };
            for field in &target_schema.fields {
                if !names.contains(field.name.as_str()) {
                    continue;
                }
                if let Some(metadata) = self
                    .effective_field_metadata(identity, snapshot, id, &field.name, &mut summaries, &mut work)
                    .await?
                {
                    let value = self
                        .composition_field_locator(identity, id, &metadata, &mut work)
                        .await?;
                    sample.fields.push(FieldRecord {
                        name: field.name.clone(),
                        value,
                    });
                }
            }
            samples.push(sample);
        }
        Ok(samples)
    }
    pub(super) async fn composition_field_locator(
        &self,
        identity: &DatasetIdentity,
        sample: &[u8],
        metadata: &FieldMetadata,
        work: &mut InspectionWork<'_>,
    ) -> Result<FieldLocator, InspectionError> {
        if metadata.storage == FieldStorageKind::Inline {
            return self
                .inspection_inline_value(identity, sample, metadata, work)
                .await;
        }
        if metadata.storage == FieldStorageKind::Tombstone {
            return Ok(FieldLocator::Tombstone);
        }
        let mut key = key::scope(identity, metadata.source_snapshot, b'V').map_err(AuthorityError::from)?;
        key::component(&mut key, sample);
        key::component(&mut key, metadata.name.as_bytes());
        let stored = work
            .get(&key, 512 * 1024)
            .await?
            .ok_or(InspectionError::Corrupt)?;
        decode_locator(metadata, &stored.bytes)
    }
}

pub(super) fn decode_locator(
    metadata: &FieldMetadata,
    bytes: &[u8],
) -> Result<FieldLocator, InspectionError> {
    let locator: FieldLocator = serde_json::from_slice(bytes).map_err(|_| InspectionError::Corrupt)?;
    match &locator {
        FieldLocator::Inline { value, md5 }
            if metadata.storage == FieldStorageKind::Inline
                && value.len() <= crate::manifest::MAX_INLINE_BYTES
                && value.len() as u64 == metadata.length
                && Some(*md5) == metadata.md5 =>
        {
            locator.verify_payload(value).map_err(AuthorityError::from)?;
        }
        FieldLocator::Chunk {
            location,
            length,
            md5,
        } if metadata.storage == FieldStorageKind::Chunk
            && !location.is_empty()
            && location.len() <= 64 * 1024
            && *length != 0
            && *length == metadata.length
            && Some(*md5) == metadata.md5 => {}
        _ => return Err(InspectionError::Corrupt),
    }
    Ok(locator)
}
