// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    key, read::PreparedInspectionRead, read_fields::FieldRequest, work::InspectionWork, FieldMetadata,
    FieldStorageKind, InspectionError,
};
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
        let fields: Vec<_> = target_schema
            .fields
            .iter()
            .filter(|field| names.contains(field.name.as_str()))
            .map(|field| field.name.clone())
            .collect();
        let mut samples = self
            .composition_members(identity, snapshot, ids, &mut work)
            .await?;
        let mut read = PreparedInspectionRead {
            authority: self,
            identity,
            snapshot,
            schema,
            summaries: BTreeMap::from([(snapshot, summary)]),
        };
        for start in (0..samples.len() * fields.len()).step_by(16) {
            let end = (start + 16).min(samples.len() * fields.len());
            let requests: Vec<_> = (start..end)
                .map(|index| FieldRequest {
                    sample: &samples[index / fields.len()].sample_id,
                    field: &fields[index % fields.len()],
                })
                .collect();
            let locators = read.fields(&requests, &mut work).await?;
            for (index, locator) in (start..end).zip(locators) {
                if let Some(value) = locator {
                    samples[index / fields.len()].fields.push(FieldRecord {
                        name: fields[index % fields.len()].clone(),
                        value,
                    });
                }
            }
        }
        Ok(samples)
    }

    async fn composition_members(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        ids: &[Vec<u8>],
        work: &mut InspectionWork<'_>,
    ) -> Result<Vec<SampleRecord>, InspectionError> {
        if ids.iter().any(|id| id.is_empty() || id.len() > 16 * 1024) {
            return Err(InspectionError::Limit);
        }
        let prefix = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
        let keys = ids
            .iter()
            .map(|id| {
                let mut member = prefix.clone();
                key::component(&mut member, id);
                member
            })
            .collect();
        let values = work.get_many(keys, 128 * 1024).await?;
        ids.iter()
            .zip(values)
            .filter_map(|(id, value)| {
                value.map(|value| {
                    let stored: Vec<u8> =
                        serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
                    if stored != *id {
                        return Err(InspectionError::Corrupt);
                    }
                    Ok(SampleRecord {
                        sample_id: id.clone(),
                        fields: Vec::new(),
                    })
                })
            })
            .collect()
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
