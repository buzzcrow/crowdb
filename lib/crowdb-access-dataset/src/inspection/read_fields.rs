// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    key, materialize::decode_locator, read::PreparedInspectionRead, work::InspectionWork, FieldMetadata,
    FieldStorageKind, InspectionError,
};
use crate::{AuthorityError, FieldLocator, SnapshotId};
use std::collections::{btree_map::Entry, BTreeMap, HashSet};

pub(super) struct FieldRequest<'a> {
    pub(super) sample: &'a [u8],
    pub(super) field: &'a str,
}

impl PreparedInspectionRead<'_> {
    pub(super) async fn inline_matrix(
        &mut self,
        ids: &[Vec<u8>],
        fields: &[String],
        work: &mut InspectionWork<'_>,
    ) -> Result<Vec<BTreeMap<String, FieldLocator>>, InspectionError> {
        let mut values = vec![BTreeMap::new(); ids.len()];
        for start in (0..ids.len() * fields.len()).step_by(16) {
            let end = (start + 16).min(ids.len() * fields.len());
            let requests: Vec<_> = (start..end)
                .map(|index| FieldRequest {
                    sample: &ids[index / fields.len()],
                    field: &fields[index % fields.len()],
                })
                .collect();
            let locators = self.inline_fields(&requests, work).await?;
            for (index, locator) in (start..end).zip(locators) {
                if let Some(locator) = locator {
                    values[index / fields.len()].insert(fields[index % fields.len()].clone(), locator);
                }
            }
        }
        Ok(values)
    }

    /// Resolve a small field window one ancestry level at a time. Shared ancestors
    /// are admitted once, and explicit tombstones stop inheritance without value IO.
    pub(super) async fn fields(
        &mut self,
        requests: &[FieldRequest<'_>],
        work: &mut InspectionWork<'_>,
    ) -> Result<Vec<Option<FieldLocator>>, InspectionError> {
        self.field_values(requests, work, false).await
    }

    /// Predicate evaluation reads inline values only, never chunk locators or payloads.
    pub(super) async fn inline_fields(
        &mut self,
        requests: &[FieldRequest<'_>],
        work: &mut InspectionWork<'_>,
    ) -> Result<Vec<Option<FieldLocator>>, InspectionError> {
        self.field_values(requests, work, true).await
    }

    async fn field_values(
        &mut self,
        requests: &[FieldRequest<'_>],
        work: &mut InspectionWork<'_>,
        inline_only: bool,
    ) -> Result<Vec<Option<FieldLocator>>, InspectionError> {
        let metadata = self.metadata(requests, work).await?;
        let keys = requests.iter().zip(&metadata).filter_map(|(request, metadata)| {
            metadata.as_ref().and_then(|metadata| {
                (metadata.storage == FieldStorageKind::Inline
                    || (!inline_only && metadata.storage != FieldStorageKind::Tombstone))
                    .then(|| field_key(self.identity, metadata.source_snapshot, b'V', request))
            })
        });
        // Key construction can fail before any read; this window contains at most 16 cells.
        let keys: Vec<_> = keys.collect::<Result<_, _>>()?;
        let mut rows = work.get_many(keys, 512 * 1024).await?.into_iter();
        metadata
            .into_iter()
            .map(|metadata| {
                metadata
                    .filter(|metadata| !inline_only || metadata.storage == FieldStorageKind::Inline)
                    .map(|metadata| {
                        if metadata.storage == FieldStorageKind::Tombstone {
                            return Ok(FieldLocator::Tombstone);
                        }
                        let row = rows.next().flatten().ok_or(InspectionError::Corrupt)?;
                        decode_locator(&metadata, &row.bytes)
                    })
                    .transpose()
            })
            .collect()
    }

    pub(super) async fn metadata(
        &mut self,
        requests: &[FieldRequest<'_>],
        work: &mut InspectionWork<'_>,
    ) -> Result<Vec<Option<FieldMetadata>>, InspectionError> {
        let mut output = vec![None; requests.len()];
        let mut unresolved: Vec<_> = (0..requests.len()).collect();
        let mut current = Some(self.snapshot);
        let mut seen = HashSet::new();
        while let Some(snapshot) = current {
            if unresolved.is_empty() {
                break;
            }
            if !seen.insert(snapshot) {
                return Err(AuthorityError::AncestryCycle.into());
            }
            if seen.len() > 64 {
                return Err(InspectionError::Limit);
            }
            if let Entry::Vacant(entry) = self.summaries.entry(snapshot) {
                entry.insert(
                    self.authority
                        .ready_inspection(self.identity, snapshot, work)
                        .await?,
                );
            }
            let keys = unresolved
                .iter()
                .map(|index| field_key(self.identity, snapshot, b'F', &requests[*index]))
                .collect::<Result<Vec<_>, _>>()?;
            let values = work.get_many(keys, 128 * 1024).await?;
            let mut missing = Vec::new();
            for (index, value) in unresolved.into_iter().zip(values) {
                if let Some(value) = value {
                    let metadata: FieldMetadata =
                        serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
                    if metadata.name != requests[index].field || metadata.source_snapshot != snapshot {
                        return Err(InspectionError::Corrupt);
                    }
                    output[index] = Some(metadata);
                } else {
                    missing.push(index);
                }
            }
            unresolved = missing;
            current = self.summaries[&snapshot].parent;
        }
        Ok(output)
    }
}

fn field_key(
    identity: &crate::DatasetIdentity,
    snapshot: SnapshotId,
    kind: u8,
    request: &FieldRequest<'_>,
) -> Result<Vec<u8>, InspectionError> {
    let mut key = key::scope(identity, snapshot, kind).map_err(AuthorityError::from)?;
    key::component(&mut key, request.sample);
    key::component(&mut key, request.field.as_bytes());
    Ok(key)
}
