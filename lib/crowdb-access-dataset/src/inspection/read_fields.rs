// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    key, materialize::decode_locator, read::PreparedInspectionRead, work::InspectionWork, FieldMetadata,
    FieldStorageKind, InspectionError,
};
use crate::{AuthorityError, FieldLocator, SnapshotId};
use std::collections::{btree_map::Entry, HashSet};

pub(super) struct FieldRequest<'a> {
    pub(super) sample: &'a [u8],
    pub(super) field: &'a str,
}

impl PreparedInspectionRead<'_> {
    /// Resolve a small field window one ancestry level at a time. Shared ancestors
    /// are admitted once, and explicit tombstones stop inheritance without value IO.
    pub(super) async fn fields(
        &mut self,
        requests: &[FieldRequest<'_>],
        work: &mut InspectionWork<'_>,
    ) -> Result<Vec<Option<FieldLocator>>, InspectionError> {
        let metadata = self.metadata(requests, work).await?;
        let keys = requests.iter().zip(&metadata).filter_map(|(request, metadata)| {
            metadata.as_ref().and_then(|metadata| {
                (metadata.storage != FieldStorageKind::Tombstone)
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

    async fn metadata(
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
