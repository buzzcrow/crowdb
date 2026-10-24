// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, read_fields::FieldRequest, work::InspectionWork, InspectionError};
use crate::{
    AuthorityError, DatasetAuthority, DatasetError, DatasetIdentity, MetadataScanBudget, ReadCancellation,
    SampleView, SnapshotId,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

impl DatasetAuthority {
    /// Resolves effective delivery locators under one metadata byte/deadline budget.
    /// No full Manifest or chunk payload is loaded; output retains request order.
    /// # Errors
    /// Rejects unknown members/fields, unavailable projection, corruption or limits.
    pub async fn resolve_inspection_batch_locators(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        ids: &[Vec<u8>],
        fields: &[String],
        cancel: &ReadCancellation,
    ) -> Result<Vec<SampleView>, InspectionError> {
        if ids.is_empty()
            || ids.len() > crate::MAX_BATCH_SIZE
            || fields.len() > 256
            || ids.iter().collect::<BTreeSet<_>>().len() != ids.len()
            || fields.iter().collect::<BTreeSet<_>>().len() != fields.len()
        {
            return Err(AuthorityError::Invalid(DatasetError::InvalidManifest).into());
        }
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            cancel,
            MetadataScanBudget {
                items: 100,
                bytes: 16 * 1024 * 1024,
                timeout: Duration::from_secs(2),
            },
        )?;
        let summary = self.ready_inspection(identity, snapshot, &mut work).await?;
        let schema = self
            .inspection_schema_definitions(identity, &summary, &mut work)
            .await?;
        PreparedInspectionRead {
            authority: self,
            identity,
            snapshot,
            schema,
            summaries: BTreeMap::from([(snapshot, summary)]),
        }
        .resolve(ids, fields, &mut work, false)
        .await
    }
}

pub(crate) struct PreparedInspectionRead<'a> {
    pub(super) authority: &'a DatasetAuthority,
    pub(super) identity: &'a DatasetIdentity,
    pub(super) snapshot: SnapshotId,
    pub(super) schema: Vec<crate::FieldDefinition>,
    pub(super) summaries: BTreeMap<SnapshotId, super::SnapshotSummary>,
}

impl PreparedInspectionRead<'_> {
    pub(crate) async fn resolve(
        &mut self,
        ids: &[Vec<u8>],
        fields: &[String],
        work: &mut InspectionWork<'_>,
        known_members: bool,
    ) -> Result<Vec<SampleView>, InspectionError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        if ids.len() > crate::MAX_BATCH_SIZE
            || fields.len() > 256
            || ids.iter().collect::<BTreeSet<_>>().len() != ids.len()
            || fields.iter().collect::<BTreeSet<_>>().len() != fields.len()
        {
            return Err(AuthorityError::Invalid(DatasetError::InvalidManifest).into());
        }
        if fields
            .iter()
            .any(|field| !self.schema.iter().any(|known| known.name == *field))
        {
            return Err(AuthorityError::Invalid(DatasetError::InvalidManifest).into());
        }
        if ids.iter().any(|id| id.is_empty() || id.len() > 16 * 1024) {
            return Err(InspectionError::Limit);
        }
        if !known_members {
            self.members(ids, work).await?;
        }
        let mut output: Vec<_> = ids
            .iter()
            .map(|id| SampleView {
                sample_id: id.clone(),
                fields: BTreeMap::new(),
                values: BTreeMap::new(),
            })
            .collect();
        for start in (0..ids.len() * fields.len()).step_by(16) {
            let end = (start + 16).min(ids.len() * fields.len());
            let requests: Vec<_> = (start..end)
                .map(|index| FieldRequest {
                    sample: &ids[index / fields.len()],
                    field: &fields[index % fields.len()],
                })
                .collect();
            let locators = self.fields(&requests, work).await?;
            for (index, (request, locator)) in (start..end).zip(requests.into_iter().zip(locators)) {
                if let Some(locator) = locator {
                    output[index / fields.len()]
                        .fields
                        .insert(request.field.to_owned(), locator);
                }
            }
        }
        Ok(output)
    }

    async fn members(&self, ids: &[Vec<u8>], work: &mut InspectionWork<'_>) -> Result<(), InspectionError> {
        let prefix = key::scope(self.identity, self.snapshot, b'M').map_err(AuthorityError::from)?;
        for window in ids.chunks(16) {
            let keys = window
                .iter()
                .map(|id| {
                    let mut key = prefix.clone();
                    key::component(&mut key, id);
                    key
                })
                .collect();
            let rows = work.get_many(keys, 128 * 1024).await?;
            for (id, row) in window.iter().zip(rows) {
                let row = row.ok_or(AuthorityError::Invalid(DatasetError::SampleNotFound))?;
                let stored: Vec<u8> =
                    serde_json::from_slice(&row.bytes).map_err(|_| InspectionError::Corrupt)?;
                if stored != *id {
                    return Err(InspectionError::Corrupt);
                }
            }
        }
        Ok(())
    }
}
