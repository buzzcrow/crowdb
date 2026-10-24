// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, work::InspectionWork, InspectionError};
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
        let mut output = Vec::new();
        for id in ids {
            if id.is_empty() || id.len() > 16 * 1024 {
                return Err(InspectionError::Limit);
            }
            if !known_members {
                let mut member =
                    key::scope(self.identity, self.snapshot, b'M').map_err(AuthorityError::from)?;
                key::component(&mut member, id);
                let value = work
                    .get(&member, 128 * 1024)
                    .await?
                    .ok_or(AuthorityError::Invalid(DatasetError::SampleNotFound))?;
                let stored: Vec<u8> =
                    serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
                if stored != *id {
                    return Err(InspectionError::Corrupt);
                }
            }
            let mut sample = SampleView {
                sample_id: id.clone(),
                fields: BTreeMap::new(),
                values: BTreeMap::new(),
            };
            for field in fields {
                if let Some(metadata) = self
                    .authority
                    .effective_field_metadata(
                        self.identity,
                        self.snapshot,
                        id,
                        field,
                        &mut self.summaries,
                        work,
                    )
                    .await?
                {
                    let locator = self
                        .authority
                        .composition_field_locator(self.identity, id, &metadata, work)
                        .await?;
                    sample.fields.insert(field.clone(), locator);
                }
            }
            output.push(sample);
        }
        Ok(output)
    }
}
