// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    content::validate_page_budget, key, work::InspectionWork, FieldMetadata, InspectionError, InspectionPage,
    SnapshotSummary,
};
use crate::{
    AuthorityError, DatasetAuthority, DatasetIdentity, FieldDefinition, MetadataScanBudget, ReadCancellation,
    SnapshotId,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InspectionSample {
    pub sample_id: Vec<u8>,
    /// Fields follow the requested projection. None means missing, not tombstoned.
    pub fields: Vec<Option<FieldMetadata>>,
}

impl DatasetAuthority {
    /// Returns a Sample window with effective field metadata, resolving inherited fields.
    /// Payload bytes, chunk storage and read-session progress are never accessed.
    /// # Errors
    /// Returns invalid projection, unavailable ancestors, cancellation or aggregate limits.
    #[allow(clippy::too_many_arguments)]
    pub async fn inspection_matrix_page(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        fields: &[String],
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<InspectionSample>, InspectionError> {
        validate_page_budget(budget)?;
        if fields.is_empty()
            || fields.len() > 256
            || fields.iter().any(|f| f.is_empty() || f.len() > 1024)
            || fields.iter().collect::<HashSet<_>>().len() != fields.len()
        {
            return Err(AuthorityError::Invalid(crate::DatasetError::InvalidManifest).into());
        }
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        let summary = self.ready_inspection(identity, snapshot, &mut work).await?;
        self.validate_inspection_projection(identity, &summary, fields, &mut work)
            .await?;
        let prefix = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
        let query = serde_json::to_vec(fields).map_err(|_| InspectionError::Corrupt)?;
        let cursor = super::cursor::CursorScope::new(&prefix, &query);
        let after = cursor.decode(after)?;
        let samples = self
            .inspection_member_window(identity, snapshot, after, budget.items, &mut work)
            .await?;
        let mut summaries = BTreeMap::from([(snapshot, summary)]);
        let mut entries = Vec::new();
        for sample_id in samples.entries {
            let mut metadata = Vec::new();
            for field in fields {
                metadata.push(
                    self.effective_field_metadata(
                        identity,
                        snapshot,
                        &sample_id,
                        field,
                        &mut summaries,
                        &mut work,
                    )
                    .await?,
                );
            }
            entries.push(InspectionSample {
                sample_id,
                fields: metadata,
            });
        }
        Ok(InspectionPage {
            entries,
            after: cursor.encode(samples.after)?,
        })
    }

    async fn validate_inspection_projection(
        &self,
        identity: &DatasetIdentity,
        summary: &SnapshotSummary,
        fields: &[String],
        work: &mut InspectionWork<'_>,
    ) -> Result<(), InspectionError> {
        if summary.field_count > 256 {
            return Err(InspectionError::Corrupt);
        }
        let prefix = key::scope(identity, summary.snapshot, b'S').map_err(AuthorityError::from)?;
        let mut names = HashSet::new();
        // This bounded schema scan reads definitions only, never Sample rows.
        let mut after = None;
        let mut count = 0;
        loop {
            let rows = work.scan(prefix.clone(), after, 256).await?;
            for (_, value) in rows.entries {
                count += 1;
                if count > summary.field_count {
                    return Err(InspectionError::Corrupt);
                }
                let field: FieldDefinition =
                    serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
                if !names.insert(field.name) {
                    return Err(InspectionError::Corrupt);
                }
            }
            after = rows.after;
            if after.is_none() {
                break;
            }
        }
        if count != summary.field_count {
            return Err(InspectionError::Corrupt);
        }
        if fields.iter().any(|field| !names.contains(field)) {
            return Err(AuthorityError::Invalid(crate::DatasetError::InvalidManifest).into());
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn effective_field_metadata(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        sample_id: &[u8],
        field: &str,
        summaries: &mut BTreeMap<SnapshotId, SnapshotSummary>,
        work: &mut InspectionWork<'_>,
    ) -> Result<Option<FieldMetadata>, InspectionError> {
        let mut current = Some(snapshot);
        let mut seen = HashSet::new();
        while let Some(snapshot) = current {
            if !seen.insert(snapshot) {
                return Err(AuthorityError::AncestryCycle.into());
            }
            if seen.len() > 64 {
                return Err(InspectionError::Limit);
            }
            if let std::collections::btree_map::Entry::Vacant(entry) = summaries.entry(snapshot) {
                entry.insert(self.ready_inspection(identity, snapshot, work).await?);
            }
            let mut key = key::scope(identity, snapshot, b'F').map_err(AuthorityError::from)?;
            key::component(&mut key, sample_id);
            key::component(&mut key, field.as_bytes());
            if let Some(value) = work.get(&key, 128 * 1024).await? {
                let metadata: FieldMetadata =
                    serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
                if metadata.name != field || metadata.source_snapshot != snapshot {
                    return Err(InspectionError::Corrupt);
                }
                return Ok(Some(metadata));
            }
            current = summaries[&snapshot].parent;
        }
        Ok(None)
    }
}
