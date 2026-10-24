// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    content::validate_page_budget, key, read::PreparedInspectionRead, read_fields::FieldRequest,
    work::InspectionWork, FieldMetadata, InspectionError, InspectionPage, SnapshotSummary,
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
        let schema = self
            .validate_inspection_projection(identity, &summary, fields, &mut work)
            .await?;
        let prefix = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
        let query = serde_json::to_vec(fields).map_err(|_| InspectionError::Corrupt)?;
        let cursor = super::cursor::CursorScope::new(&prefix, &query);
        let after = cursor.decode(after)?;
        let samples = self
            .inspection_member_window(identity, snapshot, after, budget.items, &mut work)
            .await?;
        let entries = PreparedInspectionRead {
            authority: self,
            identity,
            snapshot,
            schema,
            summaries: BTreeMap::from([(snapshot, summary)]),
        }
        .metadata_matrix(&samples.entries, fields, &mut work)
        .await?;
        Ok(InspectionPage {
            entries,
            after: cursor.encode(samples.after)?,
        })
    }

    pub(super) async fn validate_inspection_projection(
        &self,
        identity: &DatasetIdentity,
        summary: &SnapshotSummary,
        fields: &[String],
        work: &mut InspectionWork<'_>,
    ) -> Result<Vec<FieldDefinition>, InspectionError> {
        let definitions = self
            .inspection_schema_definitions(identity, summary, work)
            .await?;
        let names: HashSet<_> = definitions.iter().map(|field| &field.name).collect();
        if fields.iter().any(|field| !names.contains(field)) {
            return Err(AuthorityError::Invalid(crate::DatasetError::InvalidManifest).into());
        }
        Ok(definitions)
    }

    pub(super) async fn inspection_schema_definitions(
        &self,
        identity: &DatasetIdentity,
        summary: &SnapshotSummary,
        work: &mut InspectionWork<'_>,
    ) -> Result<Vec<FieldDefinition>, InspectionError> {
        if summary.field_count > 256 {
            return Err(InspectionError::Corrupt);
        }
        let prefix = key::scope(identity, summary.snapshot, b'S').map_err(AuthorityError::from)?;
        let mut names = HashSet::new();
        let mut definitions = Vec::new();
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
                if !names.insert(field.name.clone()) {
                    return Err(InspectionError::Corrupt);
                }
                definitions.push(field);
            }
            after = rows.after;
            if after.is_none() {
                break;
            }
        }
        if count != summary.field_count {
            return Err(InspectionError::Corrupt);
        }
        Ok(definitions)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn effective_field_metadata(
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

impl PreparedInspectionRead<'_> {
    pub(super) async fn metadata_matrix(
        &mut self,
        ids: &[Vec<u8>],
        fields: &[String],
        work: &mut InspectionWork<'_>,
    ) -> Result<Vec<InspectionSample>, InspectionError> {
        let mut rows: Vec<_> = ids
            .iter()
            .map(|id| InspectionSample {
                sample_id: id.clone(),
                fields: Vec::new(),
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
            for (index, metadata) in (start..end).zip(self.metadata(&requests, work).await?) {
                rows[index / fields.len()].fields.push(metadata);
            }
        }
        Ok(rows)
    }
}
