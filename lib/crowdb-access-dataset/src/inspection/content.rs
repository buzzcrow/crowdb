// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, work::InspectionWork, InspectionError, InspectionPage, InspectionState, SnapshotSummary};
use crate::{
    AuthorityError, DatasetAuthority, DatasetIdentity, FieldDefinition, MetadataScanBudget, ReadCancellation,
    SnapshotId,
};

impl DatasetAuthority {
    pub(super) async fn ready_inspection(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        work: &mut InspectionWork<'_>,
    ) -> Result<SnapshotSummary, InspectionError> {
        if !self
            .inspection_snapshot_committed(identity, snapshot, work)
            .await?
        {
            return Err(AuthorityError::SnapshotNotFound.into());
        }
        self.ready_inspection_projection(identity, snapshot, work).await
    }
    pub(super) async fn ready_inspection_projection(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        work: &mut InspectionWork<'_>,
    ) -> Result<SnapshotSummary, InspectionError> {
        let key = key::scope(identity, snapshot, b'H').map_err(AuthorityError::from)?;
        let value = work
            .get(&key, 16 * 1024)
            .await?
            .ok_or(InspectionError::NotPrepared)?;
        let summary: SnapshotSummary =
            serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
        if summary.version != 1 || summary.snapshot != snapshot {
            return Err(InspectionError::Corrupt);
        }
        if let InspectionState::Unsupported { reason } = summary.state {
            return Err(InspectionError::Unsupported(reason));
        }
        Ok(summary)
    }

    /// Returns a Schema window in declaration order, without sample or value reads.
    /// # Errors
    /// Returns uncommitted/unprepared metadata, unsupported format, invalid cursor or limits.
    pub async fn inspection_schema_page(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<FieldDefinition>, InspectionError> {
        validate_page_budget(budget)?;
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        let summary = self.ready_inspection(identity, snapshot, &mut work).await?;
        let prefix = key::scope(identity, snapshot, b'S').map_err(AuthorityError::from)?;
        let cursor = super::cursor::CursorScope::new(&prefix, b"schema");
        let rows = work
            .scan(prefix.clone(), cursor.decode(after)?, budget.items)
            .await?;
        let mut entries = Vec::new();
        for (key, value) in rows.entries {
            let suffix: [u8; 4] = key
                .strip_prefix(prefix.as_slice())
                .and_then(|s| s.try_into().ok())
                .ok_or(InspectionError::Corrupt)?;
            if u32::from_be_bytes(suffix) >= summary.field_count {
                return Err(InspectionError::Corrupt);
            }
            let field: FieldDefinition =
                serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
            if field.name.is_empty() || field.name.len() > 1024 {
                return Err(InspectionError::Corrupt);
            }
            entries.push(field);
        }
        Ok(InspectionPage {
            entries,
            after: cursor.encode(rows.after)?,
        })
    }

    /// Returns Sample IDs in exact byte order. No field payloads are read.
    /// # Errors
    /// Returns uncommitted/unprepared metadata, unsupported format, invalid cursor or limits.
    pub async fn inspection_samples_page(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<Vec<u8>>, InspectionError> {
        validate_page_budget(budget)?;
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        self.ready_inspection(identity, snapshot, &mut work).await?;
        let prefix = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
        let cursor = super::cursor::CursorScope::new(&prefix, b"samples");
        let mut page = self
            .inspection_member_window(identity, snapshot, cursor.decode(after)?, budget.items, &mut work)
            .await?;
        page.after = cursor.encode(page.after)?;
        Ok(page)
    }

    pub(super) async fn inspection_member_window(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        after: Option<Vec<u8>>,
        items: usize,
        work: &mut InspectionWork<'_>,
    ) -> Result<InspectionPage<Vec<u8>>, InspectionError> {
        let prefix = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
        let rows = work.scan(prefix.clone(), after, items).await?;
        let mut entries = Vec::new();
        for (stored_key, value) in rows.entries {
            let id: Vec<u8> = serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
            if id.is_empty() || id.len() > 16 * 1024 {
                return Err(InspectionError::Corrupt);
            }
            let mut expected = prefix.clone();
            key::component(&mut expected, &id);
            if stored_key != expected {
                return Err(InspectionError::Corrupt);
            }
            entries.push(id);
        }
        Ok(InspectionPage {
            entries,
            after: rows.after,
        })
    }

    /// Looks up exact binary membership without scanning or reading payloads.
    /// # Errors
    /// Returns invalid IDs, uncommitted/unprepared metadata, cancellation or limits.
    pub async fn inspection_contains_sample(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        sample_id: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<bool, InspectionError> {
        validate_page_budget(budget)?;
        if sample_id.is_empty() || sample_id.len() > 16 * 1024 {
            return Err(InspectionError::Limit);
        }
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        self.ready_inspection(identity, snapshot, &mut work).await?;
        let mut key = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
        key::component(&mut key, sample_id);
        let Some(value) = work.get(&key, 128 * 1024).await? else {
            return Ok(false);
        };
        let stored: Vec<u8> = serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
        if stored != sample_id {
            return Err(InspectionError::Corrupt);
        }
        Ok(true)
    }
}
pub(super) fn validate_page_budget(budget: MetadataScanBudget) -> Result<(), InspectionError> {
    budget.validate()?;
    if budget.items > 100 || budget.bytes > 1024 * 1024 {
        return Err(InspectionError::Limit);
    }
    Ok(())
}
