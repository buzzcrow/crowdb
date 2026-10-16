// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, work::InspectionWork, InspectionError, SnapshotSummary};
use crate::{
    AuthorityError, DatasetAuthority, DatasetIdentity, MetadataScanBudget, ReadCancellation, SnapshotId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectionPage<T> {
    pub entries: Vec<T>,
    /// Opaque, versioned continuation bound to Dataset/Snapshot/query scope.
    pub after: Option<Vec<u8>>,
}

impl DatasetAuthority {
    /// Enumerates committed Snapshot roots and branches in opaque-ID order.
    /// Empty pages can have a continuation when scanned candidates are uncommitted.
    /// # Errors
    /// Returns invalid scope/cursor, corrupt metadata, cancellation or aggregate limits.
    pub async fn snapshot_inventory_page(
        &self,
        identity: &DatasetIdentity,
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<SnapshotSummary>, InspectionError> {
        let mut prefix = key::inventory(identity, SnapshotId::random()).map_err(AuthorityError::from)?;
        prefix.truncate(prefix.len() - 16);
        self.snapshot_summary_page(identity, prefix, after, budget, cancel)
            .await
    }

    /// Enumerates committed children of a concrete parent, or roots for `None`.
    /// # Errors
    /// Returns invalid scope/cursor, corrupt metadata, cancellation or aggregate limits.
    pub async fn snapshot_children_page(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<SnapshotSummary>, InspectionError> {
        let mut prefix = key::child(identity, parent, SnapshotId::random()).map_err(AuthorityError::from)?;
        prefix.truncate(prefix.len() - 16);
        self.snapshot_summary_page(identity, prefix, after, budget, cancel)
            .await
    }

    async fn snapshot_summary_page(
        &self,
        identity: &DatasetIdentity,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<SnapshotSummary>, InspectionError> {
        if budget.items > 100 || budget.bytes > 1024 * 1024 {
            return Err(InspectionError::Limit);
        }
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        let cursor = super::cursor::CursorScope::new(&prefix, b"inventory");
        let rows = work
            .scan(prefix.clone(), cursor.decode(after)?, budget.items)
            .await?;
        let mut entries = Vec::new();
        for (key, value) in rows.entries {
            let summary: SnapshotSummary =
                serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
            if summary.version != 1
                || key.strip_prefix(prefix.as_slice()) != Some(summary.snapshot.as_bytes())
            {
                return Err(InspectionError::Corrupt);
            }
            if !self
                .inspection_snapshot_committed(identity, summary.snapshot, &mut work)
                .await?
            {
                continue;
            }
            let ready_key =
                super::key::scope(identity, summary.snapshot, b'H').map_err(AuthorityError::from)?;
            let ready = work
                .get(&ready_key, 16 * 1024)
                .await?
                .ok_or(InspectionError::NotPrepared)?;
            if ready.bytes != value.bytes {
                return Err(InspectionError::Corrupt);
            }
            entries.push(summary);
        }
        Ok(InspectionPage {
            entries,
            after: cursor.encode(rows.after)?,
        })
    }

    /// Reads a committed summary without materializing the publication Manifest.
    /// # Errors
    /// Returns missing commit evidence, unprepared metadata, cancellation or limits.
    pub async fn snapshot_inspection_summary(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<SnapshotSummary, InspectionError> {
        if budget.items > 100 || budget.bytes > 1024 * 1024 {
            return Err(InspectionError::Limit);
        }
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        if !self
            .inspection_snapshot_committed(identity, snapshot, &mut work)
            .await?
        {
            return Err(AuthorityError::SnapshotNotFound.into());
        }
        let key = key::scope(identity, snapshot, b'H').map_err(AuthorityError::from)?;
        let stored = work
            .get(&key, 16 * 1024)
            .await?
            .ok_or(InspectionError::NotPrepared)?;
        let summary: SnapshotSummary =
            serde_json::from_slice(&stored.bytes).map_err(|_| InspectionError::Corrupt)?;
        if summary.version != 1 || summary.snapshot != snapshot {
            return Err(InspectionError::Corrupt);
        }
        Ok(summary)
    }
}
