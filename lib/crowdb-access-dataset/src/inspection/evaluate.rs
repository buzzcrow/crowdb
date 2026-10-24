// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    content::validate_page_budget, cursor::CursorScope, key, work::InspectionWork, FieldMetadata,
    FieldStorageKind, InspectionError, InspectionPage, SnapshotSummary,
};
use crate::{
    selection::EffectiveSelection, AuthorityError, DatasetAuthority, DatasetError, DatasetIdentity,
    FieldLocator, MetadataScanBudget, ReadCancellation, Selection, SnapshotId,
};
use std::collections::BTreeMap;

impl DatasetAuthority {
    /// Evaluates one bounded Sample window using effective inline values.
    /// Not is relative to this Snapshot's complete membership; chunk payloads are never read.
    /// Empty matching pages may still carry a continuation.
    /// # Errors
    /// Rejects unknown fields/IDs, mismatched cursors, unavailable ancestors and limits.
    pub async fn inspection_selection_page(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        selection: &Selection,
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<Vec<u8>>, InspectionError> {
        validate_page_budget(budget)?;
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        self.prepare_inspection_selection(identity, snapshot, selection, &mut work)
            .await?
            .page(after, budget.items, &mut work)
            .await
    }

    pub(crate) async fn prepare_inspection_selection<'a>(
        &'a self,
        identity: &'a DatasetIdentity,
        snapshot: SnapshotId,
        selection: &'a Selection,
        work: &mut InspectionWork<'_>,
    ) -> Result<PreparedInspectionSelection<'a>, InspectionError> {
        let summary = self.ready_inspection(identity, snapshot, work).await?;
        let schema = self
            .inspection_schema_definitions(identity, &summary, work)
            .await?;
        let compiled = EffectiveSelection::new(selection, &schema).map_err(AuthorityError::from)?;
        let prefix = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
        let query = serde_json::to_vec(selection).map_err(|_| InspectionError::Corrupt)?;
        let cursor = CursorScope::new(&prefix, &query);
        for id in &compiled.explicit_ids {
            let mut key = prefix.clone();
            key::component(&mut key, id);
            let value = work
                .get(&key, 128 * 1024)
                .await?
                .ok_or(AuthorityError::Invalid(DatasetError::SampleNotFound))?;
            let stored: Vec<u8> =
                serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
            if stored != *id {
                return Err(InspectionError::Corrupt);
            }
        }
        Ok(PreparedInspectionSelection {
            authority: self,
            identity,
            snapshot,
            compiled,
            cursor,
            summaries: BTreeMap::from([(snapshot, summary)]),
        })
    }

    pub(super) async fn inspection_inline_value(
        &self,
        identity: &DatasetIdentity,
        sample: &[u8],
        metadata: &FieldMetadata,
        work: &mut InspectionWork<'_>,
    ) -> Result<FieldLocator, InspectionError> {
        let mut key = key::scope(identity, metadata.source_snapshot, b'V').map_err(AuthorityError::from)?;
        key::component(&mut key, sample);
        key::component(&mut key, metadata.name.as_bytes());
        let stored = work
            .get(&key, 512 * 1024)
            .await?
            .ok_or(InspectionError::Corrupt)?;
        let locator: FieldLocator =
            serde_json::from_slice(&stored.bytes).map_err(|_| InspectionError::Corrupt)?;
        let FieldLocator::Inline { value, md5 } = &locator else {
            return Err(InspectionError::Corrupt);
        };
        if value.len() > crate::manifest::MAX_INLINE_BYTES
            || value.len() as u64 != metadata.length
            || Some(*md5) != metadata.md5
        {
            return Err(InspectionError::Corrupt);
        }
        locator.verify_payload(value).map_err(AuthorityError::from)?;
        Ok(locator)
    }
}

/// Local to one bounded request; its authority and immutable query cannot be replaced.
pub(crate) struct PreparedInspectionSelection<'a> {
    authority: &'a DatasetAuthority,
    identity: &'a DatasetIdentity,
    snapshot: SnapshotId,
    compiled: EffectiveSelection<'a>,
    cursor: CursorScope,
    summaries: BTreeMap<SnapshotId, SnapshotSummary>,
}
impl PreparedInspectionSelection<'_> {
    pub(crate) async fn page(
        &mut self,
        after: Option<Vec<u8>>,
        items: usize,
        work: &mut InspectionWork<'_>,
    ) -> Result<InspectionPage<Vec<u8>>, InspectionError> {
        let members = self
            .authority
            .inspection_member_window(
                self.identity,
                self.snapshot,
                self.cursor.decode(after)?,
                items,
                work,
            )
            .await?;
        let mut entries = Vec::new();
        for id in members.entries {
            let mut values = BTreeMap::new();
            for field in &self.compiled.fields {
                if let Some(metadata) = self
                    .authority
                    .effective_field_metadata(
                        self.identity,
                        self.snapshot,
                        &id,
                        field,
                        &mut self.summaries,
                        work,
                    )
                    .await?
                {
                    if metadata.storage == FieldStorageKind::Inline {
                        values.insert(
                            field.clone(),
                            self.authority
                                .inspection_inline_value(self.identity, &id, &metadata, work)
                                .await?,
                        );
                    }
                }
            }
            if self.compiled.matches(&id, &values) {
                entries.push(id);
            }
        }
        Ok(InspectionPage {
            entries,
            after: self.cursor.encode(members.after)?,
        })
    }
}
