// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    content::validate_page_budget, cursor::CursorScope, key, read::PreparedInspectionRead,
    read_fields::FieldRequest, work::InspectionWork, FieldMetadata, FieldStorageKind, InspectionError,
    InspectionPage,
};
use crate::{
    selection::EffectiveSelection, AuthorityError, DatasetAuthority, DatasetIdentity, FieldLocator,
    MetadataScanBudget, ReadCancellation, Selection, SnapshotId,
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

    pub(crate) async fn prepare_inspection_selection<'a, 'selection>(
        &'a self,
        identity: &'a DatasetIdentity,
        snapshot: SnapshotId,
        selection: &'selection Selection,
        work: &mut InspectionWork<'_>,
    ) -> Result<PreparedInspectionSelection<'a, 'selection>, InspectionError> {
        let summary = self.ready_inspection(identity, snapshot, work).await?;
        let schema = self
            .inspection_schema_definitions(identity, &summary, work)
            .await?;
        let compiled = EffectiveSelection::new(selection, &schema).map_err(AuthorityError::from)?;
        let prefix = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
        let query = serde_json::to_vec(selection).map_err(|_| InspectionError::Corrupt)?;
        let cursor = CursorScope::new(&prefix, &query);
        let delivery = PreparedInspectionRead {
            authority: self,
            identity,
            snapshot,
            summaries: BTreeMap::from([(snapshot, summary)]),
            schema,
        };
        let ids: Vec<_> = compiled.explicit_ids.iter().cloned().collect();
        delivery.members(&ids, work).await?;
        Ok(PreparedInspectionSelection {
            compiled,
            cursor,
            delivery,
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
        if metadata.storage != FieldStorageKind::Inline {
            return Err(InspectionError::Corrupt);
        }
        super::materialize::decode_locator(metadata, &stored.bytes)
    }
}

/// Local to one bounded request; its authority and immutable query cannot be replaced.
pub(crate) struct PreparedInspectionSelection<'a, 'selection> {
    compiled: EffectiveSelection<'selection>,
    cursor: CursorScope,
    delivery: PreparedInspectionRead<'a>,
}
impl<'a> PreparedInspectionSelection<'a, '_> {
    pub(crate) fn into_delivery(self) -> super::read::PreparedInspectionRead<'a> {
        self.delivery
    }

    pub(crate) async fn page(
        &mut self,
        after: Option<Vec<u8>>,
        items: usize,
        work: &mut InspectionWork<'_>,
    ) -> Result<InspectionPage<Vec<u8>>, InspectionError> {
        let members = self
            .delivery
            .authority
            .inspection_member_window(
                self.delivery.identity,
                self.delivery.snapshot,
                self.cursor.decode(after)?,
                items,
                work,
            )
            .await?;
        let fields = &self.compiled.fields;
        let mut values = vec![BTreeMap::new(); members.entries.len()];
        for start in (0..members.entries.len() * fields.len()).step_by(16) {
            let end = (start + 16).min(members.entries.len() * fields.len());
            let requests: Vec<_> = (start..end)
                .map(|index| FieldRequest {
                    sample: &members.entries[index / fields.len()],
                    field: &fields[index % fields.len()],
                })
                .collect();
            let locators = self.delivery.inline_fields(&requests, work).await?;
            for (index, locator) in (start..end).zip(locators) {
                if let Some(locator) = locator {
                    values[index / fields.len()].insert(fields[index % fields.len()].clone(), locator);
                }
            }
        }
        let entries = members
            .entries
            .into_iter()
            .zip(values)
            .filter_map(|(id, values)| self.compiled.matches(&id, &values).then_some(id))
            .collect();
        Ok(InspectionPage {
            entries,
            after: self.cursor.encode(members.after)?,
        })
    }
}
