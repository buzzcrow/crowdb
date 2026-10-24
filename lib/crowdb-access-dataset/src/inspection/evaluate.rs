// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    content::validate_page_budget, cursor::CursorScope, key, read::PreparedInspectionRead,
    work::InspectionWork, InspectionError, InspectionPage,
};
use crate::{
    selection::EffectiveSelection, AuthorityError, DatasetAuthority, DatasetIdentity, MetadataScanBudget,
    ReadCancellation, Selection, SnapshotId,
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
        let values = self
            .delivery
            .inline_matrix(&members.entries, &self.compiled.fields, work)
            .await?;
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
