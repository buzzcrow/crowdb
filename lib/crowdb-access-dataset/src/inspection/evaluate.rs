// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    content::validate_page_budget, cursor::CursorScope, key, work::InspectionWork, FieldMetadata,
    FieldStorageKind, InspectionError, InspectionPage,
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
        let summary = self.ready_inspection(identity, snapshot, &mut work).await?;
        let schema = self
            .inspection_schema_definitions(identity, &summary, &mut work)
            .await?;
        let compiled = EffectiveSelection::new(selection, &schema).map_err(AuthorityError::from)?;
        let prefix = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
        let query = serde_json::to_vec(selection).map_err(|_| InspectionError::Corrupt)?;
        let cursor = CursorScope::new(&prefix, &query);
        let after = cursor.decode(after)?;
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
        let members = self
            .inspection_member_window(identity, snapshot, after, budget.items, &mut work)
            .await?;
        let mut summaries = BTreeMap::from([(snapshot, summary)]);
        let mut entries = Vec::new();
        for id in members.entries {
            let mut values = BTreeMap::new();
            for field in &compiled.fields {
                if let Some(metadata) = self
                    .effective_field_metadata(identity, snapshot, &id, field, &mut summaries, &mut work)
                    .await?
                {
                    if metadata.storage == FieldStorageKind::Inline {
                        values.insert(
                            field.clone(),
                            self.inspection_inline_value(identity, &id, &metadata, &mut work)
                                .await?,
                        );
                    }
                }
            }
            if compiled.matches(&id, &values) {
                entries.push(id);
            }
        }
        Ok(InspectionPage {
            entries,
            after: cursor.encode(members.after)?,
        })
    }

    async fn inspection_inline_value(
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
