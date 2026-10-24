// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    composition::{state::budget, CompositionError},
    DatasetAuthority, DatasetIdentity, ReadCancellation, SchemaRecord, SnapshotId,
};

impl DatasetAuthority {
    pub(in crate::composition) async fn composition_parent_schema(
        &self,
        identity: &DatasetIdentity,
        parent: SnapshotId,
    ) -> Result<SchemaRecord, CompositionError> {
        let cancel = ReadCancellation::new();
        let summary = self
            .snapshot_inspection_summary(identity, parent, budget(), &cancel)
            .await?;
        let mut schema = SchemaRecord {
            version: summary.schema_version,
            fields: vec![],
        };
        let mut after = None;
        loop {
            let page = self
                .inspection_schema_page(identity, parent, after, budget(), &cancel)
                .await?;
            schema.fields.extend(page.entries);
            if schema.fields.len() > 256 {
                return Err(CompositionError::Limit);
            }
            after = page.after;
            if after.is_none() {
                break;
            }
        }
        Ok(schema)
    }
}

pub(in crate::composition) fn validate_schema_extension(
    parent: &SchemaRecord,
    schema: &SchemaRecord,
) -> Result<(), CompositionError> {
    super::super::state::validate_schema(schema)?;
    if parent
        .fields
        .iter()
        .filter(|f| f.required)
        .any(|p| !schema.fields.iter().any(|f| f.name == p.name && f.required))
    {
        return Err(CompositionError::Conflict);
    }
    Ok(())
}
