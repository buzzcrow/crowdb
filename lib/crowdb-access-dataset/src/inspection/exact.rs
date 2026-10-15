// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, work::InspectionWork, InspectionError, InspectionSample};
use crate::{
    AuthorityError, DatasetAuthority, DatasetError, DatasetIdentity, MetadataScanBudget, ReadCancellation,
    SnapshotId,
};
use std::collections::{BTreeMap, BTreeSet};

impl DatasetAuthority {
    /// Projects one explicit bounded Sample window, including IDs from a Split page.
    /// This reads field metadata only and does not create or acknowledge a read plan.
    /// # Errors
    /// Rejects unknown members/fields, duplicate inputs, corrupt metadata and limits.
    #[allow(clippy::too_many_arguments)]
    pub async fn inspection_exact_matrix(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        ids: &[Vec<u8>],
        fields: &[String],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Vec<InspectionSample>, InspectionError> {
        super::content::validate_page_budget(budget)?;
        if ids.is_empty()
            || ids.len() > budget.items
            || fields.is_empty()
            || fields.len() > 12
            || ids.iter().collect::<BTreeSet<_>>().len() != ids.len()
            || fields.iter().collect::<BTreeSet<_>>().len() != fields.len()
            || fields.iter().any(|f| f.is_empty() || f.len() > 1024)
        {
            return Err(AuthorityError::Invalid(DatasetError::InvalidManifest).into());
        }
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        let summary = self.ready_inspection(identity, snapshot, &mut work).await?;
        self.validate_inspection_projection(identity, &summary, fields, &mut work)
            .await?;
        let mut summaries = BTreeMap::from([(snapshot, summary)]);
        let mut entries = Vec::new();
        for id in ids {
            if id.is_empty() || id.len() > 16 * 1024 {
                return Err(InspectionError::Limit);
            }
            let mut member = key::scope(identity, snapshot, b'M').map_err(AuthorityError::from)?;
            key::component(&mut member, id);
            let row = work
                .get(&member, 128 * 1024)
                .await?
                .ok_or(AuthorityError::Invalid(DatasetError::SampleNotFound))?;
            let stored: Vec<u8> = serde_json::from_slice(&row.bytes).map_err(|_| InspectionError::Corrupt)?;
            if stored != *id {
                return Err(InspectionError::Corrupt);
            }
            let mut metadata = Vec::new();
            for field in fields {
                metadata.push(
                    self.effective_field_metadata(identity, snapshot, id, field, &mut summaries, &mut work)
                        .await?,
                );
            }
            entries.push(InspectionSample {
                sample_id: id.clone(),
                fields: metadata,
            });
        }
        Ok(entries)
    }
}
