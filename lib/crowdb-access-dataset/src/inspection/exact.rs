// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{read::PreparedInspectionRead, work::InspectionWork, InspectionError, InspectionSample};
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
        let schema = self
            .validate_inspection_projection(identity, &summary, fields, &mut work)
            .await?;
        if ids.iter().any(|id| id.is_empty() || id.len() > 16 * 1024) {
            return Err(InspectionError::Limit);
        }
        let mut read = PreparedInspectionRead {
            authority: self,
            identity,
            snapshot,
            schema,
            summaries: BTreeMap::from([(snapshot, summary)]),
        };
        read.members(ids, &mut work).await?;
        read.metadata_matrix(ids, fields, &mut work).await
    }
}
