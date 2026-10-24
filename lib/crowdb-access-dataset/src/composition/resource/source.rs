// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{cancelled, input::ManifestBuilder, CompositionRequest};
use crate::{
    composition::{CompositionError, CompositionSource},
    inspection::work::InspectionWork,
    DatasetAuthority, DatasetIdentity, ReadCancellation,
};

impl DatasetAuthority {
    pub(super) async fn composition_resource_source(
        &self,
        identity: &DatasetIdentity,
        request: &CompositionRequest,
        builder: &mut ManifestBuilder,
        cancel: &ReadCancellation,
    ) -> Result<(), CompositionError> {
        let Some(parent) = request.parent else {
            return Ok(());
        };
        let selection = match &request.source {
            CompositionSource::All => None,
            CompositionSource::Selection(selection) => Some(selection),
            CompositionSource::Split { selection, .. } => selection.as_ref(),
        };
        let budget = super::super::state::budget();
        let frozen = if let CompositionSource::Split { source, .. } = &request.source {
            let record = self
                .get_split_revision(identity, parent, source.revision, budget, cancel)
                .await?;
            if !record.splits.iter().any(|split| split.name == source.name) {
                return Err(CompositionError::NotFound);
            }
            Some((source, record))
        } else {
            None
        };
        let mut after = None;
        loop {
            cancelled(cancel)?;
            let mut page = match selection {
                Some(selection) => {
                    self.inspection_selection_page(identity, parent, selection, after, budget, cancel)
                        .await?
                }
                None => {
                    self.inspection_samples_page(identity, parent, after, budget, cancel)
                        .await?
                }
            };
            if let Some((source, record)) = &frozen {
                let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
                let mut members = Vec::new();
                for id in page.entries {
                    if self
                        .frozen_split_contains(identity, record, &source.name, &id, &mut work)
                        .await?
                    {
                        members.push(id);
                    }
                }
                page.entries = members;
            }
            if !page.entries.is_empty() {
                for sample in self
                    .composition_source_samples(
                        identity,
                        parent,
                        &page.entries,
                        &builder.schema,
                        budget,
                        cancel,
                    )
                    .await?
                {
                    builder.insert(sample)?;
                }
            }
            after = page.after;
            if after.is_none() {
                return Ok(());
            }
        }
    }
}
