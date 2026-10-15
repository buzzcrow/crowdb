// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    key,
    state::{budget, now, CompositionState},
    CompositionDraft, CompositionError, CompositionInput, CompositionRecord, CompositionSource,
    DraftProgress,
};
use crate::{
    split::{DraftSplit, MembershipOrigin, SplitSource},
    DatasetAuthority, DatasetIdentity, OperationId, ReadCancellation, SchemaRecord, SnapshotId,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct InitialComposition {
    input: CompositionInput,
    state: CompositionState,
}
impl DatasetAuthority {
    /// Pins the data source and inherited Split revision once for a resumable draft.
    /// # Errors
    /// Rejects conflicting retries, unavailable sources and invalid schema/definitions.
    pub async fn begin_composition(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        input: &CompositionInput,
    ) -> Result<CompositionDraft, CompositionError> {
        let key = key::scope(b"DS\0CBG\0", identity, operation)?;
        let initial = if let Some(stored) = self.composition_get(&key).await? {
            let initial: InitialComposition = crate::split::codec::decode(&stored.bytes)?;
            if initial.input != *input {
                return Err(CompositionError::Conflict);
            }
            initial
        } else {
            let initial = InitialComposition {
                input: input.clone(),
                state: self.initial_composition(identity, operation, input).await?,
            };
            let bytes = crate::split::codec::encode(&initial)?;
            match self.put_composition(&key, &bytes).await {
                Ok(()) => initial,
                Err(CompositionError::Conflict) => {
                    let stored = self
                        .composition_get(&key)
                        .await?
                        .ok_or(CompositionError::Corrupt)?;
                    let winner: InitialComposition = crate::split::codec::decode(&stored.bytes)?;
                    if winner.input != *input {
                        return Err(CompositionError::Conflict);
                    }
                    winner
                }
                Err(error) => return Err(error),
            }
        };
        let state_key = key::scope(b"DS\0CD\0", identity, operation)?;
        if self.composition_get(&state_key).await?.is_none() {
            match self.put_composition(&state_key, &initial.state.encode()?).await {
                Ok(()) | Err(CompositionError::Conflict) => {}
                Err(error) => return Err(error),
            }
        }
        let (_, state) = self.load_composition(identity, operation).await?;
        if !matches!(
            state.draft.record.progress,
            DraftProgress::Published { .. } | DraftProgress::Abandoned
        ) {
            self.renew_composition_source(identity, &state.draft).await?;
        }
        Ok(state.draft)
    }
    async fn initial_composition(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        input: &CompositionInput,
    ) -> Result<CompositionState, CompositionError> {
        super::state::validate_splits(&input.splits)?;
        let parent_schema = match input.parent {
            Some(parent) => Some(self.composition_parent_schema(identity, parent).await?),
            None => None,
        };
        let schema = input
            .schema
            .clone()
            .or_else(|| parent_schema.clone())
            .ok_or(CompositionError::Conflict)?;
        if let Some(parent_schema) = &parent_schema {
            super::edit::validate_schema_extension(parent_schema, &schema)?;
        }
        let revision = match &input.source {
            CompositionSource::Split { source, .. } => {
                if input.parent != Some(source.snapshot)
                    || input.parent_split_revision.is_some_and(|r| r != source.revision)
                {
                    return Err(CompositionError::Conflict);
                }
                Some(source.revision)
            }
            _ => match (
                input.parent,
                input.parent_split_revision,
                input.inherit_parent_splits,
            ) {
                (_, Some(revision), _) => Some(revision),
                (Some(parent), None, true) => self.inspect_split_head(identity, parent).await?.revision,
                _ => None,
            },
        };
        let mut splits = Vec::new();
        let mut policy = input.policy.unwrap_or_default();
        if input.inherit_parent_splits {
            if let (Some(parent), Some(revision)) = (input.parent, revision) {
                let record = self
                    .get_split_revision(identity, parent, revision, budget(), &ReadCancellation::new())
                    .await?;
                if input.policy.is_none() {
                    policy = record.policy;
                }
                splits.extend(record.splits.into_iter().map(|split| DraftSplit {
                    origin: MembershipOrigin::Inherited(SplitSource {
                        snapshot: parent,
                        revision,
                        name: split.name.clone(),
                    }),
                    name: split.name,
                    definition: split.definition,
                }));
            }
        }
        // Explicit definitions replace inherited definitions by name.
        for split in &input.splits {
            splits.retain(|s| s.name != split.name);
            splits.push(split.clone());
        }
        let draft = CompositionDraft {
            record: CompositionRecord {
                version: 1,
                operation,
                parent: input.parent,
                parent_split_revision: revision,
                source: input.source.clone(),
                revision: 1,
                edit_pages: 0,
                source_lease_deadline: now().saturating_add(3600),
                progress: DraftProgress::Editing,
            },
            schema,
            splits,
            policy,
            preparation: None,
        };
        self.validate_composition_splits(identity, &draft).await?;
        self.renew_composition_source(identity, &draft).await?;
        let state = CompositionState {
            draft,
            edit: None,
            journal: vec![],
            evaluation: None,
        };
        state.encode()?;
        Ok(state)
    }
    pub(super) async fn composition_parent_schema(
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
    pub(super) async fn validate_composition_splits(
        &self,
        identity: &DatasetIdentity,
        draft: &CompositionDraft,
    ) -> Result<(), CompositionError> {
        super::state::validate_splits(&draft.splits)?;
        for split in &draft.splits {
            if let MembershipOrigin::Inherited(source) = &split.origin {
                if draft.record.parent != Some(source.snapshot) {
                    return Err(CompositionError::Conflict);
                }
                let record = self
                    .get_split_revision(
                        identity,
                        source.snapshot,
                        source.revision,
                        budget(),
                        &ReadCancellation::new(),
                    )
                    .await?;
                if !record.splits.iter().any(|s| s.name == source.name) {
                    return Err(CompositionError::NotFound);
                }
            }
        }
        Ok(())
    }
}
