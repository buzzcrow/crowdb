// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, state::CompositionState, CompositionError, DraftProgress};
use crate::{
    inspection::work::InspectionWork, CasOutcome, DatasetAuthority, DatasetIdentity, MetadataScanBudget,
    OperationId, ReadCancellation, StoredValue,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};
const SEALED: &[u8] = b"composition-edit-retired-v1";
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositionEditCleanup {
    pub retired_pages: usize,
    pub complete: bool,
}
impl DatasetAuthority {
    pub(super) async fn register_composition_edit(
        &self,
        identity: &DatasetIdentity,
        old: &StoredValue,
        state: &CompositionState,
        edit: OperationId,
    ) -> Result<(StoredValue, CompositionState), CompositionError> {
        if state.registered_edits.contains(&edit) {
            return Ok((old.clone(), state.clone()));
        }
        let mut next = state.clone();
        next.edit = None;
        next.draft.record.version = 3;
        let mut ids: BTreeSet<_> = state
            .registered_edits
            .iter()
            .chain(&state.journal)
            .copied()
            .collect();
        ids.insert(edit);
        if ids.len() > 4096 {
            return Err(CompositionError::Limit);
        }
        next.registered_edits = ids.into_iter().collect();
        self.replace_composition(identity, old, state, &next).await?;
        self.load_composition(identity, state.draft.record.operation)
            .await
    }
    /// Retires registered edit payloads after the draft becomes terminal, retaining edit receipts.
    /// # Errors
    /// Rejects active drafts, corrupt progress, cancelled work and unavailable storage.
    pub async fn cleanup_composition_edits_step(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        cancel: &ReadCancellation,
    ) -> Result<CompositionEditCleanup, CompositionError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.cleanup_edit_window(identity, operation, cancel),
        )
        .await
        .map_err(|_| CompositionError::Limit)?
    }
    async fn cleanup_edit_window(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
        cancel: &ReadCancellation,
    ) -> Result<CompositionEditCleanup, CompositionError> {
        let (_, state) = self.load_composition(identity, operation).await?;
        if !matches!(
            state.draft.record.progress,
            DraftProgress::Prepared { .. } | DraftProgress::Published { .. } | DraftProgress::Abandoned
        ) {
            return Err(CompositionError::Frozen);
        }
        self.help_composition_edit(identity, &state).await?;
        let ids: BTreeSet<_> = state
            .registered_edits
            .iter()
            .chain(&state.journal)
            .copied()
            .collect();
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            cancel,
            MetadataScanBudget {
                items: 20,
                bytes: 14 * 1024 * 1024,
                timeout: Duration::from_secs(2),
            },
        )?;
        let key = key::scope(b"DS\0CEG\0", identity, operation)?;
        let stored = work.get(&key, 4096).await?;
        let mut progress: CompositionEditCleanup = stored.as_ref().map_or_else(
            || Ok(CompositionEditCleanup::default()),
            |v| crate::split::codec::decode(&v.bytes),
        )?;
        if progress.retired_pages > ids.len() || (progress.complete && progress.retired_pages != ids.len()) {
            return Err(CompositionError::Corrupt);
        }
        if progress.complete {
            return Ok(progress);
        }
        for id in ids.iter().skip(progress.retired_pages).take(20) {
            let mut page = key::scope(b"DS\0CE\0", identity, operation)?;
            page.extend(id.as_bytes());
            let value = work.get(&page, 1024 * 1024 + 65536).await?;
            match self
                .store
                .compare_exchange(&page, value.as_ref().map(|v| v.bytes.as_slice()), SEALED)
                .await?
            {
                CasOutcome::Applied(_) => {}
                CasOutcome::Conflict(Some(v)) if v.bytes == SEALED => {}
                CasOutcome::Conflict(_) => return Err(CompositionError::Conflict),
            }
            progress.retired_pages += 1;
        }
        progress.complete = progress.retired_pages == ids.len();
        let bytes = crate::split::codec::encode(&progress)?;
        match self
            .store
            .compare_exchange(&key, stored.as_ref().map(|v| v.bytes.as_slice()), &bytes)
            .await?
        {
            CasOutcome::Applied(_) => Ok(progress),
            CasOutcome::Conflict(Some(v)) if v.bytes == bytes => Ok(progress),
            CasOutcome::Conflict(_) => Err(CompositionError::Conflict),
        }
    }
}
