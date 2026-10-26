// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit retirement of complete Split metadata independently of its payload owner.

use super::SplitLifecycleError;
use crate::{DatasetAuthority, DatasetIdentity, ReadCancellation, SnapshotId, SplitRevisionId};

mod admission;
mod reclaim;
pub(in crate::split) mod resource;
mod sweep;
pub use reclaim::SplitRevisionReclaimCursor;
pub use sweep::SplitRevisionReclaimSweep;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitRevisionDeletion {
    pub snapshot: SnapshotId,
    pub revision: SplitRevisionId,
    pub reclaim_after: Option<u64>,
    pub complete: bool,
}

impl DatasetAuthority {
    /// Closes one historical Split revision and leaves cleanup to bounded background work.
    /// # Errors
    /// Rejects protected or missing resources, cancellation and unconfirmed storage mutations.
    pub async fn delete_split_revision(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
        cancel: &ReadCancellation,
    ) -> Result<SplitRevisionDeletion, SplitLifecycleError> {
        self.mark_split_revision(identity, snapshot, revision, cancel)
            .await
    }
}

fn cancelled(cancel: &ReadCancellation) -> Result<(), SplitLifecycleError> {
    if cancel.is_cancelled() {
        return Err(crate::AuthorityError::Invalid(crate::DatasetError::ReadCancelled).into());
    }
    Ok(())
}
