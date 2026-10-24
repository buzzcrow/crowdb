// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{ControlClient, DraftSplit, OpaqueId, SplitPolicy, SplitRevision};
use crate::Error;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareSplitRevision {
    pub policy: SplitPolicy,
    pub splits: Vec<DraftSplit>,
}
impl ControlClient {
    /// Prepares immutable membership from complete caller input, without editing or progress state.
    /// # Errors
    /// Returns unsupported capabilities, identity conflicts, source/policy errors and transfer failures.
    pub async fn prepare_split_revision(
        &self,
        snapshot: &OpaqueId,
        revision: &OpaqueId,
        request: &PrepareSplitRevision,
    ) -> Result<SplitRevision, Error> {
        self.call(
            "split-preparation",
            "PUT",
            &[
                "snapshots",
                snapshot.as_str(),
                "split-revisions",
                revision.as_str(),
            ],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(revision.as_str()))
    }
}
