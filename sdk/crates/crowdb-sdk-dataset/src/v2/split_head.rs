// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{required_option, ControlClient, Generation, OpaqueId, SplitHead};
use crate::Error;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSplitHead {
    pub revision: OpaqueId,
    #[serde(deserialize_with = "required_option")]
    pub expected_revision: Option<OpaqueId>,
    #[serde(deserialize_with = "required_option")]
    pub expected_head: Option<Generation>,
}

impl ControlClient {
    /// Updates currentSplit using a complete result and explicit caller-owned preconditions.
    /// Does not create a request ledger or depend on an editing draft.
    /// # Errors
    /// Returns capability, missing result, deletion admission, conditional or transport errors.
    pub async fn update_split_head(
        &self,
        snapshot: &OpaqueId,
        request: &UpdateSplitHead,
    ) -> Result<SplitHead, Error> {
        self.call(
            "split-head-update",
            "PUT",
            &["snapshots", snapshot.as_str(), "split-head"],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(request.revision.as_str()))
    }
}

impl SplitHead {
    /// Extracts the failed condition's observation; another writer may already have changed it.
    #[must_use]
    pub fn from_conflict(error: &Error) -> Option<Self> {
        let Error::Service { status: 409, error } = error else {
            return None;
        };
        serde_json::from_value(error.observed_head.as_deref()?.clone()).ok()
    }
}
