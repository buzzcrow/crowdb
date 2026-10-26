// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{required_option, ControlClient, Generation, Head, OpaqueId};
use crate::Error;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSnapshotHead {
    pub snapshot: OpaqueId,
    #[serde(deserialize_with = "required_option")]
    pub expected_head: Option<Generation>,
}
impl ControlClient {
    /// Selects a complete Snapshot with an explicit condition, without a draft or attempt ID.
    /// # Errors
    /// Returns capability, unavailable resource, conditional and transport errors.
    pub async fn update_snapshot_head(&self, request: &UpdateSnapshotHead) -> Result<Head, Error> {
        self.call("snapshot-head-update", "PUT", &["head"], Some(request))
            .await
            .map_err(|error| error.with_identity(request.snapshot.as_str()))
    }
}
impl Head {
    /// Extracts the failed condition's observation; a retry requires caller review of that base.
    #[must_use]
    pub fn from_conflict(error: &Error) -> Option<Self> {
        let Error::Service { status: 409, error } = error else {
            return None;
        };
        serde_json::from_value(error.observed_head.as_deref()?.clone()).ok()
    }
}
