// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ControlClient, OpaqueId, SplitHead, SplitRevision, SplitRevisionDeletion};
use crate::Error;

impl ControlClient {
    /// Marks a complete historical revision for deletion; retries preserve both resource IDs.
    /// The fixed deadline is not extended by repeating this request.
    /// # Errors
    /// Returns missing capability, protected or missing revision, transport or service errors.
    pub async fn delete_split_revision(
        &self,
        snapshot: &OpaqueId,
        revision: &OpaqueId,
    ) -> Result<SplitRevisionDeletion, Error> {
        self.call::<(), _>(
            "split-revision-delete",
            "DELETE",
            &[
                "snapshots",
                snapshot.as_str(),
                "split-revisions",
                revision.as_str(),
            ],
            None,
        )
        .await
    }

    /// Reads the current Split reference for one available Snapshot.
    /// # Errors
    /// Returns capability, missing owner or service failures.
    pub async fn split_head(&self, snapshot: &OpaqueId) -> Result<SplitHead, Error> {
        self.call::<(), _>(
            "split-revisions",
            "GET",
            &["snapshots", snapshot.as_str(), "split-head"],
            None,
        )
        .await
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn split_revision(
        &self,
        snapshot: &OpaqueId,
        revision: &OpaqueId,
    ) -> Result<SplitRevision, Error> {
        self.call::<(), _>(
            "split-revisions",
            "GET",
            &[
                "snapshots",
                snapshot.as_str(),
                "split-revisions",
                revision.as_str(),
            ],
            None,
        )
        .await
    }
}
