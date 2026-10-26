// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::DatasetClient;
use crate::{OperationContext, Result};
use crowdb_access_dataset::{
    composition::{CompositionRequest, CompositionResult},
    split::{SplitHeadUpdate, SplitPreparation, SplitReference, SplitRevisionDeletion, SplitRevisionRecord},
    DatasetHead, ReadCancellation, SnapshotHeadUpdate, SnapshotId, SplitRevisionId,
};

impl DatasetClient {
    /// Marks one complete historical revision for deletion after its fixed grace period.
    /// Retry the same Snapshot and revision after an uncertain outcome.
    /// # Errors
    /// Rejects protected or missing revisions, admission failures and uncertain mutations.
    pub async fn delete_split_revision(
        &self,
        context: OperationContext,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
    ) -> Result<SplitRevisionDeletion> {
        self.control(context, |scope| async move {
            Ok(scope
                .authority
                .delete_split_revision(&scope.identity, snapshot, revision, &ReadCancellation::new())
                .await?)
        })
        .await
    }

    /// Prepares a complete immutable Snapshot and claims its final payload owners.
    /// Retry with the same resource ID and complete input after an uncertain outcome.
    /// # Errors
    /// Rejects unavailable sources, divergent content, invalid payloads and work limits.
    pub async fn prepare_snapshot(
        &self,
        context: OperationContext,
        snapshot: SnapshotId,
        input: &CompositionRequest,
    ) -> Result<CompositionResult> {
        self.control(context, |scope| async move {
            Ok(scope
                .authority
                .prepare_composition_resource(
                    &scope.identity,
                    snapshot,
                    input,
                    Some(&scope.uploads),
                    &ReadCancellation::new(),
                )
                .await?)
        })
        .await
    }

    /// Updates latest using the caller's exact base; a conflict carries the observed head.
    /// # Errors
    /// Rejects unavailable resources or changed bases; interrupted writes remain uncertain.
    pub async fn update_snapshot_head(
        &self,
        context: OperationContext,
        update: &SnapshotHeadUpdate,
    ) -> Result<DatasetHead> {
        self.control(context, |scope| async move {
            Ok(scope
                .authority
                .update_snapshot_head(&scope.identity, update)
                .await?)
        })
        .await
    }

    /// Prepares one immutable Split revision from complete, frozen definitions.
    /// # Errors
    /// Rejects divergent content, unavailable sources, invalid policies and work limits.
    pub async fn prepare_split_revision(
        &self,
        context: OperationContext,
        snapshot: SnapshotId,
        input: &SplitPreparation,
    ) -> Result<SplitRevisionRecord> {
        self.control(context, |scope| async move {
            Ok(scope
                .authority
                .prepare_split_revision(&scope.identity, snapshot, input, &ReadCancellation::new())
                .await?)
        })
        .await
    }

    /// Updates the Split pointer using both supplied revision and generation conditions.
    /// # Errors
    /// Rejects changed bases with an observed head or propagates uncertain writes.
    pub async fn update_split_head(
        &self,
        context: OperationContext,
        snapshot: SnapshotId,
        update: &SplitHeadUpdate,
    ) -> Result<SplitReference> {
        self.control(context, |scope| async move {
            Ok(scope
                .authority
                .update_split_head(&scope.identity, snapshot, update)
                .await?)
        })
        .await
    }
}
