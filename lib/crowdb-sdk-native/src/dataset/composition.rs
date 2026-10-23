// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{DatasetClient, DatasetControl};
use crate::{OperationContext, Result};
use crowdb_access_dataset::{
    composition::{
        CompositionDraft, CompositionEdit, CompositionInput, CompositionPreview, CompositionUploadStatus,
        DraftProgress,
    },
    CommitAttempt, OperationId, ReadCancellation, SuccessfulCommit,
};

#[derive(Debug)]
pub struct CompositionStatus {
    pub draft: CompositionDraft,
    pub uploads: CompositionUploadStatus,
}

#[derive(Debug)]
pub struct CompositionCleanup {
    pub complete: bool,
    pub retired_upload_owners: u64,
    pub physical_cleanup_deferred: bool,
}

impl DatasetControl {
    async fn composition_status(&self, operation: OperationId) -> Result<CompositionStatus> {
        Ok(CompositionStatus {
            draft: self
                .authority
                .composition_status(&self.identity, operation)
                .await?,
            uploads: self
                .authority
                .composition_upload_status(&self.identity, operation)
                .await?,
        })
    }
}

impl DatasetClient {
    /// Starts an upload-managed composition before any edits can acquire payloads.
    /// # Errors
    /// Returns divergent identity/input or uncertain storage outcomes.
    pub async fn begin_composition(
        &self,
        context: OperationContext,
        operation: OperationId,
        input: &CompositionInput,
    ) -> Result<CompositionStatus> {
        Box::pin(self.control(context, |scope| async move {
            scope
                .authority
                .begin_composition(&scope.identity, operation, input)
                .await?;
            scope
                .authority
                .manage_composition_uploads(&scope.identity, operation)
                .await?;
            scope.composition_status(operation).await
        }))
        .await
    }

    /// # Errors
    /// Returns missing/corrupt composition or unavailable metadata.
    pub async fn composition_status(
        &self,
        context: OperationContext,
        operation: OperationId,
    ) -> Result<CompositionStatus> {
        Box::pin(self.control(context, |scope| async move {
            scope.composition_status(operation).await
        }))
        .await
    }

    /// Applies one revision-bound edit with the same durable upload owners as HTTP.
    /// # Errors
    /// Returns revision conflict, invalid payloads or uncertain metadata writes.
    pub async fn edit_composition(
        &self,
        context: OperationContext,
        operation: OperationId,
        edit: &CompositionEdit,
    ) -> Result<CompositionStatus> {
        Box::pin(self.control(context, |scope| async move {
            scope
                .authority
                .edit_composition_with_uploads(&scope.identity, operation, edit, &scope.uploads)
                .await?;
            scope.composition_status(operation).await
        }))
        .await
    }

    /// Advances one bounded evaluation page; callers carry the returned cursor.
    /// # Errors
    /// Returns stale revision/page, invalid selection or uncertain metadata writes.
    pub async fn evaluate_composition(
        &self,
        context: OperationContext,
        operation: OperationId,
        revision: u64,
        page: u64,
    ) -> Result<CompositionPreview> {
        Box::pin(self.control(context, |scope| async move {
            Ok(scope
                .authority
                .evaluate_composition(
                    &scope.identity,
                    operation,
                    revision,
                    page,
                    &ReadCancellation::new(),
                )
                .await?)
        }))
        .await
    }

    /// Advances preparation and upload ownership in bounded, resumable windows.
    /// # Errors
    /// Returns stale revision, invalid preparation or uncertain metadata writes.
    pub async fn prepare_composition(
        &self,
        context: OperationContext,
        operation: OperationId,
        revision: u64,
    ) -> Result<CompositionStatus> {
        Box::pin(self.control(context, |scope| async move {
            let cancel = ReadCancellation::new();
            let draft = scope
                .authority
                .prepare_composition(&scope.identity, operation, revision, &cancel)
                .await?;
            if matches!(draft.record.progress, DraftProgress::Prepared { .. }) {
                scope
                    .authority
                    .prepare_composition_uploads(&scope.identity, operation, &scope.uploads, &cancel)
                    .await?;
            }
            scope.composition_status(operation).await
        }))
        .await
    }

    /// Publishes using the caller's durable attempt identity, without replay.
    /// # Errors
    /// Returns incomplete ownership, conflicting head or uncertain publication.
    pub async fn publish_composition(
        &self,
        context: OperationContext,
        operation: OperationId,
        attempt: &CommitAttempt,
    ) -> Result<SuccessfulCommit> {
        Box::pin(self.control(context, |scope| async move {
            Ok(scope
                .authority
                .publish_composition(&scope.identity, operation, attempt)
                .await?)
        }))
        .await
    }

    /// # Errors
    /// Returns stale revision, publication conflict or uncertain metadata writes.
    pub async fn abandon_composition(
        &self,
        context: OperationContext,
        operation: OperationId,
        revision: u64,
    ) -> Result<CompositionStatus> {
        Box::pin(self.control(context, |scope| async move {
            scope
                .authority
                .abandon_composition(&scope.identity, operation, revision)
                .await?;
            scope.composition_status(operation).await
        }))
        .await
    }

    /// Advances logical cleanup only; shared authority retains physical fences.
    /// # Errors
    /// Returns conflicting live state or uncertain cleanup progress.
    pub async fn cleanup_composition(
        &self,
        context: OperationContext,
        operation: OperationId,
    ) -> Result<CompositionCleanup> {
        Box::pin(self.control(context, |scope| async move {
            let cancel = ReadCancellation::new();
            let authority = &scope.authority;
            let identity = &scope.identity;
            let candidates = authority
                .cleanup_composition_candidates_step(identity, operation, &cancel)
                .await?;
            let owners = authority
                .cleanup_composition_uploads_step(identity, operation, &scope.uploads)
                .await?;
            let scratch = authority
                .cleanup_composition_scratch_step(identity, operation, &cancel)
                .await?;
            let draft = authority.composition_status(identity, operation).await?;
            let edits_complete = if matches!(
                draft.record.progress,
                DraftProgress::Published { .. } | DraftProgress::Abandoned
            ) {
                authority
                    .cleanup_composition_edits_step(identity, operation, &cancel)
                    .await?
                    .complete
            } else {
                true
            };
            Ok(CompositionCleanup {
                complete: candidates.complete && owners.complete && scratch.complete && edits_complete,
                retired_upload_owners: owners.retired_owners,
                physical_cleanup_deferred: true,
            })
        }))
        .await
    }
}
