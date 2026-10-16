// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{
    BeginComposition, BeginSplit, CommitReceipt, CompositionCleanup, CompositionDraft, CompositionPreview,
    ControlClient, DraftRevision, EditComposition, EditSplit, EvaluateDraft, OpaqueId, PublishComposition,
    PublishSplit, SplitCleanup, SplitDraft, SplitHead, SplitPreview, SplitRevision,
};
use crate::Error;

impl ControlClient {
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn begin_composition(&self, request: &BeginComposition) -> Result<CompositionDraft, Error> {
        self.call("composition", "POST", &["compositions"], Some(request))
            .await
            .map_err(|error| error.with_identity(request.operation.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn composition(&self, operation: &OpaqueId) -> Result<CompositionDraft, Error> {
        self.call::<(), _>("composition", "GET", &["compositions", operation.as_str()], None)
            .await
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn edit_composition(
        &self,
        operation: &OpaqueId,
        request: &EditComposition,
    ) -> Result<CompositionDraft, Error> {
        self.call(
            "composition",
            "PUT",
            &["compositions", operation.as_str()],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(request.edit.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn abandon_composition(
        &self,
        operation: &OpaqueId,
        request: &DraftRevision,
    ) -> Result<CompositionDraft, Error> {
        self.call(
            "composition",
            "DELETE",
            &["compositions", operation.as_str()],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(operation.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn evaluate_composition(
        &self,
        operation: &OpaqueId,
        request: &EvaluateDraft,
    ) -> Result<CompositionPreview, Error> {
        self.call(
            "composition",
            "POST",
            &["compositions", operation.as_str(), "evaluate"],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(operation.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn prepare_composition(
        &self,
        operation: &OpaqueId,
        request: &DraftRevision,
    ) -> Result<CompositionDraft, Error> {
        self.call(
            "composition",
            "POST",
            &["compositions", operation.as_str(), "prepare"],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(operation.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn publish_composition(
        &self,
        operation: &OpaqueId,
        request: &PublishComposition,
    ) -> Result<CommitReceipt, Error> {
        self.call(
            "composition",
            "POST",
            &["compositions", operation.as_str(), "publish"],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(request.attempt.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn cleanup_composition(&self, operation: &OpaqueId) -> Result<CompositionCleanup, Error> {
        self.call::<(), _>(
            "composition",
            "POST",
            &["compositions", operation.as_str(), "cleanup"],
            None,
        )
        .await
        .map_err(|error| error.with_identity(operation.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn begin_split(&self, snapshot: &OpaqueId, request: &BeginSplit) -> Result<SplitDraft, Error> {
        self.call(
            "split-revisions",
            "POST",
            &["snapshots", snapshot.as_str(), "split-drafts"],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(request.operation.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn split_draft(&self, operation: &OpaqueId) -> Result<SplitDraft, Error> {
        self.call::<(), _>(
            "split-revisions",
            "GET",
            &["split-drafts", operation.as_str()],
            None,
        )
        .await
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn edit_split(&self, operation: &OpaqueId, request: &EditSplit) -> Result<SplitDraft, Error> {
        self.call(
            "split-revisions",
            "PUT",
            &["split-drafts", operation.as_str()],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(request.edit.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn abandon_split(
        &self,
        operation: &OpaqueId,
        request: &DraftRevision,
    ) -> Result<SplitDraft, Error> {
        self.call(
            "split-revisions",
            "DELETE",
            &["split-drafts", operation.as_str()],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(operation.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn evaluate_split(
        &self,
        operation: &OpaqueId,
        request: &EvaluateDraft,
    ) -> Result<SplitPreview, Error> {
        self.call(
            "split-revisions",
            "POST",
            &["split-drafts", operation.as_str(), "evaluate"],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(operation.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn prepare_split(
        &self,
        operation: &OpaqueId,
        request: &DraftRevision,
    ) -> Result<SplitRevision, Error> {
        self.call(
            "split-revisions",
            "POST",
            &["split-drafts", operation.as_str(), "prepare"],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(operation.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn publish_split(
        &self,
        operation: &OpaqueId,
        request: &PublishSplit,
    ) -> Result<CommitReceipt, Error> {
        self.call(
            "split-revisions",
            "POST",
            &["split-drafts", operation.as_str(), "publish"],
            Some(request),
        )
        .await
        .map_err(|error| error.with_identity(request.attempt.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
    pub async fn cleanup_split(&self, operation: &OpaqueId) -> Result<SplitCleanup, Error> {
        self.call::<(), _>(
            "split-revisions",
            "POST",
            &["split-drafts", operation.as_str(), "cleanup"],
            None,
        )
        .await
        .map_err(|error| error.with_identity(operation.as_str()))
    }
    /// Executes one bounded operation; retain request IDs for exact retries.
    /// # Errors
    /// Returns capability, conflict, invalid input, transport or service errors.
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
