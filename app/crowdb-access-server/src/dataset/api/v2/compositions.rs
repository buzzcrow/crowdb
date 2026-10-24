// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{json, operation, read_body, snapshot, ApiError, ApiResult, DatasetHttpService},
    composition_models as input, composition_responses as output,
    query::Query,
    split_models::decimal,
    split_requests, split_responses,
};
use crowdb_access_dataset::{
    composition::DraftProgress, CommitAttempt, CommitTarget, DatasetIdentity, OperationId, ReadCancellation,
};
use hyper::{body::Incoming, Method, Request};
impl DatasetHttpService {
    pub(super) async fn v2_compositions(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        Query::parse(request.uri().query(), &[])?;
        self.reads.authority().get_dataset(identity).await?;
        let uploads = self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
        if tail == ["compositions"] && request.method() == Method::POST {
            return self.v2_composition_begin(identity, request).await;
        }
        let ["compositions", id, rest @ ..] = tail else {
            return Err(ApiError::not_found());
        };
        let operation = operation(id)?;
        let authority = self.reads.authority();
        match (request.method().clone(), rest) {
            (Method::GET, []) => {
                read_body(request, false).await?;
                self.v2_composition_status(identity, operation).await
            }
            (Method::PUT, []) => {
                let input: input::CompositionEdit = read_json(request).await?;
                let edit =
                    tokio::time::timeout(std::time::Duration::from_secs(2), input.native(identity, uploads))
                        .await
                        .map_err(|_| ApiError::timeout())??;
                let draft = authority
                    .edit_composition_with_uploads(identity, operation, &edit, uploads)
                    .await?;
                json(&output::draft(
                    draft,
                    &authority.composition_upload_status(identity, operation).await?,
                ))
            }
            (Method::DELETE, []) => {
                let input: split_requests::Prepare = read_json(request).await?;
                authority
                    .abandon_composition(identity, operation, decimal(&input.expected_revision)?)
                    .await?;
                self.v2_composition_status(identity, operation).await
            }
            (Method::POST, ["evaluate"]) => {
                let input: split_requests::Evaluate = read_json(request).await?;
                json(&output::preview(
                    &authority
                        .evaluate_composition(
                            identity,
                            operation,
                            decimal(&input.expected_revision)?,
                            decimal(&input.expected_page)?,
                            &ReadCancellation::new(),
                        )
                        .await?,
                ))
            }
            (Method::POST, ["prepare"]) => {
                let input: split_requests::Prepare = read_json(request).await?;
                let draft = authority
                    .prepare_composition(
                        identity,
                        operation,
                        decimal(&input.expected_revision)?,
                        &ReadCancellation::new(),
                    )
                    .await?;
                if matches!(draft.record.progress, DraftProgress::Prepared { .. }) {
                    authority
                        .prepare_composition_uploads(identity, operation, uploads, &ReadCancellation::new())
                        .await?;
                }
                self.v2_composition_status(identity, operation).await
            }
            (Method::POST, ["publish"]) => {
                let input: input::Publish = read_json(request).await?;
                let attempt = CommitAttempt {
                    version: 1,
                    id: super::super::operation(&input.attempt)?,
                    target: CommitTarget::Snapshot {
                        snapshot: snapshot(&input.snapshot)?,
                    },
                    expected_head: split_requests::generation(input.expected_head.as_deref())?,
                };
                json(&split_responses::receipt(
                    &authority
                        .publish_composition(identity, operation, &attempt)
                        .await?,
                ))
            }
            (Method::POST, ["cleanup"]) => {
                read_body(request, false).await?;
                self.v2_composition_cleanup(identity, operation).await
            }
            _ => Err(ApiError::not_found()),
        }
    }
    async fn v2_composition_begin(
        &self,
        identity: &DatasetIdentity,
        request: Request<Incoming>,
    ) -> ApiResult {
        let input: input::Begin = read_json(request).await?;
        let (operation, input) = input.native()?;
        let authority = self.reads.authority();
        authority.begin_composition(identity, operation, &input).await?;
        authority.manage_composition_uploads(identity, operation).await?;
        self.v2_composition_status(identity, operation).await
    }
    async fn v2_composition_status(&self, identity: &DatasetIdentity, operation: OperationId) -> ApiResult {
        let authority = self.reads.authority();
        json(&output::draft(
            authority.composition_status(identity, operation).await?,
            &authority.composition_upload_status(identity, operation).await?,
        ))
    }
    async fn v2_composition_cleanup(&self, identity: &DatasetIdentity, operation: OperationId) -> ApiResult {
        let authority = self.reads.authority();
        let uploads = self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
        let cancel = ReadCancellation::new();
        let candidates = authority
            .cleanup_composition_candidates_step(identity, operation, &cancel)
            .await?;
        let owners = authority
            .cleanup_composition_uploads_step(identity, operation, uploads)
            .await?;
        let scratch = authority
            .cleanup_composition_scratch_step(identity, operation, &cancel)
            .await?;
        let draft = authority.composition_status(identity, operation).await?;
        let edits = if matches!(
            draft.record.progress,
            DraftProgress::Prepared { .. } | DraftProgress::Published { .. } | DraftProgress::Abandoned
        ) {
            Some(
                authority
                    .cleanup_composition_edits_step(identity, operation, &cancel)
                    .await?,
            )
        } else {
            None
        };
        json(
            &serde_json::json!({"complete":candidates.complete && owners.complete && scratch.complete && edits.as_ref().map_or(true, |e| e.complete),
            "candidates":{"input_revision":candidates.input_revision.to_string(),"complete":candidates.complete},
            "uploads":{"retired_owners":owners.retired_owners.to_string(),"complete":owners.complete},
            "scratch":{"input_revision":scratch.input_revision.to_string(),"next_page":scratch.next_page.to_string(),"complete":scratch.complete},
            "edits":edits.map(|e|serde_json::json!({"retired_pages":e.retired_pages.to_string(),"complete":e.complete})), "physical_cleanup_deferred":true}),
        )
    }
}

async fn read_json<T: serde::de::DeserializeOwned>(request: Request<Incoming>) -> Result<T, ApiError> {
    let bytes = super::super::read_body_limit(request, true, 1024 * 1024).await?;
    serde_json::from_slice(&bytes).map_err(|_| ApiError::invalid())
}
