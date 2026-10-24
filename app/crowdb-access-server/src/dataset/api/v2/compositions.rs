// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{json, operation, read_body, ApiError, ApiResult, DatasetHttpService},
    composition_responses as output,
    query::Query,
    split_models::decimal,
    split_requests,
};
use crowdb_access_dataset::{composition::DraftProgress, DatasetIdentity, OperationId, ReadCancellation};
use hyper::{body::Incoming, Method, Request};
impl DatasetHttpService {
    pub(super) async fn v2_compositions(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        Query::parse(request.uri().query(), &[])?;
        if request.method() == Method::PUT
            || (request.method() == Method::POST && !matches!(tail, ["compositions", _, "cleanup"]))
        {
            return Err(ApiError::unsupported());
        }
        self.reads.authority().get_dataset(identity).await?;
        self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
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
            (Method::DELETE, []) => {
                let input: split_requests::Prepare = read_json(request).await?;
                authority
                    .abandon_composition(identity, operation, decimal(&input.expected_revision)?)
                    .await?;
                self.v2_composition_status(identity, operation).await
            }
            (Method::POST, ["cleanup"]) => {
                read_body(request, false).await?;
                self.v2_composition_cleanup(identity, operation).await
            }
            _ => Err(ApiError::not_found()),
        }
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
