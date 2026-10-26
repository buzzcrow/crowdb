// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{json, operation, read_body, ApiError, ApiResult, DatasetHttpService},
    composition_responses as output,
    query::Query,
    split_models::decimal,
    split_requests,
};
use crowdb_access_dataset::{DatasetIdentity, OperationId};
use hyper::{body::Incoming, Method, Request};
impl DatasetHttpService {
    pub(super) async fn v2_compositions(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        Query::parse(request.uri().query(), &[])?;
        if tail == ["compositions"] && matches!(*request.method(), Method::PUT | Method::POST) {
            return Err(ApiError::unsupported());
        }
        let ["compositions", id, rest @ ..] = tail else {
            return Err(ApiError::not_found());
        };
        let operation = operation(id)?;
        if request.method() == Method::PUT || request.method() == Method::POST {
            return Err(ApiError::unsupported());
        }
        self.reads.authority().get_dataset(identity).await?;
        self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
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
}

async fn read_json<T: serde::de::DeserializeOwned>(request: Request<Incoming>) -> Result<T, ApiError> {
    let bytes = super::super::read_body_limit(request, true, 1024 * 1024).await?;
    serde_json::from_slice(&bytes).map_err(|_| ApiError::invalid())
}
