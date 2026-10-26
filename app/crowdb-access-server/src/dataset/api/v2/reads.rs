// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{json, read_json, ApiError, ApiResult, DatasetHttpService};
use super::{query::Query, read_models as models};
use crowdb_access_dataset::{DatasetIdentity, DatasetReads};
use hyper::{body::Incoming, Method, Request};
impl DatasetHttpService {
    pub(super) async fn v2_read_query(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        Query::parse(request.uri().query(), &[])?;
        if request.method() != Method::POST {
            return Err(ApiError::not_found());
        }
        match tail {
            ["reads", "bind"] => {
                let input: models::Plan = read_json(request).await?;
                json(&models::binding(
                    DatasetReads::new(self.reads.authority())
                        .bind(identity, input.native()?)
                        .await?,
                ))
            }
            ["reads", "next"] => self.query_next(identity, request).await,
            _ => Err(ApiError::not_found()),
        }
    }
}
