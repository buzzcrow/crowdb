// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{json, read_json, ApiError, ApiResult, DatasetHttpService};
use super::{inspection_models::Page, query::Query, split_models::DraftSplit};
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::{DatasetIdentity, ReadCancellation, SnapshotId};
use hyper::{body::Incoming, Method, Request};

impl DatasetHttpService {
    pub(super) async fn v2_split_preview(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        request: Request<Incoming>,
    ) -> ApiResult {
        if request.method() != Method::POST {
            return Err(ApiError::not_found());
        }
        let query = Query::parse(request.uri().query(), &["after", "limit"])?;
        let input: DraftSplit = read_json(request).await?;
        let page = self
            .reads
            .authority()
            .inspection_split_page(
                identity,
                snapshot,
                &input.native()?,
                query.after()?,
                query.budget()?,
                &ReadCancellation::new(),
            )
            .await?;
        json(&Page::map(page, |id| STANDARD.encode(id)))
    }
}
