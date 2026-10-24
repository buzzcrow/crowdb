// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod fields;

use super::{
    super::{json, read_body_limit, snapshot, ApiError, ApiResult, DatasetHttpService},
    composition_models::{CompositionSource, Schema},
    query::Query,
    split_models::{required_option, DraftSplit, Policy},
};
use crowdb_access_dataset::{composition::CompositionRequest, DatasetIdentity, ReadCancellation};
use hyper::{body::Incoming, Request};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Prepare {
    #[serde(deserialize_with = "required_option")]
    parent: Option<String>,
    source: CompositionSource,
    #[serde(deserialize_with = "required_option")]
    schema: Option<Schema>,
    samples: Vec<fields::Change>,
    splits: Vec<DraftSplit>,
    policy: Policy,
}

impl DatasetHttpService {
    pub(super) async fn v2_prepare_snapshot(
        &self,
        identity: &DatasetIdentity,
        id: &str,
        request: Request<Incoming>,
    ) -> ApiResult {
        Query::parse(request.uri().query(), &[])?;
        let snapshot_id = snapshot(id)?;
        let uploads = self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
        let bytes = read_body_limit(request, true, 8 * 1024 * 1024).await?;
        let input: Prepare = serde_json::from_slice(&bytes).map_err(|_| ApiError::invalid())?;
        let input = CompositionRequest {
            parent: input.parent.as_deref().map(snapshot).transpose()?,
            source: input.source.native()?,
            schema: input.schema.map(Schema::native).transpose()?,
            samples: input
                .samples
                .into_iter()
                .map(|sample| sample.native(identity))
                .collect::<Result<_, _>>()?,
            splits: input
                .splits
                .into_iter()
                .map(DraftSplit::native)
                .collect::<Result<_, _>>()?,
            policy: input.policy.into(),
        };
        let result = self
            .reads
            .authority()
            .prepare_composition_resource(
                identity,
                snapshot_id,
                &input,
                Some(uploads),
                &ReadCancellation::new(),
            )
            .await?;
        json(&serde_json::json!({"snapshot":result.snapshot.to_string(),
            "initial_split":result.initial_split.map(|revision| revision.to_string()),
            "sample_count":result.sample_count.to_string()}))
    }
}
