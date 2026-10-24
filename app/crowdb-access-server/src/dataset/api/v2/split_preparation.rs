// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{read_body_limit, snapshot},
    json,
    split_models::{revision, DraftSplit, Policy},
    split_responses, ApiError, ApiResult, DatasetHttpService,
};
use crowdb_access_dataset::{split::SplitPreparation, DatasetIdentity, ReadCancellation};
use hyper::{body::Incoming, Request};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Prepare {
    policy: Policy,
    splits: Vec<DraftSplit>,
}
impl DatasetHttpService {
    pub(super) async fn v2_prepare_split_revision(
        &self,
        identity: &DatasetIdentity,
        owner: &str,
        revision_id: &str,
        request: Request<Incoming>,
    ) -> ApiResult {
        let snapshot = snapshot(owner)?;
        let revision = revision(revision_id)?;
        let body = read_body_limit(request, true, 1024 * 1024).await?;
        let input: Prepare = serde_json::from_slice(&body).map_err(|_| ApiError::invalid())?;
        let request = SplitPreparation {
            revision,
            policy: input.policy.into(),
            splits: input
                .splits
                .into_iter()
                .map(DraftSplit::native)
                .collect::<Result<_, _>>()?,
        };
        json(&split_responses::revision(
            self.reads
                .authority()
                .prepare_split_revision(identity, snapshot, &request, &ReadCancellation::new())
                .await?,
        ))
    }
}
