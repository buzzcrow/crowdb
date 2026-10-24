// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{read_body_limit, snapshot},
    json,
    query::Query,
    read_body,
    split_models::required_option,
    split_requests::generation,
    ApiError, ApiResult, DatasetHttpService,
};
use crowdb_access_dataset::{DatasetHead, DatasetIdentity, SnapshotHeadUpdate};
use hyper::{body::Incoming, Method, Request};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateHead {
    snapshot: String,
    #[serde(deserialize_with = "required_option")]
    expected_head: Option<String>,
}
#[derive(Serialize)]
struct Head {
    latest: Option<String>,
    stable: Option<String>,
    generation: Option<String>,
}
impl From<DatasetHead> for Head {
    fn from(head: DatasetHead) -> Self {
        Self {
            latest: head.latest.map(|id| id.to_string()),
            stable: head.stable.map(|id| id.to_string()),
            generation: head.generation.map(|value| hex::encode(value.as_bytes())),
        }
    }
}
impl DatasetHttpService {
    pub(super) async fn v2_head(&self, identity: &DatasetIdentity, request: Request<Incoming>) -> ApiResult {
        Query::parse(request.uri().query(), &[])?;
        let authority = self.reads.authority();
        let head = match *request.method() {
            Method::GET => {
                read_body(request, false).await?;
                authority.inspect_head(identity).await?
            }
            Method::PUT => {
                let body = read_body_limit(request, true, 4096).await?;
                let input: UpdateHead = serde_json::from_slice(&body).map_err(|_| ApiError::invalid())?;
                authority
                    .update_snapshot_head(
                        identity,
                        &SnapshotHeadUpdate {
                            snapshot: snapshot(&input.snapshot)?,
                            expected_head: generation(input.expected_head.as_deref())?,
                        },
                    )
                    .await?
            }
            _ => return Err(ApiError::not_found()),
        };
        json(&Head::from(head))
    }
}
