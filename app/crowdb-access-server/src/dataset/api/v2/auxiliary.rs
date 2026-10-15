// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{json, operation, read_body, read_json, snapshot, ApiError, ApiResult, DatasetHttpService},
    query::Query,
    split_responses,
};
use crowdb_access_dataset::{DatasetIdentity, ReadCancellation};
use hyper::{body::Incoming, Method, Request};
use serde::Deserialize;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Stable {
    snapshot: String,
}
impl DatasetHttpService {
    pub(super) async fn v2_auxiliary(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        Query::parse(request.uri().query(), &[])?;
        let authority = self.reads.authority();
        match (request.method().clone(), tail) {
            (Method::PUT, []) => {
                read_body(request, false).await?;
                authority.create_dataset(identity.clone()).await?;
            }
            (Method::GET, []) => {
                read_body(request, false).await?;
            }
            (Method::PUT, ["names", "stable"]) => {
                let input: Stable = read_json(request).await?;
                authority.set_stable(identity, snapshot(&input.snapshot)?).await?;
            }
            (Method::POST, ["snapshots", id, action @ ("retain" | "release")]) => {
                read_body(request, false).await?;
                let snapshot = snapshot(id)?;
                if *action == "retain" {
                    authority.retain_snapshot(identity, snapshot).await?;
                } else {
                    authority.release_snapshot(identity, snapshot).await?;
                }
                return json(
                    &serde_json::json!({"snapshot":snapshot.to_string(),"retained":*action == "retain"}),
                );
            }
            (Method::GET, ["snapshots", id, "reclaim"]) => {
                read_body(request, false).await?;
                let value = authority
                    .inspect_snapshot_lifecycle(identity, snapshot(id)?, &ReadCancellation::new())
                    .await?;
                return json(&serde_json::json!({"snapshot": value.snapshot.to_string(),
                    "operation": value.operation.map(|id| id.to_string()), "latest": value.latest,
                    "stable": value.stable, "retained": value.retained, "active_read_lease": value.active_read_lease,
                    "state": if value.completed { "completed" } else if value.retiring { "pending" } else { "not_started" },
                    "physical_cleanup_deferred": value.retiring || value.completed,
                    "parent_protection": "not_evaluated"}));
            }
            (Method::POST, ["snapshots", id, "reclaim"]) => {
                read_body(request, false).await?;
                let uploads = self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
                let progress = authority
                    .reclaim_snapshot_step(identity, snapshot(id)?, uploads, &ReadCancellation::new())
                    .await?;
                return json(
                    &serde_json::json!({"snapshot":progress.snapshot.to_string(),"phase":progress.phase,"complete":progress.complete,"physical_cleanup_deferred":progress.physical_cleanup_deferred}),
                );
            }
            (Method::GET, ["commits", id]) => {
                read_body(request, false).await?;
                return json(
                    &serde_json::json!({"receipt":authority.commit_status(identity, operation(id)?).await?.as_ref().map(split_responses::receipt)}),
                );
            }
            (Method::POST, ["split-drafts", id, "cleanup"]) => {
                read_body(request, false).await?;
                let progress = authority
                    .cleanup_split_scratch_step(identity, operation(id)?, &ReadCancellation::new())
                    .await?;
                return json(
                    &serde_json::json!({"input_revision":progress.input_revision.to_string(),"next_page":progress.next_page.to_string(),"complete":progress.complete}),
                );
            }
            _ => return Err(ApiError::not_found()),
        }
        let dataset = authority.get_dataset(identity).await?;
        json(
            &serde_json::json!({"id":dataset.id.to_string(),"namespace":dataset.identity.namespace().to_string(),"name":dataset.identity.name(),"latest":dataset.latest.map(|v|v.to_string()),"stable":dataset.stable.map(|v|v.to_string())}),
        )
    }
}
