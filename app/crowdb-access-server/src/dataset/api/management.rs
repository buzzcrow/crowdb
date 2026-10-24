// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{conversion, error::ApiError, json, read_body, snapshot, ApiResult};
use crate::dataset::DatasetHttpService;
use crowdb_access_dataset::{DatasetIdentity, ReclaimState};
use hyper::{body::Incoming, Method, Request};
use std::collections::BTreeSet;

impl DatasetHttpService {
    pub(super) async fn management_route(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        let authority = self.reads.authority();
        match (request.method().clone(), tail) {
            (Method::GET, ["snapshots"]) => {
                let mut limit = 100usize;
                let mut after = None;
                let mut seen = BTreeSet::new();
                for (key, value) in
                    url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
                {
                    if !seen.insert(key.clone()) {
                        return Err(ApiError::invalid());
                    }
                    match key.as_ref() {
                        "limit" => limit = value.parse().map_err(|_| ApiError::invalid())?,
                        "after" => after = Some(snapshot(&value)?),
                        _ => return Err(ApiError::invalid()),
                    }
                }
                if !(1..=1000).contains(&limit) {
                    return Err(ApiError::invalid());
                }
                let latest = authority.latest(identity).await?;
                let mut current = if let Some(after) = after {
                    authority.get_snapshot(identity, after).await?.publication.parent
                } else {
                    latest
                };
                let mut records = Vec::new();
                let mut visited = BTreeSet::new();
                while let Some(id) = current {
                    if records.len() == limit {
                        break;
                    }
                    if !visited.insert(id) {
                        return Err(ApiError::integrity());
                    }
                    let record = authority.get_snapshot(identity, id).await?;
                    current = record.publication.parent;
                    records.push(conversion::snapshot(record, latest));
                }
                let next = if current.is_some() {
                    records.last().map(|r| r.id.clone())
                } else {
                    None
                };
                json(&serde_json::json!({"snapshots":records,"next":next}))
            }
            (Method::GET, ["snapshots", id, "reclaim"]) => {
                let progress = authority.reclaim_progress(identity, snapshot(id)?).await?;
                authority.get_dataset(identity).await?;
                json(
                    &serde_json::json!({"snapshot":id,"state":if progress.state==ReclaimState::Completed{"completed"}else{"pending"},"chunks_reclaimed":progress.chunks_reclaimed}),
                )
            }
            (Method::POST, ["snapshots", id, "reclaim"]) => {
                if !read_body(request, false).await?.is_empty() {
                    return Err(ApiError::invalid());
                }
                let id = snapshot(id)?;
                let progress = authority.mark_snapshot_reclaim(identity, id).await?;
                json(&serde_json::json!({"snapshot":id.to_string(),
                    "state":if progress.complete {"completed"}else{"pending"},
                    "reclaim_after":authority.snapshot_reclaim_after(identity, id).await?,
                    "chunks_reclaimed":0}))
            }
            _ => Err(ApiError::not_found()),
        }
    }
}
