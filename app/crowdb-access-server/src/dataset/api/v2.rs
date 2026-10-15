// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Capability-gated control API. Unsupported operations never fall back to v1.

mod inspection;
mod inspection_models;
mod predicate;
mod query;
mod split_models;
mod split_requests;
mod split_responses;
mod splits;

use super::{json, read_body, ApiError, ApiResult, DatasetHttpService};
use crowdb_access_dataset::{DatasetIdentity, NamespacePath};
use hyper::{body::Incoming, Method, Request};
use serde::Serialize;

#[derive(Serialize)]
struct Capabilities {
    api_version: u16,
    capabilities: &'static [&'static str],
}

#[derive(Serialize)]
struct Head {
    latest: Option<String>,
    stable: Option<String>,
    generation: Option<String>,
}

impl DatasetHttpService {
    pub(super) async fn route_v2(&self, parts: &[String], request: Request<Incoming>) -> ApiResult {
        if parts == ["api", "dataset", "v2", "namespaces", "dataset-ns", "datasets"] {
            return self.v2_catalog(request).await;
        }
        if parts == ["api", "dataset", "v2", "capabilities"] && request.method() == Method::GET {
            read_body(request, false).await?;
            return json(&Capabilities {
                api_version: 2,
                // Advertise each operation family only after its authority contract is implemented.
                capabilities: &["head-inspection", "bounded-inspection", "inspection-preparation"],
            });
        }
        if parts.len() == 8
            && parts[..6] == ["api", "dataset", "v2", "namespaces", "dataset-ns", "datasets"]
            && parts[7] == "head"
            && request.method() == Method::GET
        {
            read_body(request, false).await?;
            let identity =
                DatasetIdentity::new(NamespacePath::root(), &parts[6]).map_err(|_| ApiError::invalid())?;
            let head = self.reads.authority().inspect_head(&identity).await?;
            return json(&Head {
                latest: head.latest.map(|id| id.to_string()),
                stable: head.stable.map(|id| id.to_string()),
                generation: head
                    .generation
                    .map(|generation| hex::encode(generation.as_bytes())),
            });
        }
        if parts.len() >= 7 && parts[..6] == ["api", "dataset", "v2", "namespaces", "dataset-ns", "datasets"]
        {
            let identity =
                DatasetIdentity::new(NamespacePath::root(), &parts[6]).map_err(|_| ApiError::invalid())?;
            let tail: Vec<_> = parts[7..].iter().map(String::as_str).collect();
            if matches!(
                tail.as_slice(),
                ["split-drafts", ..]
                    | [
                        "snapshots",
                        _,
                        "split-drafts" | "split-head" | "split-revisions",
                        ..
                    ]
            ) {
                return self.v2_splits(&identity, &tail, request).await;
            }
            return self.v2_inspection(&identity, &tail, request).await;
        }
        Err(ApiError::not_found())
    }
}
