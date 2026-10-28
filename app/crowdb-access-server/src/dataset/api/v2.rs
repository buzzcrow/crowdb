// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Capability-gated control API. Unsupported operations never fall back to v1.

mod auxiliary;
mod composition_models;
mod composition_preparation;
mod composition_responses;
mod compositions;
mod heads;
mod inspection;
mod inspection_models;
mod predicate;
mod preview;
mod query;
mod read_models;
mod reads;
mod split_models;
mod split_preparation;
mod split_preview;
mod split_requests;
mod split_responses;
mod splits;
mod uploads;

use super::{json, read_body, ApiError, ApiResult, DatasetHttpService};
use crowdb_access_dataset::{DatasetIdentity, NamespacePath};
use hyper::{body::Incoming, Method, Request};
use serde::Serialize;

#[derive(Serialize)]
struct Capabilities {
    api_version: u16,
    capabilities: Vec<&'static str>,
}

impl DatasetHttpService {
    pub(super) async fn route_v2(&self, parts: &[String], request: Request<Incoming>) -> ApiResult {
        const HEADER: &str = "x-crowdb-dataset-id";
        if request.headers().get_all(HEADER).iter().count() > 1 {
            return Err(ApiError::invalid());
        }
        if let Some(value) = request.headers().get(HEADER) {
            if parts.len() < 7
                || parts[..6] != ["api", "dataset", "v2", "namespaces", "dataset-ns", "datasets"]
            {
                return Err(ApiError::invalid());
            }
            let value = value.to_str().map_err(|_| ApiError::invalid())?;
            let bytes = hex::decode(value).map_err(|_| ApiError::invalid())?;
            let id = crowdb_access_dataset::DatasetId::from_bytes(&bytes).map_err(|_| ApiError::invalid())?;
            if id.to_string() != value {
                return Err(ApiError::invalid());
            }
            let scoped = Self {
                reads: std::sync::Arc::new(self.reads.for_dataset_instance(id)),
                ..self.clone()
            };
            let identity =
                DatasetIdentity::new(NamespacePath::root(), &parts[6]).map_err(|_| ApiError::invalid())?;
            scoped.reads.authority().get_dataset(&identity).await?;
            return scoped.route_v2_scoped(parts, request).await;
        }
        self.route_v2_scoped(parts, request).await
    }

    async fn route_v2_scoped(&self, parts: &[String], request: Request<Incoming>) -> ApiResult {
        if parts == ["api", "dataset", "v2", "namespaces", "dataset-ns", "datasets"] {
            return self.v2_catalog(request).await;
        }
        if parts == ["api", "dataset", "v2", "capabilities"] && request.method() == Method::GET {
            query::Query::parse(request.uri().query(), &[])?;
            read_body(request, false).await?;
            let mut capabilities = vec![
                "head-inspection",
                "snapshot-head-update",
                "bounded-inspection",
                "inspection-preparation",
                "dataset-management",
                "dataset-delete",
                "dataset-identity-fence",
                "split-revisions",
                "split-preview",
                "split-revision-delete",
                "split-preparation",
                "split-head-update",
                "stateless-reads",
            ];
            if self.uploads.is_some() {
                capabilities.extend(["uploads", "composition-preparation", "snapshot-reclaim"]);
            }
            return json(&Capabilities {
                api_version: 2,
                capabilities,
            });
        }
        if parts.len() >= 7 && parts[..6] == ["api", "dataset", "v2", "namespaces", "dataset-ns", "datasets"]
        {
            let identity =
                DatasetIdentity::new(NamespacePath::root(), &parts[6]).map_err(|_| ApiError::invalid())?;
            if parts.len() == 8 && parts[7] == "head" {
                return self.v2_head(&identity, request).await;
            }
            let tail: Vec<_> = parts[7..].iter().map(String::as_str).collect();
            if tail.first() == Some(&"uploads") {
                return self.v2_uploads(&identity, &tail, request).await;
            }
            if matches!(
                tail.as_slice(),
                [] | ["names", "stable"]
                    | ["deletion" | "commits", _]
                    | ["snapshots", _, "retain" | "release" | "reclaim"]
                    | ["split-drafts", _, "cleanup"]
            ) {
                return self.v2_auxiliary(&identity, &tail, request).await;
            }
            if let ["snapshots", id, "preview"] = tail.as_slice() {
                return self.v2_preview(&identity, super::snapshot(id)?, request).await;
            }
            if let ["snapshots", id, "matrix"] = tail.as_slice() {
                if request.method() == Method::POST {
                    return self
                        .v2_exact_matrix(&identity, super::snapshot(id)?, request)
                        .await;
                }
            }
            if let ["snapshots", id, "split-preview"] = tail.as_slice() {
                return self
                    .v2_split_preview(&identity, super::snapshot(id)?, request)
                    .await;
            }
            if let ["snapshots", id] = tail.as_slice() {
                if request.method() == Method::PUT {
                    return self.v2_prepare_snapshot(&identity, id, request).await;
                }
            }
            if tail.first() == Some(&"compositions") {
                return self.v2_compositions(&identity, &tail, request).await;
            }
            if tail.first() == Some(&"reads") {
                return self.v2_read_query(&identity, &tail, request).await;
            }
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
