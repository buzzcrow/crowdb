// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{conversion, json, read_body, snapshot, ApiError, ApiResult, DatasetHttpService};
use super::{
    inspection_models::{Page, Sample, Summary},
    query::Query,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::{
    inspection::{InspectionError, InspectionPreparationLimits, PreparationState},
    DatasetIdentity, NamespacePath, ReadCancellation,
};
use hyper::{body::Incoming, Method, Request};

impl DatasetHttpService {
    pub(super) async fn v2_catalog(&self, request: Request<Incoming>) -> ApiResult {
        if request.method() != Method::GET {
            return Err(ApiError::not_found());
        }
        let query = Query::parse(request.uri().query(), &["after", "limit"])?;
        let budget = query.budget()?;
        let after = query.after()?;
        read_body(request, false).await?;
        let page = self
            .reads
            .authority()
            .inspect_datasets_page(&NamespacePath::root(), after, budget, &ReadCancellation::new())
            .await?;
        json(&Page::map(page, |record| conversion::dataset(&record)))
    }
    pub(super) async fn v2_inspection(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        let method = request.method().clone();
        let allowed: &[&str] = match tail {
            ["snapshot-inventory" | "roots"] | ["snapshots", _, "children" | "schema" | "samples"] => {
                &["after", "limit"]
            }
            ["snapshots", _, "matrix"] => &["after", "limit", "fields"],
            ["snapshots", _, "contains"] => &["sample"],
            _ => &[],
        };
        let query = Query::parse(request.uri().query(), allowed)?;
        let budget = query.budget()?;
        let after = query.after()?;
        read_body(request, false).await?;
        let authority = self.reads.authority();
        let cancel = ReadCancellation::new();
        if method == Method::GET && tail == ["snapshot-inventory"] {
            return json(&Page::map(
                authority
                    .snapshot_inventory_page(identity, after, budget, &cancel)
                    .await?,
                Summary::from,
            ));
        }
        if method == Method::GET && tail == ["roots"] {
            return json(&Page::map(
                authority
                    .snapshot_children_page(identity, None, after, budget, &cancel)
                    .await?,
                Summary::from,
            ));
        }
        let ["snapshots", id, action] = tail else {
            return Err(ApiError::not_found());
        };
        let snapshot = snapshot(id)?;
        match (method, *action) {
            (Method::GET, "summary") => json(&Summary::from(
                authority
                    .snapshot_inspection_summary(identity, snapshot, budget, &cancel)
                    .await?,
            )),
            (Method::GET, "children") => json(&Page::map(
                authority
                    .snapshot_children_page(identity, Some(snapshot), after, budget, &cancel)
                    .await?,
                Summary::from,
            )),
            (Method::GET, "schema") => json(&Page::map(
                authority
                    .inspection_schema_page(identity, snapshot, after, budget, &cancel)
                    .await?,
                |field| super::super::models::FieldDefinition {
                    name: field.name,
                    required: field.required,
                },
            )),
            (Method::GET, "samples") => json(&Page::map(
                authority
                    .inspection_samples_page(identity, snapshot, after, budget, &cancel)
                    .await?,
                |id| STANDARD.encode(id),
            )),
            (Method::GET, "matrix") => json(&Page::map(
                authority
                    .inspection_matrix_page(identity, snapshot, &query.fields()?, after, budget, &cancel)
                    .await?,
                Sample::from,
            )),
            (Method::GET, "contains") => {
                let id = super::query::bytes(
                    query.value("sample").ok_or_else(ApiError::invalid)?,
                    16 * 1024,
                    false,
                )?;
                json(
                    &serde_json::json!({"contains":authority.inspection_contains_sample(identity, snapshot, &id, budget, &cancel).await?}),
                )
            }
            (method, "inspection") if method == Method::GET || method == Method::POST => {
                self.v2_inspection_preparation(identity, snapshot, method == Method::POST)
                    .await
            }
            _ => Err(ApiError::not_found()),
        }
    }
    async fn v2_inspection_preparation(
        &self,
        identity: &DatasetIdentity,
        snapshot: crowdb_access_dataset::SnapshotId,
        prepare: bool,
    ) -> ApiResult {
        let authority = self.reads.authority();
        let cancel = ReadCancellation::new();
        let progress = if prepare {
            authority.require_committed_snapshot(identity, snapshot).await?;
            authority
                .prepare_inspection(
                    identity,
                    snapshot,
                    InspectionPreparationLimits::default(),
                    &cancel,
                )
                .await?
        } else {
            authority
                .committed_inspection_preparation_status(identity, snapshot, &cancel)
                .await?
        };
        let (state, reason) = match progress.state {
            PreparationState::Preparing => ("preparing", None),
            PreparationState::Ready => ("ready", None),
            PreparationState::Unsupported { reason } => ("unsupported", Some(reason)),
        };
        json(
            &serde_json::json!({"snapshot":snapshot.to_string(), "revision":progress.revision.to_string(), "rows_written":progress.rows_written.to_string(), "state":state, "reason":reason}),
        )
    }
}
impl From<InspectionError> for ApiError {
    fn from(error: InspectionError) -> Self {
        use hyper::StatusCode;
        match error {
            InspectionError::Authority(error) => error.into(),
            InspectionError::Store(error) => crowdb_access_dataset::AuthorityError::Store(error).into(),
            InspectionError::Limit => Self::new(StatusCode::PAYLOAD_TOO_LARGE, "read_limit"),
            InspectionError::NotPrepared => Self::new(StatusCode::CONFLICT, "inspection_not_prepared"),
            InspectionError::Unsupported(_) => {
                Self::new(StatusCode::UNPROCESSABLE_ENTITY, "inspection_unsupported")
            }
            InspectionError::Corrupt => Self::integrity(),
            InspectionError::Cancelled => {
                Self::new(StatusCode::from_u16(499).expect("valid status"), "cancelled")
            }
        }
    }
}
