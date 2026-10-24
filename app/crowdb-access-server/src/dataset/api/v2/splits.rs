// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{
    json, operation, read_body, read_json, snapshot, ApiError, ApiResult, DatasetHttpService,
};
use super::{
    inspection_models::Page,
    query::Query,
    split_models::{decimal, revision},
    split_requests as input, split_responses as output,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::{split::SplitName, DatasetIdentity, ReadCancellation};
use hyper::{body::Incoming, Method, Request};

impl DatasetHttpService {
    pub(super) async fn v2_splits(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        let is_page = matches!(
            tail,
            ["snapshots", _, "split-revisions"]
                | ["snapshots", _, "split-revisions", _, "splits", _, "members"]
        );
        let query = Query::parse(
            request.uri().query(),
            if is_page && request.method() == Method::GET {
                &["after", "limit"]
            } else {
                &[]
            },
        )?;
        let authority = self.reads.authority();
        match (request.method().clone(), tail) {
            (Method::POST, ["snapshots", _, "split-drafts"]) => Err(ApiError::unsupported()),
            (_, ["split-drafts", ..]) => self.v2_split_draft(identity, tail, request).await,
            (Method::GET, ["snapshots", owner, "split-head"]) => {
                read_body(request, false).await?;
                let snapshot = snapshot(owner)?;
                let head = authority.inspect_split_head(identity, snapshot).await?;
                json(&output::head(snapshot, &head))
            }
            (Method::PUT, ["snapshots", owner, "split-head"]) => {
                self.v2_update_split_head(identity, owner, request).await
            }
            (Method::PUT, ["snapshots", owner, "split-revisions", revision]) => {
                self.v2_prepare_split_revision(identity, owner, revision, request)
                    .await
            }
            (Method::GET, ["snapshots", owner, "split-revisions"]) => {
                read_body(request, false).await?;
                let page = authority
                    .split_revision_history(
                        identity,
                        snapshot(owner)?,
                        query.after()?,
                        query.budget()?,
                        &ReadCancellation::new(),
                    )
                    .await?;
                json(&Page::map(
                    page,
                    |r| serde_json::json!({"snapshot":r.snapshot.to_string(), "revision":r.revision.to_string(), "operation":r.operation.to_string(), "split_count":r.split_count}),
                ))
            }
            (Method::GET, ["snapshots", owner, "split-revisions", rev]) => {
                read_body(request, false).await?;
                json(&output::revision(
                    authority
                        .get_split_revision(
                            identity,
                            snapshot(owner)?,
                            revision(rev)?,
                            query.budget()?,
                            &ReadCancellation::new(),
                        )
                        .await?,
                ))
            }
            (Method::GET, ["snapshots", owner, "split-revisions", rev, "splits", name, "members"]) => {
                read_body(request, false).await?;
                let name = SplitName::new(*name).map_err(|_| ApiError::invalid())?;
                let page = authority
                    .split_members_page(
                        identity,
                        snapshot(owner)?,
                        revision(rev)?,
                        &name,
                        query.after()?,
                        query.budget()?,
                        &ReadCancellation::new(),
                    )
                    .await?;
                json(&Page::map(page, |id| STANDARD.encode(id)))
            }
            _ => Err(ApiError::not_found()),
        }
    }
    async fn v2_update_split_head(
        &self,
        identity: &DatasetIdentity,
        owner: &str,
        request: Request<Incoming>,
    ) -> ApiResult {
        let input: input::UpdateHead = read_json(request).await?;
        let snapshot = snapshot(owner)?;
        let update = crowdb_access_dataset::split::SplitHeadUpdate {
            revision: revision(&input.revision)?,
            expected_revision: input.expected_revision.as_deref().map(revision).transpose()?,
            expected_head: input::generation(input.expected_head.as_deref())?,
        };
        let head = self
            .reads
            .authority()
            .update_split_head(identity, snapshot, &update)
            .await?;
        json(&output::head(snapshot, &head))
    }

    async fn v2_split_draft(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        let ["split-drafts", id, rest @ ..] = tail else {
            return Err(ApiError::not_found());
        };
        let operation = operation(id)?;
        let authority = self.reads.authority();
        match (request.method().clone(), rest) {
            (Method::GET, []) => {
                read_body(request, false).await?;
                json(&output::draft(
                    authority.split_draft_status(identity, operation).await?,
                ))
            }
            (Method::DELETE, []) => {
                let input: input::Prepare = read_json(request).await?;
                json(&output::draft(
                    authority
                        .abandon_split_draft(identity, operation, decimal(&input.expected_revision)?)
                        .await?,
                ))
            }
            (Method::PUT | Method::POST, _) => Err(ApiError::unsupported()),
            _ => Err(ApiError::not_found()),
        }
    }
}
