// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{
    json, operation, read_body, read_json, snapshot, ApiError, ApiResult, DatasetHttpService,
};
use super::{
    inspection_models::Page,
    query::Query,
    split_models::{decimal, revision, DraftSplit},
    split_requests as input, split_responses as output,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::{
    split::{SplitDraftEdit, SplitName},
    CommitAttempt, CommitTarget, DatasetIdentity, ReadCancellation,
};
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
            (Method::POST, ["snapshots", owner, "split-drafts"]) => {
                let input: input::Begin = read_json(request).await?;
                let splits = input
                    .splits
                    .into_iter()
                    .map(DraftSplit::native)
                    .collect::<Result<_, _>>()?;
                let record = authority
                    .begin_split_draft(
                        identity,
                        operation(&input.operation)?,
                        snapshot(owner)?,
                        input.expected_revision.as_deref().map(revision).transpose()?,
                        input.policy.into(),
                        splits,
                    )
                    .await?;
                json(&output::draft(record))
            }
            (_, ["split-drafts", ..]) => self.v2_split_draft(identity, tail, request).await,
            (Method::GET, ["snapshots", owner, "split-head"]) => {
                read_body(request, false).await?;
                let head = authority.inspect_split_head(identity, snapshot(owner)?).await?;
                json(
                    &serde_json::json!({"snapshot":owner, "revision":head.revision.map(|r|r.to_string()), "generation":head.generation.map(|h|hex::encode(h.as_bytes()))}),
                )
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
            (Method::PUT, []) => {
                let input: input::SplitEdit = read_json(request).await?;
                let edit = SplitDraftEdit {
                    id: super::super::operation(&input.edit)?,
                    expected_revision: decimal(&input.expected_revision)?,
                    policy: input.policy.into(),
                    splits: input
                        .splits
                        .into_iter()
                        .map(DraftSplit::native)
                        .collect::<Result<_, _>>()?,
                };
                json(&output::draft(
                    authority.edit_split_draft(identity, operation, &edit).await?,
                ))
            }
            (Method::POST, ["evaluate"]) => {
                let input: input::Evaluate = read_json(request).await?;
                json(&output::preview(
                    authority
                        .evaluate_split_draft(
                            identity,
                            operation,
                            decimal(&input.expected_revision)?,
                            decimal(&input.expected_page)?,
                            &ReadCancellation::new(),
                        )
                        .await?,
                ))
            }
            (Method::POST, ["prepare"]) => {
                let input: input::Prepare = read_json(request).await?;
                json(&output::revision(
                    authority
                        .prepare_split_draft(identity, operation, decimal(&input.expected_revision)?)
                        .await?,
                ))
            }
            (Method::POST, ["publish"]) => {
                let input: input::Publish = read_json(request).await?;
                let draft = authority.split_draft_status(identity, operation).await?;
                let attempt = CommitAttempt {
                    version: 1,
                    id: super::super::operation(&input.attempt)?,
                    target: CommitTarget::SplitRevision {
                        snapshot: draft.snapshot,
                        revision: revision(&input.revision)?,
                    },
                    expected_head: input::generation(input.expected_head.as_deref())?,
                };
                json(&output::receipt(
                    &authority
                        .publish_split_draft(identity, operation, &attempt)
                        .await?,
                ))
            }
            _ => Err(ApiError::not_found()),
        }
    }
}
