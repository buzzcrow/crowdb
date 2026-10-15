// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{
    batch::BatchRequest, json, operation, read_body, read_json, ApiError, ApiResult, DatasetHttpService,
    RequestId,
};
use super::{query::Query, read_models as models};
use crate::dataset::unix_seconds;
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::{DatasetIdentity, DatasetSessions, ReadCancellation};
use hyper::{body::Incoming, Method, Request};

impl DatasetHttpService {
    pub(super) async fn v2_read_session(
        &self,
        identity: &DatasetIdentity,
        tail: &[&str],
        request: Request<Incoming>,
    ) -> ApiResult {
        Query::parse(request.uri().query(), &[])?;
        let sessions = DatasetSessions::new(self.reads.authority());
        let now = unix_seconds();
        let cancel = ReadCancellation::new();
        if request.method() == Method::POST && tail == ["read-plans"] {
            let input: models::Plan = read_json(request).await?;
            let (spec, page) = input.native()?;
            return json(&models::preparation(
                sessions
                    .prepare_split_read(identity, &spec, page, now, &cancel)
                    .await?,
            ));
        }
        let ["read-plans", id, rest @ ..] = tail else {
            return Err(ApiError::not_found());
        };
        let id = operation(id)?;
        if request.method() == Method::DELETE && rest.is_empty() {
            read_body(request, false).await?;
            return json(&models::session(&sessions.cancel_split_read(identity, id).await?));
        }
        let status = sessions.split_read_status(identity, id, now, &cancel).await?;
        match (request.method().clone(), rest) {
            (Method::GET, []) => {
                read_body(request, false).await?;
                json(&models::preparation(status))
            }
            (Method::GET, ["progress"]) => {
                read_body(request, false).await?;
                let session = status.session.ok_or_else(ApiError::conflict)?;
                json(&serde_json::json!({"plan":session.plan, "cursor":session.acknowledged}))
            }
            (Method::PUT, ["progress"]) => {
                let ack: models::Acknowledge = read_json(request).await?;
                if ack.expected.len() > 4096 || ack.next.len() > 4096 {
                    return Err(ApiError::too_large());
                }
                let session = sessions
                    .acknowledge(identity, id, &ack.expected, &ack.next, now)
                    .await?;
                json(&serde_json::json!({"plan":session.plan,"cursor":session.acknowledged}))
            }
            (Method::POST, ["next"]) => {
                let request_id = request
                    .extensions()
                    .get::<RequestId>()
                    .ok_or_else(ApiError::integrity)?
                    .0
                    .clone();
                read_body(request, false).await?;
                if !status.complete {
                    return Err(ApiError::conflict());
                }
                let batch = sessions.next(identity, id, now).await?;
                self.batch(
                    identity,
                    BatchRequest {
                        snapshot: batch.snapshot.to_string(),
                        sample_ids: batch.ids.iter().map(|id| STANDARD.encode(id)).collect(),
                        fields: batch.fields,
                        sample_id_limit: Some(16 * 1024),
                    },
                    Some(batch.next),
                    batch.end,
                    Some(id),
                    request_id,
                )
                .await
            }
            _ => Err(ApiError::not_found()),
        }
    }
}
