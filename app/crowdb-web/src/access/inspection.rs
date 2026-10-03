// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::{err_400, err_502};
use crate::state::AppState;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LocationQuery {
    bucket: String,
    key: String,
    limit: Option<usize>,
    cursor: Option<String>,
}

pub(super) async fn locations(
    State(state): State<AppState>,
    Query(query): Query<LocationQuery>,
) -> Result<Json<Value>, super::ApiError> {
    let limit = query.limit.unwrap_or(20);
    if !(1..=100).contains(&limit)
        || query.bucket.is_empty()
        || query.key.is_empty()
        || query.bucket.len() > 1024
        || query.key.len() > 1024
        || query.cursor.as_ref().is_some_and(|value| value.len() > 9216)
    {
        return Err(err_400("Invalid object inspection scope, cursor or page limit"));
    }
    let origin = super::origin(&state, "s3")?.ok_or_else(|| err_502("Cluster S3 endpoint is unavailable"))?;
    let manager = super::credentials::manager(&state).map_err(err_502)?;
    let mut target = reqwest::Url::parse(&origin).map_err(|_| err_502("Invalid configured S3 endpoint"))?;
    target.set_path("/_crowdb/admin/object-locations");
    {
        let mut params = target.query_pairs_mut();
        params
            .append_pair("bucket", &query.bucket)
            .append_pair("key", &query.key)
            .append_pair("limit", &limit.to_string());
        if let Some(cursor) = query.cursor {
            params.append_pair("cursor", &cursor);
        }
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(6))
        .build()
        .map_err(|error| err_502(error.to_string()))?;
    let mut response = client
        .get(target)
        .bearer_auth(manager)
        .send()
        .await
        .map_err(|error| err_502(error.to_string()))?;
    let status = response.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| err_502(error.to_string()))?
    {
        if bytes.len().saturating_add(chunk.len()) > 1024 * 1024 {
            return Err(err_502("Object inspection response exceeds the 1 MiB limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let body: Value =
        serde_json::from_slice(&bytes).map_err(|_| err_502("Invalid object inspection response"))?;
    if !status.is_success() {
        let message = body
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("Object metadata inspection failed");
        return Err((
            StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
            Json(crate::error::ErrorBody {
                error: message.to_owned(),
            }),
        ));
    }
    Ok(Json(body))
}
