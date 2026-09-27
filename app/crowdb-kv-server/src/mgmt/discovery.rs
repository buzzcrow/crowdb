// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use crowdb_protocol::mgmt::Group0DiscoveryRequest;

use crate::background::discovery;

use super::{err_json, ErrorResponse, RegistryArc};

#[utoipa::path(
    post,
    path = "/system/group0-discovery",
    tag = "management",
    request_body = Group0DiscoveryRequest,
    responses(
        (status = 200, description = "Discovery hints persisted", body = Group0DiscoveryRequest),
        (status = 400, description = "Invalid discovery hints", body = ErrorResponse),
        (status = 500, description = "Persistence failed", body = ErrorResponse)
    )
)]
pub(super) async fn update(
    State(state): State<RegistryArc>,
    Json(request): Json<Group0DiscoveryRequest>,
) -> Result<Json<Group0DiscoveryRequest>, (StatusCode, Json<ErrorResponse>)> {
    discovery::validate(&request).map_err(|error| err_json(StatusCode::BAD_REQUEST, error.to_string()))?;
    discovery::save(&state.config.config_root, &request)
        .await
        .map_err(|error| err_json(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(request))
}
