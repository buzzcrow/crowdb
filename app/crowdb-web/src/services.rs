// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Typed deployment identity and local service lifecycle.

pub(crate) mod access;
mod deployment;
mod lifecycle;
pub(crate) mod observation;
mod operation;
mod publication;

pub(crate) fn routes() -> axum::Router<crate::state::AppState> {
    use axum::routing::{delete, post};
    axum::Router::new()
        .route("/api/nodes/:id/services/deploy", post(deployment::deploy))
        .route("/api/services/:id/restart", post(lifecycle::restart))
        .route("/api/services/:id/stop", post(lifecycle::stop))
        .route("/api/services/:id", delete(lifecycle::delete))
}

type Failure = (axum::http::StatusCode, axum::Json<crate::error::ErrorBody>);

pub(crate) fn node_removal(
    state: &crate::state::AppState,
    node: u64,
) -> Result<operation::Operation, Failure> {
    let operation = operation::Operation::claim(state, vec![format!("node/{node}")])?;
    if state.config.read().unwrap().servers.iter().any(|service| {
        service.node_id == Some(node)
            && !matches!(
                service.service_type,
                crowdb_console_shared::config::ServiceType::Kv
                    | crowdb_console_shared::config::ServiceType::Diskdb
            )
    }) {
        return Err(crate::error::err_409(
            "Remove auxiliary service deployments before removing this node",
        ));
    }
    Ok(operation)
}
