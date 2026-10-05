// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Typed deployment identity and local service lifecycle.

pub(crate) mod access;
pub(crate) mod defaults;
mod deployment;
mod lifecycle;
pub(crate) mod observation;
mod operation;
pub(crate) use operation::Operation;
mod plans;
mod publication;

pub(crate) fn routes() -> axum::Router<crate::state::AppState> {
    use axum::routing::{delete, get, post, put};
    axum::Router::new()
        .route("/api/deployment-defaults", get(defaults::get))
        .route("/api/service-plans", get(plans::list))
        .route("/api/nodes/:id/service-plan", put(plans::put))
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
                crowdb_console_shared::config::ServiceType::PaxosKv
                    | crowdb_console_shared::config::ServiceType::Diskdb
            )
    }) {
        return Err(crate::error::err_409(
            "Remove auxiliary service deployments before removing this node",
        ));
    }
    Ok(operation)
}

/// Stop and remove consumers before tearing down their KV metadata authority.
pub(crate) async fn remove_for_reset(state: &crate::state::AppState) -> Result<(), Failure> {
    use crowdb_console_shared::config::ServiceType;
    let mut services: Vec<_> = state
        .config
        .read()
        .unwrap()
        .servers
        .iter()
        .filter_map(|entry| {
            let order = match entry.service_type {
                ServiceType::AccessServer => 0,
                ServiceType::ChunkKv => 1,
                ServiceType::Chunkdb => 2,
                ServiceType::Diskio => 3,
                _ => return None,
            };
            Some((order, entry.id.clone()))
        })
        .collect();
    services.sort();
    for (_, id) in services {
        let _ = lifecycle::delete_for_reset(state.clone(), id).await?;
    }
    Ok(())
}

pub(crate) fn forget_plan(state: &crate::state::AppState, node: u64) -> Result<(), Failure> {
    plans::remove(state, node)
}
