// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! System initialization and health-check endpoints.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use tracing::info;

use crowdb_kv::cluster::group_election::LeaderElection;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::local_replica::PxLocalReplicaRole;
use crowdb_kv::cluster::status::{StatusLevel, StoreStatus};
use crowdb_protocol::mgmt::{HealthResponse, SystemInitRequest, SystemInitResponse};

use super::{err_json, ErrorResponse, RegistryArc};

/// `GET /health` — hierarchical cluster health report.
///
/// Aggregates per-layer cached status (no active probing in V1). Returns `200`
/// when overall status is `ok` / `degraded`, `503` when `unhealthy`
/// (load-balancer signal).
#[utoipa::path(
        get,
        path = "/health",
        tag = "management",
        responses(
            (status = 200, description = "Cluster is live", body = HealthResponse),
            (status = 503, description = "Cluster is unhealthy", body = HealthResponse)
        )
    )]
pub(super) async fn health_check(State(state): State<RegistryArc>) -> (StatusCode, Json<HealthResponse>) {
    let mut overall = StatusLevel::Ok;
    let mut messages: Vec<String> = Vec::new();
    let stores: Vec<StoreStatus> = state
        .stores_snapshot()
        .values()
        .map(|store| {
            let s = store.status();
            overall = StatusLevel::worst(overall, s.status);
            s
        })
        .collect();

    if state.is_empty() {
        messages.push("no stores configured".to_string());
    }

    let http_status = if overall == StatusLevel::Unhealthy {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (
        http_status,
        Json(HealthResponse {
            status: overall.as_str().to_string(),
            messages,
            stores,
        }),
    )
}

/// `POST /system/init` — bootstrap the system group (store 0, group 0).
///
/// Creates store 0 (if it does not already exist) and group 0 with a
/// local replica on this node. For single-node init, `start_election`
/// defaults to `true` (self-elect). For multi-node, the caller sets
/// `start_election: false` and wires remotes afterward via
/// `POST /stores/0/groups/0/remotes`.
#[utoipa::path(
        post,
        path = "/system/init",
        tag = "management",
        request_body = SystemInitRequest,
        responses(
            (status = 201, description = "System group created", body = SystemInitResponse),
            (status = 409, description = "Group 0 already exists", body = ErrorResponse),
            (status = 500, description = "Store or group creation failed", body = ErrorResponse)
        )
    )]
#[allow(clippy::too_many_lines)]
pub(super) async fn system_init(
    State(state): State<RegistryArc>,
    req: Option<Json<SystemInitRequest>>,
) -> Result<(StatusCode, Json<SystemInitResponse>), (StatusCode, Json<ErrorResponse>)> {
    const SYSTEM_STORE_ID: u64 = 0;
    const SYSTEM_GROUP_ID: u64 = 0;

    let req = req.map_or(
        SystemInitRequest {
            replica_id: 1,
            start_election: true,
            bootstrap: None,
        },
        |Json(r)| r,
    );

    let _execution = super::system_bootstrap::begin(&state)?;
    super::system_bootstrap::accept(&state, req.replica_id, req.bootstrap.as_ref())?;

    let store = super::system_store::ensure(&state).await?;

    // Check if group 0 already exists.
    if let Some(group) = store.get_group(SYSTEM_GROUP_ID) {
        if req.bootstrap.is_some() && group.local_replica().id == req.replica_id {
            return Ok((
                StatusCode::OK,
                Json(SystemInitResponse {
                    store_id: 0,
                    group_id: 0,
                    replica_id: req.replica_id,
                    listen_addr: store.listen_addr().map(|address| address.to_string()),
                }),
            ));
        }
        return Err(err_json(
            StatusCode::CONFLICT,
            "group 0 already exists in store 0",
        ));
    }

    let group = crate::recovery::startup::create_group_with_wal(
        SYSTEM_STORE_ID,
        SYSTEM_GROUP_ID,
        req.replica_id,
        if req.start_election {
            PxLocalReplicaRole::Leader
        } else {
            PxLocalReplicaRole::Follower
        },
        &state.config,
        state.wal_backend.clone(),
        state.crowtree_backend,
    )
    .await
    .map_err(|e| {
        err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to create group 0: {e}"),
        )
    })?;

    if req.start_election && group.quorum() == 1 {
        let current_term = group.local_replica().current_term_snapshot();
        if current_term == 0 {
            group.local_replica().become_candidate(1);
            group.local_replica().persist_current_vote().await;
            group.local_replica().become_leader();
        }
        group.stamp_proposing_term(group.local_replica().current_term_snapshot());
    }

    let listen_addr = store.listen_addr().map(|a| a.to_string());

    if req.start_election {
        store.add_group(group);
    } else {
        store.add_group_without_election(group);
    }

    // Persist the group config to node-config.json so the replica_id,
    // endpoint, and membership survive a restart. Without this,
    // single-node init (no remote-wiring step) leaves no node-config
    // entry for store 0, and restore mode cannot recover the store's
    // listen port — it falls back to the port pool, which may collide
    // with another store's persisted port.
    if let Some(g) = store.get_group(SYSTEM_GROUP_ID) {
        g.persist_config().await;
    }

    info!(
        s = SYSTEM_STORE_ID,
        g = SYSTEM_GROUP_ID,
        replica = req.replica_id,
        start_election = req.start_election,
        "system group 0 created via /system/init"
    );

    Ok((
        StatusCode::CREATED,
        Json(SystemInitResponse {
            store_id: SYSTEM_STORE_ID,
            group_id: SYSTEM_GROUP_ID,
            replica_id: req.replica_id,
            listen_addr,
        }),
    ))
}
