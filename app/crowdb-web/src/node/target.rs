// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Confirmed remote attach identity, without granting runtime ownership.

use axum::{extract::State, http::StatusCode, Json};
use crowdb_console_shared::deployment::{PreparedBootstrap, CLUSTER_KEY};
use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::mgmt::cluster::ClusterTargetIdentity;

use crate::{
    error::{err_409, err_502, map_config_err},
    state::AppState,
};

type Failure = (StatusCode, Json<crate::error::ErrorBody>);

pub(crate) async fn identity(State(state): State<AppState>) -> Result<Json<ClusterTargetIdentity>, Failure> {
    tokio::time::timeout(std::time::Duration::from_secs(3), observe(&state))
        .await
        .map_err(|_| err_502("Confirmed cluster target observation timed out"))?
        .map(Json)
}

async fn observe(state: &AppState) -> Result<ClusterTargetIdentity, Failure> {
    let kv = state.kv_client().await;
    let GetOutcome::Found { value, .. } = kv
        .get(0, 0, CLUSTER_KEY, ReadMode::Linearizable, None)
        .await
        .map_err(|_| err_502("Confirmed cluster target is unavailable"))?
    else {
        return Err(err_409("Cluster identity has not been committed"));
    };
    if value.len() > 1024 * 1024 {
        return Err(err_409("Committed cluster identity exceeds its size bound"));
    }
    let prepared: PreparedBootstrap =
        serde_json::from_slice(&value).map_err(|_| err_409("Committed cluster identity is invalid"))?;
    prepared.validate().map_err(map_config_err)?;
    if let Some(binding) = super::cluster_binding(state)? {
        if binding.bootstrap != prepared.identity {
            return Err(err_409("Bound and committed cluster identities differ"));
        }
    }
    let config = prepared.intent.to_config();
    let mut management = Vec::new();
    let mut rpc = Vec::new();
    for member in prepared.intent.members() {
        let server = config
            .server_for_node(*member)
            .ok_or_else(|| err_409("Committed cluster member has no management endpoint"))?;
        management.push(server.url.clone());
        rpc.push(
            server
                .rpc_url
                .clone()
                .ok_or_else(|| err_409("Committed cluster member has no native RPC endpoint"))?,
        );
    }
    Ok(ClusterTargetIdentity {
        version: 1,
        cluster_id: prepared.identity.cluster_id,
        group0_management_endpoints: management,
        group0_rpc_endpoints: rpc,
    })
}
