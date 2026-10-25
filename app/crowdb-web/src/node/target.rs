// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Confirmed remote attach identity, without granting runtime ownership.

use axum::{extract::State, http::StatusCode, Json};
use crowdb_console_shared::deployment::{registry, PreparedBootstrap, CLUSTER_KEY};
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
    let current = registry::read(&kv).await.map_err(map_config_err)?;
    if current
        .as_ref()
        .is_some_and(|(registry, _)| registry.cluster_id != prepared.identity.cluster_id)
    {
        return Err(err_409("Confirmed node registry belongs to another cluster"));
    }
    let mut management = Vec::new();
    let mut rpc = Vec::new();
    for member in prepared.intent.members() {
        let server = config
            .server_for_node(*member)
            .ok_or_else(|| err_409("Committed cluster member has no management endpoint"))?;
        let rpc_url = server
            .rpc_url
            .as_deref()
            .ok_or_else(|| err_409("Committed cluster member has no native RPC endpoint"))?;
        if let Some((registry, _)) = &current {
            let node = registry
                .nodes
                .iter()
                .find(|node| node.node_id == *member && node.confirmed && !node.cancelled)
                .ok_or_else(|| err_409("Confirmed bootstrap member is unavailable"))?;
            if prepared
                .nodes
                .iter()
                .find(|expected| expected.node_id == *member)
                .is_some_and(|expected| expected.discovery_id != node.discovery_id)
            {
                return Err(err_409("Confirmed member discovery identity differs"));
            }
            management.push(with_host(&server.url, &node.host)?);
            rpc.push(
                with_host(rpc_url, &node.host)?
                    .trim_start_matches("http://")
                    .to_owned(),
            );
        } else {
            management.push(server.url.clone());
            rpc.push(rpc_url.to_owned());
        }
    }
    Ok(ClusterTargetIdentity {
        version: 1,
        cluster_id: prepared.identity.cluster_id,
        group0_management_endpoints: management,
        group0_rpc_endpoints: rpc,
    })
}

fn with_host(endpoint: &str, host: &str) -> Result<String, Failure> {
    let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    };
    let mut url = reqwest::Url::parse(&endpoint).map_err(|_| err_409("Invalid committed member endpoint"))?;
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };
    url.set_host(Some(&host))
        .map_err(|_| err_409("Invalid confirmed member host"))?;
    Ok(url.as_str().trim_end_matches('/').to_owned())
}
