// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cluster initialization and optional first data group provisioning.

use crate::error::{err_502, map_config_err, map_persist_err, ErrorBody};
use crate::mgmt::refresh_node_cache;
use crate::state::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::ops;
use serde::Deserialize;

/// Request body for `POST /api/cluster/init`.
#[derive(Debug, Deserialize)]
pub(crate) struct ClusterInitBody {
    /// Node IDs to include in the system group (store 0, group 0).
    /// Must be non-empty. For a single node, group 0 self-elects.
    /// For multiple nodes, remotes are wired and election starts after.
    pub nodes: Vec<u64>,
    /// Console bootstrap includes an ordinary data group on the same nodes.
    /// Omitted by callers that manage their data topology separately.
    #[serde(default)]
    pub create_data_group: bool,
    /// Optional versioned bootstrap topology file for a first bare-metal init.
    #[serde(default)]
    pub bootstrap_file: Option<std::path::PathBuf>,
}

/// `POST /api/cluster/init` — initialize the cluster by bootstrapping
/// the system group (store 0, group 0) on the selected nodes, wiring
/// remotes, and writing hardware + KV topology into group-0 sysdata.
///
/// Delegates to `ops::cluster::init` which handles the 5-phase
/// bootstrap (system/init, remote wiring, config update, topology
/// write). The handler retains web-specific concerns: monitor cache
/// refresh after init so health badges reflect the new state.
///
/// # Errors
/// Returns `502` if a node is unreachable or `system/init` fails,
/// `500` if config persistence fails.
pub(crate) async fn http_cluster_init(
    State(state): State<AppState>,
    Json(body): Json<ClusterInitBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    let _operation = crate::services::Operation::claim(&state, vec!["cluster/init".into()])?;
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let summary = if state.node_monitor_url.is_some() {
        let operation = crowdb_console_shared::deployment::PreparedBootstrap::open_with_nodes(
            &state.runtime_root.join("prepared-bootstrap.json"),
            &ctx.config(),
            &body.nodes,
            crate::node::records(&state)?,
        )
        .map_err(map_config_err)?;
        prepare_monitors(&state, &operation).await?;
        let result = operation.execute(&ctx).await;
        if result.is_ok() {
            bind_monitors(&state, &operation).await?;
        }
        result
    } else if state.web_mode.is_some() || state.config_path.is_some() {
        let path = state.runtime_root.join("bootstrap-intent.toml");
        if state.web_mode == Some(crowdb_console_shared::config::web::WebMode::BareMetal) {
            if let Some(source) = &body.bootstrap_file {
                let intent = crowdb_console_shared::bootstrap_intent::BootstrapIntent::load(source)
                    .map_err(map_config_err)?;
                if intent.members() != body.nodes.as_slice() {
                    return Err(map_config_err(crowdb_console_shared::error::Error::Validation {
                        field: "nodes".into(),
                        message: "bootstrap file members differ from requested nodes".into(),
                    }));
                }
                intent.seal(&path).map_err(map_config_err)?;
            } else if !path.exists() {
                return Err(map_config_err(crowdb_console_shared::error::Error::Validation {
                    field: "bootstrap_file".into(),
                    message: "required for the first bare-metal cluster init".into(),
                }));
            }
        }
        ops::cluster::init_with_intent(&ctx, &body.nodes, &path).await
    } else {
        ops::cluster::init(&ctx, &body.nodes).await
    }
    .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;

    // The cluster is now live — re-seed the shared kv_client with the
    // current config's server URLs so topology refresh can find the
    // new group-0 leader. Without this, a client left over from before
    // reset has stale/empty seeds and every KV op retries for ~5s.
    state.reseed_kv_client().await;

    if body.create_data_group {
        let nodes: Vec<_> = summary.nodes.iter().map(|(node, _)| *node).collect();
        ensure_data_group(&ctx, &nodes).await.map_err(|error| {
            err_502(format!("Group 0 is initialized, but Group 1 setup did not complete: {error}. Retry Initialize Cluster with the same nodes."))
        })?;
    }

    // Refresh the monitor cache for all init nodes so health badges
    // and RPC endpoint resolution reflect the new group-0 state.
    futures::future::join_all(
        summary
            .nodes
            .iter()
            .map(|&(node_id, _)| refresh_node_cache(&state, node_id)),
    )
    .await;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "store_id": summary.store_id,
            "group_id": summary.group_id,
            "data_group_id": body.create_data_group.then_some(1),
            "nodes": summary.nodes.iter().map(|(n, r)| serde_json::json!({
                "node_id": n,
                "replica_id": r,
            })).collect::<Vec<_>>(),
        })),
    ))
}

async fn prepare_monitors(
    state: &AppState,
    operation: &crowdb_console_shared::deployment::PreparedBootstrap,
) -> Result<(), (StatusCode, Json<ErrorBody>)> {
    let config = operation.intent.to_config();
    let credentials = crowdb_monitor::ServerCredentials::load_or_create(state.runtime_root.as_ref())
        .map_err(|error| err_502(error.to_string()))?;
    let results = futures::future::join_all(operation.intent.members().iter().map(async |member| {
        let mut node = config
            .node(*member)
            .cloned()
            .ok_or_else(|| err_502("Sealed bootstrap member is missing"))?;
        node.ssh_key = Some(crate::node::node_key_path(state).to_string_lossy().into_owned());
        crowdb_console_shared::deployment::admission::remote_control(
            &node,
            &crowdb_protocol::mgmt::node::NodeControl::StartKv {
                node_id: *member,
                bootstrap: operation.identity.clone(),
                manifest: Some(serde_json::to_value(operation).map_err(|error| err_502(error.to_string()))?),
                credentials: Some(crowdb_protocol::mgmt::node::NodeServiceCredentials {
                    environment: credentials.server_env(),
                }),
                admission: None,
            },
        )
        .await
        .map_err(map_config_err)?;
        crate::node::save(
            &state.runtime_root.join("bootstrap-progress.json"),
            &serde_json::json!({"operation_id": operation.identity.operation_id, "phase": "preparing", "node_id": member}),
        )?;
        Ok::<(), (StatusCode, Json<ErrorBody>)>(())
    }))
    .await;
    for result in results {
        result?;
    }
    Ok(())
}

async fn bind_monitors(
    state: &AppState,
    operation: &crowdb_console_shared::deployment::PreparedBootstrap,
) -> Result<(), (StatusCode, Json<ErrorBody>)> {
    let config = operation.intent.to_config();
    let seeds = config
        .servers
        .iter()
        .map(|server| server.url.clone())
        .collect::<Vec<_>>();
    let results = futures::future::join_all(operation.intent.members().iter().map(async |member| {
        let mut node = config
            .node(*member)
            .cloned()
            .ok_or_else(|| err_502("Sealed bootstrap member is missing"))?;
        node.ssh_key = Some(crate::node::node_key_path(state).to_string_lossy().into_owned());
        crowdb_console_shared::deployment::admission::remote_control(
            &node,
            &crowdb_protocol::mgmt::node::NodeControl::Bind {
                binding: crowdb_protocol::mgmt::node::NodeBinding {
                    node_id: *member,
                    bootstrap: operation.identity.clone(),
                    management_seeds: seeds.clone(),
                },
            },
        )
        .await
        .map_err(map_config_err)?;
        Ok::<(), (StatusCode, Json<ErrorBody>)>(())
    }))
    .await;
    for result in results {
        result?;
    }
    crate::node::save(
        &state.runtime_root.join("bootstrap-progress.json"),
        &serde_json::json!({"operation_id": operation.identity.operation_id, "phase": "active"}),
    )?;
    Ok(())
}

async fn ensure_data_group(ctx: &ops::OpContext, nodes: &[u64]) -> crowdb_console_shared::error::Result<()> {
    if ctx.sysmd().get_group(0, 1).await?.is_some() {
        let mut actual: Vec<_> = ctx
            .sysmd()
            .list_replicas_in_group(0, 1)
            .await?
            .iter()
            .map(|replica| replica.node_id)
            .collect();
        let mut expected = nodes.to_vec();
        actual.sort_unstable();
        expected.sort_unstable();
        if actual != expected {
            return Err(crowdb_console_shared::error::Error::Conflict {
                kind: "existing Group 1 membership differs from initialization nodes".into(),
                id: "0/1".into(),
            });
        }
        return Ok(());
    }
    ops::kv_logical::add_group(ctx, 0, 1, 1, nodes).await
}
