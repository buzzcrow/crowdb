// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use crowdb_console_shared::deployment::PreparedBootstrap;
use crowdb_kv_client::{GetOutcome, ReadMode};
use serde_json::{json, Value};

use crate::{
    error::{err_409, err_502, map_config_err, ErrorBody},
    state::AppState,
};
type Failure = (StatusCode, Json<ErrorBody>);

pub(crate) async fn status(State(state): State<AppState>) -> Json<Value> {
    if reconcile_cleanup(&state)
        .and_then(|()| super::preparation::recover(&state))
        .is_err()
    {
        return Json(json!({"phase": "recovery_required", "available": false}));
    }
    if state.managed_mode {
        let (status, _) = crate::managed::authority(State(state.clone())).await;
        return Json(
            json!({"phase": if status.is_success() { "active" } else { "authority_unavailable" }, "available": status.is_success(), "readonly": true}),
        );
    }
    let prepared = state.runtime_root.join("prepared-bootstrap.json");
    if let Ok(bytes) = std::fs::read(state.runtime_root.join("cleanup-progress.json")) {
        return Json(
            json!({"phase": "cleanup_in_progress", "available": false, "cleanup": serde_json::from_slice::<Value>(&bytes).ok()}),
        );
    }
    let Ok(binding) = super::admission::binding(&state) else {
        return Json(json!({"phase": "authority_unavailable", "available": false}));
    };
    if let Some(binding) = binding {
        let mut available = confirmed(&state, &binding).await.is_ok();
        if available {
            available = refresh(&state).await.is_ok();
        }
        if available {
            let _ = state.persist();
        }
        return Json(
            json!({"phase": if available { "active" } else { "authority_unavailable" }, "available": available, "cluster_id": binding.bootstrap.cluster_id, "operation_id": binding.bootstrap.operation_id}),
        );
    }
    if prepared.exists() {
        let operation = PreparedBootstrap::load(&prepared);
        return Json(match operation {
            Ok(operation) => bootstrap_status(&operation).await,
            Err(_) => json!({"phase": "recovery_required", "available": false}),
        });
    }
    Json(json!({"phase": "unbound_draft", "available": false}))
}

async fn bootstrap_status(operation: &PreparedBootstrap) -> Value {
    let Ok(client) = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(1))
        .build()
    else {
        return json!({"phase": "recovery_required", "available": false});
    };
    let config = operation.intent.to_config();
    let nodes = futures::future::join_all(operation.intent.members().iter().map(|id| {
        let client = &client; let config = &config;
        async move {
            let Some(server) = config.server_for_node(*id) else { return json!({"node_id": id, "phase": "unreachable"}); };
            let base = server.url.trim_end_matches('/');
            let store = client.get(format!("{base}/stores/0")).send().await;
            let ready = super::read::<Value>(client, &format!("{base}/stores/0/groups/0/ready")).await;
            json!({"node_id": id, "store_created": store.is_ok_and(|response| response.status().is_success()), "group_ready": ready.as_ref().is_ok_and(|value| value["ready"] == true), "phase": if ready.as_ref().is_ok_and(|value| value["ready"] == true) { "ready" } else { "preparing" }})
        }
    })).await;
    let publishing = nodes.iter().all(|node| node["group_ready"] == true);
    json!({"phase": if publishing { "topology_publishing" } else { "bootstrap_in_progress" }, "available": false, "cluster_id": operation.identity.cluster_id, "operation_id": operation.identity.operation_id, "members": operation.intent.members(), "nodes": nodes})
}

pub(crate) async fn guard(State(state): State<AppState>, request: Request, next: Next) -> Response {
    if state.node_monitor_url.is_none() {
        return next.run(request).await;
    }
    if let Err(error) = reconcile_cleanup(&state).and_then(|()| super::preparation::recover(&state)) {
        return error.into_response();
    }
    let path = request.uri().path().to_owned();
    let mutation = !matches!(*request.method(), Method::GET | Method::HEAD | Method::OPTIONS);
    if path == "/api/node/cleanup" && mutation {
        return next.run(request).await;
    }
    if path.starts_with("/api/node/") && !mutation {
        return next.run(request).await;
    }
    let binding = match super::admission::binding(&state) {
        Ok(binding) => binding,
        Err(error) => return error.into_response(),
    };
    if let Some(binding) = binding {
        let parts: Vec<_> = path.trim_matches('/').split('/').collect();
        if parts.len() == 7
            && parts[0] == "api"
            && parts[1] == "stores"
            && parts[3] == "groups"
            && parts[5] == "kv"
            && (parts[2] != "0" || parts[4] != "0")
        {
            return next.run(request).await;
        }

        if mutation || path.starts_with("/api/racks") || path == "/api/nodes" {
            if let Err(error) = confirmed(&state, &binding).await {
                return error.into_response();
            }
            if let Err(error) = refresh(&state).await {
                return error.into_response();
            }
        }
        if mutation
            && path == "/api/cluster/init"
            && !state.runtime_root.join("prepared-bootstrap.json").exists()
        {
            return err_409("Cluster is already bound; replacement bootstrap requires explicit cleanup")
                .into_response();
        }
        if mutation && path == "/api/racks" && *request.method() == Method::POST {
            return create_rack(&state, request).await;
        }
        if mutation
            && path.starts_with("/api/racks/")
            && *request.method() == Method::DELETE
            && path[11..].parse::<u64>().is_ok()
        {
            let id = path[11..].parse::<u64>().unwrap();
            let ctx = match state.op_context().await {
                Ok(ctx) => ctx,
                Err(error) => return map_config_err(error).into_response(),
            };
            return match crowdb_console_shared::ops::hardware::remove_rack_from_group0(&ctx, id).await {
                Ok(()) => StatusCode::NO_CONTENT.into_response(),
                Err(error) => map_config_err(error).into_response(),
            };
        }
    } else if mutation {
        let draft = path == "/api/racks" || path.starts_with("/api/racks/");
        let permitted =
            path == "/api/node/admit" || path == "/api/node/cancel" || path == "/api/cluster/init" || draft;
        if !permitted {
            return err_409(
                "Only candidate, rack and Group 0 bootstrap operations are available before publication",
            )
            .into_response();
        }
        if state.runtime_root.join("prepared-bootstrap.json").exists() && path != "/api/cluster/init" {
            return err_409("Bootstrap inputs are fixed; resume or explicitly clean up").into_response();
        }
    }
    next.run(request).await
}

fn reconcile_cleanup(state: &AppState) -> Result<(), Failure> {
    if super::admission::binding(state)?.is_some()
        || state.runtime_root.join("cleanup-progress.json").exists()
    {
        return Ok(());
    }
    let Some(root) = state.runtime_root.parent() else {
        return Ok(());
    };
    let Ok(bytes) = std::fs::read(root.join("node-cleaned.json")) else {
        return Ok(());
    };
    let bootstrap: crowdb_protocol::mgmt::SystemBootstrapIdentity =
        serde_json::from_slice(&bytes).map_err(|error| err_502(error.to_string()))?;
    let acknowledged = state
        .runtime_root
        .join(format!("observed-cleanup-{}.json", bootstrap.operation_id));
    if acknowledged.exists() {
        return Ok(());
    }
    for name in [
        "prepared-bootstrap.json",
        "confirmed-cluster.json",
        "confirmed-nodes.json",
        "bootstrap-progress.json",
    ] {
        match std::fs::remove_file(state.runtime_root.join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(err_502(error.to_string())),
        }
    }
    for entry in std::fs::read_dir(state.runtime_root.as_ref()).map_err(|error| err_502(error.to_string()))? {
        let entry = entry.map_err(|error| err_502(error.to_string()))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if (name.starts_with("admission-") || name.starts_with("cancel-")) && name.ends_with(".json") {
            std::fs::remove_file(entry.path()).map_err(|error| err_502(error.to_string()))?;
        }
    }
    *state.config.write().map_err(|error| err_502(error.to_string()))? =
        crowdb_console_shared::ConsoleConfig::default();
    state.persist().map_err(|error| err_502(error.to_string()))?;
    super::save(&acknowledged, &bootstrap)
}

async fn confirmed(
    state: &AppState,
    binding: &crowdb_protocol::mgmt::node::NodeBinding,
) -> Result<(), Failure> {
    tokio::time::timeout(std::time::Duration::from_secs(3), confirmed_inner(state, binding))
        .await
        .map_err(|_| err_502("Group 0 authority unavailable; last confirmed state is stale"))?
}

async fn refresh(state: &AppState) -> Result<(), Failure> {
    tokio::time::timeout(std::time::Duration::from_secs(3), state.reload_group0())
        .await
        .map_err(|_| err_502("Group 0 topology refresh unavailable; last confirmed state is stale"))?
        .map_err(map_config_err)
}

async fn confirmed_inner(
    state: &AppState,
    binding: &crowdb_protocol::mgmt::node::NodeBinding,
) -> Result<(), Failure> {
    let kv = state.kv_client().await;
    kv.set_mgmt_seeds(binding.management_seeds.clone());
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        kv.get(0, 0, b"/deployment/cluster", ReadMode::Linearizable, None),
    )
    .await
    .map_err(|_| err_502("Group 0 authority unavailable; last confirmed state is stale"))?
    .map_err(|error| err_502(error.to_string()))?;
    let GetOutcome::Found { value, .. } = result else {
        return Err(err_409(
            "Bound cluster publication is absent; cleanup is required",
        ));
    };
    let operation: PreparedBootstrap =
        serde_json::from_slice(&value).map_err(|error| err_502(error.to_string()))?;
    if operation.identity != binding.bootstrap {
        return Err(err_409("Confirmed cluster identity differs"));
    }
    super::admission::save(&state.runtime_root.join("confirmed-cluster.json"), &operation)?;
    let registry = crowdb_console_shared::deployment::registry::read(&kv)
        .await
        .map_err(map_config_err)?;
    let Some((registry, _)) =
        registry.filter(|(registry, _)| registry.cluster_id == binding.bootstrap.cluster_id)
    else {
        return Err(err_409("Topology publication is incomplete"));
    };
    let mut config = state.config.write().map_err(|error| err_502(error.to_string()))?;
    if config.servers.is_empty() {
        config.servers = operation.intent.to_config().servers;
    }
    for node in registry
        .nodes
        .iter()
        .filter(|node| node.confirmed && !node.cancelled)
    {
        let host = if node.host.contains(':') {
            format!("[{}]", node.host)
        } else {
            node.host.clone()
        };
        if let Some(server) = config
            .servers
            .iter_mut()
            .find(|server| server.node_id == Some(node.node_id))
        {
            server.url = format!("http://{host}:10000");
            server.rpc_url = Some(format!("{host}:10100"));
        } else {
            let mut server = crowdb_console_shared::config::ServerEntry::new(
                format!("kv-{}", node.node_id),
                format!("http://{host}:10000"),
            );
            server.node_id = Some(node.node_id);
            server.rpc_url = Some(format!("{host}:10100"));
            config.servers.push(server);
        }
    }
    super::admission::save(&state.runtime_root.join("confirmed-nodes.json"), &registry.nodes)?;
    Ok(())
}

async fn create_rack(state: &AppState, request: Request) -> Response {
    #[derive(serde::Deserialize)]
    struct Rack {
        id: u64,
        #[serde(default)]
        name: String,
    }
    let bytes = match axum::body::to_bytes(request.into_body(), 65536).await {
        Ok(bytes) => bytes,
        Err(error) => return err_502(error.to_string()).into_response(),
    };
    let rack: Rack = match serde_json::from_slice(&bytes) {
        Ok(rack) => rack,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let ctx = match state.op_context().await {
        Ok(ctx) => ctx,
        Err(error) => return map_config_err(error).into_response(),
    };
    return match crowdb_console_shared::ops::hardware::add_rack_to_group0(&ctx, rack.id, &rack.name).await {
        Ok(rack) => (StatusCode::CREATED, Json(rack)).into_response(),
        Err(error) => map_config_err(error).into_response(),
    };
}
