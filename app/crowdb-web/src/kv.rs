// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV data-plane handlers — delegate to `ops::kv_data::*`.

use crate::error::{err_400, err_502, map_config_err, ErrorBody};
use crate::mgmt::refresh_node_cache;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::cluster::{GroupHealth, NodeId};
use crowdb_console_shared::ops;
use crowdb_kv_client::{GetOutcome, ScanOutcome};
use hex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::time::{sleep, Duration};

#[derive(Debug, Deserialize)]
pub struct KvGetQuery {
    /// Key as UTF-8 string. For binary, use `key_hex`.
    #[serde(default)]
    key: Option<String>,
    /// Hex-encoded raw key. Wins over `key` when present.
    #[serde(default)]
    key_hex: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct KvWriteBody {
    /// Key as UTF-8 string. For binary, use `key_hex`.
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    key_hex: Option<String>,
    /// Value as UTF-8 string. For binary, use `value_hex`. Optional for delete.
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    value_hex: Option<String>,
    #[serde(default)]
    client_id: u64,
    #[serde(default)]
    seq: u64,
}

#[derive(Debug, Serialize)]
pub struct KvGetResponse {
    found: bool,
    revision: u64,
    /// UTF-8 lossy decoding of the value; absent when `found=false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    value_utf8: Option<String>,
    /// Hex-encoded raw bytes of the value; absent when `found=false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    value_hex: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct KvWriteResponse {
    ok: bool,
    revision: u64,
}

/// Decode a key from either UTF-8 or hex encoding.
///
/// # Errors
/// Returns an error if neither encoding is provided or if hex decoding fails.
pub fn decode_key(
    utf8: Option<String>,
    hex_enc: Option<String>,
) -> Result<Vec<u8>, (axum::http::StatusCode, Json<ErrorBody>)> {
    if let Some(h) = hex_enc {
        return decode_hex(&h);
    }
    if let Some(s) = utf8 {
        return Ok(s.into_bytes());
    }
    Err(err_400("missing `key` or `key_hex`"))
}

fn decode_hex(s: &str) -> Result<Vec<u8>, (axum::http::StatusCode, Json<ErrorBody>)> {
    hex::decode(s.trim()).map_err(|e| err_400(format!("invalid hex: {e}")))
}

/// Resolve a group's current leader from Group 0 membership, live service
/// registration, and a self-reported leader with a per-store listen port.
///
/// # Errors
/// Returns `404` if the group has no replicas, or `502` if discovery or
/// leader confirmation is unavailable.
pub async fn resolve_kv_endpoint(
    state: &AppState,
    sid: u64,
    gid: u64,
) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    let (_, nodes, registered) = group_discovery(state, sid, gid).await?;
    authoritative_leader_hint(state, sid, gid, &nodes, &registered)
        .await
        .ok_or_else(|| err_502(format!("group {gid} in store {sid} has no confirmed live leader")))
}

#[derive(Debug, Serialize)]
pub struct EndpointResponse {
    /// crowdb-rpc URL of the group's current leader (`http://host:port`),
    /// ready to hand to `KvClient::connect`.
    rpc_url: String,
}

/// `GET /api/stores/:sid/groups/:gid/endpoint`. Resolve the crowdb-rpc
/// endpoint of the group's leader via Group 0, so a direct
/// crowdb-rpc client (the CLI bench engine) can dial it without touching any
/// registry. Same resolution as the KV data plane uses internally.
///
/// # Errors
/// `404` if the group has no replicas; `502` if discovery or leader
/// confirmation is unavailable.
pub async fn http_kv_endpoint(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<EndpointResponse>, (StatusCode, Json<ErrorBody>)> {
    let rpc_url = resolve_kv_endpoint(&state, sid, gid).await?;
    Ok(Json(EndpointResponse { rpc_url }))
}

/// Extract the port from a `host:port` (or `scheme://host:port`) string.
fn port_of(addr: &str) -> Option<u16> {
    addr.rsplit(':').next()?.trim().parse::<u16>().ok()
}

/// Extract the host from a `scheme://host:port` or `host:port` string,
/// defaulting to `127.0.0.1` when it cannot be parsed.
fn host_of(rpc_url: &str) -> String {
    let without_scheme = rpc_url.split_once("://").map_or(rpc_url, |(_, rest)| rest);
    let host = without_scheme.split(':').next().unwrap_or("").trim();
    if host.is_empty() || host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        host.to_string()
    }
}

/// Build an `OpContext` for a KV data-plane request on `(sid, gid)`.
///
/// Uses Group 0 membership and live service registrations for discovery.
async fn kv_op_context(
    state: &AppState,
    sid: u64,
    gid: u64,
) -> Result<crowdb_console_shared::ops::OpContext, (StatusCode, Json<ErrorBody>)> {
    let (ctx, nodes, registered) = group_discovery(state, sid, gid).await?;
    if let Some(endpoint) = authoritative_leader_hint(state, sid, gid, &nodes, &registered).await {
        ctx.kv().seed_leader(sid, gid, endpoint);
    }
    let seeds = registered
        .into_values()
        .filter(|endpoints| endpoints.len() == 1)
        .flatten()
        .collect();
    ctx.kv().set_mgmt_seeds(seeds);
    Ok(ctx)
}

type GroupDiscovery = (
    crowdb_console_shared::ops::OpContext,
    HashSet<NodeId>,
    HashMap<NodeId, Vec<String>>,
);

async fn group_discovery(
    state: &AppState,
    sid: u64,
    gid: u64,
) -> Result<GroupDiscovery, (StatusCode, Json<ErrorBody>)> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let replicas = ctx
        .sysmd()
        .list_replicas_in_group(sid, gid)
        .await
        .map_err(|error| err_502(format!("Group 0 replica lookup failed: {error}")))?;
    if replicas.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found or has no replicas"),
            }),
        ));
    }
    let nodes: HashSet<_> = replicas.into_iter().map(|replica| replica.node_id).collect();
    let instances = ctx
        .sysmd()
        .read_all_kv_server_instances()
        .await
        .map_err(|error| err_502(format!("Group 0 service lookup failed: {error}")))?;
    let mut registered = HashMap::<NodeId, Vec<String>>::new();
    for (_, instance) in instances {
        if let Some(node_id) = instance
            .extra
            .as_ref()
            .and_then(|extra| extra.kv_server.as_ref())
            .and_then(|extra| extra.node_id)
        {
            registered.entry(node_id).or_default().push(instance.rpc_endpoint);
        }
    }
    if !nodes.iter().any(|node_id| {
        registered
            .get(node_id)
            .is_some_and(|endpoints| endpoints.len() == 1)
    }) {
        return Err(err_502(format!(
            "group {gid} in store {sid} has no live KV registration in Group 0"
        )));
    }
    Ok((ctx, nodes, registered))
}

async fn authoritative_leader_hint(
    state: &AppState,
    sid: u64,
    gid: u64,
    nodes: &HashSet<NodeId>,
    registered: &HashMap<NodeId, Vec<String>>,
) -> Option<String> {
    for attempt in 0..5 {
        let group_healthy = state
            .monitor_cache
            .resolve_group(sid, gid)
            .await
            .is_some_and(|view| !matches!(view.state, GroupHealth::Unavailable | GroupHealth::Unknown));
        if let Some((_, node_id)) = state
            .monitor_cache
            .strict_leader_for(sid, gid)
            .await
            .filter(|_| group_healthy)
        {
            if nodes.contains(&node_id) {
                if let Some(endpoints) = registered.get(&node_id).filter(|endpoints| endpoints.len() == 1) {
                    let snapshot = state.monitor_cache.snapshot().await;
                    let store_port = snapshot
                        .get(&node_id)
                        .and_then(|record| record.stores.get(&sid))
                        .and_then(|store| store.listen_addr.as_deref())
                        .and_then(port_of);
                    if let Some(port) = store_port {
                        return Some(format!("http://{}:{port}", host_of(&endpoints[0])));
                    }
                }
            }
        }
        if attempt < 4 {
            futures::future::join_all(nodes.iter().map(|node_id| refresh_node_cache(state, *node_id))).await;
            sleep(Duration::from_millis(50 * (1 + attempt))).await;
        }
    }
    None
}

/// Get a value from the KV store.
///
/// # Errors
/// Returns an error if the key decoding, endpoint resolution, or crowdb-rpc call fails.
pub async fn http_kv_get(
    State(state): State<AppState>,
    Query(q): Query<KvGetQuery>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<KvGetResponse>, (StatusCode, Json<ErrorBody>)> {
    let key = decode_key(q.key, q.key_hex)?;
    let ctx = kv_op_context(&state, sid, gid).await?;
    let t_get = std::time::Instant::now();
    let outcome = ops::kv_data::get(&ctx, sid, gid, &key)
        .await
        .map_err(map_config_err)?;
    tracing::debug!(
        "http_kv_get: store={sid} group={gid} key={} get in {}ms",
        String::from_utf8_lossy(&key),
        t_get.elapsed().as_millis()
    );
    match outcome {
        GetOutcome::NotFound => Ok(Json(KvGetResponse {
            found: false,
            revision: 0,
            value_utf8: None,
            value_hex: None,
        })),
        GetOutcome::Found { value, revision } => Ok(Json(KvGetResponse {
            found: true,
            revision,
            value_utf8: Some(String::from_utf8_lossy(&value).into_owned()),
            value_hex: Some(hex::encode(&value)),
        })),
    }
}

#[derive(Debug, Deserialize)]
pub struct KvScanQuery {
    /// UTF-8 prefix; mutually exclusive with `prefix_hex`. Empty means
    /// "every key in the group".
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    prefix_hex: Option<String>,
    /// Exclusive lower bound for pagination (UTF-8). Mutually exclusive
    /// with `start_after_hex`. Empty means "start from the beginning".
    #[serde(default)]
    start_after: Option<String>,
    #[serde(default)]
    start_after_hex: Option<String>,
    /// `0` = no limit. Defaults to 100 to keep the JSON payload small
    /// for human consumers.
    #[serde(default = "default_scan_limit")]
    limit: u32,
}

fn default_scan_limit() -> u32 {
    100
}

#[derive(Debug, Serialize)]
pub struct KvScanItemView {
    key_utf8: String,
    key_hex: String,
    value_utf8: String,
    value_hex: String,
}

#[derive(Debug, Serialize)]
pub struct KvScanResponseView {
    items: Vec<KvScanItemView>,
    truncated: bool,
}

/// Scan keys in the KV store.
///
/// # Errors
/// Returns an error if the endpoint resolution or crowdb-rpc call fails.
pub async fn http_kv_scan(
    State(state): State<AppState>,
    Query(q): Query<KvScanQuery>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<KvScanResponseView>, (StatusCode, Json<ErrorBody>)> {
    let prefix = match (q.prefix_hex.as_ref(), q.prefix.as_ref()) {
        (Some(h), _) => decode_hex(h)?,
        (None, Some(s)) => s.as_bytes().to_vec(),
        (None, None) => Vec::new(),
    };
    let start_after = match (q.start_after_hex.as_ref(), q.start_after.as_ref()) {
        (Some(h), _) => decode_hex(h)?,
        (None, Some(s)) => s.as_bytes().to_vec(),
        (None, None) => Vec::new(),
    };
    let limit = q.limit;
    let ctx = kv_op_context(&state, sid, gid).await?;
    let ScanOutcome { items, truncated, .. } =
        ops::kv_data::scan(&ctx, sid, gid, &prefix, &start_after, limit)
            .await
            .map_err(map_config_err)?;
    let items = items
        .into_iter()
        .map(|(k, v)| KvScanItemView {
            key_utf8: String::from_utf8_lossy(&k).into_owned(),
            key_hex: hex::encode(&k),
            value_utf8: String::from_utf8_lossy(&v).into_owned(),
            value_hex: hex::encode(&v),
        })
        .collect();
    Ok(Json(KvScanResponseView { items, truncated }))
}

/// Put a value into the KV store.
///
/// # Errors
/// Returns an error if the key/value decoding, endpoint resolution, or crowdb-rpc call fails.
pub async fn http_kv_put(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(body): Json<KvWriteBody>,
) -> Result<Json<KvWriteResponse>, (StatusCode, Json<ErrorBody>)> {
    if sid == 0 && gid == 0 {
        return Err(err_400("Group 0 is read-only through the KV data API"));
    }
    let key = decode_key(body.key, body.key_hex)?;
    let value = if let Some(h) = body.value_hex {
        decode_hex(&h)?
    } else if let Some(v) = body.value {
        v.into_bytes()
    } else {
        return Err(err_400("missing `value` or `value_hex`"));
    };
    let ctx = kv_op_context(&state, sid, gid).await?;
    let t_put = std::time::Instant::now();
    let out = ops::kv_data::put(&ctx, sid, gid, &key, &value, Some((body.client_id, body.seq)))
        .await
        .map_err(map_config_err)?;
    tracing::debug!(
        "http_kv_put: store={sid} group={gid} key={} put in {}ms",
        String::from_utf8_lossy(&key),
        t_put.elapsed().as_millis()
    );
    Ok(Json(KvWriteResponse {
        ok: true,
        revision: out.revision,
    }))
}

/// Delete a value from the KV store.
///
/// # Errors
/// Returns an error if the key decoding, endpoint resolution, or crowdb-rpc call fails.
pub async fn http_kv_delete(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(body): Json<KvWriteBody>,
) -> Result<Json<KvWriteResponse>, (StatusCode, Json<ErrorBody>)> {
    if sid == 0 && gid == 0 {
        return Err(err_400("Group 0 is read-only through the KV data API"));
    }
    let key = decode_key(body.key, body.key_hex)?;
    let ctx = kv_op_context(&state, sid, gid).await?;
    let out = ops::kv_data::delete(&ctx, sid, gid, &key, Some((body.client_id, body.seq)))
        .await
        .map_err(map_config_err)?;
    Ok(Json(KvWriteResponse {
        ok: true,
        revision: out.revision,
    }))
}
