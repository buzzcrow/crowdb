// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded, read-only `ChunkDB` diagnostics with routed detail and real placements.

pub(crate) mod slots;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_chunkdb_client::ChunkdbClient;
use crowdb_console_shared::ops::hardware;
use crowdb_kv_client::{RangeBindingClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{Chunk, ListChunksRequest, QueryChunkRequest};
use crowdb_protocol::common::ChunkId;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::{err_400, err_404, err_502, ErrorBody};
use crate::state::AppState;

type Response = Result<Json<Value>, (StatusCode, Json<ErrorBody>)>;

#[derive(Default, Deserialize)]
pub(crate) struct ListQuery {
    #[serde(default)]
    prefix: String,
    chunk_type: Option<i32>,
    after: Option<String>,
    limit: Option<u32>,
}

fn parse_id(id: &str) -> Result<ChunkId, (StatusCode, Json<ErrorBody>)> {
    let bytes = hex::decode(id).map_err(|_| err_400("Chunk ID must be 32 hex digits"))?;
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| err_400("Chunk ID must be 32 hex digits"))?;
    Ok(crowdb_protocol::chunk_id::ChunkIdParts::from_bytes(&bytes).to_proto())
}

fn id_text(id: ChunkId) -> String {
    format!("{:016x}{:016x}", id.high, id.low)
}

fn protect_integer_precision(value: &mut Value) {
    match value {
        Value::Number(number) if number.as_u64().is_some_and(|n| n > 9_007_199_254_740_991) => {
            *value = Value::String(number.to_string());
        }
        Value::Array(values) => values.iter_mut().for_each(protect_integer_precision),
        Value::Object(values) => values.values_mut().for_each(protect_integer_precision),
        _ => {}
    }
}

fn chunk_value(chunk: &Chunk) -> Value {
    let mut value = json!(chunk);
    protect_integer_precision(&mut value);
    value["id_hex"] = chunk.id.map_or(Value::Null, |id| json!(id_text(id)));
    value
}

fn observed_at() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

pub(crate) async fn list(State(state): State<AppState>, Query(query): Query<ListQuery>) -> Response {
    let prefix = query.prefix.to_ascii_lowercase();
    if prefix.len() > 32 || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(err_400("Chunk prefix must contain at most 32 hex digits"));
    }
    let limit = query.limit.unwrap_or(100);
    if !(1..=256).contains(&limit) {
        return Err(err_400("limit must be between 1 and 256"));
    }
    let start_token = query.after.as_deref().map(parse_id).transpose()?;
    let kv = state.kv_client().await;
    let registry = ServiceRegistryClient::from_shared(kv);
    let instances = registry
        .read_all_instances("chunkdb")
        .await
        .map_err(|error| err_502(error.to_string()))?;
    if instances.len() > 32 {
        return Err(err_502("Chunk query supports at most 32 live owners"));
    }
    let results = futures::future::join_all(instances.iter().map(|(id, instance)| {
        let endpoint = instance.rpc_endpoint.clone();
        let transport = Arc::clone(&state.chunk_rpc_transport);
        async move {
            let request = ListChunksRequest {
                start_token,
                max_keys: limit,
                ..Default::default()
            };
            (
                *id,
                tokio::time::timeout(
                    Duration::from_secs(3),
                    transport.send_list_chunks(&endpoint, &request),
                )
                .await,
            )
        }
    }))
    .await;
    let mut candidates = BTreeMap::new();
    let mut failures = Vec::new();
    let mut more = false;
    for (owner, result) in results {
        match result {
            Ok(Ok(page)) => {
                more |= page.next_token.is_some();
                for chunk in page.chunks {
                    if let Some(id) = chunk.id {
                        candidates.insert(id_text(id), (owner.to_string(), chunk));
                    }
                }
            }
            Ok(Err(error)) => failures.push(json!({"owner":owner.to_string(),"error":error.to_string()})),
            Err(_) => failures.push(json!({"owner":owner.to_string(),"error":"query timed out"})),
        }
    }
    more |= candidates.len() > limit as usize;
    let scanned: Vec<_> = candidates.into_iter().take(limit as usize).collect();
    let after = scanned.last().map(|(id, _)| id.clone());
    let chunks: Vec<_> = scanned
        .iter()
        .filter(|(id, (_, chunk))| {
            id.starts_with(&prefix) && query.chunk_type.map_or(true, |kind| kind == chunk.chunk_type)
        })
        .map(|(_, (owner, chunk))| {
            let mut value = chunk_value(chunk);
            value["owner"] = json!(owner);
            value
        })
        .collect();
    Ok(Json(
        json!({"chunks":chunks,"scanned":scanned.len(),"next":if more && failures.is_empty() {after} else {None},
        "failures":failures,"observed_at_ms":observed_at(),"source":"chunkdb","owners":instances.len()}),
    ))
}

#[derive(Debug, Deserialize)]
pub(crate) struct PxgroupListQuery {
    #[serde(default)]
    start_after: Option<String>,
    #[serde(default = "default_pxgroup_limit")]
    limit: u32,
}

fn default_pxgroup_limit() -> u32 {
    100
}

/// Scan only ChunkDB chunk records owned by one Paxos group directly through
/// the KV client. Other KV records in the group are intentionally excluded.
pub(crate) async fn pxgroup_list(
    State(state): State<AppState>,
    Path((store_id, group_id)): Path<(u64, u64)>,
    Query(query): Query<PxgroupListQuery>,
) -> Response {
    if query.limit == 0 || query.limit > 256 {
        return Err(err_400("limit must be between 1 and 256"));
    }
    let prefix = b"/chunk/";
    let start_after = query
        .start_after
        .map(|token| hex::decode(token).map_err(|_| err_400("start_after must be a hex key token")))
        .transpose()?
        .unwrap_or_default();
    let ctx = state
        .op_context()
        .await
        .map_err(|error| err_502(error.to_string()))?;
    let outcome = crowdb_console_shared::ops::kv_data::scan(
        &ctx,
        store_id,
        group_id,
        prefix,
        &start_after,
        query.limit,
    )
    .await
    .map_err(|error| err_502(error.to_string()))?;
    let next_start_after = outcome
        .truncated
        .then(|| outcome.items.last().map(|(key, _)| hex::encode(key)))
        .flatten();
    let items = outcome
        .items
        .into_iter()
        .filter_map(|(key, value)| {
            let chunk_id = key
                .strip_prefix(prefix)
                .filter(|id| id.len() == 16)
                .map(hex::encode)?;
            let metadata = bincode::deserialize::<Chunk>(&value).ok();
            json!({
                "chunk_id": chunk_id,
                "key_hex": hex::encode(&key),
                "chunk_type": metadata.as_ref().map(|chunk| chunk.chunk_type),
                "state": metadata.as_ref().map(|chunk| chunk.state),
                "capacity": metadata.as_ref().map(|chunk| chunk.capacity),
                "sealed_length": metadata.as_ref().map(|chunk| chunk.sealed_length),
                "strip_count": metadata.as_ref().map(|chunk| chunk.strips.len()),
            })
            .into()
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "store_id": store_id, "group_id": group_id, "items": items,
        "truncated": outcome.truncated, "next_start_after": next_start_after, "source": "pxgroup",
    })))
}

pub(crate) async fn detail(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let chunk_id = parse_id(&id)?;
    let kv = state.kv_client().await;
    let client = ChunkdbClient::new(
        ServiceRegistryClient::from_shared(kv.clone()),
        Arc::clone(&state.chunk_rpc_transport),
    )
    .with_range_binding(RangeBindingClient::from_shared(kv));
    let result = client
        .query_chunk(QueryChunkRequest {
            chunk_id: Some(chunk_id),
        })
        .await
        .map_err(|error| err_502(error.to_string()))?;
    let chunk = result.chunk.ok_or_else(|| err_404("Chunk not found"))?;
    let chunk_observed_at_ms = observed_at();
    let (placements, placement_error) = match placement_snapshot(&state).await {
        Ok(placements) => (placements, None),
        Err(error) => (Vec::new(), Some(error)),
    };
    Ok(Json(
        json!({"chunk":chunk_value(&chunk),"layout_validity_ms":result.layout_validity_ms,
        "observed_at_ms":chunk_observed_at_ms,"placements":placements,"placement_error":placement_error,
        "placement_observed_at_ms":observed_at()}),
    ))
}

async fn placement_snapshot(state: &AppState) -> Result<Vec<Value>, String> {
    let ctx = state.op_context().await.map_err(|error| error.to_string())?;
    let nodes = hardware::list_nodes_from_group0(&ctx, None)
        .await
        .map_err(|error| error.to_string())?;
    let mut placements = Vec::new();
    for node in nodes {
        for group in hardware::list_disk_groups_from_group0(&ctx, node.id)
            .await
            .map_err(|error| error.to_string())?
        {
            for disk in hardware::list_disks_from_group0(&ctx, node.id, group.id)
                .await
                .map_err(|error| error.to_string())?
            {
                placements.push(json!({"disk_id":disk.disk_id,"rack_id":node.rack_id.to_string(),
                    "node_id":node.id.to_string(),"disk_group_id":group.id.to_string(),
                    "unit_size":disk.unit_size_bytes}));
            }
        }
    }
    Ok(placements)
}
