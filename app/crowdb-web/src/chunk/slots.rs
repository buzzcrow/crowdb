// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded owner-specific windows over validated fixed slot maps.

use axum::extract::{Query, State};
use crowdb_kv_client::ChunkSlotMapClient;
use crowdb_protocol::chunk_slot::{ChunkSlotBitmap, ChunkStorageGroup, CHUNK_SLOT_COUNT};
use serde::Deserialize;
use serde_json::json;

use super::Response;
use crate::{
    error::{err_400, err_409, err_502},
    state::AppState,
};

#[derive(Deserialize)]
pub(crate) struct SlotQuery {
    layer: String,
    instance_id: Option<u64>,
    store_id: Option<u64>,
    group_id: Option<u64>,
    after: Option<u16>,
    generation: Option<u64>,
    limit: Option<usize>,
}

pub(crate) async fn list(State(state): State<AppState>, Query(query): Query<SlotQuery>) -> Response {
    let limit = query.limit.unwrap_or(32);
    if !(1..=100).contains(&limit) || query.after.is_some_and(|after| after >= CHUNK_SLOT_COUNT) {
        return Err(err_400(
            "Slot limit must be 1–100 and after must be within 0–1023",
        ));
    }
    if !matches!(query.layer.as_str(), "service" | "storage") {
        return Err(err_400("Slot layer must be service or storage"));
    }
    state
        .op_context()
        .await
        .map_err(|error| err_502(error.to_string()))?;
    let client = ChunkSlotMapClient::new(state.kv_client().await);
    let (generation, slots) = if query.layer == "service" {
        let owner = query
            .instance_id
            .filter(|id| *id != 0)
            .ok_or_else(|| err_400("instance_id is required"))?;
        let map = client
            .read_service()
            .await
            .map_err(|error| err_502(error.to_string()))?;
        (
            map.head().generation,
            map.bindings()
                .iter()
                .find(|binding| binding.owner == owner)
                .map(|binding| binding.slots.clone()),
        )
    } else {
        let owner = ChunkStorageGroup {
            store_id: query.store_id.ok_or_else(|| err_400("store_id is required"))?,
            group_id: query
                .group_id
                .filter(|id| *id != 0)
                .ok_or_else(|| err_400("An ordinary group_id is required"))?,
        };
        let map = client
            .read_storage()
            .await
            .map_err(|error| err_502(error.to_string()))?;
        (
            map.head().generation,
            map.bindings()
                .iter()
                .find(|binding| binding.owner == owner)
                .map(|binding| binding.slots.clone()),
        )
    };
    if query.generation.is_some_and(|expected| expected != generation) {
        return Err(err_409(
            "Slot map generation changed; refresh ownership from the first window",
        ));
    }
    let assigned = slots.is_some();
    let slots = slots.unwrap_or_else(ChunkSlotBitmap::default);
    let count = slots.slots().count();
    let mut window = slots
        .slots()
        .map(crowdb_protocol::chunk_slot::ChunkSlot::value)
        .filter(|slot| query.after.map_or(true, |after| *slot > after))
        .take(limit + 1)
        .collect::<Vec<_>>();
    let more = window.len() > limit;
    window.truncate(limit);
    Ok(axum::Json(json!({
        "layer": query.layer, "generation": generation.to_string(), "slot_count": CHUNK_SLOT_COUNT,
        "assigned": assigned, "owned_count": count, "slots": window,
        "next": if more { window.last().copied() } else { None },
    })))
}
