// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resolve one catalog assignment to its configured management endpoint.

use std::time::Duration;

use axum::{
    extract::{Query, State},
    Json,
};
use crowdb_protocol::{
    chunk_kv::ChunkKvRangeCatalogHead,
    key::{ChunkKvRangeCatalogHeadKey, TextKey},
};
use serde::Deserialize;
use serde_json::Value;

use super::{observe, read, CatalogQuery, Failure};
use crate::{
    error::{err_400, err_404, err_409, err_502},
    state::AppState,
};

mod discovery;

#[derive(Deserialize)]
pub(crate) struct RuntimeQuery {
    page: usize,
    offset: usize,
    generation: u64,
    id: String,
    epoch: u64,
    stream_generation: Option<u64>,
    #[serde(default)]
    stream_offset: usize,
}

pub(crate) async fn runtime(
    State(state): State<AppState>,
    Query(query): Query<RuntimeQuery>,
) -> Result<Json<Value>, Failure> {
    if query.id.len() != 32 || !query.id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(err_400("Invalid partition identity"));
    }
    if query.stream_offset > 0 && query.stream_generation.is_none() {
        return Err(err_400("Extent continuation requires a stream generation"));
    }
    tokio::time::timeout(Duration::from_secs(5), inspect(&state, &query))
        .await
        .map_err(|_| err_502("Partition observation timed out"))?
}

async fn inspect(state: &AppState, query: &RuntimeQuery) -> Result<Json<Value>, Failure> {
    let Json(page) = observe(
        state,
        &CatalogQuery {
            page: query.page,
            offset: query.offset,
            generation: Some(query.generation),
        },
    )
    .await?;
    let entry = page["entries"]
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["id"] == query.id))
        .ok_or_else(|| err_404("Partition is outside the selected catalog window"))?;
    if integer(&entry["epoch"]) != Some(query.epoch) {
        return Err(err_409("Partition ownership changed; refresh the catalog"));
    }
    let endpoint = entry["endpoint"]
        .as_str()
        .ok_or_else(|| err_502("Missing owner endpoint"))?;
    let instance_id = integer(&entry["owner_id"]).ok_or_else(|| err_502("Invalid owner identity"))?;
    let origin = discovery::origin(state, instance_id, endpoint).await?;
    let mut url = reqwest::Url::parse(&origin).map_err(|_| err_502("Invalid owner management endpoint"))?;
    if !matches!(url.scheme(), "http" | "https") || !url.username().is_empty() || url.password().is_some() {
        return Err(err_502("Owner management endpoint must be an HTTP origin"));
    }
    url.set_path(&format!("/partitions/{}/observation", query.id));
    url.set_query(None);
    url.set_fragment(None);
    url.query_pairs_mut()
        .append_pair("generation", &query.generation.to_string())
        .append_pair("epoch", &query.epoch.to_string());
    if let Some(generation) = query.stream_generation {
        url.query_pairs_mut()
            .append_pair("stream_generation", &generation.to_string());
    }
    url.query_pairs_mut()
        .append_pair("stream_offset", &query.stream_offset.to_string());
    let observation = request(url).await?;
    validate_identity(&observation, entry, query)?;
    if query
        .stream_generation
        .is_some_and(|generation| integer(&observation["journal"]["generation"]) != Some(generation))
        || (query.stream_offset > 0
            && observation["journal"]["offset"].as_u64() != Some(query.stream_offset as u64))
    {
        return Err(err_409("Stream manifest changed during observation"));
    }
    let kv = state.kv_client().await;
    let current: ChunkKvRangeCatalogHead =
        read(&kv, &ChunkKvRangeCatalogHeadKey.to_path(), 1024 * 1024).await?;
    if current.generation != query.generation {
        return Err(err_409("Catalog changed during runtime observation"));
    }
    Ok(Json(observation))
}

async fn request(url: reqwest::Url) -> Result<Value, Failure> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(3))
        .build()
        .map_err(|error| err_502(error.to_string()))?;
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|error| err_502(error.to_string()))?;
    if response.status() == reqwest::StatusCode::CONFLICT {
        return Err(err_409(
            "Owner has a different catalog or writer; refresh the catalog",
        ));
    }
    if !response.status().is_success() {
        return Err(err_502(format!(
            "Owner observation unavailable (HTTP {})",
            response.status()
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| err_502(error.to_string()))?
    {
        if body.len() + chunk.len() > 64 * 1024 {
            return Err(err_502("Owner observation exceeds 64 KiB"));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|error| err_502(format!("Invalid owner observation: {error}")))
}

fn validate_identity(observation: &Value, entry: &Value, query: &RuntimeQuery) -> Result<(), Failure> {
    let stream = &entry["artifact"]["stream_name"];
    let word = |name| stream[name].as_str().and_then(|value| value.parse::<u64>().ok());
    let stream_id = word("high")
        .zip(word("low"))
        .map(|(high, low)| format!("{high:016x}{low:016x}"));
    if observation["partition_id"] != query.id
        || integer(&observation["catalog_generation"]) != Some(query.generation)
        || integer(&observation["owner_epoch"]) != Some(query.epoch)
        || observation["instance_id"] != entry["owner_id"]
        || observation["tree_id"] != entry["artifact"]["tree_id"]
        || observation["stream_id"].as_str() != stream_id.as_deref()
    {
        return Err(err_409("Owner observation does not match the selected partition"));
    }
    Ok(())
}

fn integer(value: &Value) -> Option<u64> {
    value.as_str()?.parse().ok()
}
