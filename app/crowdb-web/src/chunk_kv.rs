// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded catalog observations for the Chunk-KV workbench.

use std::time::Duration;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use crowdb_kv_client::{CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage};
use crowdb_protocol::key::{ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, TextKey};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};

use crate::{
    error::{err_400, err_404, err_409, err_502, ErrorBody},
    state::AppState,
};

type Failure = (StatusCode, Json<ErrorBody>);

#[derive(Default, Deserialize)]
pub(crate) struct CatalogQuery {
    #[serde(default)]
    page: usize,
    #[serde(default)]
    offset: usize,
    generation: Option<u64>,
}

pub(crate) async fn catalog(
    State(state): State<AppState>,
    Query(query): Query<CatalogQuery>,
) -> Result<Json<Value>, Failure> {
    if (query.page != 0 || query.offset != 0) && query.generation.is_none() {
        return Err(err_400("Catalog continuation requires generation"));
    }
    tokio::time::timeout(Duration::from_secs(5), observe(&state, &query))
        .await
        .map_err(|_| err_502("Chunk-KV catalog observation timed out"))?
}

async fn read<T: DeserializeOwned>(kv: &CrowdbKvClient, key: &str, max_bytes: usize) -> Result<T, Failure> {
    let value = match kv
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .map_err(|error| err_502(error.to_string()))?
    {
        GetOutcome::Found { value, .. } => value,
        GetOutcome::NotFound => {
            return Err(err_404(
                "Chunk-KV catalog is not initialized or its referenced page is unavailable",
            ))
        }
    };
    if value.len() > max_bytes {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(ErrorBody {
                error: "Catalog record exceeds inspection byte budget".into(),
            }),
        ));
    }
    serde_json::from_slice(&value).map_err(|error| err_502(format!("Invalid catalog record: {error}")))
}

fn validate_head(head: &ChunkKvRangeCatalogHead) -> Result<(), Failure> {
    if head.generation == 0
        || head.pages.is_empty()
        || head.pages.len() > 4096
        || head
            .previous_generation
            .is_some_and(|previous| previous >= head.generation)
        || !head.pages[0].first_key.is_empty()
        || head
            .pages
            .windows(2)
            .any(|pair| pair[0].first_key >= pair[1].first_key)
    {
        return Err(err_502("Invalid or oversized Chunk-KV catalog head"));
    }
    let mut sealed = head.clone();
    sealed.seal().map_err(|error| err_502(error.to_string()))?;
    if sealed.checksum != head.checksum {
        return Err(err_502("Chunk-KV catalog head checksum mismatch"));
    }
    Ok(())
}

fn validate_page(
    head: &ChunkKvRangeCatalogHead,
    page: &ChunkKvRangeCatalogPage,
    index: usize,
) -> Result<(), Failure> {
    page.validate().map_err(|error| err_502(error.to_string()))?;
    let reference = &head.pages[index];
    let end = head.pages.get(index + 1).map(|reference| &reference.first_key);
    if page.generation != reference.page_generation
        || page.generation > head.generation
        || page.page_index != reference.page_index
        || page.checksum != reference.page_checksum
        || page.entries.first().map(|entry| &entry.range.start) != Some(&reference.first_key)
        || page.entries.last().and_then(|entry| entry.range.end.as_ref()) != end
        || page
            .entries
            .windows(2)
            .any(|pair| pair[0].range.end.as_ref() != Some(&pair[1].range.start))
    {
        return Err(err_502(
            "Catalog page does not match its authoritative range fences",
        ));
    }
    Ok(())
}

async fn observe(state: &AppState, query: &CatalogQuery) -> Result<Json<Value>, Failure> {
    let kv = state.kv_client().await;
    let head_key = ChunkKvRangeCatalogHeadKey.to_path();
    let head: ChunkKvRangeCatalogHead = read(&kv, &head_key, 1024 * 1024).await?;
    validate_head(&head)?;
    if query
        .generation
        .is_some_and(|generation| generation != head.generation)
    {
        return Err(err_409("Chunk-KV catalog changed; refresh the range map"));
    }
    let reference = head
        .pages
        .get(query.page)
        .ok_or_else(|| err_400("Catalog page is outside the current head"))?;
    let path = ChunkKvRangeCatalogPageKey {
        generation: reference.page_generation,
        page_index: reference.page_index,
    }
    .to_path();
    let page: ChunkKvRangeCatalogPage = read(&kv, &path, 2 * 1024 * 1024).await?;
    validate_page(&head, &page, query.page)?;
    if query.offset >= page.entries.len() {
        return Err(err_400("Catalog offset is outside the referenced page"));
    }
    let entries: Vec<_> = page
        .entries
        .iter()
        .skip(query.offset)
        .take(100)
        .map(|entry| {
            let mut artifact = json!(entry.artifact);
            exact_integers(&mut artifact);
            json!({
                "id": format!("{:016x}{:016x}", entry.partition_id.high, entry.partition_id.low),
                "start": hex::encode(&entry.range.start), "end": entry.range.end.as_ref().map(hex::encode),
                "owner_id": entry.owner.instance_id.to_string(), "endpoint": entry.owner.rpc_endpoint,
                "epoch": entry.owner_epoch.to_string(), "state": entry.state, "artifact": artifact,
                "transition_id": entry.transition_id.map(|id| format!("{:016x}{:016x}", id.high, id.low)),
            })
        })
        .collect();
    let next = if query.offset + entries.len() < page.entries.len() {
        Some(json!({"page":query.page,"offset":query.offset + entries.len()}))
    } else if query.page + 1 < head.pages.len() {
        Some(json!({"page":query.page + 1,"offset":0}))
    } else {
        None
    };
    let current: ChunkKvRangeCatalogHead = read(&kv, &head_key, 1024 * 1024).await?;
    if current != head {
        return Err(err_409(
            "Chunk-KV catalog changed during observation; refresh the range map",
        ));
    }
    Ok(Json(
        json!({"generation":head.generation.to_string(),"page":query.page,
        "offset":query.offset,"catalog_pages":head.pages.len(),"entries":entries,"next":next,
        "source":"group0","coverage":"referenced page","runtime_available":false}),
    ))
}

fn exact_integers(value: &mut Value) {
    match value {
        Value::Number(number) if number.as_u64().is_some() => *value = Value::String(number.to_string()),
        Value::Array(values) => values.iter_mut().for_each(exact_integers),
        Value::Object(values) => values.values_mut().for_each(exact_integers),
        _ => {}
    }
}
