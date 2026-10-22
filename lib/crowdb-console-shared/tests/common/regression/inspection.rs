// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::TestResult;
use crowdb_console_shared::ops::OpContext;
use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::{
    chunk_kv::{ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage},
    chunk_stream::{StreamBinding, StreamManifest, StreamName},
    key::{
        BinaryKey, ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, StreamBindingKey,
        StreamManifestHeadKey, TextKey,
    },
};
use serde_json::{json, Value};
use std::collections::BTreeSet;

async fn get(context: &OpContext, group: u64, key: &[u8]) -> TestResult<Option<Vec<u8>>> {
    match context
        .kv()
        .get(0, group, key, ReadMode::Linearizable, None)
        .await?
    {
        GetOutcome::Found { value, .. } => Ok(Some(value.to_vec())),
        GetOutcome::NotFound => Ok(None),
    }
}

async fn records(context: &OpContext) -> TestResult<Vec<Value>> {
    let outcome = crowdb_console_shared::ops::kv_data::scan(context, 0, 0, b"/chunk-kv/", &[], 256).await?;
    Ok(outcome
        .items
        .into_iter()
        .map(|(key, value)| {
            json!({
                "key": String::from_utf8_lossy(&key),
                "value": serde_json::from_slice::<Value>(&value)
                    .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&value).into_owned()))
            })
        })
        .collect())
}

pub async fn read(context: &OpContext) -> TestResult<Value> {
    let Some(bytes) = get(context, 0, ChunkKvRangeCatalogHeadKey.to_path().as_bytes()).await? else {
        return Ok(json!({"catalog_head": null}));
    };
    let head: ChunkKvRangeCatalogHead = serde_json::from_slice(&bytes)?;
    if head.pages.len() > 128 {
        return Err("inspection catalog exceeds its page bound".into());
    }
    let mut pages = Vec::new();
    let mut streams = BTreeSet::new();
    for page in &head.pages {
        let key = ChunkKvRangeCatalogPageKey {
            generation: page.page_generation,
            page_index: page.page_index,
        };
        let bytes = get(context, 0, key.to_path().as_bytes())
            .await?
            .ok_or("catalog page is absent")?;
        let page: ChunkKvRangeCatalogPage = serde_json::from_slice(&bytes)?;
        for entry in &page.entries {
            streams.insert((entry.artifact.stream_name.high, entry.artifact.stream_name.low));
            if let Some(overlay) = &entry.artifact.tail_overlay {
                streams.insert((overlay.source_stream_name.high, overlay.source_stream_name.low));
            }
        }
        if streams.len() > 256 {
            return Err("inspection stream count exceeds its bound".into());
        }
        pages.push(page);
    }
    let mut observations = Vec::new();
    for (high, low) in streams {
        let name = StreamName { high, low };
        let binding = get(
            context,
            0,
            StreamBindingKey { stream_name: name }.to_path().as_bytes(),
        )
        .await?;
        let binding: Option<StreamBinding> =
            binding.map(|bytes| serde_json::from_slice(&bytes)).transpose()?;
        let manifest = if let Some(binding) = &binding {
            get(
                context,
                binding.metadata_group_id,
                &StreamManifestHeadKey { stream_name: name }.to_bytes(),
            )
            .await?
            .map(|bytes| bincode::deserialize::<StreamManifest>(&bytes))
            .transpose()?
        } else {
            None
        };
        observations.push(json!({"stream": name, "binding": binding, "manifest": manifest}));
    }
    Ok(
        json!({"catalog_head": head, "pages": pages, "streams": observations,
        "records": records(context).await?}),
    )
}
