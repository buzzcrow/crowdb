// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::chunkdb::rpc::{AllocateChunkRequest, ChunkType, Strip, StripType};
use crowdb_web::AppState;
use serde_json::{json, Value};
use std::sync::Arc;

pub(super) async fn seed(state: &AppState) -> Value {
    let client = ChunkdbClient::new(
        ServiceRegistryClient::from_shared(state.kv_client().await),
        Arc::new(ChunkdbRpcTransport::new()),
    );
    let mut ids = Vec::new();
    for index in 0..21 {
        let ec = index == 1;
        let chunk = client
            .allocate_chunk(AllocateChunkRequest {
                write_granularity: 1024,
                strip_count: if index == 0 {
                    17
                } else if ec {
                    3
                } else {
                    1
                },
                strip_type: if ec { StripType::Ec } else { StripType::Mirror }.into(),
                data_num: if ec { 8 } else { 0 },
                code_num: if ec { 4 } else { 0 },
                copy_count: if ec { 0 } else { 3 },
                chunk_type: ChunkType::S3.into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .chunk
            .unwrap();
        for strip in &chunk.strips {
            let count = match strip.strip.as_ref().unwrap() {
                Strip::MirrorStrip(mirror) => mirror.segments.len(),
                Strip::EcStrip(ec) => ec.segments.len(),
            };
            assert_eq!(count, if ec { 12 } else { 3 });
        }
        let id = chunk.id.unwrap();
        ids.push(format!("{:016x}{:016x}", id.high, id.low));
    }
    json!({"mirror":ids[0], "ec":ids[1], "ids":ids})
}
