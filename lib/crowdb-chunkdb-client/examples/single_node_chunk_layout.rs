// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb_client::ChunkdbRpcTransport;
use crowdb_protocol::chunkdb::rpc::{ChunkType, ListChunksRequest, Strip, StripType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    crowdb_rpc_ffi::init_logging("", "warn", 30, 5, "chunk-layout-check");
    let transport = ChunkdbRpcTransport::new();
    let mut start_token = None;
    let mut seen = [false; 2];
    loop {
        let listed = transport
            .send_list_chunks(
                "127.0.0.1:12200",
                &ListChunksRequest {
                    start_token,
                    max_keys: 256,
                    ..ListChunksRequest::default()
                },
            )
            .await?;
        for chunk in listed.chunks {
            let index = match ChunkType::try_from(chunk.chunk_type) {
                Ok(ChunkType::S3) => 0,
                Ok(ChunkType::IcebergTable) => 1,
                _ => continue,
            };
            seen[index] = true;
            let id = chunk.id.expect("protocol chunk must have an ID");
            assert_eq!(id.high >> 56, u64::try_from(chunk.chunk_type)?);
            assert!(!chunk.strips.is_empty(), "protocol chunk must have a strip");
            for strip in chunk.strips {
                assert_eq!(strip.strip_type, StripType::Mirror as i32);
                assert_eq!(strip.capacity, 1024);
                assert!(!strip.placement_repair_required);
                let Some(Strip::MirrorStrip(mirror)) = strip.strip else {
                    panic!("single-node protocol strip must use mirror I/O");
                };
                assert_eq!(mirror.segments.len(), 1);
            }
        }
        let Some(next_token) = listed.next_token else {
            break;
        };
        start_token = Some(next_token);
    }
    assert!(
        seen.into_iter().all(|found| found),
        "both protocol chunk types must exist"
    );
    println!("single-node S3 and Iceberg chunk layouts verified");
    Ok(())
}
