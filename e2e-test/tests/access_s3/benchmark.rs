// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoClient, ChunkIoClientConfig, LargeWritePolicy, SmallWritePolicy,
};
use crowdb_common::ec::EcScheme;
use serde_json::json;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires owned release S3 storage containers"]
async fn direct_chunk_path() {
    let ips = crate::common::management::addresses(14);
    let seeds: Vec<_> = ips[..3]
        .iter()
        .map(|ip| crate::common::management::origin(ip))
        .collect();
    let artifacts = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    run_direct_chunk_benchmark(&seeds, &artifacts).await;
}

async fn run_direct_chunk_benchmark(seeds: &[String], artifacts_dir: &Path) {
    let chunks = Arc::new(
        ChunkIoClient::connect(ChunkIoClientConfig {
            management_seeds: seeds.to_vec(),
            diskio_connections_per_endpoint: 2,
            diskio_rpc_workers: 1,
            small_write: SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3),
        })
        .await
        .expect("connect benchmark chunk client"),
    );
    let policy = LargeWritePolicy {
        ec_scheme: EcScheme::new(2, 1),
        client: Arc::new(ChunkClientConfig {
            max_chunk_size: 4 * 1024 * 1024,
            ..ChunkClientConfig::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
        }),
    };
    let mut samples = Vec::new();
    for size in [65_536_usize, 1_048_576] {
        for concurrency in [1_usize, 2] {
            let mut tasks = Vec::new();
            for index in 0..concurrency {
                let chunks = Arc::clone(&chunks);
                let policy = policy.clone();
                tasks.push(tokio::spawn(async move {
                    let payload = Bytes::from(
                        (0..size)
                            .map(|offset| u8::try_from(offset % 256).expect("byte value is bounded"))
                            .collect::<Vec<_>>(),
                    );
                    let started = Instant::now();
                    let result = chunks
                        .prepare_large_write(Some(size as u64), policy)
                        .write_buffers([payload.clone()])
                        .await
                        .expect("direct chunk write");
                    let put_ns = started.elapsed().as_nanos();
                    let started = Instant::now();
                    let read = chunks
                        .read_object(&result.locations)
                        .await
                        .expect("direct chunk read");
                    let get_ns = started.elapsed().as_nanos();
                    assert_eq!(read.concat(), payload);
                    let started = Instant::now();
                    let range = chunks
                        .read_range(&result.locations, 0, 4096)
                        .await
                        .expect("direct chunk range read");
                    let range_ns = started.elapsed().as_nanos();
                    assert_eq!(range.concat(), payload.slice(..4096));
                    json!({
                        "size": size,
                        "concurrency": concurrency,
                        "index": index,
                        "put_ns": put_ns,
                        "get_ns": get_ns,
                        "range_ns": range_ns,
                        "logical_bytes": result.logical_bytes,
                        "physical_bytes": result.physical_bytes,
                        "assembly_copies": result.assembly_copies,
                    })
                }));
            }
            for task in tasks {
                samples.push(task.await.expect("direct chunk benchmark task"));
            }
        }
    }
    let artifact = artifacts_dir.join("direct-chunk-path.json");
    std::fs::write(
        &artifact,
        serde_json::to_vec_pretty(&json!({
            "scope": "chunk-client only; excludes HTTP, SigV4, namespace metadata, and client network",
            "ec_data": 2,
            "ec_code": 1,
            "samples": samples,
        }))
        .expect("serialize direct chunk benchmark"),
    )
    .expect("write direct chunk baseline samples");
    eprintln!("direct chunk benchmark samples: {}", artifact.display());
}
