// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use crowdb_access_iceberg::storage::{self as iceberg_storage, IcebergLargeWriteSettings};
use crowdb_access_s3::storage::{S3LargeWriteSettings, S3StorageClients, S3WritePolicies, S3WriteSettings};
use crowdb_chunk_client::{
    ChunkIoClient, ChunkIoWriter, ChunkReadPolicy, LargeWritePolicy, SmallWritePolicy,
};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_console_shared::{config::ServiceType, ops::s3};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{ChunkType, Location, QueryChunkRequest, Strip};
use crowdb_test_harness::test_dirs::TestDir;
use hyper::body::Bytes;

const MIB: usize = 1024 * 1024;

struct StopClusterOnDrop<'a>(&'a Path);

impl Drop for StopClusterOnDrop<'_> {
    fn drop(&mut self) {
        let _ = s3::stop(self.0);
    }
}

async fn write_small(client: &ChunkIoClient, payload: Bytes) -> Location {
    let mut writer = client.prepare_small_write(payload.len()).await.unwrap();
    writer.on_data(payload).await.unwrap();
    writer.on_finish().await.unwrap().remove(0)
}

fn production_policies() -> (S3WritePolicies, LargeWritePolicy, SmallWritePolicy) {
    let s3_small = SmallWritePolicy {
        conversion_enabled: false,
        mirror_copies: 2,
        memory_budget: 64 * MIB,
        ..SmallWritePolicy::default()
    };
    let s3 = S3WriteSettings {
        small: s3_small,
        threshold_ratio: 0.5,
        disk_block_bytes: MIB,
        ec_data: 2,
        ec_code: 1,
        large: S3LargeWriteSettings {
            max_chunk_size: Some(8 * MIB as u64),
            memory_budget_bytes: Some(64 * MIB),
            prefetch_strips_per_chunk: Some(2),
            ..S3LargeWriteSettings::default()
        },
    }
    .policies()
    .unwrap();
    let iceberg_large = IcebergLargeWriteSettings {
        ec_data: 4,
        ec_code: 2,
        disk_block_bytes: MIB,
        mirror_copies: None,
        max_chunk_size: Some(16 * MIB as u64),
        memory_budget_bytes: Some(96 * MIB),
        prefetch_strips_per_chunk: Some(3),
        chunk_preparation_depth: Some(1),
    }
    .policy()
    .unwrap();
    let iceberg_small = SmallWritePolicy {
        conversion_enabled: false,
        mirror_copies: 2,
        memory_budget: 96 * MIB,
        ..SmallWritePolicy::default()
    };
    (s3, iceberg_large, iceberg_small)
}

async fn assert_chunk_layouts(
    seeds: Vec<String>,
    s3_small: Location,
    iceberg_small: Location,
    s3_large: Location,
    iceberg_large: Location,
) {
    let registry = ServiceRegistryClient::new(CrowdbKvClient::new(ClientConfig::new(seeds)));
    let chunkdb = ChunkdbClient::new(registry, Arc::new(ChunkdbRpcTransport::new()));
    for (location, expected_type, expected_ec) in [
        (s3_small, ChunkType::S3, None),
        (iceberg_small, ChunkType::IcebergTable, None),
        (s3_large, ChunkType::S3, Some((2, 1))),
        (iceberg_large, ChunkType::IcebergTable, Some((4, 2))),
    ] {
        let chunk = chunkdb
            .query_chunk(QueryChunkRequest {
                chunk_id: location.chunk_id,
            })
            .await
            .unwrap()
            .chunk
            .unwrap();
        assert_eq!(chunk.chunk_type, expected_type as i32);
        assert_eq!(chunk.id.unwrap().high >> 56, expected_type as u64);
        let strip = &chunk.strips[0];
        match (strip.strip.as_ref().unwrap(), expected_ec) {
            (Strip::MirrorStrip(mirror), None) => assert_eq!(mirror.segments.len(), 2),
            (Strip::EcStrip(ec), Some((data, code))) => {
                assert_eq!((ec.data_num, ec.code_num), (data, code));
            }
            _ => panic!("protocol chunk used a different protection policy"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "starts a complete simulated three-rack production storage stack"]
async fn s3_and_iceberg_keep_distinct_policies_on_protected_storage() {
    let dir = TestDir::new("access-production-protocol-policy").unwrap();
    s3::start_protected_test_cluster(dir.path()).await.unwrap();
    let _cleanup = StopClusterOnDrop(dir.path());
    let (config, _) = s3::load(dir.path()).unwrap();
    let seeds = config
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::Kv)
        .map(|server| server.url.clone())
        .collect::<Vec<_>>();

    let (s3_policies, iceberg_large, iceberg_small) = production_policies();
    assert_ne!(s3_policies.large.ec_scheme, iceberg_large.ec_scheme);
    assert_ne!(
        s3_policies.large.client.prefetch_strips_per_chunk,
        iceberg_large.client.prefetch_strips_per_chunk
    );
    assert_ne!(
        s3_policies.large.client.memory_budget,
        iceberg_large.client.memory_budget
    );

    let s3_storage = S3StorageClients::connect_with_read_policy(
        seeds.clone(),
        2,
        1,
        s3_policies.small,
        ChunkReadPolicy::default(),
    )
    .await
    .unwrap();
    let (_, _, iceberg_chunks) =
        iceberg_storage::connect(seeds.clone(), ChunkReadPolicy::default(), iceberg_small, 2, 1)
            .await
            .unwrap();
    let s3_chunks = Arc::clone(&s3_storage.chunks);
    let (s3_small, iceberg_small) = tokio::join!(
        write_small(&s3_chunks, Bytes::from_static(b"s3-small")),
        write_small(&iceberg_chunks, Bytes::from_static(b"iceberg-small"))
    );
    let s3_data = vec![0x31; 2 * MIB];
    let iceberg_data = vec![0x42; 4 * MIB];
    let (s3_large, iceberg_large_result) = tokio::join!(
        s3_chunks
            .prepare_large_write(Some(s3_data.len() as u64), s3_policies.large)
            .write_stream(s3_data.as_slice()),
        iceberg_chunks
            .prepare_large_write(Some(iceberg_data.len() as u64), iceberg_large)
            .write_stream(iceberg_data.as_slice())
    );
    let s3_large = s3_large.unwrap();
    let iceberg_large_result = iceberg_large_result.unwrap();
    assert_eq!(
        s3_chunks
            .read_object(std::slice::from_ref(&s3_small))
            .await
            .unwrap()
            .concat(),
        b"s3-small"
    );
    assert_eq!(
        iceberg_chunks
            .read_object(std::slice::from_ref(&iceberg_small))
            .await
            .unwrap()
            .concat(),
        b"iceberg-small"
    );
    assert_eq!(
        s3_chunks.read_object(&s3_large.locations).await.unwrap().concat(),
        s3_data
    );
    assert_eq!(
        iceberg_chunks
            .read_object(&iceberg_large_result.locations)
            .await
            .unwrap()
            .concat(),
        iceberg_data
    );

    assert_chunk_layouts(
        seeds,
        s3_small,
        iceberg_small,
        s3_large.locations[0].clone(),
        iceberg_large_result.locations[0].clone(),
    )
    .await;
    s3_chunks.shutdown_small_writes().await.unwrap();
    iceberg_chunks.shutdown_small_writes().await.unwrap();
    s3::delete(dir.path()).unwrap();
}
