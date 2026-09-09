// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Benchmark flows through real KV, `DiskDB`, `ChunkDB`, and `NullDisk` services.

#[allow(dead_code)]
#[path = "common/e2e_stack.rs"]
mod e2e_stack;

use std::sync::Arc;

use crowdb_chunk_client::{
    run_read_benchmark, run_small_write_benchmark, ChunkClientConfig, LargeWritePolicy, ReadBenchmarkConfig,
    ReadBenchmarkWorkload, SmallWriteBenchmarkConfig, SmallWritePolicy,
};
use crowdb_common::ec::EcScheme;

use e2e_stack::{all_binaries_available, E2eStack};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

fn small_policy() -> SmallWritePolicy {
    SmallWritePolicy {
        mirror_copies: 1,
        conversion_enabled: false,
        max_pipelines: 2,
        max_batch_bytes: 4 * KIB,
        max_batch_objects: 4,
        scale_out_queue_bytes: 2 * KIB,
        scale_out_queue_objects: 2,
        ..SmallWritePolicy::default()
    }
}

fn large_policy() -> LargeWritePolicy {
    LargeWritePolicy {
        ec_scheme: EcScheme::new(4, 1),
        client: Arc::new(ChunkClientConfig {
            read_buffer_size: MIB,
            max_cached_buffer: 4 * MIB,
            max_chunk_size: 16 * MIB as u64,
            ..ChunkClientConfig::default()
        }),
    }
}

#[tokio::test]
async fn small_write_benchmark_uses_real_metadata_and_null_disk() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start_null(small_policy()).await;
    let result = run_small_write_benchmark(
        stack.client.clone(),
        SmallWriteBenchmarkConfig {
            object_count: 8,
            duration: None,
            object_size: KIB,
            concurrency: 4,
            seed: 9,
        },
    )
    .await;

    assert_eq!(result.objects, 8);
    assert_eq!(result.logical_bytes, 8 * KIB as u64);
    assert_eq!(result.errors, 0, "{:?}", result.error_messages);
    assert_eq!(result.incomplete_objects, 0);
    assert_eq!(result.stop_reason, "complete");
    assert!(result.batches > 0);
    assert_eq!(result.active_pipelines, 0);
}

#[tokio::test]
async fn mixed_read_benchmark_prepares_real_locations_then_reads_null_disk() {
    if !all_binaries_available() {
        return;
    }
    let stack = E2eStack::start_null(small_policy()).await;
    let result = run_read_benchmark(
        stack.client.clone(),
        ReadBenchmarkConfig {
            request_count: 4,
            duration: None,
            dataset_objects: 1,
            concurrency: 2,
            small_object_size: KIB,
            large_object_size: 2 * MIB as u64,
            mixed_large_percent: 50,
            seed: 0,
            workload: ReadBenchmarkWorkload::Mixed,
            large_policy: large_policy(),
        },
    )
    .await;

    assert_eq!(result.reads, 4);
    assert_eq!(result.small_reads, 2);
    assert_eq!(result.large_reads, 2);
    assert_eq!(result.logical_bytes, 2 * KIB as u64 + 4 * MIB as u64);
    assert_eq!(result.errors, 0, "{:?}", result.error_messages);
    assert_eq!(result.incomplete_reads, 0);
    assert_eq!(result.stop_reason, "complete");
    assert!(result.preparation_secs > 0.0);
}
