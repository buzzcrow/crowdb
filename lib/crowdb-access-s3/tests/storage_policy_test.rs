// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::storage::{S3LargeWriteSettings, S3WriteSettings};
use crowdb_chunk_client::SmallWritePolicy;
use crowdb_protocol::chunkdb::rpc::ChunkType;

#[test]
fn s3_policies_own_both_chunk_types_and_independent_limits() {
    let policies = S3WriteSettings {
        small: SmallWritePolicy::default(),
        threshold_ratio: 0.9,
        disk_block_bytes: 1024 * 1024,
        ec_data: 4,
        ec_code: 2,
        large: S3LargeWriteSettings {
            max_chunk_size: Some(64 * 1024 * 1024),
            prefetch_strips_per_chunk: Some(3),
            memory_budget_bytes: Some(32 * 1024 * 1024),
            ..S3LargeWriteSettings::default()
        },
    }
    .policies()
    .unwrap();

    assert_eq!(policies.small.chunk_type, ChunkType::S3);
    assert_eq!(policies.small.conversion_data_num, 4);
    assert_eq!(policies.small_threshold, 3_774_874);
    assert_eq!(policies.large.ec_scheme.data_num, 4);
    assert_eq!(policies.large.client.chunk_type, ChunkType::S3);
    assert_eq!(policies.large.client.max_chunk_size, 64 * 1024 * 1024);
    assert_eq!(policies.large.client.prefetch_strips_per_chunk, 3);
}

#[test]
fn s3_policy_rejects_invalid_large_capacity() {
    let result = S3WriteSettings {
        small: SmallWritePolicy::default(),
        threshold_ratio: 0.9,
        disk_block_bytes: 1024 * 1024,
        ec_data: 8,
        ec_code: 4,
        large: S3LargeWriteSettings {
            max_chunk_size: Some(0),
            ..S3LargeWriteSettings::default()
        },
    }
    .policies();
    assert!(result.is_err());
}
