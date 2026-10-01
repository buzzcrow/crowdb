// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::storage::IcebergLargeWriteSettings;
use crowdb_protocol::chunkdb::rpc::ChunkType;

#[test]
fn iceberg_large_policy_owns_type_and_capacity() {
    let policy = IcebergLargeWriteSettings {
        ec_data: 2,
        ec_code: 1,
        disk_block_bytes: 1024 * 1024,
        mirror_copies: Some(1),
        max_chunk_size: Some(32 * 1024 * 1024),
        memory_budget_bytes: Some(16 * 1024 * 1024),
        prefetch_strips_per_chunk: Some(2),
        prefetch_max_strips_per_batch: Some(20),
        parallel_strip_writes: Some(4),
        held_buffers: Some(4),
        chunk_preparation_depth: Some(2),
    }
    .policy()
    .unwrap();

    assert_eq!(policy.ec_scheme.data_num, 2);
    assert_eq!(policy.client.chunk_type, ChunkType::IcebergTable);
    assert_eq!(policy.client.large_mirror_copies, Some(1));
    assert_eq!(policy.client.max_chunk_size, 32 * 1024 * 1024);
    assert_eq!(policy.client.prefetch_strips_per_chunk, 2);
    assert_eq!(policy.client.large_prefetch_max_strips_per_batch, 20);
}

#[test]
fn iceberg_large_policy_rejects_zero_prefetch() {
    let result = IcebergLargeWriteSettings {
        ec_data: 2,
        ec_code: 1,
        disk_block_bytes: 1024 * 1024,
        mirror_copies: None,
        max_chunk_size: None,
        memory_budget_bytes: None,
        prefetch_strips_per_chunk: Some(0),
        prefetch_max_strips_per_batch: None,
        parallel_strip_writes: None,
        held_buffers: None,
        chunk_preparation_depth: None,
    }
    .policy();
    assert!(result.is_err());
}
