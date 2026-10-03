// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::{compute_sub_range_assignment, DEFAULT_SUB_RANGE_COUNT};
use crowdb_protocol::common::InstanceValue;

#[tokio::test]
async fn legacy_partition_writer_cannot_publish_assignments() {
    use crowdb_kv_client::{BindingStrategy, ChunkdbRangeStrategy, ClientConfig, CrowdbKvClient};
    let client = CrowdbKvClient::new(ClientConfig::new(Vec::new()));
    let strategy = ChunkdbRangeStrategy::new();
    let error = strategy.write_bindings(&client, &[]).await.unwrap_err();
    assert!(error.to_string().contains("legacy range assignment is disabled"));
}

#[test]
fn twelve_partitions_cover_the_hash_space_with_balanced_widths() {
    assert_eq!(DEFAULT_SUB_RANGE_COUNT, 12);
    let instance = InstanceValue {
        instance_id: 7,
        rpc_endpoint: "127.0.0.1:17007".into(),
        last_heartbeat_ms: u64::MAX,
        extra: None,
    };
    let bindings = compute_sub_range_assignment(&[(7, instance)], DEFAULT_SUB_RANGE_COUNT);
    assert_eq!(bindings.len(), 12);
    assert_eq!(bindings[0].range_start, 0);
    assert_eq!(bindings[11].range_end, u32::from(u16::MAX));
    for (index, binding) in bindings.iter().enumerate() {
        assert_eq!(binding.instance_id, 7);
        assert!((5461..=5462).contains(&(binding.range_end - binding.range_start + 1)));
        if index > 0 {
            assert_eq!(bindings[index - 1].range_end + 1, binding.range_start);
        }
    }
}
