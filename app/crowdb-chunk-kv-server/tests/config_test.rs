// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_server::{ChunkKvServerConfig, ConfigError};

#[test]
fn defaults_close_the_documented_timing_contract() {
    let mut config = ChunkKvServerConfig {
        instance_id: 1,
        ..ChunkKvServerConfig::default()
    };
    config.validate().unwrap();
    assert_eq!(config.monitor.heartbeat_interval_ms, 2_000);
    assert_eq!(config.monitor.suspect_after_ms, 6_000);
    assert_eq!(config.monitor.dead_after_ms, 10_000);
    assert_eq!(config.monitor.lease_duration_ms, 12_000);
    assert_eq!(config.balance.target_partitions_per_owner, 4);
    assert_eq!(config.balance.minimum_weighted_improvement_percent, 25);
    assert_eq!(config.balance.cooldown_ms, 600_000);
    assert_eq!(config.storage.metadata_store_id, 1);
    assert_eq!(config.storage.stream_writer_lease_ms, 30_000);
    assert_eq!(config.storage.diskio_connections_per_endpoint, 1);
    assert_eq!(config.storage.diskio_rpc_workers, 2);

    config.monitor.self_fence_margin_ms = 3_000;
    assert!(matches!(config.validate(), Err(ConfigError::Invalid(_))));
}

#[test]
fn invalid_identity_address_and_capacity_fail_closed() {
    let mut config = ChunkKvServerConfig::default();
    assert_eq!(
        config.validate(),
        Err(ConfigError::Invalid("instance_id must be nonzero".into()))
    );
    config.instance_id = 1;
    config.rpc_listen_addr = "not-an-address".into();
    assert!(config.validate().is_err());
    config.rpc_listen_addr = "127.0.0.1:15200".into();
    config.max_hosted_partitions = 0;
    assert!(config.validate().is_err());
    config.max_hosted_partitions = 1;
    config.storage.metadata_store_id = 0;
    assert!(config.validate().is_err());
}
