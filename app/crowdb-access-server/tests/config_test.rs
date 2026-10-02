// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::Path;

use crowdb_access_server::config::{load_args, AccessConfig, SmallWriteConfig};
use crowdb_common::config::{load_from_file, BaseConfig};

#[test]
fn tracked_access_configs_load_and_set_bounded_read_resources() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let canonical: AccessConfig =
        load_from_file(&root.join("conf/crowdb_access_server_config.toml")).unwrap();
    let container: AccessConfig =
        load_from_file(&root.join("../../container/single-node-container/templates/access.toml")).unwrap();
    for (config, expected_ec, expected_threshold, expected_budget) in
        [(canonical, (8, 4), 7_549_748, 1), (container, (2, 1), 943_719, 0)]
    {
        assert_eq!(config.deployment.max_node_failures, expected_budget);
        assert_eq!(config.read.stream_slots, 3);
        assert_eq!(config.read.stream_window_bytes, 1024 * 1024);
        assert_eq!(config.read.global_stream_bytes, 256 * 1024 * 1024);
        assert_eq!(config.read.recovery_memory_bytes, 256 * 1024 * 1024);
        assert_eq!(config.read.policy().stream_slots, 3);
        assert_eq!(config.small_write.memory_budget_bytes, 1280 * 1024 * 1024);
        assert_eq!(config.small_write.disk_block_bytes, 1024 * 1024);
        assert_eq!(
            (config.small_write.ec_data, config.small_write.ec_code),
            expected_ec
        );
        assert_eq!(
            (config.s3.ec_data, config.s3.ec_code),
            (Some(expected_ec.0), Some(expected_ec.1))
        );
        assert_eq!(config.small_write.threshold_exclusive(), expected_threshold);
        assert_eq!(config.small_write.policy().max_pipelines, 32);
        assert!(config.s3.listen.is_some());
        assert!(config.iceberg.listen.is_some());
        assert_eq!(config.iceberg.native_budget_bytes, Some(256 * 1024 * 1024));
        assert_eq!(config.s3.list_scan_bytes, Some(4 * 1024 * 1024));
        assert_eq!(config.iceberg.gc.kv_bytes, Some(64 * 1024 * 1024));
    }
}

#[test]
fn deployment_rejects_mismatched_node_failure_budget() {
    let mut config = AccessConfig::default();
    config.deployment.max_node_failures = 0;
    assert_eq!(
        config.validate(),
        Err("production requires max_node_failures = 1".to_string())
    );
}

#[test]
fn config_argument_is_removed_from_service_commands() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = root.join("conf/crowdb_access_server_config.toml");
    let (_, remaining) = load_args(vec![
        "serve".into(),
        "--config".into(),
        path.display().to_string(),
    ])
    .unwrap();
    assert_eq!(remaining, ["serve"]);
    assert!(load_args(vec!["--config".into()]).is_err());
}

#[test]
fn invalid_read_budget_is_rejected() {
    let mut config = AccessConfig::default();
    config.read.stream_slots = 0;
    assert!(config.validate().is_err());
    config.read.stream_slots = 3;
    config.small_write.memory_budget_bytes = 1;
    assert!(config.validate().is_err());
}

#[test]
fn small_threshold_uses_strip_data_capacity_for_ec_and_mirror() {
    let mut config = SmallWriteConfig {
        ec_data: 8,
        ec_code: 2,
        ..SmallWriteConfig::default()
    };
    assert_eq!(config.threshold_exclusive(), 7_549_748);
    config.disk_block_bytes = 512 * 1024;
    assert_eq!(config.threshold_exclusive(), 3_774_874);
    config.conversion_enabled = false;
    assert_eq!(config.threshold_exclusive(), 471_860);
    config.disk_block_bytes = 1024 * 1024;
    assert_eq!(config.threshold_exclusive(), 943_719);
}

#[test]
fn unsupported_disk_block_size_is_rejected_before_routing() {
    let mut config = AccessConfig::default();
    config.small_write.disk_block_bytes = 0;
    assert!(config.validate().is_err());
    config.small_write.disk_block_bytes = 2 * 1024 * 1024;
    assert!(config.validate().is_err());
    config.small_write.disk_block_bytes = 3 * 1024;
    assert!(config.validate().is_err());
    config.small_write.disk_block_bytes = 768 * 1024;
    assert!(config.validate().is_err());
}

#[test]
fn protocol_small_write_overrides_are_independent() {
    let mut config = AccessConfig::default();
    config.s3.small_write = Some(SmallWriteConfig {
        ec_data: 4,
        ec_code: 2,
        ..SmallWriteConfig::default()
    });
    config.iceberg.small_write = Some(SmallWriteConfig {
        ec_data: 2,
        ec_code: 1,
        ..SmallWriteConfig::default()
    });
    assert!(config.validate().is_ok());
    assert_eq!(config.s3_small_write().ec_data, 4);
    assert_eq!(config.iceberg_small_write().ec_data, 2);
    config.s3.small_write.as_mut().unwrap().chunk_capacity_bytes = 32 * 1024 * 1024;
    assert_eq!(config.s3_small_write().policy().chunk_capacity, 32 * 1024 * 1024);
    assert_eq!(
        config.iceberg_small_write().policy().chunk_capacity,
        256 * 1024 * 1024
    );
    assert_eq!(
        config.iceberg_small_write().policy().small_strip_prefetch_count,
        32
    );
    assert_eq!(config.small_write.ec_data, 8);
}
