// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Mirror-to-EC conversion policy and lifecycle integration tests.

use crowdb_chunkdb::chunkdb_config::ChunkdbConfig;
use crowdb_common::config::BaseConfig;

#[test]
fn default_conversion_policy_is_safe_and_valid() {
    let config = ChunkdbConfig::default();
    assert!(!config.conversion.enabled);
    assert_eq!(config.conversion.data_num, 8);
    assert_eq!(config.conversion.code_num, 4);
    assert_eq!(config.conversion.min_mirror_strips, 8);
    assert_eq!(config.conversion.max_concurrency, 4);
    assert!(config.validate().is_ok());
}

#[test]
fn conversion_policy_rejects_invalid_bounds() {
    let mut config = ChunkdbConfig::default();
    config.conversion.data_num = 0;
    assert!(config.validate().unwrap_err().contains("data_num"));

    let mut config = ChunkdbConfig::default();
    config.conversion.min_mirror_strips = 7;
    assert!(config.validate().unwrap_err().contains("min_mirror_strips"));

    let mut config = ChunkdbConfig::default();
    config.conversion.max_concurrency = 0;
    assert!(config.validate().unwrap_err().contains("max_concurrency"));

    let mut config = ChunkdbConfig::default();
    config.conversion.max_bandwidth_mbps = 0;
    assert!(config.validate().unwrap_err().contains("max_bandwidth_mbps"));

    let mut config = ChunkdbConfig::default();
    config.conversion.scan_interval_secs = 0;
    assert!(config.validate().unwrap_err().contains("scan_interval_secs"));

    let mut config = ChunkdbConfig::default();
    config.conversion.task_lease_secs = 0;
    assert!(config.validate().unwrap_err().contains("task_lease_secs"));
}

#[test]
fn repair_policy_has_bounded_defaults_and_rejects_zero_limits() {
    let config = ChunkdbConfig::default();
    assert!(config.repair.enabled);
    assert_eq!(config.repair.max_concurrency, 4);
    assert_eq!(config.repair.memory_bytes, 64 * 1024 * 1024);
    assert_eq!(config.repair.scan_interval_secs, 1);

    let mut config = ChunkdbConfig::default();
    config.repair.max_concurrency = 0;
    assert!(config.validate().unwrap_err().contains("max_concurrency"));

    let mut config = ChunkdbConfig::default();
    config.repair.memory_bytes = 0;
    assert!(config.validate().unwrap_err().contains("memory_bytes"));

    let mut config = ChunkdbConfig::default();
    config.repair.scan_interval_secs = 0;
    assert!(config.validate().unwrap_err().contains("scan_interval_secs"));
}

#[test]
fn reservation_policy_has_cluster_bounds_and_rejects_zero_limits() {
    let config = ChunkdbConfig::default();
    assert_eq!(config.reservation.max_blocks, 1_048_576);
    assert_eq!(config.reservation.max_bytes, 1_u64 << 40);
    assert_eq!(config.reservation.scan_interval_secs, 1);

    let mut config = ChunkdbConfig::default();
    config.reservation.max_blocks = 0;
    assert!(config.validate().unwrap_err().contains("reservation max_blocks"));

    let mut config = ChunkdbConfig::default();
    config.reservation.max_bytes = 0;
    assert!(config.validate().unwrap_err().contains("reservation max_blocks"));

    let mut config = ChunkdbConfig::default();
    config.reservation.scan_interval_secs = 0;
    assert!(config
        .validate()
        .unwrap_err()
        .contains("reservation.scan_interval_secs"));
}
