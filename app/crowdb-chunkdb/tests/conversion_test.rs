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
