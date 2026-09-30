// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::chunkdb_config::{ChunkdbConfig, DeploymentMode, PlacementMode};
use crowdb_chunkdb::selector::FailureDomainPriority;
use crowdb_common::config::BaseConfig;

#[test]
fn rpc_workers_defaults_and_validates() {
    let config: ChunkdbConfig = toml::from_str("[server]\n").expect("partial config parses");
    assert_eq!(config.server.rpc_workers, 2);
    assert_eq!(config.conversion_io.rpc_workers, 2);
    config.validate().expect("default workers validate");

    let mut invalid = config;
    invalid.server.rpc_workers = 0;
    assert_eq!(
        invalid.validate(),
        Err("server.rpc_workers must be > 0".to_string())
    );
}

#[test]
fn tracked_config_file_loads_and_validates() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("conf")
        .join("crowdb_chunkdb_config.toml");
    let config = crowdb_common::config::load_from_file::<ChunkdbConfig>(&path).expect("load tracked config");
    assert_eq!(config.server.rpc_workers, 2);
    assert_eq!(config.conversion_io.normal_connections_per_endpoint, 1);
    assert_eq!(
        config.placement.failure_domain_priority,
        FailureDomainPriority::RackFirst
    );
    assert!(!config.placement.allow_degraded_failure_domains);
}

#[test]
fn single_node_container_declares_test_only_deployment() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../container/single-node-container/templates/chunkdb.toml");
    let config = crowdb_common::config::load_from_file::<ChunkdbConfig>(&path).unwrap();
    assert_eq!(config.deployment.mode, DeploymentMode::TestSingleNode);
    assert_eq!(config.placement.mode, PlacementMode::UnsafeColocated);
    assert_eq!(config.conversion_io.rpc_workers, 1);
}

#[test]
fn conversion_io_transport_rejects_zero_resources() {
    let mut config = ChunkdbConfig::default();
    config.conversion_io.priority_connections_per_endpoint = 0;
    assert_eq!(
        config.validate(),
        Err("conversion_io connections and RPC workers must be > 0".to_string())
    );
}

#[test]
fn unsafe_fixture_mode_is_explicit_and_only_available_to_debug_builds() {
    let config: ChunkdbConfig = toml::from_str(
        "[deployment]\nmode = \"test_unsafe_placement\"\n[placement]\nmode = \"unsafe_colocated\"\nallow_unsafe_ec = true\n",
    )
    .unwrap();
    assert_eq!(config.deployment.mode, DeploymentMode::TestUnsafePlacement);
    assert_eq!(config.validate().is_ok(), cfg!(debug_assertions));
}

#[test]
fn placement_policy_parses_both_priorities() {
    let rack: ChunkdbConfig = toml::from_str(
        "[placement]\nfailure_domain_priority = \"rack_first\"\nallow_degraded_failure_domains = true\n",
    )
    .expect("rack-first config parses");
    assert_eq!(
        rack.placement.failure_domain_priority,
        FailureDomainPriority::RackFirst
    );
    assert!(rack.placement.allow_degraded_failure_domains);

    let node: ChunkdbConfig = toml::from_str("[placement]\nfailure_domain_priority = \"node_first\"\n")
        .expect("node-first config parses");
    assert_eq!(
        node.placement.failure_domain_priority,
        FailureDomainPriority::NodeFirst
    );
    assert!(!node.placement.allow_degraded_failure_domains);
}

#[test]
fn unsafe_colocated_placement_mode_is_explicit() {
    let protected: ChunkdbConfig = toml::from_str("").expect("defaults parse");
    assert_eq!(protected.placement.mode, PlacementMode::Protected);

    let colocated: ChunkdbConfig =
        toml::from_str("[placement]\nmode = \"unsafe_colocated\"\n").expect("mode parses");
    assert_eq!(colocated.placement.mode, PlacementMode::UnsafeColocated);
    assert!(colocated.validate().is_err());

    let single: ChunkdbConfig = toml::from_str(
        "[deployment]\nmode = \"test_single_node\"\n[placement]\nmode = \"unsafe_colocated\"\n",
    )
    .expect("explicit test mode parses");
    assert_eq!(single.deployment.mode, DeploymentMode::TestSingleNode);
    single.validate().expect("explicit test mode validates");

    let mut unsafe_production = protected;
    unsafe_production.placement.allow_unsafe_ec = true;
    assert!(unsafe_production.validate().is_err());
}

#[test]
fn placement_rebalance_defaults_and_enforces_slow_single_move_cycles() {
    let config: ChunkdbConfig = toml::from_str("").expect("defaults parse");
    assert!(config.placement_rebalance.enabled);
    assert_eq!(config.placement_rebalance.scan_interval_secs, 300);
    assert_eq!(config.placement_rebalance.imbalance_threshold_pct, 20);
    assert_eq!(config.placement_rebalance.hysteresis_secs, 900);
    assert_eq!(config.placement_rebalance.max_moves_per_cycle, 1);
    config.validate().expect("defaults validate");

    let mut invalid = config.clone();
    invalid.placement_rebalance.scan_interval_secs = 0;
    assert_eq!(
        invalid.validate(),
        Err("placement_rebalance.scan_interval_secs must be > 0".to_string())
    );

    let mut invalid = config.clone();
    invalid.placement_rebalance.imbalance_threshold_pct = 101;
    assert_eq!(
        invalid.validate(),
        Err("placement_rebalance.imbalance_threshold_pct must be <= 100".to_string())
    );

    let mut invalid = config;
    invalid.placement_rebalance.max_moves_per_cycle = 2;
    assert_eq!(
        invalid.validate(),
        Err("placement_rebalance.max_moves_per_cycle must be 1".to_string())
    );
}
