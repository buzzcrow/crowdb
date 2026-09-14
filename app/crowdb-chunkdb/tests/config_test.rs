// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::chunkdb_config::ChunkdbConfig;
use crowdb_chunkdb::selector::FailureDomainPriority;
use crowdb_common::config::BaseConfig;

#[test]
fn rpc_workers_defaults_and_validates() {
    let config: ChunkdbConfig = toml::from_str("[server]\n").expect("partial config parses");
    assert_eq!(config.server.rpc_workers, 2);
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
    assert_eq!(
        config.placement.failure_domain_priority,
        FailureDomainPriority::RackFirst
    );
    assert!(!config.placement.allow_degraded_failure_domains);
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
