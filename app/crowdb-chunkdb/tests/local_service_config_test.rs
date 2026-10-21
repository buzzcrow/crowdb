// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::chunkdb_config::{ChunkdbConfig, DeploymentMode, PlacementMode};
use crowdb_common::config::BaseConfig;
use crowdb_console_shared::{
    config::NodeEntry,
    lifecycle::{chunkdb_config, ChunkdbDeployRequest},
};

#[test]
fn local_chunkdb_configuration_is_release_supported_without_ec_or_production_claims() {
    let node = NodeEntry {
        id: 2,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 2222,
        ssh_user: "crowdb".into(),
        ssh_credential_ref: None,
        ssh_key: None,
        ssh_password: None,
    };
    let mut request = ChunkdbDeployRequest {
        local_deployment: true,
        dynamic_ownership: false,
        server_id: "chunkdb-2".into(),
        instance_id: 2,
        http_port: 7300,
        rpc_port: 7301,
        kv_server_mgmt_seeds: vec!["http://127.0.0.1:10000".into()],
        allow_unsafe_ec: false,
        rpc_workers: None,
        kv_connections: None,
        kv_client_rpc_workers: None,
        diskdb_connections: None,
        diskdb_client_rpc_workers: None,
        metrics_interval: None,
    };
    let local: ChunkdbConfig = toml::from_str(&chunkdb_config(&request, &node)).unwrap();
    local.validate().unwrap();
    assert_eq!(local.deployment.mode, DeploymentMode::Local);
    assert_eq!(local.deployment.max_node_failures, 0);
    assert_eq!(local.placement.mode, PlacementMode::UnsafeColocated);
    assert!(local.placement.allow_degraded_failure_domains);
    assert!(!local.placement.allow_unsafe_ec);
    assert!(!local.conversion.enabled);
    request.local_deployment = false;
    let production: ChunkdbConfig = toml::from_str(&chunkdb_config(&request, &node)).unwrap();
    production.validate().unwrap();
    assert_eq!(production.deployment.mode, DeploymentMode::Production);
    assert_eq!(production.placement.mode, PlacementMode::Protected);
    assert!(!production.placement.allow_degraded_failure_domains);
}
