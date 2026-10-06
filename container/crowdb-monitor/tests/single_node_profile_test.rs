// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crowdb_monitor::{DeploymentProfile, GroupRole};

fn profile_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/profile.toml")
}

#[test]
fn single_node_preview_has_exact_topology_and_endpoints() {
    let profile = DeploymentProfile::load(profile_path()).unwrap();
    assert_eq!(profile.name, "single-node-container");
    assert_eq!(profile.display_name, "CROWDB Single-Node Container");
    assert_eq!(profile.logs.max_file_bytes, 30 * 1024 * 1024);
    assert_eq!(profile.logs.max_files, 5);
    assert!(profile.logs.mirror_warnings_to_stderr);
    assert_eq!(profile.nodes.len(), 1);
    assert_eq!(profile.groups.len(), 2);
    assert_eq!(profile.groups[0].role, GroupRole::System);
    assert_eq!(profile.groups[0].group_id, 0);
    assert_eq!(profile.groups[1].role, GroupRole::Data);
    assert_eq!(profile.groups[1].group_id, 1);
    assert_eq!(profile.disks.len(), 4);
    assert!(profile
        .disks
        .iter()
        .all(|disk| disk.capacity_bytes == 16 * 1024 * 1024 * 1024
            && disk.zone_size_bytes == disk.capacity_bytes));
    let endpoints = profile
        .public_endpoints
        .iter()
        .map(|endpoint| (endpoint.id.as_str(), endpoint.port))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        endpoints,
        BTreeMap::from([("iceberg", 9092), ("s3", 9091), ("web", 9090)])
    );
    let access_service = profile
        .services
        .iter()
        .find(|service| service.id == "access")
        .unwrap();
    assert_eq!(
        access_service.env.get("CROWDB_ICEBERG_PUBLIC_URI"),
        Some(&"http://localhost:9092".to_owned())
    );
    assert_eq!(access_service.probe.target, "http://127.0.0.1:9092/v1/config");
    assert_eq!(access_service.additional_probes.len(), 1);
    assert_eq!(
        access_service.additional_probes[0].target,
        "http://127.0.0.1:9093/_crowdb/health/ready"
    );
    assert_eq!(
        access_service.additional_probes[0].failure_threshold,
        access_service.probe.failure_threshold
    );
    assert_eq!(
        access_service.env.get("CROWDB_MANAGEMENT_SEEDS"),
        Some(&"http://127.0.0.1:10000".to_owned())
    );
    assert_eq!(access_service.args[1], "/opt/crowdb/run/config/access.toml");
    assert_eq!(access_service.fence_listeners.len(), 3);
    let access = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/templates/access.toml"),
    )
    .unwrap();
    let access: toml::Value = toml::from_str(&access).unwrap();
    assert_eq!(access["iceberg"]["listen"].as_str(), Some("0.0.0.0:9092"));
    assert_eq!(access["s3"]["listen"].as_str(), Some("0.0.0.0:9091"));
    let web = profile
        .services
        .iter()
        .find(|service| service.id == "web")
        .unwrap();
    assert_eq!(web.probe.target, "http://127.0.0.1:9090/healthz");
    let template = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/templates/crowdb-web.toml"),
    )
    .unwrap();
    let config: toml::Value = toml::from_str(&template).unwrap();
    assert_eq!(config["port"].as_integer(), Some(9090));
}

#[test]
fn single_node_preview_declares_complete_dependency_order() {
    let profile = DeploymentProfile::load(profile_path()).unwrap();
    let order = profile
        .services_in_start_order()
        .unwrap()
        .into_iter()
        .map(|service| service.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        order,
        ["kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"]
    );
    for service in &profile.services {
        if let Some(template) = &service.config_template {
            let source = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../single-node-container/templates")
                .join(template.file_name().unwrap());
            assert!(source.is_file(), "missing template for {}", service.id);
        }
    }
    let chunkdb = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/templates/chunkdb.toml"),
    )
    .unwrap();
    let config: toml::Value = toml::from_str(&chunkdb).unwrap();
    assert_eq!(config["placement"]["mode"].as_str(), Some("unsafe_colocated"));
}
