// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crowdb_monitor::{DeploymentProfile, GroupRole};

fn profile_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-preview/profile.toml")
}

#[test]
fn single_node_preview_has_exact_topology_and_endpoints() {
    let profile = DeploymentProfile::load(profile_path()).unwrap();
    assert_eq!(profile.name, "crowdb-single-node-preview");
    assert_eq!(profile.display_name, "CROWDB Single-Node Preview");
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
        BTreeMap::from([("iceberg", 8181), ("s3", 16000), ("web", 14000)])
    );
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
        ["kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "s3", "iceberg", "web"]
    );
    for service in &profile.services {
        if let Some(template) = &service.config_template {
            let source = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../single-node-preview/templates")
                .join(template.file_name().unwrap());
            assert!(source.is_file(), "missing template for {}", service.id);
        }
    }
}
