// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::DeploymentProfile;
use std::{collections::BTreeMap, path::Path};

fn catalog() -> DeploymentProfile {
    DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
        .unwrap()
}

#[test]
fn node_catalog_has_no_prescribed_members_media_or_rendered_service_configuration() {
    let profile = catalog();
    profile.validate_node_catalog().unwrap();
    assert!(profile.nodes.is_empty() && profile.groups.is_empty() && profile.disks.is_empty());
    assert!(profile
        .services
        .iter()
        .all(|service| service.args.is_empty() && service.config_template.is_none()));
    let web = profile
        .services
        .iter()
        .find(|service| service.id == "web")
        .unwrap();
    assert!(
        web.dependencies.is_empty(),
        "management starts before cluster preparation"
    );
    let endpoints: BTreeMap<_, _> = profile
        .public_endpoints
        .iter()
        .map(|endpoint| (endpoint.id.as_str(), endpoint.port))
        .collect();
    assert_eq!(
        endpoints,
        BTreeMap::from([("web", 9090), ("s3", 9091), ("iceberg", 9092), ("dataset", 9093)])
    );
}

#[test]
fn incomplete_catalog_and_preset_or_partial_topology_are_rejected() {
    let mut missing = catalog();
    missing.services.pop();
    assert!(missing.validate_node_catalog().is_err());
    let mut unknown = catalog();
    unknown.services.last_mut().unwrap().id = "unknown".into();
    assert!(unknown.validate_node_catalog().is_err());
    let mut partial = catalog();
    partial.nodes.push(crowdb_monitor::NodeProfile {
        node_id: 1,
        rack_id: 1,
    });
    assert!(partial.validate().is_err());
    let fixture = DeploymentProfile::load(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/profile.toml"),
    )
    .unwrap();
    fixture.validate().unwrap();
    assert!(fixture.validate_node_catalog().is_err());
}

#[test]
fn obsolete_startup_modes_are_absent_from_monitor_arguments() {
    for mode in ["single", "manual"] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
            .args(["run", "--mode", mode])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument '--mode'"));
    }
}
